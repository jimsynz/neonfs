defmodule NeonFS.Integration.EscalationTest do
  @moduledoc """
  Integration test for the full operator escalation flow across a 3-node
  cluster:

    1. An escalation raised on node1 fires `[:neonfs, :escalation, :raised]`
       telemetry, the webhook dispatcher posts JSON to the configured URL,
       and the body matches the expected shape.
    2. The escalation replicates via Ra so every node sees it.
    3. Resolution issued against a different node (via
       `NeonFS.CLI.Handler.handle_escalation_resolve/2`) transitions the
       record to `:resolved` with the chosen value, visible back on node1.
    4. `[:neonfs, :escalation, :pending_by_category]` gauge fires on both
       the raise and the resolve so operators can graph pending load.

  Unit tests cover the CRUD/telemetry plumbing in isolation; this test
  proves the *cluster-wide* behaviour operators actually rely on.
  """
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 180_000
  @moduletag nodes: 3

  setup %{cluster: cluster} do
    init_multi_node_cluster(cluster, name: "escalation-test")
    :ok
  end

  defmodule WebhookCapture do
    @moduledoc false
    @behaviour Plug

    @impl true
    def init(opts), do: opts

    @impl true
    def call(conn, opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      {:ok, body, conn} = Plug.Conn.read_body(conn, length: 1_000_000)

      send(test_pid, {:webhook_received, body})

      conn
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.send_resp(200, "{}")
    end
  end

  test "raises → webhook fires → resolves on a different node → state replicates",
       %{cluster: cluster} do
    test_pid = self()

    {server, port} =
      start_bandit_with_retry(
        plug: {WebhookCapture, test_pid: test_pid},
        port: 0,
        ip: :loopback,
        startup_log: false
      )

    on_exit(fn ->
      if Process.alive?(server) do
        try do
          Supervisor.stop(server, :normal, 1_000)
        catch
          :exit, _ -> :ok
        end
      end
    end)

    webhook_url = "http://127.0.0.1:#{port}/"

    :ok =
      PeerCluster.rpc(cluster, :node1, Application, :put_env, [
        :neonfs_core,
        :escalation_webhook_url,
        webhook_url
      ])

    on_exit(fn ->
      case PeerCluster.get_node(cluster, :node1) do
        {:ok, _} ->
          PeerCluster.rpc(cluster, :node1, Application, :delete_env, [
            :neonfs_core,
            :escalation_webhook_url
          ])

        _ ->
          :ok
      end
    end)

    # Telemetry fires on the node that handles the GenServer call (not via
    # Ra replication), so attach handlers on both node1 (where we raise)
    # and node3 (where we resolve) to observe both transitions.
    node1_state_ref = make_ref()
    node1_per_category_ref = make_ref()
    node3_state_ref = make_ref()

    :ok =
      PeerCluster.rpc(cluster, :node1, NeonFS.Integration.TelemetryForwarder, :attach, [
        test_pid,
        node1_state_ref,
        [:neonfs, :escalation, :state]
      ])

    :ok =
      PeerCluster.rpc(cluster, :node1, NeonFS.Integration.TelemetryForwarder, :attach, [
        test_pid,
        node1_per_category_ref,
        [:neonfs, :escalation, :pending_by_category]
      ])

    :ok =
      PeerCluster.rpc(cluster, :node3, NeonFS.Integration.TelemetryForwarder, :attach, [
        test_pid,
        node3_state_ref,
        [:neonfs, :escalation, :state]
      ])

    on_exit(fn ->
      for {node, ref} <- [
            {:node1, node1_state_ref},
            {:node1, node1_per_category_ref},
            {:node3, node3_state_ref}
          ] do
        case PeerCluster.get_node(cluster, node) do
          {:ok, _} ->
            PeerCluster.rpc(cluster, node, NeonFS.Integration.TelemetryForwarder, :detach, [ref])

          _ ->
            :ok
        end
      end
    end)

    # Raise an escalation on node1.
    attrs = %{
      category: "quorum.degraded",
      severity: :warning,
      description: "Integration test escalation",
      options: [%{value: "wait", label: "Wait"}, %{value: "proceed", label: "Proceed"}]
    }

    {:ok, escalation} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.Escalation, :create, [attrs])

    assert escalation.status == :pending
    assert escalation.category == "quorum.degraded"

    # Webhook body is JSON with the raised escalation details.
    assert_receive {:webhook_received, body}, 5_000
    decoded = Jason.decode!(body)
    assert decoded["id"] == escalation.id
    assert decoded["category"] == "quorum.degraded"
    assert decoded["status"] == "pending"
    assert length(decoded["options"]) == 2

    # Per-category gauge ticked up on raise: at least one pending
    # "quorum.degraded" escalation.
    assert_receive {:telemetry_forwarded, ^node1_per_category_ref,
                    [:neonfs, :escalation, :pending_by_category], %{count: raised_count},
                    %{category: "quorum.degraded"}},
                   5_000

    assert raised_count >= 1

    # Aggregate state gauge fired with a non-zero pending count on raise.
    assert_receive {:telemetry_forwarded, ^node1_state_ref, [:neonfs, :escalation, :state],
                    %{pending_count: pending_after_raise}, _},
                   5_000

    assert pending_after_raise >= 1

    # Replication: node2 and node3 see the same pending escalation.
    assert_eventually timeout: 5_000 do
      Enum.all?([:node2, :node3], fn node_name ->
        case PeerCluster.rpc(cluster, node_name, NeonFS.Core.Escalation, :get, [escalation.id]) do
          {:ok, replica} -> replica.status == :pending and replica.choice == nil
          _ -> false
        end
      end)
    end

    # Resolve via CLI handler on node3 (a *different* node from where it was raised).
    # The CLI handler returns a serialisable map with string-valued enums
    # for stable JSON output to operators.
    {:ok, resolved} =
      PeerCluster.rpc(cluster, :node3, NeonFS.CLI.Handler, :handle_escalation_resolve, [
        escalation.id,
        "proceed"
      ])

    assert resolved.status == "resolved"
    assert resolved.choice == "proceed"
    assert resolved.id == escalation.id

    # Aggregate state gauge fires on node3 (where resolve was processed)
    # with a decremented pending count — the gauge ticks on the node that
    # handled the transition, not on every cluster member.
    #
    # The Ra apply of the resolve on node3's local state machine is async
    # relative to `resolve/2` returning `:ok` (the command commits via
    # consensus, but the local apply runs from the normal log-apply loop).
    # `emit_pending_metrics/0` uses a `local_query` and can therefore fire
    # with stale counts until the local apply lands. Drain those stale
    # events and wait for one that reflects the decremented state — a
    # slight timing tolerance that matches how operators see the gauge
    # anyway (eventually consistent with the underlying Ra state).
    # See #434.
    pending_after_resolve =
      drain_until_pending_below(node3_state_ref, pending_after_raise, 10_000)

    assert pending_after_resolve < pending_after_raise

    # State replicates back to node1 and node2. The Escalation module caches
    # ETS reads and only refreshes on cache miss, so we verify replication
    # by querying the Ra state machine directly on each node — which
    # reflects the fully replicated, consensus-applied state.
    assert_eventually timeout: 5_000 do
      Enum.all?([:node1, :node2], fn node_name ->
        case PeerCluster.rpc(
               cluster,
               node_name,
               NeonFS.Integration.TelemetryForwarder,
               :query_escalation,
               [escalation.id]
             ) do
          {:ok, %{status: :resolved, choice: "proceed"}} -> true
          _ -> false
        end
      end)
    end
  end

  # Wait for a `[:neonfs, :escalation, :state]` telemetry event forwarded
  # from the given node-ref whose `pending_count` is strictly less than the
  # pre-resolve baseline. Stale events (with `pending_count >= threshold`,
  # fired before the Ra apply landed on the local state machine) are
  # dropped. Returns the post-resolve count; flunks on timeout.
  defp drain_until_pending_below(state_ref, threshold, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    drain_loop(state_ref, threshold, deadline)
  end

  defp drain_loop(state_ref, threshold, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:telemetry_forwarded, ^state_ref, [:neonfs, :escalation, :state], %{pending_count: count},
       _}
      when count < threshold ->
        count

      {:telemetry_forwarded, ^state_ref, [:neonfs, :escalation, :state], %{pending_count: _stale},
       _} ->
        drain_loop(state_ref, threshold, deadline)
    after
      remaining ->
        ExUnit.Assertions.flunk(
          "Did not receive :escalation :state telemetry with pending_count < " <>
            "#{threshold} within deadline"
        )
    end
  end
end
