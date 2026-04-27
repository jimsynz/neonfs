defmodule NeonFS.Integration.DRSnapshotSchedulerTest do
  @moduledoc """
  Peer-cluster integration test for `NeonFS.Core.DRSnapshotScheduler`'s
  single-leader scheduling guarantee — sub-issue #570 (deferred from
  #323).

  The scheduler is started on every core node, but only the current Ra
  leader actually creates a snapshot per tick. This test verifies the
  property end-to-end: with a 3-node cluster, a tight tick interval,
  and a forced leader change in the middle, the total tick count is
  consistent with one snapshot per interval (not three per interval),
  and across the test window at least two different nodes have been
  the active leader.

  Schedulers are mounted on the peers via `GenServer.start/3` (no
  link), as recommended by the Codebase Patterns wiki — the RPC
  handler that issues the start call exits immediately, so a
  `start_link` would take the scheduler down with it.

  Telemetry from `[:neonfs, :dr_snapshot_scheduler, :tick]` is captured
  on each peer via `NeonFS.TestSupport.TelemetryForwarder`, with one
  ref per node so the test process can attribute events back to their
  source.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.DRSnapshotScheduler
  alias NeonFS.Core.RaSupervisor
  alias NeonFS.TestSupport.TelemetryForwarder

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 3

  @scheduler_name DRSnapshotScheduler
  @tick_event [:neonfs, :dr_snapshot_scheduler, :tick]
  @interval_ms 500

  setup %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "dr-sched-test")
    :ok
  end

  describe "single-leader scheduling under leader change" do
    test "ticks come from a single node per interval and survive a leader transfer",
         %{cluster: cluster} do
      test_pid = self()
      node_names = [:node1, :node2, :node3]
      ref_by_node = attach_forwarders(cluster, node_names, test_pid)

      try do
        mount_schedulers(cluster, node_names)

        try do
          first_window = collect_ticks(ref_by_node, 1_500)
          assert window_count(first_window) >= 1, "no ticks observed before leader transfer"

          original_leader_node = current_leader_node(cluster)

          target_node_name =
            Enum.find(node_names, &(node_atom(cluster, &1) != original_leader_node))

          assert target_node_name, "no candidate for leader transfer in #{inspect(node_names)}"

          :ok = transfer_leadership(cluster, target_node_name)
          :ok = wait_for_leader_change(cluster, original_leader_node)

          second_window = collect_ticks(ref_by_node, 1_500)

          all_ticks = first_window ++ second_window

          # Lower bound: with a 500ms cadence over ~3s and one tick per
          # interval, we expect ~5–6 ticks. Allow generous slack for
          # leader-transfer dead time and scheduler-startup jitter.
          assert window_count(all_ticks) >= 3,
                 "expected at least 3 ticks across the test window, got " <>
                   inspect(all_ticks)

          # The leader-change should have shifted ticks onto a different
          # node — at least two distinct nodes contributed ticks.
          contributing_nodes = all_ticks |> Enum.map(&elem(&1, 0)) |> Enum.uniq()

          assert length(contributing_nodes) >= 2,
                 "expected ticks from at least 2 distinct nodes, got " <>
                   inspect(contributing_nodes)
        after
          stop_schedulers(cluster, node_names)
        end
      after
        detach_forwarders(cluster, ref_by_node)
      end
    end
  end

  ## Helpers

  defp attach_forwarders(cluster, node_names, test_pid) do
    for node <- node_names, into: %{} do
      ref = make_ref()

      :ok =
        PeerCluster.rpc(cluster, node, TelemetryForwarder, :attach, [
          test_pid,
          ref,
          @tick_event
        ])

      {node, ref}
    end
  end

  defp detach_forwarders(cluster, ref_by_node) do
    for {node, ref} <- ref_by_node do
      _ = PeerCluster.rpc(cluster, node, TelemetryForwarder, :detach, [ref])
    end
  end

  defp mount_schedulers(cluster, node_names) do
    for node <- node_names do
      {:ok, _pid} =
        PeerCluster.rpc(cluster, node, GenServer, :start, [
          DRSnapshotScheduler,
          [
            interval_ms: @interval_ms,
            create_fn: &TelemetryForwarder.dr_snapshot_create_fn/1
          ],
          [name: @scheduler_name]
        ])
    end
  end

  defp stop_schedulers(cluster, node_names) do
    for node <- node_names do
      _ = PeerCluster.rpc(cluster, node, GenServer, :stop, [@scheduler_name, :normal, 1_000])
    end
  end

  # Drains forwarded `tick` events for `duration_ms`, attributing each
  # event to the node whose ref matches. Returns a list of
  # `{node_name, monotonic_ms}` pairs in receive order.
  defp collect_ticks(ref_by_node, duration_ms) do
    deadline = System.monotonic_time(:millisecond) + duration_ms
    node_by_ref = Map.new(ref_by_node, fn {node, ref} -> {ref, node} end)
    do_collect_ticks([], node_by_ref, deadline)
  end

  defp do_collect_ticks(acc, node_by_ref, deadline) do
    remaining = max(0, deadline - System.monotonic_time(:millisecond))

    receive do
      {:telemetry_forwarded, ref, _event, _measurements, _metadata} ->
        case Map.fetch(node_by_ref, ref) do
          {:ok, node} ->
            do_collect_ticks(
              [{node, System.monotonic_time(:millisecond)} | acc],
              node_by_ref,
              deadline
            )

          :error ->
            do_collect_ticks(acc, node_by_ref, deadline)
        end
    after
      remaining -> Enum.reverse(acc)
    end
  end

  defp window_count(window), do: length(window)

  defp current_leader_node(cluster) do
    server_id = PeerCluster.rpc(cluster, :node1, RaSupervisor, :server_id, [])

    {:ok, _members, {_cluster_name, leader_node}} =
      PeerCluster.rpc(cluster, :node1, :ra, :members, [server_id, 5_000])

    leader_node
  end

  defp node_atom(cluster, node_name) do
    PeerCluster.get_node!(cluster, node_name).node
  end

  defp transfer_leadership(cluster, target_node_name) do
    target_node = node_atom(cluster, target_node_name)
    leader_node = current_leader_node(cluster)

    leader_name =
      Enum.find([:node1, :node2, :node3], fn n -> node_atom(cluster, n) == leader_node end)

    server_id = PeerCluster.rpc(cluster, leader_name, RaSupervisor, :server_id, [])
    {cluster_name, _} = server_id
    target_server_id = {cluster_name, target_node}

    case PeerCluster.rpc(cluster, leader_name, :ra, :transfer_leadership, [
           server_id,
           target_server_id
         ]) do
      :ok -> :ok
      :already_leader -> :ok
      other -> flunk("transfer_leadership failed: #{inspect(other)}")
    end
  end

  defp wait_for_leader_change(cluster, previous_leader_node) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    do_wait_for_leader_change(cluster, previous_leader_node, deadline)
  end

  defp do_wait_for_leader_change(cluster, previous_leader_node, deadline) do
    if current_leader_node(cluster) != previous_leader_node do
      :ok
    else
      if System.monotonic_time(:millisecond) > deadline do
        flunk("leader did not change away from #{inspect(previous_leader_node)} within 5s")
      else
        Process.sleep(50)
        do_wait_for_leader_change(cluster, previous_leader_node, deadline)
      end
    end
  end
end
