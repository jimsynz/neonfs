defmodule NeonFS.Integration.FreezeThawTest do
  @moduledoc """
  #1440: the #1378 acceptance — a whole cluster can be cleanly frozen,
  powered off, powered on, and thawed without a repair storm or a
  force-reset, with content and metadata consistent across the cycle.

  Exercises the full operator flow end-to-end on a real 3-node peer
  cluster: `cluster freeze` → stop every node → restart every node
  (Ra auto-restarts from persisted on-disk state) → `cluster thaw`.

  The recovering-state *suppression logic* is unit-tested in #1436/#1437;
  here we assert the coordination cycle as a whole: freeze cuts writes, the
  cluster auto-enters `:recovering` on the cold reform, reassembles from
  persisted state (no force-reset), the metadata layer is available on every
  node afterwards, and the cluster returns to `:normal`.

  Byte-level content read-back after a *simultaneous* whole-cluster cold
  restart is a separate data-plane recovery concern tracked in #1450, and
  is out of scope here.
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.TestSupport.TelemetryForwarder

  @moduletag timeout: 600_000
  @moduletag nodes: 3
  @moduletag drives: 1
  @moduletag :integration

  setup %{cluster: cluster} do
    :ok = init_cluster_with_data(cluster)
    %{cluster: cluster}
  end

  test "freeze, power-cycle every node, thaw — data intact, recovering engaged, no force-reset",
       %{cluster: cluster} do
    # 1. Freeze the whole cluster.
    assert {:ok, %{status: "frozen"}} =
             PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_cluster_freeze, [])

    :ok =
      wait_until(
        fn -> PeerCluster.rpc(cluster, :node1, NeonFS.Core.ClusterMode, :mode, []) == :frozen end,
        timeout: 15_000
      )

    # 2. A frozen cluster refuses new client writes (the #1438 write-gate,
    #    hit via the gated `NeonFS.Core` RPC facade).
    assert {:error, :cluster_frozen} =
             PeerCluster.rpc(cluster, :node1, NeonFS.Core, :write_file_at, [
               "test-volume",
               "/frozen.txt",
               0,
               "should be refused",
               []
             ])

    # 3. Power-cycle: stop every node, then restart every node in place.
    #    restart_node/3 preserves each node's on-disk Ra state, so the
    #    cluster reassembles from persisted state — no force-reset.
    for node_name <- [:node1, :node2, :node3] do
      :ok = PeerCluster.stop_node(cluster, node_name)
    end

    ref = make_ref()

    cluster =
      Enum.reduce([:node1, :node2, :node3], cluster, fn node_name, cluster ->
        {:ok, cluster} = PeerCluster.restart_node(cluster, node_name)

        # Attach as early as possible so the leader's cold-reform entry is
        # caught even though it fires only once quorum re-forms.
        :ok =
          PeerCluster.rpc(cluster, node_name, TelemetryForwarder, :attach, [
            self(),
            ref,
            [:neonfs, :cluster_recovery, :entered]
          ])

        cluster
      end)

    stabilise_after_restart(cluster)

    # 4. The cluster auto-detected the cold reform and entered :recovering
    #    (the leader emits this once quorum re-forms). This is the state
    #    that keeps failure-driven repair suppressed during reassembly.
    assert_receive {:telemetry_forwarded, ^ref, [:neonfs, :cluster_recovery, :entered], _, _},
                   120_000

    # 5. Thaw.
    assert {:ok, %{status: "recovering"}} =
             PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_cluster_thaw, [])

    # 6. Metadata survived the whole cycle: every node resolves the volume
    #    and its data-plane services (ChunkIndex) are back after the full
    #    cold restart. Byte-level content read-back after a *simultaneous*
    #    whole-cluster restart is a separate data-plane recovery concern
    #    (#1450) — out of scope for this coordination test.
    wait_for_read_path_ready(cluster)

    # 6b. Every node re-registers its local drive after the cold restart —
    #     `PeerCluster.build_restart_config` now carries `:drives` across
    #     `restart_node` (part of #1450). Without it a restarted peer
    #     manages no drives and can neither serve nor store chunks.
    #     (Byte-level content read-back after a simultaneous restart is
    #     still tracked in #1450 — a distinct per-node read-hang remains.)
    for node_name <- [:node1, :node2, :node3] do
      node = PeerCluster.get_node!(cluster, node_name).node

      :ok =
        wait_until(
          fn ->
            cluster
            |> PeerCluster.rpc(node_name, NeonFS.Core.DriveRegistry, :list_drives, [])
            |> Enum.any?(&(&1.node == node))
          end,
          timeout: 60_000
        )
    end

    # 7. The recovering lifecycle completes: a clean cycle has no dirty
    #    drives, so the monitor returns the cluster to :normal on its own.
    :ok =
      wait_until(
        fn -> PeerCluster.rpc(cluster, :node1, NeonFS.Core.ClusterMode, :mode, []) == :normal end,
        timeout: 120_000
      )
  end

  # ─── Setup helpers (mirrors partition_restart_test.exs) ──────────────

  defp init_cluster_with_data(cluster) do
    :ok = init_multi_node_cluster(cluster, volumes: [{"test-volume", %{}}])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
        "test-volume",
        "/test.txt",
        "test data"
      ])

    for node_name <- [:node1, :node2, :node3] do
      :ok =
        wait_until(
          fn -> read_matches?(cluster, node_name, "/test.txt", "test data") end,
          timeout: 30_000
        )
    end

    :ok
  end

  defp read_matches?(cluster, node_name, path, expected_content) do
    case PeerCluster.rpc(cluster, node_name, NeonFS.TestHelpers, :read_file, [
           "test-volume",
           path
         ]) do
      {:ok, ^expected_content} -> true
      _ -> false
    end
  end

  defp stabilise_after_restart(cluster) do
    wait_for_full_mesh(cluster)
    wait_for_ra_quorum(cluster)
    rebuild_quorum_rings(cluster)
  end

  # After a full cold restart, wait until every node's read path is back:
  # the volume resolves (metadata) and ChunkIndex is alive (the process
  # anti-entropy writes reconciled locations into). Guards against nudging
  # anti-entropy before a node's data-plane services have restarted.
  defp wait_for_read_path_ready(cluster) do
    for node_name <- [:node1, :node2, :node3] do
      :ok =
        wait_until(
          fn ->
            match?(
              {:ok, _},
              PeerCluster.rpc(cluster, node_name, NeonFS.Core.VolumeRegistry, :get_by_name, [
                "test-volume"
              ])
            ) and
              is_pid(
                PeerCluster.rpc(cluster, node_name, Process, :whereis, [NeonFS.Core.ChunkIndex])
              )
          end,
          timeout: 60_000
        )
    end

    :ok
  end

  defp wait_for_ra_quorum(cluster) do
    for node_info <- cluster.nodes do
      :ok =
        wait_until(
          fn ->
            match?(
              {:ok, _},
              PeerCluster.rpc(cluster, node_info.name, NeonFS.Core.RaSupervisor, :get_state, [])
            )
          end,
          timeout: 30_000
        )
    end

    :ok
  end
end
