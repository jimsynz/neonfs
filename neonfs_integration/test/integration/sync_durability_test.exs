defmodule NeonFS.Integration.SyncDurabilityTest do
  @moduledoc """
  #1500: the `sync_file` durability barrier holds end-to-end on a real
  3-node peer cluster.

  On a `write_ack: :local` volume the extra replicas are placed by a
  fire-and-forget background task after the write acks, so a whole-cluster
  cold restart immediately after a write can leave the metadata pointing at
  replica locations that never actually received the chunk — which is why
  the #1440 freeze/thaw test wraps its post-restart read-back in a
  `wait_until` retry loop.

  `NeonFS.Core.sync_file/2` closes that gap: it blocks until every chunk of
  the file has at least the volume's `min_copies` durable replicas, driving
  synchronous replication for any shortfall. This test asserts the payoff —
  after `sync_file`, a cold restart of the whole cluster reads the file back
  on the **first** attempt on every node, with no retry loop.
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  @moduletag timeout: 600_000
  @moduletag nodes: 3
  @moduletag drives: 1
  @moduletag :integration

  test "sync_file guarantees a file survives a whole-cluster cold restart with no read retry",
       %{cluster: cluster} do
    :ok =
      init_multi_node_cluster(cluster,
        volumes: [
          {"durable-vol", %{durability: %{type: :replicate, factor: 3, min_copies: 2}}}
        ]
      )

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
        "durable-vol",
        "/durable.txt",
        "durable data"
      ])

    # The barrier: block until every chunk has min_copies durable replicas.
    assert :ok =
             PeerCluster.rpc(cluster, :node1, NeonFS.Core, :sync_file, [
               "durable-vol",
               "/durable.txt"
             ])

    # Cold restart: stop every node, then restart every node in place. Ra
    # auto-restarts from persisted on-disk state, so the cluster reassembles
    # without a force-reset.
    for node_name <- [:node1, :node2, :node3] do
      :ok = PeerCluster.stop_node(cluster, node_name)
    end

    cluster =
      Enum.reduce([:node1, :node2, :node3], cluster, fn node_name, cluster ->
        {:ok, cluster} = PeerCluster.restart_node(cluster, node_name)
        cluster
      end)

    stabilise_after_restart(cluster)
    wait_for_read_path_ready(cluster)
    wait_for_drive_registration(cluster)

    # The payoff: a single read on every node returns the file — no
    # `wait_until` retry loop, because the sync barrier made the metadata's
    # replica locations genuinely durable before the restart.
    for node_name <- [:node1, :node2, :node3] do
      assert {:ok, "durable data"} =
               PeerCluster.rpc(cluster, node_name, NeonFS.TestHelpers, :read_file, [
                 "durable-vol",
                 "/durable.txt"
               ])
    end
  end

  defp stabilise_after_restart(cluster) do
    wait_for_full_mesh(cluster)
    wait_for_ra_quorum(cluster)
    rebuild_quorum_rings(cluster)
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

  # Infrastructure readiness after the cold reform — the volume resolves
  # (metadata) and ChunkIndex is alive on every node. This is not the #1440
  # durability workaround: it waits for services to restart, not for a
  # not-yet-durable chunk to converge.
  defp wait_for_read_path_ready(cluster) do
    for node_name <- [:node1, :node2, :node3] do
      :ok =
        wait_until(
          fn ->
            match?(
              {:ok, _},
              PeerCluster.rpc(cluster, node_name, NeonFS.Core.VolumeRegistry, :get_by_name, [
                "durable-vol"
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

  # Every node re-registers its local drive after the cold restart, so it can
  # serve and store chunks again.
  defp wait_for_drive_registration(cluster) do
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

    :ok
  end
end
