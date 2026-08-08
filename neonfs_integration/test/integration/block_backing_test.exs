defmodule NeonFS.Integration.BlockBackingTest do
  @moduledoc """
  Crash consistency for the file-backed block device store.

  A block device makes two promises a filesystem does not have to make as
  loudly: an acknowledged flush must survive the loss of the node that
  took the write, and no sector may ever read back content it held before
  a later write. Both are asserted here across a whole-node restart,
  because that is the failure a device attached to a guest actually meets
  — the node it was talking to goes away and the guest keeps reading.

  The device is written from `node1` and read back from `node2` after
  `node1` restarts, so a read that succeeds proves the chunks reached
  another replica rather than merely surviving locally. Durability is
  `factor: 2, min_copies: 2` for that reason: the integration suite's
  default durability is single-copy, under which nothing here could be
  distinguished from a local read.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.BlockBacking
  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 300_000
  @moduletag nodes: 3
  @moduletag :integration

  @volume "block-backing"
  @volume_opts %{
    durability: %{type: :replicate, factor: 2, min_copies: 2},
    compression: %{algorithm: :none, level: 0, min_size: 0}
  }

  @chunk 131_072
  @block 4096
  @device_chunks 8

  setup %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, volumes: [{@volume, @volume_opts}])
    :ok
  end

  test "a flushed write survives the restart of the node that took it, with no stale sector",
       %{cluster: cluster} do
    {:ok, device} =
      block_rpc(cluster, :node1, :create_device, [@volume, "/dev.img", @device_chunks * @chunk])

    stale = :binary.copy(<<0xA1>>, @block)
    fresh = :binary.copy(<<0xB2>>, @block)
    untouched = :binary.copy(<<0xC3>>, @block)

    overwritten_at = 2 * @chunk
    untouched_at = 5 * @chunk

    :ok = block_rpc(cluster, :node1, :write, [@volume, device.file_id, overwritten_at, stale])
    :ok = block_rpc(cluster, :node1, :write, [@volume, device.file_id, untouched_at, untouched])
    :ok = block_rpc(cluster, :node1, :flush, [@volume, device.file_id])

    :ok = block_rpc(cluster, :node1, :write, [@volume, device.file_id, overwritten_at, fresh])
    :ok = block_rpc(cluster, :node1, :flush, [@volume, device.file_id])

    {:ok, cluster} = PeerCluster.restart_node(cluster, :node1)
    stabilise_after_restart(cluster)

    assert {:ok, ^fresh} =
             block_rpc(cluster, :node2, :read, [@volume, device.file_id, overwritten_at, @block])

    assert {:ok, ^untouched} =
             block_rpc(cluster, :node2, :read, [@volume, device.file_id, untouched_at, @block])

    assert {:ok, zeroes} =
             block_rpc(cluster, :node2, :read, [@volume, device.file_id, 7 * @chunk, @block])

    assert zeroes == :binary.copy(<<0>>, @block)

    {:ok, info} = block_rpc(cluster, :node2, :device_info, [@volume, device.file_id])
    assert info.size == @device_chunks * @chunk
  end

  test "zeroing a range is durable and does not resurrect the bytes it replaced",
       %{cluster: cluster} do
    {:ok, device} =
      block_rpc(cluster, :node1, :create_device, [@volume, "/zeroed.img", @device_chunks * @chunk])

    payload = :binary.copy(<<0xD4>>, 2 * @chunk)

    :ok = block_rpc(cluster, :node1, :write, [@volume, device.file_id, 0, payload])
    :ok = block_rpc(cluster, :node1, :flush, [@volume, device.file_id])

    :ok = block_rpc(cluster, :node1, :write_zeroes, [@volume, device.file_id, 0, @chunk])
    :ok = block_rpc(cluster, :node1, :flush, [@volume, device.file_id])

    {:ok, cluster} = PeerCluster.restart_node(cluster, :node1)
    stabilise_after_restart(cluster)

    assert {:ok, zeroed} = block_rpc(cluster, :node2, :read, [@volume, device.file_id, 0, @block])
    assert zeroed == :binary.copy(<<0>>, @block)

    assert {:ok, kept} =
             block_rpc(cluster, :node2, :read, [@volume, device.file_id, @chunk, @block])

    assert kept == :binary.copy(<<0xD4>>, @block)
  end

  defp block_rpc(cluster, node, function, args) do
    PeerCluster.rpc(cluster, node, BlockBacking, function, args)
  end

  defp stabilise_after_restart(cluster) do
    wait_for_full_mesh(cluster)
    wait_for_ra_quorum(cluster)
    rebuild_quorum_rings(cluster)
    wait_for_drive_registration(cluster)
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

  # The restarted node has to re-register its drive before it can serve or
  # store chunks again; a read issued while it is missing falls to the
  # surviving replica, which would pass this test for the wrong reason.
  defp wait_for_drive_registration(cluster) do
    for node_info <- cluster.nodes do
      :ok =
        wait_until(
          fn ->
            cluster
            |> PeerCluster.rpc(node_info.name, NeonFS.Core.DriveRegistry, :list_drives, [])
            |> Enum.any?(&(&1.node == node_info.node))
          end,
          timeout: 60_000
        )
    end

    :ok
  end
end
