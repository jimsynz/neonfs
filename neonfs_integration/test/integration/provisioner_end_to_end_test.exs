defmodule NeonFS.Integration.ProvisionerEndToEndTest do
  @moduledoc """
  End-to-end coverage for `Volume.Provisioner` against a real 1-node
  Ra cluster + real `BlobStore` (#853).

  The unit tests in `provisioner_test.exs` stub the bootstrap
  registrar with `{:ok, :ok}` (a 2-tuple), which masked the
  `:ra.process_command/3` `{:ok, result, leader}` 3-tuple that
  `RaSupervisor.command/1` actually returns in production. That
  shape mismatch made every successful Ra commit surface as
  `{:bootstrap_register_failed, {:ok, :ok, leader}}` and broke
  `create_volume/2` on any cluster with a registered drive — the
  bug fixed in PR #852.

  This test exercises the full path through `cluster_init` →
  `handle_add_drive` → `create_volume` and asserts both the
  bootstrap-layer entry and the on-disk root-segment chunk are
  produced. It would have caught #852 immediately.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 60_000
  @moduletag nodes: 1
  @moduletag cluster_mode: :per_test
  @moduletag :integration

  setup %{cluster: cluster} do
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["provisioner-e2e"])

    :ok = wait_for_cluster_stable(cluster)

    drive_path = Path.join([cluster.data_dir, "node1", "drive1"])
    File.mkdir_p!(drive_path)

    {:ok, _drive} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_add_drive, [
        %{"id" => "drive1", "path" => drive_path, "tier" => "hot"}
      ])

    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaSupervisor, :local_query, [
                 &MetadataStateMachine.get_drives/1
               ]) do
            {:ok, drives} when is_map(drives) and map_size(drives) > 0 -> true
            _ -> false
          end
        end,
        timeout: 30_000
      )

    %{cluster: cluster, drive_path: drive_path}
  end

  test "create_volume registers a bootstrap entry and writes the root segment to the drive",
       %{cluster: cluster, drive_path: drive_path} do
    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "provisioner-vol",
        %{"durability" => "replicate:1"}
      ])

    volume_id = volume.id
    assert is_binary(volume_id)

    :ok =
      wait_until(
        fn ->
          case Map.get(volume_roots(cluster), volume_id) do
            %{root_chunk_hash: hash} when is_binary(hash) -> true
            _ -> false
          end
        end,
        timeout: 30_000
      )

    %{root_chunk_hash: root_hash, drive_locations: locations} =
      Map.fetch!(volume_roots(cluster), volume_id)

    # The bootstrap entry should record the drive that holds the
    # root segment, not just the volume id.
    assert Enum.any?(locations, &(&1.drive_id == "drive1"))

    # The root segment chunk should exist on disk under the drive's
    # blobs/ tree, which is what the on-disk reconstruction walker
    # in #844 relies on.
    assert root_chunk_on_disk?(drive_path, root_hash),
           "root segment chunk #{Base.encode16(root_hash, case: :lower)} not found under #{drive_path}/blobs"
  end

  ## Helpers

  defp volume_roots(cluster) do
    {:ok, roots} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaSupervisor, :local_query, [
        &MetadataStateMachine.get_volume_roots/1
      ])

    roots
  end

  defp root_chunk_on_disk?(drive_path, hash) do
    hex = Base.encode16(hash, case: :lower)

    [drive_path, "blobs", "**", "#{hex}.*"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.any?()
  end
end
