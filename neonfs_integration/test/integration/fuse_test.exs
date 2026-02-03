defmodule NeonFS.Integration.FUSETest do
  @moduledoc """
  FUSE filesystem integration tests.

  These tests verify FUSE mount/unmount operations and filesystem access
  through the mounted FUSE interface.
  """
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 120_000
  @moduletag nodes: 1
  @moduletag applications: [:neonfs_core, :neonfs_fuse]
  @moduletag :fuse_integration

  describe "FUSE mount operations" do
    test "mount volume and perform filesystem operations", %{cluster: cluster} do
      :ok = init_cluster_with_volume(cluster)

      # Create mount point on the peer node
      mount_point = "/tmp/neonfs_fuse_test_#{System.unique_integer([:positive])}"

      :ok =
        PeerCluster.rpc(cluster, :node1, File, :mkdir_p!, [mount_point])

      # Mount the volume
      {:ok, mount_info} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :mount, [
          "test-volume",
          mount_point,
          %{}
        ])

      assert is_map(mount_info)

      try do
        # Write file via mounted filesystem
        test_data = :crypto.strong_rand_bytes(100 * 1024)
        file_path = Path.join(mount_point, "fuse_test.bin")

        :ok = PeerCluster.rpc(cluster, :node1, File, :write, [file_path, test_data])

        # Read file back
        {:ok, read_data} = PeerCluster.rpc(cluster, :node1, File, :read, [file_path])
        assert read_data == test_data

        # List directory
        {:ok, files} = PeerCluster.rpc(cluster, :node1, File, :ls, [mount_point])
        assert "fuse_test.bin" in files

        # Get file stats
        {:ok, stat} = PeerCluster.rpc(cluster, :node1, File, :stat, [file_path])
        assert stat.size == byte_size(test_data)
      after
        # Unmount
        {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :unmount, [mount_point])

        # Clean up mount point
        PeerCluster.rpc(cluster, :node1, File, :rm_rf, [mount_point])
      end

      # Verify mount is gone
      {:ok, mounts} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :list_mounts, [])

      refute Enum.any?(mounts, fn m ->
               (is_map(m) and Map.get(m, :mount_point) == mount_point) or
                 (is_map(m) and Map.get(m, "mount_point") == mount_point)
             end)
    end

    test "data persists across unmount and remount", %{cluster: cluster} do
      :ok = init_cluster_with_volume(cluster)

      mount_point = "/tmp/neonfs_fuse_test_#{System.unique_integer([:positive])}"
      :ok = PeerCluster.rpc(cluster, :node1, File, :mkdir_p!, [mount_point])

      # Mount the volume
      {:ok, _mount_info} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :mount, [
          "test-volume",
          mount_point,
          %{}
        ])

      # Write test file
      test_data = "Persistent test data"
      file_path = Path.join(mount_point, "persistent.txt")
      :ok = PeerCluster.rpc(cluster, :node1, File, :write, [file_path, test_data])

      # Unmount
      {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :unmount, [mount_point])

      # Re-mount
      {:ok, _mount_info} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :mount, [
          "test-volume",
          mount_point,
          %{}
        ])

      try do
        # Verify data persisted
        {:ok, read_data} = PeerCluster.rpc(cluster, :node1, File, :read, [file_path])
        assert read_data == test_data
      after
        {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :unmount, [mount_point])
        PeerCluster.rpc(cluster, :node1, File, :rm_rf, [mount_point])
      end
    end
  end

  defp init_cluster_with_volume(cluster) do
    {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])

    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_status, []) do
            {:ok, _status} -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )

    {:ok, _volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "test-volume",
        %{}
      ])

    :ok
  end
end
