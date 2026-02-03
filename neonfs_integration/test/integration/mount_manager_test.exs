defmodule NeonFS.Integration.MountManagerTest do
  @moduledoc """
  Integration tests for the MountManager GenServer.

  These tests verify mount/unmount operations, mount tracking, and error handling
  in a proper multi-node environment where MountManager can communicate with
  the core node via RPC.
  """
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 120_000
  @moduletag nodes: 1
  @moduletag applications: [:neonfs_core, :neonfs_fuse]
  @moduletag :fuse_integration

  @test_volume "test_volume"

  setup %{cluster: cluster} do
    :ok = init_cluster_with_volume(cluster)

    # Create a unique mount point for each test
    test_id = System.unique_integer([:positive])
    test_mount_point = "/tmp/neonfs_test_mount_#{test_id}"

    # Create mount point on the peer node
    :ok = PeerCluster.rpc(cluster, :node1, File, :mkdir_p!, [test_mount_point])

    on_exit(fn ->
      # Clean up mount point
      PeerCluster.rpc(cluster, :node1, File, :rm_rf, [test_mount_point])
    end)

    %{cluster: cluster, test_mount_point: test_mount_point}
  end

  describe "mount/3" do
    test "successfully mounts a volume", %{cluster: cluster, test_mount_point: test_mount_point} do
      {:ok, mount_id} =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :mount, [
          @test_volume,
          test_mount_point
        ])

      assert is_binary(mount_id)
      assert String.starts_with?(mount_id, "mount_")

      # Verify mount is tracked
      {:ok, mount_info} =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :get_mount, [mount_id])

      assert mount_info.volume_name == @test_volume
      assert mount_info.mount_point == Path.expand(test_mount_point)
      assert %DateTime{} = mount_info.started_at
      assert is_reference(mount_info.mount_session)
      assert is_pid(mount_info.handler_pid)

      # Clean up
      :ok = PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :unmount, [mount_id])
    end

    test "fails for non-existent volume", %{cluster: cluster, test_mount_point: test_mount_point} do
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :mount, [
          "nonexistent",
          test_mount_point
        ])

      assert {:error, :volume_not_found} = result
    end

    test "fails for non-existent mount point", %{cluster: cluster} do
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :mount, [
          @test_volume,
          "/nonexistent/path/to/mount"
        ])

      assert {:error, :mount_point_not_found} = result
    end

    test "fails for mount point that is not a directory", %{
      cluster: cluster,
      test_mount_point: test_mount_point
    } do
      file_path = Path.join(test_mount_point, "testfile")
      :ok = PeerCluster.rpc(cluster, :node1, File, :write!, [file_path, "test"])

      try do
        result =
          PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :mount, [
            @test_volume,
            file_path
          ])

        assert {:error, :mount_point_not_directory} = result
      after
        PeerCluster.rpc(cluster, :node1, File, :rm!, [file_path])
      end
    end

    test "prevents duplicate mounts to same mount point", %{
      cluster: cluster,
      test_mount_point: test_mount_point
    } do
      {:ok, mount_id} =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :mount, [
          @test_volume,
          test_mount_point
        ])

      try do
        # Try to mount again at the same point
        result =
          PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :mount, [
            @test_volume,
            test_mount_point
          ])

        assert {:error, :already_mounted} = result
      after
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :unmount, [mount_id])
      end
    end

    test "supports mount options", %{cluster: cluster, test_mount_point: test_mount_point} do
      opts = [
        auto_unmount: false,
        allow_other: true,
        allow_root: true,
        ro: true
      ]

      {:ok, mount_id} =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :mount, [
          @test_volume,
          test_mount_point,
          opts
        ])

      try do
        {:ok, _mount_info} =
          PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :get_mount, [mount_id])
      after
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :unmount, [mount_id])
      end
    end
  end

  describe "unmount/1" do
    test "successfully unmounts a mounted volume", %{
      cluster: cluster,
      test_mount_point: test_mount_point
    } do
      {:ok, mount_id} =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :mount, [
          @test_volume,
          test_mount_point
        ])

      {:ok, mount_info} =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :get_mount, [mount_id])

      handler_pid = mount_info.handler_pid

      :ok = PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :unmount, [mount_id])

      # Verify mount is removed
      result = PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :get_mount, [mount_id])
      assert {:error, :not_found} = result

      # Verify handler is stopped
      refute PeerCluster.rpc(cluster, :node1, Process, :alive?, [handler_pid])
    end

    test "fails for non-existent mount ID", %{cluster: cluster} do
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :unmount, ["mount_nonexistent"])

      assert {:error, :not_found} = result
    end
  end

  describe "list_mounts/0" do
    test "returns empty list when no mounts", %{cluster: cluster} do
      mounts = PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :list_mounts, [])
      assert [] = mounts
    end

    test "returns list of active mounts", %{cluster: cluster, test_mount_point: test_mount_point} do
      {:ok, mount_id} =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :mount, [
          @test_volume,
          test_mount_point
        ])

      try do
        mounts = PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :list_mounts, [])
        assert length(mounts) == 1
        assert [mount_info] = mounts
        assert mount_info.id == mount_id
        assert mount_info.volume_name == @test_volume
      after
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :unmount, [mount_id])
      end
    end

    test "multiple mounts are tracked", %{cluster: cluster, test_mount_point: test_mount_point} do
      mount_point_2 = "/tmp/neonfs_test_mount_#{System.unique_integer([:positive])}"
      :ok = PeerCluster.rpc(cluster, :node1, File, :mkdir_p!, [mount_point_2])

      try do
        {:ok, mount_id_1} =
          PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :mount, [
            @test_volume,
            test_mount_point
          ])

        {:ok, mount_id_2} =
          PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :mount, [
            @test_volume,
            mount_point_2
          ])

        mounts = PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :list_mounts, [])
        assert length(mounts) == 2
        mount_ids = Enum.map(mounts, & &1.id)
        assert mount_id_1 in mount_ids
        assert mount_id_2 in mount_ids

        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :unmount, [mount_id_1])
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :unmount, [mount_id_2])
      after
        PeerCluster.rpc(cluster, :node1, File, :rm_rf!, [mount_point_2])
      end
    end
  end

  describe "get_mount/1" do
    test "returns mount info for valid ID", %{
      cluster: cluster,
      test_mount_point: test_mount_point
    } do
      {:ok, mount_id} =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :mount, [
          @test_volume,
          test_mount_point
        ])

      try do
        {:ok, mount_info} =
          PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :get_mount, [mount_id])

        assert mount_info.id == mount_id
        assert mount_info.volume_name == @test_volume
        assert mount_info.mount_point == Path.expand(test_mount_point)
      after
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :unmount, [mount_id])
      end
    end

    test "returns error for non-existent ID", %{cluster: cluster} do
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :get_mount, [
          "mount_nonexistent"
        ])

      assert {:error, :not_found} = result
    end
  end

  describe "get_mount_by_path/1" do
    test "returns mount info for valid path", %{
      cluster: cluster,
      test_mount_point: test_mount_point
    } do
      {:ok, mount_id} =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :mount, [
          @test_volume,
          test_mount_point
        ])

      try do
        {:ok, mount_info} =
          PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :get_mount_by_path, [
            test_mount_point
          ])

        assert mount_info.id == mount_id
      after
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :unmount, [mount_id])
      end
    end

    test "returns error for non-mounted path", %{cluster: cluster} do
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :get_mount_by_path, [
          "/nonexistent"
        ])

      assert {:error, :not_found} = result
    end
  end

  describe "handler crash handling" do
    test "cleans up mount when handler crashes", %{
      cluster: cluster,
      test_mount_point: test_mount_point
    } do
      {:ok, mount_id} =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :mount, [
          @test_volume,
          test_mount_point
        ])

      {:ok, mount_info} =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :get_mount, [mount_id])

      handler_pid = mount_info.handler_pid

      # Kill the handler process
      PeerCluster.rpc(cluster, :node1, Process, :exit, [handler_pid, :kill])

      # Wait for cleanup
      assert_eventually timeout: 5_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :get_mount, [mount_id]) do
          {:error, :not_found} -> true
          _ -> false
        end
      end
    end
  end

  describe "path normalization" do
    test "handles paths with trailing slashes", %{
      cluster: cluster,
      test_mount_point: test_mount_point
    } do
      path_with_slash = test_mount_point <> "/"

      {:ok, mount_id} =
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :mount, [
          @test_volume,
          path_with_slash
        ])

      try do
        {:ok, mount_info} =
          PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :get_mount, [mount_id])

        # Mount point should be normalized (no trailing slash)
        assert mount_info.mount_point == Path.expand(test_mount_point)

        # Should be able to query with either path
        {:ok, _} =
          PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :get_mount_by_path, [
            test_mount_point
          ])

        {:ok, _} =
          PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :get_mount_by_path, [
            path_with_slash
          ])
      after
        PeerCluster.rpc(cluster, :node1, NeonFS.FUSE.MountManager, :unmount, [mount_id])
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
        @test_volume,
        %{}
      ])

    :ok
  end
end
