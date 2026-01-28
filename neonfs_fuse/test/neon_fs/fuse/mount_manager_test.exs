defmodule NeonFS.FUSE.MountManagerTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.VolumeRegistry
  alias NeonFS.FUSE.{MountInfo, MountManager}

  @test_volume "test_volume"
  @test_mount_point "/tmp/neonfs_test_mount"

  setup do
    # Ensure test mount point exists
    File.mkdir_p!(@test_mount_point)

    # Create a test volume (or use existing one)
    volume_id =
      case VolumeRegistry.create(@test_volume,
             durability: %{type: :replicate, factor: 1, min_copies: 1},
             compression: %{algorithm: :zstd, level: 3, min_size: 4096},
             verification: %{on_read: :never}
           ) do
        {:ok, vol_id} ->
          vol_id

        {:error, _} ->
          # Volume already exists, get it
          {:ok, vol} = VolumeRegistry.get_by_name(@test_volume)
          vol.id
      end

    on_exit(fn ->
      # Clean up any remaining mounts
      case MountManager.list_mounts() do
        mounts when is_list(mounts) ->
          for mount <- mounts do
            MountManager.unmount(mount.id)
          end

        _ ->
          :ok
      end

      # Delete test volume
      VolumeRegistry.delete(volume_id)

      # Clean up mount point
      File.rm_rf!(@test_mount_point)
    end)

    %{volume_id: volume_id}
  end

  describe "mount/3" do
    test "successfully mounts a volume", %{volume_id: _volume_id} do
      # Note: This test requires FUSE to be available
      # It will fail gracefully if FUSE is not installed
      result = MountManager.mount(@test_volume, @test_mount_point)

      case result do
        {:ok, mount_id} ->
          assert is_binary(mount_id)
          assert String.starts_with?(mount_id, "mount_")

          # Verify mount is tracked
          assert {:ok, mount_info} = MountManager.get_mount(mount_id)
          assert mount_info.volume_name == @test_volume
          assert mount_info.mount_point == Path.expand(@test_mount_point)
          assert %DateTime{} = mount_info.started_at
          assert is_reference(mount_info.mount_session)
          assert is_pid(mount_info.handler_pid)
          assert Process.alive?(mount_info.handler_pid)

          # Clean up
          assert :ok = MountManager.unmount(mount_id)

        {:error, {:mount_failed, reason}} ->
          # FUSE not available or other system issue
          IO.puts("Skipping FUSE mount test: #{inspect(reason)}")
          :ok
      end
    end

    test "fails for non-existent volume" do
      assert {:error, :volume_not_found} =
               MountManager.mount("nonexistent", @test_mount_point)
    end

    test "fails for non-existent mount point" do
      assert {:error, :mount_point_not_found} =
               MountManager.mount(@test_volume, "/nonexistent/path/to/mount")
    end

    test "fails for mount point that is not a directory", %{volume_id: _volume_id} do
      file_path = Path.join(@test_mount_point, "testfile")
      File.write!(file_path, "test")

      assert {:error, :mount_point_not_directory} =
               MountManager.mount(@test_volume, file_path)

      File.rm!(file_path)
    end

    test "prevents duplicate mounts to same mount point", %{volume_id: _volume_id} do
      case MountManager.mount(@test_volume, @test_mount_point) do
        {:ok, mount_id} ->
          # Try to mount again at the same point
          assert {:error, :already_mounted} =
                   MountManager.mount(@test_volume, @test_mount_point)

          # Clean up
          MountManager.unmount(mount_id)

        {:error, {:mount_failed, _reason}} ->
          # FUSE not available, skip
          :ok
      end
    end

    test "supports mount options", %{volume_id: _volume_id} do
      opts = [
        auto_unmount: false,
        allow_other: true,
        allow_root: true,
        ro: true
      ]

      result = MountManager.mount(@test_volume, @test_mount_point, opts)

      case result do
        {:ok, mount_id} ->
          # Mount succeeded with options
          assert {:ok, _mount_info} = MountManager.get_mount(mount_id)
          MountManager.unmount(mount_id)

        {:error, {:mount_failed, _reason}} ->
          # FUSE not available or permissions issue
          :ok
      end
    end
  end

  describe "unmount/1" do
    test "successfully unmounts a mounted volume", %{volume_id: _volume_id} do
      case MountManager.mount(@test_volume, @test_mount_point) do
        {:ok, mount_id} ->
          assert {:ok, mount_info} = MountManager.get_mount(mount_id)
          handler_pid = mount_info.handler_pid

          assert :ok = MountManager.unmount(mount_id)

          # Verify mount is removed
          assert {:error, :not_found} = MountManager.get_mount(mount_id)

          # Verify handler is stopped
          refute Process.alive?(handler_pid)

        {:error, {:mount_failed, _reason}} ->
          # FUSE not available
          :ok
      end
    end

    test "fails for non-existent mount ID" do
      assert {:error, :not_found} = MountManager.unmount("mount_nonexistent")
    end
  end

  describe "list_mounts/0" do
    test "returns empty list when no mounts" do
      assert [] = MountManager.list_mounts()
    end

    test "returns list of active mounts", %{volume_id: _volume_id} do
      case MountManager.mount(@test_volume, @test_mount_point) do
        {:ok, mount_id} ->
          mounts = MountManager.list_mounts()
          assert length(mounts) == 1
          assert [mount_info] = mounts
          assert %MountInfo{} = mount_info
          assert mount_info.id == mount_id
          assert mount_info.volume_name == @test_volume

          # Clean up
          MountManager.unmount(mount_id)

        {:error, {:mount_failed, _reason}} ->
          # FUSE not available
          :ok
      end
    end

    test "multiple mounts are tracked", %{volume_id: _volume_id} do
      # Create second mount point
      mount_point_2 = "/tmp/neonfs_test_mount_2"
      File.mkdir_p!(mount_point_2)

      try do
        case MountManager.mount(@test_volume, @test_mount_point) do
          {:ok, mount_id_1} ->
            case MountManager.mount(@test_volume, mount_point_2) do
              {:ok, mount_id_2} ->
                mounts = MountManager.list_mounts()
                assert length(mounts) == 2
                mount_ids = Enum.map(mounts, & &1.id)
                assert mount_id_1 in mount_ids
                assert mount_id_2 in mount_ids

                # Clean up
                MountManager.unmount(mount_id_1)
                MountManager.unmount(mount_id_2)

              {:error, {:mount_failed, _reason}} ->
                # FUSE not available
                MountManager.unmount(mount_id_1)
            end

          {:error, {:mount_failed, _reason}} ->
            # FUSE not available
            :ok
        end
      after
        File.rm_rf!(mount_point_2)
      end
    end
  end

  describe "get_mount/1" do
    test "returns mount info for valid ID", %{volume_id: _volume_id} do
      case MountManager.mount(@test_volume, @test_mount_point) do
        {:ok, mount_id} ->
          assert {:ok, mount_info} = MountManager.get_mount(mount_id)
          assert %MountInfo{} = mount_info
          assert mount_info.id == mount_id
          assert mount_info.volume_name == @test_volume
          assert mount_info.mount_point == Path.expand(@test_mount_point)

          # Clean up
          MountManager.unmount(mount_id)

        {:error, {:mount_failed, _reason}} ->
          # FUSE not available
          :ok
      end
    end

    test "returns error for non-existent ID" do
      assert {:error, :not_found} = MountManager.get_mount("mount_nonexistent")
    end
  end

  describe "get_mount_by_path/1" do
    test "returns mount info for valid path", %{volume_id: _volume_id} do
      case MountManager.mount(@test_volume, @test_mount_point) do
        {:ok, mount_id} ->
          # Test with exact path
          assert {:ok, mount_info} = MountManager.get_mount_by_path(@test_mount_point)
          assert mount_info.id == mount_id

          # Test with relative path
          relative_path = Path.relative_to_cwd(@test_mount_point)
          assert {:ok, mount_info} = MountManager.get_mount_by_path(relative_path)
          assert mount_info.id == mount_id

          # Clean up
          MountManager.unmount(mount_id)

        {:error, {:mount_failed, _reason}} ->
          # FUSE not available
          :ok
      end
    end

    test "returns error for non-mounted path" do
      assert {:error, :not_found} = MountManager.get_mount_by_path("/nonexistent")
    end
  end

  describe "handler crash handling" do
    test "cleans up mount when handler crashes", %{volume_id: _volume_id} do
      case MountManager.mount(@test_volume, @test_mount_point) do
        {:ok, mount_id} ->
          assert {:ok, mount_info} = MountManager.get_mount(mount_id)
          handler_pid = mount_info.handler_pid

          # Kill the handler process
          Process.exit(handler_pid, :kill)

          # Wait for cleanup
          Process.sleep(100)

          # Verify mount is removed
          assert {:error, :not_found} = MountManager.get_mount(mount_id)

        {:error, {:mount_failed, _reason}} ->
          # FUSE not available
          :ok
      end
    end
  end

  describe "path normalization" do
    test "handles paths with trailing slashes", %{volume_id: _volume_id} do
      path_with_slash = @test_mount_point <> "/"

      case MountManager.mount(@test_volume, path_with_slash) do
        {:ok, mount_id} ->
          # Mount point should be normalized (no trailing slash)
          assert {:ok, mount_info} = MountManager.get_mount(mount_id)
          assert mount_info.mount_point == Path.expand(@test_mount_point)

          # Should be able to query with either path
          assert {:ok, _} = MountManager.get_mount_by_path(@test_mount_point)
          assert {:ok, _} = MountManager.get_mount_by_path(path_with_slash)

          # Clean up
          MountManager.unmount(mount_id)

        {:error, {:mount_failed, _reason}} ->
          # FUSE not available
          :ok
      end
    end
  end
end
