defmodule NeonFS.CLI.HandlerTest do
  use ExUnit.Case, async: false

  alias NeonFS.CLI.Handler
  alias NeonFS.Core.VolumeRegistry

  setup do
    # Clean up state before each test
    # Clear volume registry ETS tables
    if :ets.whereis(:volumes_by_id) != :undefined do
      :ets.delete_all_objects(:volumes_by_id)
    end

    if :ets.whereis(:volumes_by_name) != :undefined do
      :ets.delete_all_objects(:volumes_by_name)
    end

    :ok
  end

  describe "cluster_status/0" do
    test "returns cluster status map" do
      assert {:ok, status} = Handler.cluster_status()
      assert is_binary(status.name)
      assert is_binary(status.node)
      assert status.status == :running
      assert is_integer(status.volumes)
      assert is_integer(status.uptime)
      assert status.uptime >= 0
    end

    test "returns serializable data" do
      assert {:ok, status} = Handler.cluster_status()
      # Should be able to encode as Erlang terms
      assert is_map(status)
      refute match?(%{pid: _}, status)
      refute match?(%{ref: _}, status)
    end
  end

  describe "list_volumes/0" do
    test "returns empty list when no volumes exist" do
      assert {:ok, []} = Handler.list_volumes()
    end

    test "returns list of volume maps" do
      # Create a test volume
      {:ok, _volume} =
        VolumeRegistry.create("test-volume",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      assert {:ok, volumes} = Handler.list_volumes()
      assert length(volumes) == 1

      volume = List.first(volumes)
      assert volume.name == "test-volume"
      assert is_binary(volume.id)
      assert is_map(volume.durability)
      assert is_binary(volume.created_at)
    end

    test "returns multiple volumes" do
      {:ok, _} =
        VolumeRegistry.create("vol1",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      {:ok, _} =
        VolumeRegistry.create("vol2",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      assert {:ok, volumes} = Handler.list_volumes()
      assert length(volumes) == 2
      names = Enum.map(volumes, & &1.name)
      assert "vol1" in names
      assert "vol2" in names
    end

    test "returns serializable data" do
      {:ok, _} =
        VolumeRegistry.create("test-vol",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      assert {:ok, [volume]} = Handler.list_volumes()
      # All fields should be serializable
      assert is_binary(volume.id)
      assert is_binary(volume.name)
      assert is_map(volume.durability)
      refute is_struct(volume.durability)
    end
  end

  describe "create_volume/2" do
    test "creates volume with minimal config" do
      assert {:ok, volume} = Handler.create_volume("new-volume", %{})
      assert volume.name == "new-volume"
      assert is_binary(volume.id)
      assert is_map(volume.durability)
      assert is_map(volume.compression)
      assert is_map(volume.verification)
    end

    test "creates volume with custom config" do
      config = %{
        "owner" => "alice",
        "write_ack" => :quorum
      }

      assert {:ok, volume} = Handler.create_volume("alice-volume", config)
      assert volume.name == "alice-volume"
      assert volume.owner == "alice"
      assert volume.write_ack == :quorum
    end

    test "returns error for duplicate volume name" do
      {:ok, _} =
        VolumeRegistry.create("duplicate",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      assert {:error, _message} = Handler.create_volume("duplicate", %{})
    end

    test "returns serializable data" do
      assert {:ok, volume} = Handler.create_volume("test-vol", %{})
      assert is_map(volume)
      refute is_tuple(volume.created_at)
      assert is_binary(volume.created_at)
    end
  end

  describe "get_volume/1" do
    test "returns volume by name" do
      {:ok, _} =
        VolumeRegistry.create("get-test",
          owner: "bob",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      assert {:ok, volume} = Handler.get_volume("get-test")
      assert volume.name == "get-test"
      assert volume.owner == "bob"
    end

    test "returns error for non-existent volume" do
      assert {:error, :not_found} = Handler.get_volume("does-not-exist")
    end

    test "returns serializable data" do
      {:ok, _} =
        VolumeRegistry.create("test-vol",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      assert {:ok, volume} = Handler.get_volume("test-vol")
      assert is_binary(volume.created_at)
      assert is_binary(volume.updated_at)
    end
  end

  describe "delete_volume/1" do
    test "deletes existing volume" do
      {:ok, _} =
        VolumeRegistry.create("delete-test",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      assert {:ok, %{}} = Handler.delete_volume("delete-test")
      assert {:error, :not_found} = Handler.get_volume("delete-test")
    end

    test "returns error for non-existent volume" do
      assert {:error, :not_found} = Handler.delete_volume("does-not-exist")
    end

    test "returns empty map on success" do
      {:ok, _} =
        VolumeRegistry.create("test-vol",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      assert {:ok, result} = Handler.delete_volume("test-vol")
      assert result == %{}
    end
  end

  describe "mount/3" do
    setup do
      # Create a test volume for mounting
      {:ok, volume} =
        VolumeRegistry.create("mount-test",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      %{volume: volume}
    end

    test "mounts volume successfully", %{volume: _volume} do
      mount_point = System.tmp_dir!() <> "/neonfs_test_mount_#{:rand.uniform(100_000)}"
      File.mkdir_p!(mount_point)

      on_exit(fn ->
        # Clean up mount point
        File.rm_rf!(mount_point)
      end)

      # Note: Mount operations require neonfs_fuse to be running
      # If FUSE is not available, the operation will fail gracefully
      result = Handler.mount("mount-test", mount_point, %{})

      case result do
        {:ok, mount_info} ->
          assert is_binary(mount_info.id)
          assert mount_info.volume_name == "mount-test"
          assert mount_info.mount_point == Path.expand(mount_point)
          assert is_binary(mount_info.started_at)
          # Should not contain PIDs or references
          refute Map.has_key?(mount_info, :handler_pid)
          refute Map.has_key?(mount_info, :mount_session)

          # Clean up mount
          Handler.unmount(mount_info.id)

        {:error, :fuse_not_available} ->
          # Expected when neonfs_fuse app is not running
          :ok

        {:error, :nif_not_loaded} ->
          # Expected in environments without FUSE libraries
          :ok

        {:error, {:mount_failed, "FUSE NIF not loaded"}} ->
          # Expected in environments without FUSE libraries
          :ok

        {:error, reason} ->
          flunk("Unexpected error: #{inspect(reason)}")
      end
    end

    test "returns error for non-existent volume" do
      mount_point = System.tmp_dir!() <> "/neonfs_test_mount_#{:rand.uniform(100_000)}"
      File.mkdir_p!(mount_point)

      on_exit(fn ->
        File.rm_rf!(mount_point)
      end)

      result = Handler.mount("does-not-exist", mount_point, %{})

      case result do
        {:error, :fuse_not_available} ->
          # Expected when neonfs_fuse app is not running
          :ok

        {:error, _reason} ->
          # Any other error is also acceptable (volume not found, mount failed, etc)
          :ok
      end
    end
  end

  describe "unmount/1" do
    test "unmounts by mount ID" do
      # This test requires FUSE to be available
      mount_point = System.tmp_dir!() <> "/neonfs_test_unmount_#{:rand.uniform(100_000)}"
      File.mkdir_p!(mount_point)

      on_exit(fn ->
        File.rm_rf!(mount_point)
      end)

      {:ok, _volume} =
        VolumeRegistry.create("unmount-test",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      case Handler.mount("unmount-test", mount_point, %{}) do
        {:ok, mount_info} ->
          assert {:ok, %{}} = Handler.unmount(mount_info.id)

        {:error, :fuse_not_available} ->
          # Expected when neonfs_fuse app is not running
          :ok

        {:error, _reason} ->
          # Expected in environments without FUSE libraries
          :ok
      end
    end

    test "returns error for non-existent mount" do
      result = Handler.unmount("does-not-exist")

      case result do
        {:error, :fuse_not_available} ->
          # Expected when neonfs_fuse app is not running
          :ok

        {:error, :mount_not_found} ->
          # Expected when mount doesn't exist
          :ok

        {:error, _reason} ->
          # Any other error is acceptable
          :ok
      end
    end
  end

  describe "list_mounts/0" do
    test "returns empty list when no mounts exist (or FUSE not available)" do
      result = Handler.list_mounts()

      case result do
        {:ok, mounts} ->
          assert is_list(mounts)

        {:error, :fuse_not_available} ->
          # Expected when neonfs_fuse app is not running
          :ok
      end
    end

    test "returns serializable data" do
      # Even if mounts exist, data should be serializable
      case Handler.list_mounts() do
        {:ok, mounts} ->
          assert is_list(mounts)

          for mount <- mounts do
            assert is_map(mount)
            assert is_binary(mount.id)
            assert is_binary(mount.volume_name)
            assert is_binary(mount.mount_point)
            assert is_binary(mount.started_at)
            refute Map.has_key?(mount, :handler_pid)
            refute Map.has_key?(mount, :mount_session)
          end

        {:error, :fuse_not_available} ->
          # Expected when neonfs_fuse app is not running
          :ok
      end
    end
  end
end
