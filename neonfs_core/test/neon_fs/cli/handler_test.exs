defmodule NeonFS.CLI.HandlerTest do
  use ExUnit.Case, async: false

  alias NeonFS.CLI.Handler
  alias NeonFS.Cluster.State
  alias NeonFS.Core.VolumeRegistry

  @tmp_dir "/tmp/neonfs_handler_test"

  setup do
    # Clean up state before each test
    # Clear volume registry ETS tables
    if :ets.whereis(:volumes_by_id) != :undefined do
      :ets.delete_all_objects(:volumes_by_id)
    end

    if :ets.whereis(:volumes_by_name) != :undefined do
      :ets.delete_all_objects(:volumes_by_name)
    end

    # Clean up cluster state
    File.rm_rf!(@tmp_dir)
    File.mkdir_p!(@tmp_dir)
    Application.put_env(:neonfs_core, :meta_dir, @tmp_dir)

    on_exit(fn ->
      File.rm_rf!(@tmp_dir)
      Application.delete_env(:neonfs_core, :meta_dir)
    end)

    :ok
  end

  describe "cluster_status/0" do
    test "returns cluster status map" do
      assert {:ok, status} = Handler.cluster_status()
      assert is_binary(status.name)
      assert is_binary(status.node)
      assert status.status == :running
      assert is_integer(status.volumes)
      assert is_integer(status.uptime_seconds)
      assert status.uptime_seconds >= 0
    end

    test "returns serializable data" do
      assert {:ok, status} = Handler.cluster_status()
      # Should be able to encode as Erlang terms
      assert is_map(status)
      # Verify no non-serializable keys are present (PIDs, references)
      refute Map.has_key?(status, :pid)
      refute Map.has_key?(status, :ref)
    end
  end

  describe "cluster_init/1" do
    @tag :ra
    test "initializes cluster successfully" do
      assert {:ok, result} = Handler.cluster_init("test-cluster")

      assert is_binary(result.cluster_id)
      assert String.starts_with?(result.cluster_id, "clust_")
      assert result.cluster_name == "test-cluster"
      assert is_binary(result.node_id)
      assert String.starts_with?(result.node_id, "node_")
      assert is_binary(result.node_name)
      assert is_binary(result.created_at)
    end

    @tag :ra
    test "creates cluster state file" do
      assert {:ok, _result} = Handler.cluster_init("my-cluster")

      assert State.exists?()
      assert {:ok, state} = State.load()
      assert state.cluster_name == "my-cluster"
    end

    @tag :ra
    test "returns error if already initialized" do
      # Initialize once
      assert {:ok, _result} = Handler.cluster_init("first-cluster")

      # Try again
      assert {:error, :already_initialised} = Handler.cluster_init("second-cluster")
    end

    test "returns error if node is not named" do
      # This test only works if running without named node
      if Node.self() == :nonode@nohost do
        assert {:error, :node_not_named} = Handler.cluster_init("test-cluster")
      else
        # Skip if running with named node
        :ok
      end
    end

    @tag :ra
    test "returns serializable data" do
      assert {:ok, result} = Handler.cluster_init("test-cluster")

      # Should be able to encode as Erlang terms
      assert is_map(result)
      # All values should be basic types
      for {_key, value} <- result do
        assert is_binary(value) or is_atom(value) or is_number(value)
      end
    end
  end

  describe "create_invite/1" do
    setup do
      # Initialize cluster for invite tests
      if Node.self() != :nonode@nohost do
        Handler.cluster_init("invite-test-cluster")
      end

      :ok
    end

    @tag :ra
    test "creates invite token successfully" do
      assert {:ok, result} = Handler.create_invite(3600)
      assert is_binary(result.token)
      assert String.starts_with?(result.token, "nfs_inv_")
    end

    @tag :ra
    test "creates tokens with different durations" do
      assert {:ok, result1} = Handler.create_invite(60)
      assert {:ok, result2} = Handler.create_invite(3600)

      assert result1.token != result2.token
    end

    test "returns error if cluster not initialized" do
      # Don't initialize cluster
      assert {:error, :cluster_not_initialized} = Handler.create_invite(3600)
    end

    @tag :ra
    test "returns serializable data" do
      assert {:ok, result} = Handler.create_invite(3600)
      assert is_map(result)
      assert is_binary(result.token)
    end
  end

  describe "join_cluster/2" do
    setup do
      # Initialize cluster and create invite for join tests
      if Node.self() != :nonode@nohost do
        Handler.cluster_init("join-test-cluster")
      end

      :ok
    end

    @tag :ra
    test "validates parameters" do
      # Create a valid token
      {:ok, invite_result} = Handler.create_invite(3600)
      token = invite_result.token
      via_node = Atom.to_string(Node.self())

      # Should fail because we can't join our own cluster
      result = Handler.join_cluster(token, via_node)

      # Either already_in_cluster or join failure expected
      assert match?({:error, _}, result)
    end

    test "returns error with string node name" do
      token = "nfs_inv_fake_token"
      via_node = "fake_node@localhost"

      # Should fail due to invalid token or RPC failure
      assert {:error, _reason} = Handler.join_cluster(token, via_node)
    end

    @tag :ra
    test "join result has correct structure" do
      # We can't fully test join without multi-node setup
      # But we can verify the function signature and error handling

      token = "nfs_inv_invalid"
      via_node = "fake@localhost"

      result = Handler.join_cluster(token, via_node)

      # Should return error tuple
      assert match?({:error, _}, result)
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
