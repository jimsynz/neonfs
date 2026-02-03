defmodule NeonFS.CLI.HandlerTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.CLI.Handler
  alias NeonFS.Cluster.State
  alias NeonFS.Core.RaServer

  @moduletag :tmp_dir

  # Reset Ra state between ALL tests in this module to ensure isolation
  setup do
    if Process.whereis(RaServer), do: RaServer.reset!()
    :ok
  end

  describe "cluster_status/0" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

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
      assert is_map(status)

      for {_key, value} <- status do
        assert is_binary(value) or is_atom(value) or is_number(value)
      end
    end
  end

  describe "cluster_init/1" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      start_ra()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

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

    test "creates cluster state file" do
      assert {:ok, _result} = Handler.cluster_init("my-cluster")

      assert State.exists?()
      assert {:ok, state} = State.load()
      assert state.cluster_name == "my-cluster"
    end

    test "returns error if already initialized" do
      assert {:ok, _result} = Handler.cluster_init("first-cluster")
      assert {:error, :already_initialised} = Handler.cluster_init("second-cluster")
    end

    test "returns serializable data" do
      assert {:ok, result} = Handler.cluster_init("test-cluster")
      assert is_map(result)

      for {_key, value} <- result do
        assert is_binary(value) or is_atom(value) or is_number(value)
      end
    end
  end

  describe "create_invite/1" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      start_ra()
      Handler.cluster_init("invite-test-cluster")

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "creates invite token successfully" do
      assert {:ok, result} = Handler.create_invite(3600)
      assert is_binary(result["token"])
      assert String.starts_with?(result["token"], "nfs_inv_")
    end

    test "creates tokens with different durations" do
      assert {:ok, result1} = Handler.create_invite(60)
      assert {:ok, result2} = Handler.create_invite(3600)
      assert result1["token"] != result2["token"]
    end

    test "returns serializable data" do
      assert {:ok, result} = Handler.create_invite(3600)
      assert is_map(result)
      assert is_binary(result["token"])
    end
  end

  describe "create_invite/1 without cluster" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      start_ra()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "returns error if cluster not initialized" do
      assert {:error, :cluster_not_initialized} = Handler.create_invite(3600)
    end
  end

  describe "join_cluster/2" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      start_ra()
      Handler.cluster_init("join-test-cluster")

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "validates parameters" do
      {:ok, invite_result} = Handler.create_invite(3600)
      token = invite_result["token"]
      via_node = Atom.to_string(Node.self())

      result = Handler.join_cluster(token, via_node)
      assert match?({:error, _}, result)
    end

    test "returns error with invalid token" do
      token = "nfs_inv_fake_token"
      via_node = "fake_node@localhost"
      assert {:error, _reason} = Handler.join_cluster(token, via_node)
    end
  end

  describe "list_volumes/0" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)

      # Ensure Ra is stopped so VolumeRegistry starts fresh
      stop_ra()

      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "returns empty list when no volumes exist" do
      assert {:ok, []} = Handler.list_volumes()
    end

    test "returns list of volume maps" do
      vol_name = "test-vol-#{:rand.uniform(999_999)}"
      Handler.create_volume(vol_name, %{})
      assert {:ok, volumes} = Handler.list_volumes()
      assert is_list(volumes)
      assert volumes != []
      assert Enum.any?(volumes, fn v -> v.name == vol_name end)
    end

    test "returns multiple volumes" do
      # Create 3 volumes with unique names
      vol1 = "vol1-#{:rand.uniform(999_999)}"
      vol2 = "vol2-#{:rand.uniform(999_999)}"
      vol3 = "vol3-#{:rand.uniform(999_999)}"

      Handler.create_volume(vol1, %{})
      Handler.create_volume(vol2, %{})
      Handler.create_volume(vol3, %{})

      assert {:ok, volumes} = Handler.list_volumes()
      # Should have at least these 3 volumes
      assert length(volumes) >= 3
    end

    test "returns serializable data" do
      vol_name = "test-vol-#{:rand.uniform(999_999)}"
      Handler.create_volume(vol_name, %{})
      assert {:ok, volumes} = Handler.list_volumes()
      assert is_list(volumes)

      for vol <- volumes do
        assert is_map(vol)
      end
    end
  end

  describe "create_volume/2" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "creates volume with minimal config" do
      vol_name = "new-vol-#{:rand.uniform(999_999)}"
      assert {:ok, volume} = Handler.create_volume(vol_name, %{})
      assert volume.name == vol_name
      assert is_binary(volume.id)
    end

    test "creates volume with custom config" do
      vol_name = "custom-vol-#{:rand.uniform(999_999)}"
      config = %{owner: "test-user"}
      assert {:ok, volume} = Handler.create_volume(vol_name, config)
      assert volume.name == vol_name
      assert volume.owner == "test-user"
    end

    test "returns error for duplicate volume name" do
      vol_name = "dup-vol-#{:rand.uniform(999_999)}"
      Handler.create_volume(vol_name, %{})
      assert {:error, _reason} = Handler.create_volume(vol_name, %{})
    end

    test "returns serializable data" do
      vol_name = "test-vol-#{:rand.uniform(999_999)}"
      assert {:ok, volume} = Handler.create_volume(vol_name, %{})
      assert is_map(volume)
    end
  end

  describe "get_volume/1" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_volume_registry()
      vol_name = "get-test-vol-#{:rand.uniform(999_999)}"
      {:ok, vol} = Handler.create_volume(vol_name, %{})

      on_exit(fn -> cleanup_test_dirs() end)
      {:ok, volume: vol}
    end

    test "returns volume by name", %{volume: vol} do
      assert {:ok, found} = Handler.get_volume(vol.name)
      assert found.id == vol.id
    end

    test "returns error for non-existent volume" do
      assert {:error, :not_found} = Handler.get_volume("no-such-vol")
    end

    test "returns serializable data", %{volume: vol} do
      assert {:ok, found} = Handler.get_volume(vol.name)
      assert is_map(found)
    end
  end

  describe "delete_volume/1" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_file_index()
      start_volume_registry()
      vol_name = "delete-test-vol-#{:rand.uniform(999_999)}"
      {:ok, vol} = Handler.create_volume(vol_name, %{})

      on_exit(fn -> cleanup_test_dirs() end)
      {:ok, volume: vol}
    end

    test "deletes existing volume", %{volume: vol} do
      assert {:ok, _} = Handler.delete_volume(vol.name)
      assert {:error, :not_found} = Handler.get_volume(vol.name)
    end

    test "returns error for non-existent volume" do
      assert {:error, :not_found} = Handler.delete_volume("no-such-vol")
    end

    test "returns empty map on success", %{volume: vol} do
      assert {:ok, result} = Handler.delete_volume(vol.name)
      assert result == %{}
    end
  end

  describe "mount/3" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_volume_registry()
      vol_name = "mount-test-vol-#{:rand.uniform(999_999)}"
      {:ok, vol} = Handler.create_volume(vol_name, %{})

      on_exit(fn -> cleanup_test_dirs() end)
      {:ok, volume: vol}
    end

    test "returns error for non-existent volume or FUSE unavailable" do
      result = Handler.mount("no-such-vol", "/mnt/test", %{})
      # Either volume not found or FUSE not available
      assert match?({:error, _}, result)
    end
  end

  describe "unmount/1" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "returns error for non-existent mount or FUSE unavailable" do
      result = Handler.unmount("no-such-mount")
      # Either not found or FUSE not available
      assert match?({:error, _}, result)
    end
  end

  describe "list_mounts/0" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "returns list or error depending on FUSE availability" do
      result = Handler.list_mounts()
      assert match?({:ok, _}, result) or match?({:error, _}, result)
    end
  end
end
