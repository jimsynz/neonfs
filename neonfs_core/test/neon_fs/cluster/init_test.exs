defmodule NeonFS.Cluster.InitTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Cluster.Init
  alias NeonFS.Core.{SystemVolume, VolumeRegistry}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()

    # Start storage subsystems (needed for system volume write path)
    start_drive_registry()
    start_blob_store()
    start_chunk_index()
    start_file_index()
    start_stripe_index()
    start_volume_registry()
    ensure_chunk_access_tracker()

    # Start Ra so init_cluster can bootstrap
    start_ra()

    on_exit(fn ->
      stop_ra()
      cleanup_test_dirs()
    end)

    :ok
  end

  describe "init_cluster/1" do
    test "creates system volume and writes identity file" do
      cluster_name = "test-cluster"
      {:ok, _cluster_id} = Init.init_cluster(cluster_name)

      # System volume exists with correct properties
      assert {:ok, volume} = VolumeRegistry.get_system_volume()
      assert volume.name == "_system"
      assert volume.system == true
      assert volume.owner == :system
      assert volume.durability.type == :replicate
      assert volume.durability.factor == 1
      assert volume.write_ack == :quorum
      assert volume.compression.algorithm == :zstd
      assert volume.encryption.mode == :none

      # Identity file is readable and contains correct data
      assert {:ok, json} = SystemVolume.read("/cluster/identity.json")
      identity = :json.decode(json)

      assert identity["cluster_name"] == cluster_name
      assert identity["format_version"] == 1
      assert is_binary(identity["initialized_at"])

      # Verify it's valid ISO 8601
      assert {:ok, _datetime, _offset} =
               DateTime.from_iso8601(identity["initialized_at"])
    end

    test "returns error when already initialised" do
      {:ok, _cluster_id} = Init.init_cluster("test-cluster")
      assert {:error, :already_initialised} = Init.init_cluster("test-cluster")
    end

    test "idempotent — system volume not duplicated on re-init attempt" do
      {:ok, _cluster_id} = Init.init_cluster("test-cluster")
      {:ok, volume} = VolumeRegistry.get_system_volume()

      # Second init fails (State.exists? is true)
      assert {:error, :already_initialised} = Init.init_cluster("test-cluster")

      # System volume still exists and is unchanged
      assert {:ok, ^volume} = VolumeRegistry.get_system_volume()
    end
  end
end
