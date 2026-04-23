defmodule NeonFS.CLI.HandlerTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.CLI.Handler
  alias NeonFS.Cluster.State
  alias NeonFS.Core.{CertificateAuthority, RaServer, VolumeRegistry}
  alias NeonFS.Transport.TLS

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

    test "returns not_initialised when no cluster state" do
      assert {:ok, status} = Handler.cluster_status()
      assert status.name == nil
      assert is_binary(status.node)
      assert status.status == :not_initialised
      assert status.volumes == 0
      assert is_integer(status.uptime_seconds)
    end

    test "returns running status when cluster exists" do
      ensure_cluster_state()
      assert {:ok, status} = Handler.cluster_status()
      assert is_binary(status.name)
      assert is_binary(status.node)
      assert status.status == :running
      assert is_integer(status.volumes)
      assert is_integer(status.uptime_seconds)
      assert status.uptime_seconds >= 0
    end

    test "returns serializable data" do
      ensure_cluster_state()
      assert {:ok, status} = Handler.cluster_status()
      assert is_map(status)

      for {_key, value} <- status do
        assert is_binary(value) or is_atom(value) or is_number(value)
      end
    end
  end

  # Consolidated into a single test because Ra's :default system is a
  # singleton — restarting it between tests is fragile.
  describe "cluster_init/1" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_stripe_index()
      start_volume_registry()
      ensure_chunk_access_tracker()
      start_ra()

      on_exit(fn ->
        stop_ra()
        cleanup_test_dirs()
      end)

      :ok
    end

    test "initializes cluster, persists state, returns serializable data, and rejects re-init" do
      assert {:ok, result} = Handler.cluster_init("test-cluster")

      # --- Init result properties ---
      assert is_binary(result.cluster_id)
      assert String.starts_with?(result.cluster_id, "clust_")
      assert result.cluster_name == "test-cluster"
      assert is_binary(result.node_id)
      assert String.starts_with?(result.node_id, "node_")
      assert is_binary(result.node_name)
      assert is_binary(result.created_at)

      # --- Cluster state persisted ---
      assert State.exists?()
      assert {:ok, state} = State.load()
      assert state.cluster_name == "test-cluster"

      # --- Result is serializable ---
      assert is_map(result)

      for {_key, value} <- result do
        assert is_binary(value) or is_atom(value) or is_number(value)
      end

      # --- Idempotency: second init returns structured error ---
      assert {:error, %NeonFS.Error.Invalid{message: "Cluster already initialised"}} =
               Handler.cluster_init("second-cluster")
    end
  end

  describe "create_invite/1" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_stripe_index()
      start_volume_registry()
      ensure_chunk_access_tracker()
      start_ra()
      Handler.cluster_init("invite-test-cluster")

      on_exit(fn ->
        stop_ra()
        cleanup_test_dirs()
      end)

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
      stop_ra()
      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_stripe_index()
      start_volume_registry()
      ensure_chunk_access_tracker()
      start_ra()

      on_exit(fn ->
        stop_ra()
        cleanup_test_dirs()
      end)

      :ok
    end

    test "returns error if cluster not initialized" do
      assert {:error, %NeonFS.Error.NotFound{}} = Handler.create_invite(3600)
    end
  end

  describe "join_cluster/2" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_stripe_index()
      start_volume_registry()
      ensure_chunk_access_tracker()
      start_ra()
      Handler.cluster_init("join-test-cluster")

      on_exit(fn ->
        stop_ra()
        cleanup_test_dirs()
      end)

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

  describe "list_volumes/1" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      ensure_cluster_state()

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

    test "excludes system volumes by default", %{tmp_dir: tmp_dir} do
      master_key = :crypto.strong_rand_bytes(32) |> Base.encode64()
      write_cluster_json(tmp_dir, master_key)

      vol_name = "user-vol-#{:rand.uniform(999_999)}"
      Handler.create_volume(vol_name, %{})
      VolumeRegistry.create_system_volume()

      assert {:ok, volumes} = Handler.list_volumes()
      refute Enum.any?(volumes, fn v -> v.name == "_system" end)
      assert Enum.any?(volumes, fn v -> v.name == vol_name end)
    end

    test "includes system volumes when all filter is true", %{tmp_dir: tmp_dir} do
      master_key = :crypto.strong_rand_bytes(32) |> Base.encode64()
      write_cluster_json(tmp_dir, master_key)

      vol_name = "user-vol-#{:rand.uniform(999_999)}"
      Handler.create_volume(vol_name, %{})
      VolumeRegistry.create_system_volume()

      assert {:ok, volumes} = Handler.list_volumes(%{"all" => true})
      assert Enum.any?(volumes, fn v -> v.name == "_system" end)
      assert Enum.any?(volumes, fn v -> v.name == vol_name end)
    end
  end

  describe "create_volume/2" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      ensure_cluster_state()
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
      ensure_cluster_state()
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
      assert {:error, %NeonFS.Error.VolumeNotFound{volume_name: "no-such-vol"}} =
               Handler.get_volume("no-such-vol")
    end

    test "returns serializable data", %{volume: vol} do
      assert {:ok, found} = Handler.get_volume(vol.name)
      assert is_map(found)
    end
  end

  describe "delete_volume/1" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      ensure_cluster_state()
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
      assert {:error, %NeonFS.Error.VolumeNotFound{}} = Handler.get_volume(vol.name)
    end

    test "returns error for non-existent volume" do
      assert {:error, %NeonFS.Error.VolumeNotFound{volume_name: "no-such-vol"}} =
               Handler.delete_volume("no-such-vol")
    end

    test "returns empty map on success", %{volume: vol} do
      assert {:ok, result} = Handler.delete_volume(vol.name)
      assert result == %{}
    end
  end

  describe "create_volume/2 with durability strings" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      ensure_cluster_state()
      stop_ra()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "parses replicate:3 durability string" do
      vol_name = "rep-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "replicate:3"}
      assert {:ok, volume} = Handler.create_volume(vol_name, config)
      assert volume.durability == %{type: :replicate, factor: 3, min_copies: 2}
      assert volume.durability_display == "replicate:3"
    end

    test "parses replicate:1 durability string" do
      vol_name = "rep1-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "replicate:1"}
      assert {:ok, volume} = Handler.create_volume(vol_name, config)
      assert volume.durability == %{type: :replicate, factor: 1, min_copies: 1}
      assert volume.durability_display == "replicate:1"
    end

    test "parses erasure:10:4 durability string" do
      vol_name = "ec-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "erasure:10:4"}
      assert {:ok, volume} = Handler.create_volume(vol_name, config)
      assert volume.durability == %{type: :erasure, data_chunks: 10, parity_chunks: 4}
      assert volume.durability_display == "erasure:10+4 (1.40x overhead)"
    end

    test "parses erasure:4:2 durability string" do
      vol_name = "ec2-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "erasure:4:2"}
      assert {:ok, volume} = Handler.create_volume(vol_name, config)
      assert volume.durability == %{type: :erasure, data_chunks: 4, parity_chunks: 2}
      assert volume.durability_display == "erasure:4+2 (1.50x overhead)"
    end

    test "parses erasure:8:3 durability string" do
      vol_name = "ec3-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "erasure:8:3"}
      assert {:ok, volume} = Handler.create_volume(vol_name, config)
      assert volume.durability == %{type: :erasure, data_chunks: 8, parity_chunks: 3}
      assert volume.durability_display == "erasure:8+3 (1.38x overhead)"
    end

    test "rejects erasure:0:4 (data_chunks < 1)" do
      vol_name = "bad-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "erasure:0:4"}

      assert {:error, %NeonFS.Error.InvalidConfig{field: :durability}} =
               Handler.create_volume(vol_name, config)
    end

    test "rejects erasure:4:0 (parity_chunks < 1)" do
      vol_name = "bad2-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "erasure:4:0"}

      assert {:error, %NeonFS.Error.InvalidConfig{field: :durability}} =
               Handler.create_volume(vol_name, config)
    end

    test "rejects erasure:abc (malformed)" do
      vol_name = "bad3-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "erasure:abc"}

      assert {:error, %NeonFS.Error.InvalidConfig{field: :durability}} =
               Handler.create_volume(vol_name, config)
    end

    test "rejects replicate:0 (factor < 1)" do
      vol_name = "bad4-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "replicate:0"}

      assert {:error, %NeonFS.Error.InvalidConfig{field: :durability}} =
               Handler.create_volume(vol_name, config)
    end

    test "rejects replicate:abc (non-integer)" do
      vol_name = "bad5-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "replicate:abc"}

      assert {:error, %NeonFS.Error.InvalidConfig{field: :durability}} =
               Handler.create_volume(vol_name, config)
    end

    test "rejects unknown durability format" do
      vol_name = "bad6-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "mirror:3"}

      assert {:error, %NeonFS.Error.InvalidConfig{field: :durability}} =
               Handler.create_volume(vol_name, config)
    end

    test "default durability when no durability specified" do
      vol_name = "default-vol-#{:rand.uniform(999_999)}"
      assert {:ok, volume} = Handler.create_volume(vol_name, %{})
      assert volume.durability == %{type: :replicate, factor: 3, min_copies: 2}
      assert volume.durability_display == "replicate:3"
    end

    test "map durability config passes through unchanged" do
      vol_name = "map-vol-#{:rand.uniform(999_999)}"
      dur = %{type: :erasure, data_chunks: 6, parity_chunks: 3}
      config = %{"durability" => dur}
      assert {:ok, volume} = Handler.create_volume(vol_name, config)
      assert volume.durability == dur
      assert volume.durability_display == "erasure:6+3 (1.50x overhead)"
    end
  end

  describe "volume display with erasure config" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      ensure_cluster_state()
      stop_ra()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "get_volume returns durability_display for erasure volume" do
      vol_name = "disp-ec-#{:rand.uniform(999_999)}"
      config = %{"durability" => "erasure:10:4"}
      {:ok, _} = Handler.create_volume(vol_name, config)

      assert {:ok, vol} = Handler.get_volume(vol_name)
      assert vol.durability_display == "erasure:10+4 (1.40x overhead)"
    end

    test "list_volumes includes durability_display" do
      vol_name = "list-ec-#{:rand.uniform(999_999)}"
      config = %{"durability" => "erasure:4:2"}
      {:ok, _} = Handler.create_volume(vol_name, config)

      assert {:ok, volumes} = Handler.list_volumes()
      vol = Enum.find(volumes, fn v -> v.name == vol_name end)
      assert vol.durability_display == "erasure:4+2 (1.50x overhead)"
    end

    test "list_volumes shows mixed durability types" do
      rep_name = "mix-rep-#{:rand.uniform(999_999)}"
      ec_name = "mix-ec-#{:rand.uniform(999_999)}"

      {:ok, _} = Handler.create_volume(rep_name, %{"durability" => "replicate:3"})
      {:ok, _} = Handler.create_volume(ec_name, %{"durability" => "erasure:10:4"})

      assert {:ok, volumes} = Handler.list_volumes()

      rep_vol = Enum.find(volumes, fn v -> v.name == rep_name end)
      ec_vol = Enum.find(volumes, fn v -> v.name == ec_name end)

      assert rep_vol.durability_display == "replicate:3"
      assert ec_vol.durability_display == "erasure:10+4 (1.40x overhead)"
    end
  end

  describe "mount/3" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      ensure_cluster_state()
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
      ensure_cluster_state()
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
      ensure_cluster_state()
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

  describe "nfs_export/1" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      ensure_cluster_state()
      stop_ra()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "returns error for non-existent volume or NFS unavailable" do
      result = Handler.nfs_export("no-such-vol")
      assert match?({:error, _}, result)
    end
  end

  describe "nfs_unexport/1" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      ensure_cluster_state()
      stop_ra()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "returns error for non-existent export or NFS unavailable" do
      result = Handler.nfs_unexport("no-such-export")
      assert match?({:error, _}, result)
    end
  end

  describe "nfs_list_exports/0" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      ensure_cluster_state()
      stop_ra()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "returns list or error depending on NFS availability" do
      result = Handler.nfs_list_exports()
      assert match?({:ok, _}, result) or match?({:error, _}, result)
    end
  end

  describe "update_volume/2" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      ensure_cluster_state()
      stop_ra()
      start_volume_registry()
      vol_name = "update-test-vol-#{:rand.uniform(999_999)}"
      {:ok, vol} = Handler.create_volume(vol_name, %{})

      on_exit(fn -> cleanup_test_dirs() end)
      {:ok, volume: vol, vol_name: vol_name}
    end

    test "updates compression on existing volume", %{vol_name: vol_name} do
      config = %{"compression" => %{"algorithm" => "zstd"}}
      assert {:ok, updated} = Handler.update_volume(vol_name, config)
      assert updated.compression.algorithm == :zstd
    end

    test "updates simple fields", %{vol_name: vol_name} do
      config = %{"io_weight" => 5, "write_ack" => "all", "atime_mode" => "noatime"}
      assert {:ok, updated} = Handler.update_volume(vol_name, config)
      assert updated.io_weight == 5
      assert updated.write_ack == :all
      assert updated.atime_mode == :noatime
    end

    test "updates tiering config", %{vol_name: vol_name} do
      config = %{"initial_tier" => "warm", "promotion_threshold" => 10}
      assert {:ok, updated} = Handler.update_volume(vol_name, config)
      assert updated.tiering.initial_tier == :warm
      assert updated.tiering.promotion_threshold == 10
      # Existing demotion_delay should be preserved
      assert is_integer(updated.tiering.demotion_delay)
    end

    test "updates verification config", %{vol_name: vol_name} do
      config = %{"on_read" => "always", "scrub_interval" => 86_400}
      assert {:ok, updated} = Handler.update_volume(vol_name, config)
      assert updated.verification.on_read == :always
      assert updated.verification.scrub_interval == 86_400
    end

    test "updates caching config", %{vol_name: vol_name} do
      config = %{"transformed_chunks" => "false", "remote_chunks" => "true"}
      assert {:ok, updated} = Handler.update_volume(vol_name, config)
      assert updated.caching.transformed_chunks == false
      assert updated.caching.remote_chunks == true
      # Existing reconstructed_stripes should be preserved
      assert is_boolean(updated.caching.reconstructed_stripes)
    end

    test "updates metadata consistency config", %{vol_name: vol_name} do
      config = %{"metadata_replicas" => 5, "read_quorum" => 3, "write_quorum" => 3}
      assert {:ok, updated} = Handler.update_volume(vol_name, config)
      assert updated.metadata_consistency.replicas == 5
      assert updated.metadata_consistency.read_quorum == 3
      assert updated.metadata_consistency.write_quorum == 3
    end

    test "rejects update for non-existent volume" do
      assert {:error, %NeonFS.Error.VolumeNotFound{volume_name: "no-such-vol"}} =
               Handler.update_volume("no-such-vol", %{"io_weight" => 5})
    end

    test "rejects attempt to change immutable fields", %{vol_name: vol_name} do
      assert {:error, %NeonFS.Error.InvalidConfig{field: :immutable} = error} =
               Handler.update_volume(vol_name, %{"durability" => "replicate:1"})

      assert Exception.message(error) =~ "durability"

      assert {:error, %NeonFS.Error.InvalidConfig{field: :immutable}} =
               Handler.update_volume(vol_name, %{"encryption" => "on"})

      assert {:error, %NeonFS.Error.InvalidConfig{field: :immutable}} =
               Handler.update_volume(vol_name, %{"name" => "new-name"})

      assert {:error, %NeonFS.Error.InvalidConfig{field: :immutable}} =
               Handler.update_volume(vol_name, %{"id" => "new-id"})
    end

    test "emits telemetry event on success", %{vol_name: vol_name} do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :cli, :volume_updated]
        ])

      Handler.update_volume(vol_name, %{"io_weight" => 3})

      assert_received {[:neonfs, :cli, :volume_updated], ^ref, %{}, meta}
      assert meta.name == vol_name
      assert "io_weight" in meta.fields
    end
  end

  describe "handle_ca_info/0" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      ensure_cluster_state()
      stop_ra()

      master_key = :crypto.strong_rand_bytes(32) |> Base.encode64()
      write_cluster_json(tmp_dir, master_key)

      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_stripe_index()
      start_volume_registry()
      ensure_chunk_access_tracker()

      {:ok, _volume} = VolumeRegistry.create_system_volume()
      {:ok, _ca_cert, _ca_key} = CertificateAuthority.init_ca("handler-test")

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "returns CA info after cluster init" do
      assert {:ok, info} = Handler.handle_ca_info()
      assert info.subject =~ "handler-test CA"
      assert info.algorithm == "ECDSA P-256"
      assert is_binary(info.valid_from)
      assert is_binary(info.valid_to)
      assert info.current_serial == 0
      assert info.nodes_issued == 0
    end
  end

  describe "handle_ca_info/0 without CA" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      ensure_cluster_state()
      stop_ra()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "returns error before CA init" do
      assert {:error, %NeonFS.Error.Unavailable{}} = Handler.handle_ca_info()
    end
  end

  describe "handle_ca_list/0 and handle_ca_revoke/1" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      ensure_cluster_state()
      stop_ra()

      master_key = :crypto.strong_rand_bytes(32) |> Base.encode64()
      write_cluster_json(tmp_dir, master_key)

      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_stripe_index()
      start_volume_registry()
      ensure_chunk_access_tracker()

      {:ok, _volume} = VolumeRegistry.create_system_volume()
      {:ok, _ca_cert, _ca_key} = CertificateAuthority.init_ca("ca-list-test")

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "returns initial node after signing a cert" do
      key = TLS.generate_node_key()
      csr = TLS.create_csr(key, "list-node")
      {:ok, _cert, _ca} = CertificateAuthority.sign_node_csr(csr, "list-node.test")

      assert {:ok, certs} = Handler.handle_ca_list()
      assert length(certs) == 1

      [cert] = certs
      assert cert.node_name =~ "list-node"
      assert cert.hostname == "list-node.test"
      assert cert.serial == 1
      assert cert.status == "valid"
      assert is_binary(cert.expires)
    end

    test "returns multiple nodes" do
      for n <- 1..3 do
        key = TLS.generate_node_key()
        csr = TLS.create_csr(key, "node-#{n}")
        {:ok, _cert, _ca} = CertificateAuthority.sign_node_csr(csr, "node-#{n}.test")
      end

      assert {:ok, certs} = Handler.handle_ca_list()
      assert length(certs) == 3
    end

    test "revoke marks cert as revoked in list" do
      key = TLS.generate_node_key()
      csr = TLS.create_csr(key, "revoke-target")

      {:ok, _cert, _ca} =
        CertificateAuthority.sign_node_csr(csr, "revoke-target.test")

      assert {:ok, %{serial: 1, status: "revoked"}} = Handler.handle_ca_revoke("revoke-target")

      assert {:ok, [cert]} = Handler.handle_ca_list()
      assert cert.status == "revoked"
    end

    test "revoke returns error for unknown node" do
      assert {:error, %NeonFS.Error.NotFound{}} = Handler.handle_ca_revoke("nonexistent-node")
    end
  end

  describe "handle_node_status/0" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_drive_registry()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "returns ok tuple with expected keys" do
      assert {:ok, report} = Handler.handle_node_status()
      assert Map.has_key?(report, :node)
      assert Map.has_key?(report, :status)
      assert Map.has_key?(report, :checked_at)
      assert Map.has_key?(report, :checks)
    end

    test "status is a string" do
      assert {:ok, report} = Handler.handle_node_status()
      assert report.status in ["healthy", "degraded", "unhealthy"]
    end

    test "checked_at is an ISO8601 string" do
      assert {:ok, report} = Handler.handle_node_status()
      assert is_binary(report.checked_at)
      assert {:ok, _dt, _offset} = DateTime.from_iso8601(report.checked_at)
    end

    test "checks is a map with subsystem keys" do
      assert {:ok, report} = Handler.handle_node_status()
      assert is_map(report.checks)

      for {name, check} <- report.checks do
        assert is_binary(name), "subsystem name should be a string"
        assert is_map(check), "subsystem check should be a map"
        assert Map.has_key?(check, "status"), "subsystem check should have a status key"
      end
    end

    test "node is a string" do
      assert {:ok, report} = Handler.handle_node_status()
      assert is_binary(report.node)
      assert report.node == Atom.to_string(Node.self())
    end
  end

  describe "handle_worker_status/0" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      ensure_cluster_state()

      start_supervised!({Task.Supervisor, name: NeonFS.Core.BackgroundTaskSupervisor})
      start_supervised!(NeonFS.Core.BackgroundWorker)

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "returns list of per-node worker status maps" do
      assert {:ok, [status | _]} = Handler.handle_worker_status()
      assert is_binary(status.node)
      assert is_integer(status.max_concurrent)
      assert is_integer(status.max_per_minute)
      assert is_integer(status.drive_concurrency)
      assert is_integer(status.queued)
      assert is_integer(status.running)
      assert is_integer(status.completed_total)
      assert is_map(status.by_priority)
      assert is_integer(status.by_priority.high)
      assert is_integer(status.by_priority.normal)
      assert is_integer(status.by_priority.low)
    end

    test "returns serializable data" do
      assert {:ok, statuses} = Handler.handle_worker_status()
      assert is_list(statuses)

      for status <- statuses do
        assert is_map(status)
        assert is_binary(status.node)

        for {key, value} <- status, key != :node do
          assert is_integer(value) or is_map(value)
        end
      end
    end
  end

  describe "handle_worker_configure/1" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      ensure_cluster_state()

      # Write a minimal cluster.json so persistence works
      meta_dir = Application.get_env(:neonfs_core, :meta_dir)
      File.mkdir_p!(meta_dir)

      state = %{
        "cluster_id" => "clust_test",
        "cluster_name" => "test-cluster",
        "created_at" => DateTime.to_iso8601(DateTime.utc_now()),
        "drives" => [],
        "master_key" => Base.encode64(:crypto.strong_rand_bytes(32)),
        "this_node" => %{
          "id" => "node_test",
          "name" => Atom.to_string(Node.self()),
          "joined_at" => DateTime.to_iso8601(DateTime.utc_now())
        },
        "known_peers" => [],
        "ra_cluster_members" => [Atom.to_string(Node.self())],
        "node_type" => "core",
        "gc" => %{},
        "scrub" => %{},
        "worker" => %{}
      }

      json = :json.format(state) |> IO.iodata_to_binary()
      File.write!(State.state_file_path(), json)

      start_supervised!({Task.Supervisor, name: NeonFS.Core.BackgroundTaskSupervisor})
      start_supervised!(NeonFS.Core.BackgroundWorker)

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "updates worker settings" do
      config = %{"max_concurrent" => 10, "max_per_minute" => 200}
      assert {:ok, result} = Handler.handle_worker_configure(config)
      assert result.max_concurrent == 10
      assert result.max_per_minute == 200
    end

    test "persists changes to cluster.json" do
      config = %{"max_concurrent" => 12}
      assert {:ok, _} = Handler.handle_worker_configure(config)

      assert {:ok, state} = State.load()
      assert state.worker["max_concurrent"] == 12
    end

    test "merges with existing worker config" do
      # Set one field first
      config1 = %{"max_concurrent" => 8}
      assert {:ok, _} = Handler.handle_worker_configure(config1)

      # Set a different field
      config2 = %{"drive_concurrency" => 4}
      assert {:ok, _} = Handler.handle_worker_configure(config2)

      # Both should be persisted
      assert {:ok, state} = State.load()
      assert state.worker["max_concurrent"] == 8
      assert state.worker["drive_concurrency"] == 4
    end

    test "rejects negative values" do
      config = %{"max_concurrent" => -1}

      assert {:error, %NeonFS.Error.InvalidConfig{field: :max_concurrent} = error} =
               Handler.handle_worker_configure(config)

      assert Exception.message(error) =~ "positive integer"
    end

    test "rejects zero values" do
      config = %{"max_per_minute" => 0}

      assert {:error, %NeonFS.Error.InvalidConfig{field: :max_per_minute} = error} =
               Handler.handle_worker_configure(config)

      assert Exception.message(error) =~ "positive integer"
    end

    test "rejects non-integer values" do
      config = %{"max_concurrent" => "fast"}

      assert {:error, %NeonFS.Error.InvalidConfig{field: :max_concurrent} = error} =
               Handler.handle_worker_configure(config)

      assert Exception.message(error) =~ "positive integer"
    end

    test "rejects empty config" do
      assert {:error, %NeonFS.Error.InvalidConfig{reason: "no valid settings provided"}} =
               Handler.handle_worker_configure(%{})
    end

    test "returns config map with current values" do
      config = %{"max_concurrent" => 5, "max_per_minute" => 100, "drive_concurrency" => 3}
      assert {:ok, result} = Handler.handle_worker_configure(config)
      assert result.max_concurrent == 5
      assert result.max_per_minute == 100
      assert result.drive_concurrency == 3
    end
  end

  describe "handle_remove_node/2 safety checks" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_stripe_index()
      start_volume_registry()
      ensure_chunk_access_tracker()
      start_ra()

      ensure_cluster_state()

      on_exit(fn ->
        stop_ra()
        cleanup_test_dirs()
      end)

      :ok
    end

    test "refuses to remove the node running the command" do
      self_name = Atom.to_string(node())

      assert {:error, %NeonFS.Error.Unavailable{message: msg}} =
               Handler.handle_remove_node(self_name)

      assert msg =~ "Cannot remove the node running this command"
    end

    test "returns NotFound when the target is not in the cluster" do
      assert {:error, %NeonFS.Error.NotFound{message: msg}} =
               Handler.handle_remove_node("nonexistent-node-xyz")

      assert msg =~ "No node 'nonexistent-node-xyz'"
    end

    test "accepts the force option via the opts map" do
      # Still refused because the target doesn't exist, but the force path
      # is exercised — `opts["force"] = true` skips the drive-check gate
      # so the NotFound surfaces from node resolution rather than drives.
      assert {:error, %NeonFS.Error.NotFound{}} =
               Handler.handle_remove_node("still-nonexistent", %{"force" => true})
    end
  end
end
