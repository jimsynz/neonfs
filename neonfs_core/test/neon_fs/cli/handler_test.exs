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

      # --- Idempotency: second init returns already_initialised ---
      assert {:error, :already_initialised} = Handler.cluster_init("second-cluster")
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
      assert {:error, :cluster_not_initialized} = Handler.create_invite(3600)
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

  describe "create_volume/2 with durability strings" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
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
      assert {:error, msg} = Handler.create_volume(vol_name, config)
      assert msg =~ "Invalid durability format"
    end

    test "rejects erasure:4:0 (parity_chunks < 1)" do
      vol_name = "bad2-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "erasure:4:0"}
      assert {:error, msg} = Handler.create_volume(vol_name, config)
      assert msg =~ "Invalid durability format"
    end

    test "rejects erasure:abc (malformed)" do
      vol_name = "bad3-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "erasure:abc"}
      assert {:error, msg} = Handler.create_volume(vol_name, config)
      assert msg =~ "Invalid durability format"
    end

    test "rejects replicate:0 (factor < 1)" do
      vol_name = "bad4-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "replicate:0"}
      assert {:error, msg} = Handler.create_volume(vol_name, config)
      assert msg =~ "Invalid durability format"
    end

    test "rejects replicate:abc (non-integer)" do
      vol_name = "bad5-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "replicate:abc"}
      assert {:error, msg} = Handler.create_volume(vol_name, config)
      assert msg =~ "Invalid durability format"
    end

    test "rejects unknown durability format" do
      vol_name = "bad6-vol-#{:rand.uniform(999_999)}"
      config = %{"durability" => "mirror:3"}
      assert {:error, msg} = Handler.create_volume(vol_name, config)
      assert msg =~ "Invalid durability format"
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

  describe "handle_ca_info/0" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
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
      stop_ra()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "returns error before CA init" do
      assert {:error, :ca_not_initialized} = Handler.handle_ca_info()
    end
  end

  describe "handle_ca_list/0 and handle_ca_revoke/1" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
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
      assert {:error, :node_not_found} = Handler.handle_ca_revoke("nonexistent-node")
    end
  end
end
