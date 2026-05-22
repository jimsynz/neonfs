defmodule NeonFS.Cluster.InitTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  import Bitwise

  alias NeonFS.Cluster.Init
  alias NeonFS.Cluster.State
  alias NeonFS.Core.{SystemVolume, VolumeRegistry}
  alias NeonFS.Transport.TLS

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
    # Consolidated into a single test because Ra's :default system is a
    # singleton — restarting it between tests is fragile.
    test "creates system volume, CA materials, identity file, and node cert" do
      cluster_name = "test-cluster"
      {:ok, _cluster_id} = Init.init_cluster(cluster_name)

      # --- System volume properties ---
      assert {:ok, volume} = VolumeRegistry.get_system_volume()
      assert volume.name == "_system"
      assert volume.system == true
      assert volume.owner == :system
      assert volume.durability.type == :replicate
      assert volume.durability.factor == 1
      assert volume.write_ack == :quorum
      assert volume.compression.algorithm == :zstd
      assert volume.encryption.mode == :none

      # --- Identity file ---
      assert {:ok, json} = SystemVolume.read("/cluster/identity.json")
      identity = :json.decode(json)

      assert identity["cluster_name"] == cluster_name
      assert identity["format_version"] == 1
      assert is_binary(identity["initialized_at"])

      assert {:ok, _datetime, _offset} =
               DateTime.from_iso8601(identity["initialized_at"])

      # --- CA materials in system volume ---
      assert {:ok, ca_cert_pem} = SystemVolume.read("/tls/ca.crt")
      assert {:ok, ca_key_pem} = SystemVolume.read("/tls/ca.key")
      assert {:ok, crl_pem} = SystemVolume.read("/tls/crl.pem")

      ca_info = TLS.certificate_info(ca_cert_pem)
      assert ca_info.subject =~ "#{cluster_name} CA"
      assert ca_info.subject =~ "NeonFS"

      _key = TLS.decode_key!(ca_key_pem)
      _entries = TLS.parse_crl_entries(crl_pem)

      # Serial incremented once (first node cert issued)
      assert {:ok, "2"} = SystemVolume.read("/tls/serial")

      # --- Node certificate stored locally ---
      assert {:ok, node_cert} = TLS.read_local_cert()
      assert {:ok, local_ca_cert} = TLS.read_local_ca_cert()

      node_info = TLS.certificate_info(node_cert)
      local_ca_info = TLS.certificate_info(local_ca_cert)

      assert node_info.issuer == local_ca_info.subject
      assert node_info.subject =~ Atom.to_string(Node.self())

      key_path = Path.join(TLS.tls_dir(), "node.key")
      assert File.exists?(key_path)
      {:ok, stat} = File.stat(key_path)
      assert (stat.mode &&& 0o777) == 0o600

      # --- Idempotency: second init returns already_initialised ---
      assert {:error, :already_initialised} = Init.init_cluster(cluster_name)

      # System volume unchanged after failed re-init
      assert {:ok, ^volume} = VolumeRegistry.get_system_volume()
    end
  end

  describe "drive preflight (#1012)" do
    test "refuses init when the supplied drive path does not exist" do
      drive_config = %{"path" => "/nonexistent/neonfs-preflight-test", "tier" => "hot"}

      assert {:error, {:drive_preflight_failed, reason}} =
               Init.init_cluster("preflight-missing", drive_config)

      assert is_binary(reason)
      assert reason =~ "does not exist"

      # Critical: cluster state must NOT exist after a refused init
      # (#1012 — preflight runs before any mutation).
      refute State.exists?()
    end

    test "refuses init when the supplied drive path is not writable", %{tmp_dir: tmp_dir} do
      readonly_path = Path.join(tmp_dir, "readonly-drive")
      File.mkdir_p!(readonly_path)
      File.chmod!(readonly_path, 0o555)

      on_exit(fn ->
        _ = File.chmod(readonly_path, 0o755)
      end)

      drive_config = %{"path" => readonly_path, "tier" => "hot"}

      assert {:error, {:drive_preflight_failed, reason}} =
               Init.init_cluster("preflight-readonly", drive_config)

      assert is_binary(reason)
      assert reason =~ "not writable"
      refute State.exists?()
    end
  end
end
