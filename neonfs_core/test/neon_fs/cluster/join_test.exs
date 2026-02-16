defmodule NeonFS.Cluster.JoinTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Cluster.{Init, Invite, Join}
  alias NeonFS.Core.VolumeRegistry
  alias NeonFS.Transport.TLS

  @moduletag :tmp_dir

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

    # Initialise a cluster so we have state, a valid invite, and a system volume
    {:ok, _cluster_id} = Init.init_cluster("join-test-cluster")

    # Start ServiceRegistry after init_cluster so Ra is fully ready
    start_service_registry()

    on_exit(fn ->
      stop_ra()
      cleanup_test_dirs()
    end)

    :ok
  end

  # Consolidated into a single test because Ra's :default system is a
  # singleton — restarting it between tests is fragile.
  describe "accept_join/4 certificate issuance and replication" do
    test "signs CSR, increments serial, returns certs, and adjusts replication" do
      # --- Certificate issuance for non-core join ---
      {:ok, token1} = Invite.create_invite(3600)
      fuse_key = TLS.generate_node_key()
      fuse_csr = TLS.create_csr(fuse_key, "fuse_peer@localhost")

      {:ok, cluster_info} = Join.accept_join(token1, :fuse_peer@localhost, :fuse, fuse_csr)

      # Response includes signed cert and CA cert
      assert is_binary(cluster_info.node_cert_pem)
      assert is_binary(cluster_info.ca_cert_pem)

      node_info = TLS.certificate_info(cluster_info.node_cert_pem)
      ca_info = TLS.certificate_info(cluster_info.ca_cert_pem)

      # Cert subject matches joining node, issuer matches CA
      assert node_info.subject =~ "fuse_peer@localhost"
      assert node_info.issuer == ca_info.subject

      # First node cert was serial 1 (from init), this should be serial 2
      assert node_info.serial == 2

      # --- Serial increments on subsequent join ---
      {:ok, token2} = Invite.create_invite(3600)
      key2 = TLS.generate_node_key()
      csr2 = TLS.create_csr(key2, "another_fuse@localhost")

      {:ok, cluster_info2} = Join.accept_join(token2, :another_fuse@localhost, :fuse, csr2)

      node_info2 = TLS.certificate_info(cluster_info2.node_cert_pem)
      assert node_info2.serial == 3

      # --- Backwards compatibility: accept_join without CSR still works ---
      {:ok, token3} = Invite.create_invite(3600)
      {:ok, cluster_info3} = Join.accept_join(token3, :legacy_peer@localhost, :fuse)

      assert is_nil(cluster_info3.node_cert_pem)
      assert is_nil(cluster_info3.ca_cert_pem)

      # --- System volume replication (existing behaviour preserved) ---
      {:ok, volume} = VolumeRegistry.get_system_volume()
      # Non-core joins don't change replication factor
      assert volume.durability.factor == 1

      # Verify sequential core join replication adjustments (1 → 2 → 3).
      # We call VolumeRegistry directly rather than going through accept_join
      # because adding a fake core node via accept_join changes Ra quorum,
      # preventing subsequent Ra operations from succeeding.
      {:ok, vol2} = VolumeRegistry.adjust_system_volume_replication(2)
      assert vol2.durability.factor == 2

      {:ok, vol3} = VolumeRegistry.adjust_system_volume_replication(3)
      assert vol3.durability.factor == 3

      # Reset for test isolation
      VolumeRegistry.adjust_system_volume_replication(1)
    end
  end

  describe "accept_join/4 CSR validation" do
    test "rejects invalid CSR" do
      {:ok, token} = Invite.create_invite(3600)

      # A non-nil, non-CSR value should be rejected
      result = Join.accept_join(token, :bad_csr_peer@localhost, :fuse, :not_a_csr)

      assert {:error, {:cert_signing_failed, :invalid_csr}} = result
    end
  end
end
