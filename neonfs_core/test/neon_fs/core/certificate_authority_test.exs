defmodule NeonFS.Core.CertificateAuthorityTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{CertificateAuthority, SystemVolume, VolumeRegistry}
  alias NeonFS.Transport.TLS

  @moduletag :tmp_dir

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

    on_exit(fn -> cleanup_test_dirs() end)
    :ok
  end

  describe "init_ca/1" do
    test "writes CA cert, key, serial, and CRL to system volume" do
      assert {:ok, _ca_cert, _ca_key} = CertificateAuthority.init_ca("test-cluster")

      assert {:ok, ca_cert_pem} = SystemVolume.read("/tls/ca.crt")
      assert {:ok, ca_key_pem} = SystemVolume.read("/tls/ca.key")
      assert {:ok, "1"} = SystemVolume.read("/tls/serial")
      assert {:ok, crl_pem} = SystemVolume.read("/tls/crl.pem")

      assert is_binary(ca_cert_pem)
      assert is_binary(ca_key_pem)
      assert is_binary(crl_pem)

      # Verify PEMs are decodable
      assert %{subject: _} = TLS.certificate_info(ca_cert_pem)
      _key = TLS.decode_key!(ca_key_pem)
      _entries = TLS.parse_crl_entries(crl_pem)
    end

    test "returns valid self-signed CA cert matching cluster name" do
      {:ok, ca_cert, _ca_key} = CertificateAuthority.init_ca("my-cluster")

      info = TLS.certificate_info(ca_cert)
      assert info.subject =~ "my-cluster CA"
      assert info.subject =~ "NeonFS"
      # Self-signed: issuer matches subject
      assert info.issuer == info.subject
    end
  end

  describe "sign_node_csr/2" do
    setup do
      {:ok, _ca_cert, _ca_key} = CertificateAuthority.init_ca("test-cluster")
      :ok
    end

    test "returns a cert signed by the CA" do
      node_key = TLS.generate_node_key()
      csr = TLS.create_csr(node_key, "test-node")

      assert {:ok, node_cert, ca_cert} = CertificateAuthority.sign_node_csr(csr, "test-node")

      node_info = TLS.certificate_info(node_cert)
      ca_info = TLS.certificate_info(ca_cert)

      assert node_info.subject =~ "test-node"
      assert node_info.issuer == ca_info.subject
    end

    test "increments the serial counter on each call" do
      key1 = TLS.generate_node_key()
      csr1 = TLS.create_csr(key1, "node-1")
      {:ok, cert1, _ca} = CertificateAuthority.sign_node_csr(csr1, "node-1")

      key2 = TLS.generate_node_key()
      csr2 = TLS.create_csr(key2, "node-2")
      {:ok, cert2, _ca} = CertificateAuthority.sign_node_csr(csr2, "node-2")

      info1 = TLS.certificate_info(cert1)
      info2 = TLS.certificate_info(cert2)

      assert info1.serial == 1
      assert info2.serial == 2

      # Serial file should now be "3" (next to allocate)
      assert {:ok, "3"} = SystemVolume.read("/tls/serial")
    end
  end

  describe "next_serial/0" do
    setup do
      {:ok, _ca_cert, _ca_key} = CertificateAuthority.init_ca("test-cluster")
      :ok
    end

    test "returns sequential values starting from 1" do
      assert {:ok, 1} = CertificateAuthority.next_serial()
      assert {:ok, 2} = CertificateAuthority.next_serial()
      assert {:ok, 3} = CertificateAuthority.next_serial()
    end
  end

  describe "ca_info/0" do
    test "returns correct subject and validity dates" do
      {:ok, _ca_cert, _ca_key} = CertificateAuthority.init_ca("info-cluster")

      assert {:ok, info} = CertificateAuthority.ca_info()

      assert info.subject =~ "info-cluster CA"
      assert info.algorithm == "ECDSA P-256"
      assert %DateTime{} = info.valid_from
      assert %DateTime{} = info.valid_to
      # No certs issued yet
      assert info.current_serial == 0
      assert info.nodes_issued == 0
    end

    test "reflects issued certificates in serial count" do
      {:ok, _ca_cert, _ca_key} = CertificateAuthority.init_ca("serial-cluster")

      node_key = TLS.generate_node_key()
      csr = TLS.create_csr(node_key, "some-node")
      {:ok, _cert, _ca} = CertificateAuthority.sign_node_csr(csr, "some-node")

      assert {:ok, info} = CertificateAuthority.ca_info()
      assert info.current_serial == 1
      assert info.nodes_issued == 1
    end

    test "returns error when CA is not initialised" do
      assert {:error, _reason} = CertificateAuthority.ca_info()
    end
  end

  describe "revoke_certificate/2" do
    setup do
      {:ok, _ca_cert, _ca_key} = CertificateAuthority.init_ca("revoke-cluster")
      :ok
    end

    test "adds entry to CRL (verify via list_revoked)" do
      node_key = TLS.generate_node_key()
      csr = TLS.create_csr(node_key, "revoke-node")
      {:ok, node_cert, _ca} = CertificateAuthority.sign_node_csr(csr, "revoke-node")
      serial = TLS.certificate_info(node_cert).serial

      assert :ok = CertificateAuthority.revoke_certificate(node_cert)

      {:ok, revoked} = CertificateAuthority.list_revoked()
      assert length(revoked) == 1
      assert hd(revoked).serial == serial
      assert hd(revoked).reason == :unspecified
    end

    test "preserves existing entries (revoke two certs, verify both)" do
      key1 = TLS.generate_node_key()
      csr1 = TLS.create_csr(key1, "node-a")
      {:ok, cert1, _ca} = CertificateAuthority.sign_node_csr(csr1, "node-a")

      key2 = TLS.generate_node_key()
      csr2 = TLS.create_csr(key2, "node-b")
      {:ok, cert2, _ca} = CertificateAuthority.sign_node_csr(csr2, "node-b")

      assert :ok = CertificateAuthority.revoke_certificate(cert1, :key_compromise)
      assert :ok = CertificateAuthority.revoke_certificate(cert2, :cessation_of_operation)

      {:ok, revoked} = CertificateAuthority.list_revoked()
      assert length(revoked) == 2

      serials = Enum.map(revoked, & &1.serial) |> Enum.sort()
      serial1 = TLS.certificate_info(cert1).serial
      serial2 = TLS.certificate_info(cert2).serial
      assert serials == Enum.sort([serial1, serial2])
    end

    test "stores correct reason codes" do
      key = TLS.generate_node_key()
      csr = TLS.create_csr(key, "reason-node")
      {:ok, cert, _ca} = CertificateAuthority.sign_node_csr(csr, "reason-node")

      assert :ok = CertificateAuthority.revoke_certificate(cert, :superseded)

      {:ok, [entry]} = CertificateAuthority.list_revoked()
      assert entry.reason == :superseded
    end

    test "is idempotent — revoking same cert twice returns :ok" do
      key = TLS.generate_node_key()
      csr = TLS.create_csr(key, "idem-node")
      {:ok, cert, _ca} = CertificateAuthority.sign_node_csr(csr, "idem-node")

      assert :ok = CertificateAuthority.revoke_certificate(cert)
      assert :ok = CertificateAuthority.revoke_certificate(cert)

      {:ok, revoked} = CertificateAuthority.list_revoked()
      assert length(revoked) == 1
    end

    test "accepts serial number directly" do
      assert :ok = CertificateAuthority.revoke_certificate(42, :key_compromise)

      {:ok, revoked} = CertificateAuthority.list_revoked()
      assert length(revoked) == 1
      assert hd(revoked).serial == 42
      assert hd(revoked).reason == :key_compromise
    end
  end

  describe "is_revoked?/1" do
    setup do
      {:ok, _ca_cert, _ca_key} = CertificateAuthority.init_ca("revoked-cluster")
      :ok
    end

    test "returns true for revoked serial" do
      key = TLS.generate_node_key()
      csr = TLS.create_csr(key, "check-node")
      {:ok, cert, _ca} = CertificateAuthority.sign_node_csr(csr, "check-node")
      serial = TLS.certificate_info(cert).serial

      CertificateAuthority.revoke_certificate(cert)

      assert {:ok, true} = CertificateAuthority.is_revoked?(serial)
    end

    test "returns false for non-revoked serial" do
      assert {:ok, false} = CertificateAuthority.is_revoked?(999)
    end
  end

  describe "list_issued/0" do
    setup do
      {:ok, _ca_cert, _ca_key} = CertificateAuthority.init_ca("issued-cluster")
      :ok
    end

    test "returns empty list when no certs issued" do
      assert {:ok, []} = CertificateAuthority.list_issued()
    end

    test "tracks issued certificates" do
      key1 = TLS.generate_node_key()
      csr1 = TLS.create_csr(key1, "node-a")
      {:ok, _cert1, _ca} = CertificateAuthority.sign_node_csr(csr1, "node-a.example.com")

      key2 = TLS.generate_node_key()
      csr2 = TLS.create_csr(key2, "node-b")
      {:ok, _cert2, _ca} = CertificateAuthority.sign_node_csr(csr2, "node-b.example.com")

      {:ok, issued} = CertificateAuthority.list_issued()
      assert length(issued) == 2

      [first, second] = issued
      assert first.serial == 1
      assert first.node_name =~ "node-a"
      assert first.hostname == "node-a.example.com"
      assert first.revoked == false

      assert second.serial == 2
      assert second.node_name =~ "node-b"
      assert second.hostname == "node-b.example.com"
      assert second.revoked == false
    end

    test "marks revoked certificates" do
      key = TLS.generate_node_key()
      csr = TLS.create_csr(key, "revoke-me")
      {:ok, cert, _ca} = CertificateAuthority.sign_node_csr(csr, "revoke-me.example.com")
      serial = TLS.certificate_info(cert).serial

      CertificateAuthority.revoke_certificate(serial)

      {:ok, issued} = CertificateAuthority.list_issued()
      assert length(issued) == 1
      assert hd(issued).revoked == true
    end
  end

  describe "get_crl/0" do
    test "returns valid CRL PEM" do
      {:ok, _ca_cert, _ca_key} = CertificateAuthority.init_ca("crl-cluster")

      assert {:ok, crl_pem} = CertificateAuthority.get_crl()
      assert crl_pem =~ "BEGIN X509 CRL"
    end

    test "returns error when CA is not initialised" do
      assert {:error, _reason} = CertificateAuthority.get_crl()
    end
  end
end
