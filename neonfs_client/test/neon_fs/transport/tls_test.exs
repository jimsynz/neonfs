defmodule NeonFS.Transport.TLSTest do
  use ExUnit.Case, async: true
  import Bitwise

  alias NeonFS.Transport.TLS
  alias X509.CRL.Entry, as: CRLEntry

  describe "generate_ca/1" do
    test "returns a valid self-signed certificate" do
      {ca_cert, _ca_key} = TLS.generate_ca("test-cluster")

      info = TLS.certificate_info(ca_cert)
      assert info.subject == "/O=NeonFS/CN=test-cluster CA"
      assert info.issuer == info.subject
      assert info.serial > 0
    end

    test "CA cert has correct validity period" do
      {ca_cert, _ca_key} = TLS.generate_ca("test-cluster")

      days = TLS.days_until_expiry(ca_cert)
      assert days >= 3649
      assert days <= 3651
    end
  end

  describe "generate_node_key/0" do
    test "returns an ECDSA P-256 key" do
      key = TLS.generate_node_key()
      pem = TLS.encode_key(key)
      # Wrapped in PKCS#8 (`PRIVATE KEY`) since #521 — see
      # `TLS.encode_key/1` for the rationale.
      assert pem =~ "BEGIN PRIVATE KEY"
      refute pem =~ "BEGIN EC PRIVATE KEY"
    end
  end

  describe "create_csr/2 and validate_csr/1" do
    test "creates a valid CSR" do
      key = TLS.generate_node_key()
      csr = TLS.create_csr(key, "node-1")

      assert TLS.validate_csr(csr)
    end

    test "CSR has correct subject" do
      key = TLS.generate_node_key()
      csr = TLS.create_csr(key, "node-1")

      subject = csr |> X509.CSR.subject() |> X509.RDNSequence.to_string()
      assert subject == "/O=NeonFS/CN=node-1"
    end

    test "validate_csr returns false for tampered CSR" do
      key1 = TLS.generate_node_key()
      csr = TLS.create_csr(key1, "node-1")
      csr_pem = TLS.encode_csr(csr)

      # Corrupt the signature bytes in the PEM
      # Base64-encoded data starts after the header line
      [header, body, footer] = String.split(csr_pem, "\n", parts: 3)
      corrupted_body = String.replace(body, String.first(body), "Z", global: false)
      corrupted_pem = Enum.join([header, corrupted_body, footer], "\n")

      try do
        decoded = TLS.decode_csr!(corrupted_pem)
        refute TLS.validate_csr(decoded)
      rescue
        _ -> :ok
      end
    end
  end

  describe "sign_csr/5" do
    setup do
      {ca_cert, ca_key} = TLS.generate_ca("test-cluster")
      node_key = TLS.generate_node_key()
      csr = TLS.create_csr(node_key, "node-1")
      node_cert = TLS.sign_csr(csr, "node-1.example.com", ca_cert, ca_key, 2)

      %{ca_cert: ca_cert, ca_key: ca_key, node_cert: node_cert, node_key: node_key}
    end

    test "produces a cert signed by the CA", %{ca_cert: ca_cert, node_cert: node_cert} do
      info = TLS.certificate_info(node_cert)
      ca_info = TLS.certificate_info(ca_cert)

      assert info.issuer == ca_info.subject
    end

    test "cert has correct subject", %{node_cert: node_cert} do
      info = TLS.certificate_info(node_cert)
      assert info.subject == "/O=NeonFS/CN=node-1"
    end

    test "cert has correct serial", %{node_cert: node_cert} do
      info = TLS.certificate_info(node_cert)
      assert info.serial == 2
    end

    test "cert has correct SAN", %{node_cert: node_cert} do
      san_ext = X509.Certificate.extension(node_cert, :subject_alt_name)
      assert san_ext != nil
      {:Extension, _oid, _critical, value} = san_ext
      assert {:dNSName, ~c"node-1.example.com"} in value
    end

    test "cert has correct ext_key_usage", %{node_cert: node_cert} do
      ext = X509.Certificate.extension(node_cert, :ext_key_usage)
      assert ext != nil
      {:Extension, _oid, _critical, usages} = ext
      assert {1, 3, 6, 1, 5, 5, 7, 3, 1} in usages
      assert {1, 3, 6, 1, 5, 5, 7, 3, 2} in usages
    end

    test "cert has correct validity period", %{node_cert: node_cert} do
      days = TLS.days_until_expiry(node_cert)
      assert days >= 364
      assert days <= 366
    end

    test "cert validates against CA chain", %{ca_cert: ca_cert, node_cert: node_cert} do
      ca_der = X509.Certificate.to_der(ca_cert)
      node_der = X509.Certificate.to_der(node_cert)

      {:ok, {_public_key_info, _policy_tree}} =
        :public_key.pkix_path_validation(ca_der, [node_der], [])
    end
  end

  describe "CRL operations" do
    setup do
      {ca_cert, ca_key} = TLS.generate_ca("test-cluster")
      %{ca_cert: ca_cert, ca_key: ca_key}
    end

    test "create_empty_crl returns valid PEM", %{ca_cert: ca_cert, ca_key: ca_key} do
      crl_pem = TLS.create_empty_crl(ca_cert, ca_key)
      assert crl_pem =~ "BEGIN X509 CRL"
    end

    test "empty CRL has no entries", %{ca_cert: ca_cert, ca_key: ca_key} do
      crl_pem = TLS.create_empty_crl(ca_cert, ca_key)
      entries = TLS.parse_crl_entries(crl_pem)
      assert entries == []
    end

    test "add_crl_entry adds entry and parse_crl_entries returns it", ctx do
      %{ca_cert: ca_cert, ca_key: ca_key} = ctx

      node_key = TLS.generate_node_key()
      csr = TLS.create_csr(node_key, "revoked-node")
      node_cert = TLS.sign_csr(csr, "revoked-node.example.com", ca_cert, ca_key, 5)

      updated_crl_pem = TLS.add_crl_entry(node_cert, [], ca_cert, ca_key)
      entries = TLS.parse_crl_entries(updated_crl_pem)

      assert length(entries) == 1

      [entry] = entries
      assert CRLEntry.serial(entry) == TLS.certificate_info(node_cert).serial
    end
  end

  describe "PEM encode/decode roundtrip" do
    test "certificate roundtrip" do
      {ca_cert, _ca_key} = TLS.generate_ca("test-cluster")

      pem = TLS.encode_cert(ca_cert)
      decoded = TLS.decode_cert!(pem)

      assert TLS.certificate_info(decoded) == TLS.certificate_info(ca_cert)
    end

    test "key roundtrip" do
      key = TLS.generate_node_key()

      pem = TLS.encode_key(key)
      # Wrapped in PKCS#8 (`PRIVATE KEY`) since #521 — see
      # `TLS.encode_key/1` for the rationale.
      assert pem =~ "BEGIN PRIVATE KEY"
      refute pem =~ "BEGIN EC PRIVATE KEY"

      decoded = TLS.decode_key!(pem)
      assert TLS.encode_key(decoded) == pem
    end

    test "CSR roundtrip" do
      key = TLS.generate_node_key()
      csr = TLS.create_csr(key, "roundtrip-node")

      pem = TLS.encode_csr(csr)
      decoded = TLS.decode_csr!(pem)

      assert TLS.validate_csr(decoded)

      subject = decoded |> X509.CSR.subject() |> X509.RDNSequence.to_string()
      assert subject == "/O=NeonFS/CN=roundtrip-node"
    end
  end

  describe "certificate_info/1" do
    test "extracts correct fields from decoded cert" do
      {ca_cert, _ca_key} = TLS.generate_ca("info-cluster")

      info = TLS.certificate_info(ca_cert)
      assert info.subject == "/O=NeonFS/CN=info-cluster CA"
      assert info.issuer == "/O=NeonFS/CN=info-cluster CA"
      assert is_integer(info.serial)
      assert %DateTime{} = info.not_before
      assert %DateTime{} = info.not_after
    end

    test "works with PEM input" do
      {ca_cert, _ca_key} = TLS.generate_ca("pem-cluster")
      pem = TLS.encode_cert(ca_cert)

      info = TLS.certificate_info(pem)
      assert info.subject == "/O=NeonFS/CN=pem-cluster CA"
    end
  end

  describe "days_until_expiry/1" do
    test "returns correct value for CA cert" do
      {ca_cert, _ca_key} = TLS.generate_ca("expiry-cluster")

      days = TLS.days_until_expiry(ca_cert)
      assert days >= 3649
      assert days <= 3651
    end

    test "returns correct value for node cert" do
      {ca_cert, ca_key} = TLS.generate_ca("expiry-cluster")
      node_key = TLS.generate_node_key()
      csr = TLS.create_csr(node_key, "expiry-node")
      node_cert = TLS.sign_csr(csr, "expiry-node.example.com", ca_cert, ca_key, 10)

      days = TLS.days_until_expiry(node_cert)
      assert days >= 364
      assert days <= 366
    end

    test "works with PEM input" do
      {ca_cert, _ca_key} = TLS.generate_ca("pem-expiry")
      pem = TLS.encode_cert(ca_cert)

      days = TLS.days_until_expiry(pem)
      assert days >= 3649
    end
  end

  describe "write_local_tls/3 and read_local_cert/0 and read_local_ca_cert/0" do
    @tag :tmp_dir
    test "roundtrip write and read", %{tmp_dir: tmp_dir} do
      Application.put_env(:neonfs_client, :tls_dir, tmp_dir)

      on_exit(fn ->
        Application.delete_env(:neonfs_client, :tls_dir)
      end)

      {ca_cert, ca_key} = TLS.generate_ca("local-cluster")
      node_key = TLS.generate_node_key()
      csr = TLS.create_csr(node_key, "local-node")
      node_cert = TLS.sign_csr(csr, "local-node.example.com", ca_cert, ca_key, 3)

      assert :ok = TLS.write_local_tls(ca_cert, node_cert, node_key)

      {:ok, read_cert} = TLS.read_local_cert()
      assert TLS.certificate_info(read_cert) == TLS.certificate_info(node_cert)

      {:ok, read_ca} = TLS.read_local_ca_cert()
      assert TLS.certificate_info(read_ca) == TLS.certificate_info(ca_cert)
    end

    @tag :tmp_dir
    test "file permissions are correct", %{tmp_dir: tmp_dir} do
      Application.put_env(:neonfs_client, :tls_dir, tmp_dir)

      on_exit(fn ->
        Application.delete_env(:neonfs_client, :tls_dir)
      end)

      {ca_cert, ca_key} = TLS.generate_ca("perms-cluster")
      node_key = TLS.generate_node_key()
      csr = TLS.create_csr(node_key, "perms-node")
      node_cert = TLS.sign_csr(csr, "perms-node.example.com", ca_cert, ca_key, 4)

      TLS.write_local_tls(ca_cert, node_cert, node_key)

      {:ok, ca_stat} = File.stat(Path.join(tmp_dir, "ca.crt"))
      {:ok, cert_stat} = File.stat(Path.join(tmp_dir, "node.crt"))
      {:ok, key_stat} = File.stat(Path.join(tmp_dir, "node.key"))

      assert (ca_stat.mode &&& 0o777) == 0o644
      assert (cert_stat.mode &&& 0o777) == 0o644
      assert (key_stat.mode &&& 0o777) == 0o600
    end

    @tag :tmp_dir
    test "read_local_cert returns error when file missing", %{tmp_dir: tmp_dir} do
      missing_dir = Path.join(tmp_dir, "missing")

      Application.put_env(
        :neonfs_client,
        :tls_dir,
        missing_dir
      )

      on_exit(fn ->
        Application.delete_env(:neonfs_client, :tls_dir)
      end)

      assert {:error, :not_found} = TLS.read_local_cert()
    end

    @tag :tmp_dir
    test "read_local_ca_cert returns error when file missing", %{tmp_dir: tmp_dir} do
      missing_dir = Path.join(tmp_dir, "missing")

      Application.put_env(
        :neonfs_client,
        :tls_dir,
        missing_dir
      )

      on_exit(fn ->
        Application.delete_env(:neonfs_client, :tls_dir)
      end)

      assert {:error, :not_found} = TLS.read_local_ca_cert()
    end
  end

  describe "validity period configuration" do
    test "ca_validity_days is configurable" do
      Application.put_env(:neonfs_client, :ca_validity_days, 100)

      on_exit(fn ->
        Application.delete_env(:neonfs_client, :ca_validity_days)
      end)

      {ca_cert, _ca_key} = TLS.generate_ca("short-ca")
      days = TLS.days_until_expiry(ca_cert)

      assert days >= 99
      assert days <= 101
    end

    test "node_validity_days is configurable" do
      Application.put_env(:neonfs_client, :node_validity_days, 30)

      on_exit(fn ->
        Application.delete_env(:neonfs_client, :node_validity_days)
      end)

      {ca_cert, ca_key} = TLS.generate_ca("short-node")
      node_key = TLS.generate_node_key()
      csr = TLS.create_csr(node_key, "short-node")
      node_cert = TLS.sign_csr(csr, "short-node.example.com", ca_cert, ca_key, 20)

      days = TLS.days_until_expiry(node_cert)
      assert days >= 29
      assert days <= 31
    end
  end
end
