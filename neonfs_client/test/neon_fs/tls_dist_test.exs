defmodule NeonFS.TLSDistTest do
  use ExUnit.Case, async: true

  describe "verify_peer/3" do
    test "accepts valid peer certificate" do
      assert {:valid, :state} = NeonFS.TLSDist.verify_peer(:cert, :valid_peer, :state)
    end

    test "accepts valid intermediate certificate" do
      assert {:valid, :state} = NeonFS.TLSDist.verify_peer(:cert, :valid, :state)
    end

    test "passes unknown extensions through" do
      extension = {:Extension, {2, 5, 29, 17}, false, "value"}

      assert {:unknown, :state} =
               NeonFS.TLSDist.verify_peer(:cert, {:extension, extension}, :state)
    end

    test "rejects bad certificates" do
      assert {:fail, :cert_expired} =
               NeonFS.TLSDist.verify_peer(:cert, {:bad_cert, :cert_expired}, :state)
    end

    test "rejects unknown CA" do
      assert {:fail, :unknown_ca} =
               NeonFS.TLSDist.verify_peer(:cert, {:bad_cert, :unknown_ca}, :state)
    end

    test "rejects self-signed peer" do
      assert {:fail, :selfsigned_peer} =
               NeonFS.TLSDist.verify_peer(:cert, {:bad_cert, :selfsigned_peer}, :state)
    end
  end
end

defmodule NeonFS.TLSDistConfigTest do
  use ExUnit.Case, async: true

  alias NeonFS.TLSDistConfig
  alias X509.Certificate.Extension

  setup do
    tmp_dir =
      Path.join(System.tmp_dir!(), "neonfs_tls_dist_test_#{:erlang.unique_integer([:positive])}")

    File.mkdir_p!(tmp_dir)

    # Generate a local CA and certs using the x509 library
    local_ca_key = X509.PrivateKey.new_ec(:secp256r1)

    local_ca_cert =
      X509.Certificate.self_signed(local_ca_key, "/O=NeonFS/CN=local CA",
        template: :root_ca,
        validity: 365
      )

    node_local_key = X509.PrivateKey.new_ec(:secp256r1)

    node_local_cert =
      node_local_key
      |> X509.PublicKey.derive()
      |> X509.Certificate.new("/O=NeonFS/CN=node-local", local_ca_cert, local_ca_key,
        template: :server,
        validity: 365,
        extensions: [
          subject_alt_name: Extension.subject_alt_name(["localhost"])
        ]
      )

    File.write!(Path.join(tmp_dir, "local-ca.crt"), X509.Certificate.to_pem(local_ca_cert))
    File.write!(Path.join(tmp_dir, "local-ca.key"), X509.PrivateKey.to_pem(local_ca_key))
    File.write!(Path.join(tmp_dir, "node-local.crt"), X509.Certificate.to_pem(node_local_cert))
    File.write!(Path.join(tmp_dir, "node-local.key"), X509.PrivateKey.to_pem(node_local_key))

    on_exit(fn -> File.rm_rf!(tmp_dir) end)

    %{tmp_dir: tmp_dir, local_ca_cert: local_ca_cert, local_ca_key: local_ca_key}
  end

  describe "regenerate_ca_bundle/1" do
    test "creates bundle with local CA only when no cluster CA", %{tmp_dir: tmp_dir} do
      :ok = TLSDistConfig.regenerate_ca_bundle(tmp_dir)

      bundle = File.read!(Path.join(tmp_dir, "ca_bundle.crt"))
      local_ca = File.read!(Path.join(tmp_dir, "local-ca.crt"))

      assert bundle == local_ca
    end

    test "creates bundle with both CAs when cluster CA present", %{tmp_dir: tmp_dir} do
      cluster_ca_key = X509.PrivateKey.new_ec(:secp256r1)

      cluster_ca_cert =
        X509.Certificate.self_signed(cluster_ca_key, "/O=NeonFS/CN=test-cluster CA",
          template: :root_ca,
          validity: 365
        )

      # Write cluster CA (simulating the file written during join by TLS.write_local_tls)
      cluster_ca_pem = X509.Certificate.to_pem(cluster_ca_cert)
      File.write!(Path.join(tmp_dir, "ca.crt"), cluster_ca_pem)

      # Also write a cluster node cert (signed by cluster CA)
      node_key = X509.PrivateKey.new_ec(:secp256r1)

      node_cert =
        node_key
        |> X509.PublicKey.derive()
        |> X509.Certificate.new("/O=NeonFS/CN=neonfs_core@node1", cluster_ca_cert, cluster_ca_key,
          template: :server,
          validity: 365
        )

      File.write!(Path.join(tmp_dir, "node.crt"), X509.Certificate.to_pem(node_cert))
      File.write!(Path.join(tmp_dir, "node.key"), X509.PrivateKey.to_pem(node_key))

      :ok = TLSDistConfig.regenerate_ca_bundle(tmp_dir)

      bundle = File.read!(Path.join(tmp_dir, "ca_bundle.crt"))
      local_ca = File.read!(Path.join(tmp_dir, "local-ca.crt"))

      assert String.contains?(bundle, local_ca)
      assert String.contains?(bundle, cluster_ca_pem)
      assert byte_size(bundle) > byte_size(local_ca)
    end

    test "creates a three-cert bundle when an incoming CA is staged", %{tmp_dir: tmp_dir} do
      cluster_ca_key = X509.PrivateKey.new_ec(:secp256r1)

      cluster_ca_cert =
        X509.Certificate.self_signed(cluster_ca_key, "/O=NeonFS/CN=test-cluster CA",
          template: :root_ca,
          validity: 365
        )

      cluster_ca_pem = X509.Certificate.to_pem(cluster_ca_cert)
      File.write!(Path.join(tmp_dir, "ca.crt"), cluster_ca_pem)

      incoming_ca_key = X509.PrivateKey.new_ec(:secp256r1)

      incoming_ca_cert =
        X509.Certificate.self_signed(incoming_ca_key, "/O=NeonFS/CN=test-cluster CA (rotated)",
          template: :root_ca,
          validity: 365
        )

      incoming_ca_pem = X509.Certificate.to_pem(incoming_ca_cert)
      File.write!(Path.join(tmp_dir, "incoming-ca.crt"), incoming_ca_pem)

      :ok = TLSDistConfig.regenerate_ca_bundle(tmp_dir)

      bundle = File.read!(Path.join(tmp_dir, "ca_bundle.crt"))
      local_ca = File.read!(Path.join(tmp_dir, "local-ca.crt"))

      assert String.contains?(bundle, local_ca)
      assert String.contains?(bundle, incoming_ca_pem)
      assert String.contains?(bundle, cluster_ca_pem)

      # Order: local-ca, incoming-ca, ca — bundle order doesn't affect
      # X.509 verification, but it documents the rotation's "preferred"
      # CA hierarchy.
      local_pos = :binary.match(bundle, local_ca) |> elem(0)
      incoming_pos = :binary.match(bundle, incoming_ca_pem) |> elem(0)
      cluster_pos = :binary.match(bundle, cluster_ca_pem) |> elem(0)

      assert local_pos < incoming_pos
      assert incoming_pos < cluster_pos
    end

    test "round-trip: each cert in the bundle is decodable", %{tmp_dir: tmp_dir} do
      cluster_ca_key = X509.PrivateKey.new_ec(:secp256r1)

      cluster_ca_cert =
        X509.Certificate.self_signed(cluster_ca_key, "/O=NeonFS/CN=test-cluster CA",
          template: :root_ca,
          validity: 365
        )

      File.write!(
        Path.join(tmp_dir, "ca.crt"),
        X509.Certificate.to_pem(cluster_ca_cert)
      )

      incoming_ca_key = X509.PrivateKey.new_ec(:secp256r1)

      incoming_ca_cert =
        X509.Certificate.self_signed(incoming_ca_key, "/O=NeonFS/CN=test-cluster CA (rotated)",
          template: :root_ca,
          validity: 365
        )

      File.write!(
        Path.join(tmp_dir, "incoming-ca.crt"),
        X509.Certificate.to_pem(incoming_ca_cert)
      )

      :ok = TLSDistConfig.regenerate_ca_bundle(tmp_dir)

      bundle = File.read!(Path.join(tmp_dir, "ca_bundle.crt"))

      decoded =
        bundle
        |> :public_key.pem_decode()
        |> Enum.filter(fn {type, _, _} -> type == :Certificate end)
        |> Enum.map(fn {_type, der, _} ->
          X509.Certificate.from_der!(der)
        end)

      assert length(decoded) == 3

      subjects =
        decoded
        |> Enum.map(fn cert ->
          cert |> X509.Certificate.subject() |> X509.RDNSequence.to_string()
        end)

      assert Enum.any?(subjects, &(&1 =~ "local CA"))
      assert Enum.any?(subjects, &(&1 =~ "test-cluster CA (rotated)"))
      assert Enum.any?(subjects, &(&1 =~ "test-cluster CA"))
    end
  end

  describe "reload_listener/1" do
    test "regenerates the bundle and emits telemetry", %{tmp_dir: tmp_dir} do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :tls, :bundle_reloaded]
        ])

      incoming_ca_key = X509.PrivateKey.new_ec(:secp256r1)

      incoming_ca_cert =
        X509.Certificate.self_signed(incoming_ca_key, "/O=NeonFS/CN=incoming CA",
          template: :root_ca,
          validity: 365
        )

      File.write!(
        Path.join(tmp_dir, "incoming-ca.crt"),
        X509.Certificate.to_pem(incoming_ca_cert)
      )

      :ok = TLSDistConfig.reload_listener(tmp_dir)

      bundle = File.read!(Path.join(tmp_dir, "ca_bundle.crt"))
      assert String.contains?(bundle, X509.Certificate.to_pem(incoming_ca_cert))

      assert_receive {[:neonfs, :tls, :bundle_reloaded], ^ref, %{}, %{tls_dir: ^tmp_dir}}, 1_000
    end
  end

  describe "regenerate_config/1" do
    test "generates config with local cert only pre-cluster", %{tmp_dir: tmp_dir} do
      :ok = TLSDistConfig.regenerate_config(tmp_dir)

      conf = File.read!(Path.join(tmp_dir, "ssl_dist.conf"))

      assert String.contains?(conf, "node-local.crt")
      assert String.contains?(conf, "node-local.key")
      refute String.contains?(conf, "node.crt\"")
      assert String.contains?(conf, "verify_peer")
      assert String.contains?(conf, "tlsv1.3")
    end

    test "generates config with cluster cert first post-cluster", %{tmp_dir: tmp_dir} do
      File.write!(Path.join(tmp_dir, "node.crt"), "cluster-cert-pem")
      File.write!(Path.join(tmp_dir, "node.key"), "cluster-key-pem")

      :ok = TLSDistConfig.regenerate_config(tmp_dir)

      conf = File.read!(Path.join(tmp_dir, "ssl_dist.conf"))

      assert String.contains?(conf, "node.crt")
      assert String.contains?(conf, "node-local.crt")

      # Cluster cert should appear before local cert
      node_pos = :binary.match(conf, "node.crt") |> elem(0)
      local_pos = :binary.match(conf, "node-local.crt") |> elem(0)
      assert node_pos < local_pos
    end

    test "generates valid Erlang term format", %{tmp_dir: tmp_dir} do
      :ok = TLSDistConfig.regenerate_config(tmp_dir)

      conf_path = Path.join(tmp_dir, "ssl_dist.conf")
      charlist_path = String.to_charlist(conf_path)

      assert {:ok, [term]} = :file.consult(charlist_path)
      assert is_list(term)

      server_opts = Keyword.fetch!(term, :server)
      client_opts = Keyword.fetch!(term, :client)

      assert Keyword.has_key?(server_opts, :certs_keys)
      assert Keyword.has_key?(client_opts, :certs_keys)
      assert Keyword.get(server_opts, :verify) == :verify_peer
      assert Keyword.get(server_opts, :fail_if_no_peer_cert) == true
      assert Keyword.get(server_opts, :versions) == [:"tlsv1.3"]
      assert Keyword.get(client_opts, :verify) == :verify_peer
    end
  end

  describe "regenerate/1" do
    test "regenerates both ca_bundle and config", %{tmp_dir: tmp_dir} do
      :ok = TLSDistConfig.regenerate(tmp_dir)

      assert File.exists?(Path.join(tmp_dir, "ca_bundle.crt"))
      assert File.exists?(Path.join(tmp_dir, "ssl_dist.conf"))
    end
  end
end
