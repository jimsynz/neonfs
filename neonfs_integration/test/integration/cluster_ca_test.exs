defmodule NeonFS.Integration.ClusterCATest do
  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Integration.PeerCluster

  @moduletag timeout: 300_000
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster)
    %{}
  end

  describe "CA lifecycle" do
    test "cluster init creates CA stored in system volume", %{cluster: cluster} do
      assert {:ok, info} =
               PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_ca_info, [])

      assert info.subject =~ "test"
      assert info.subject =~ "CA"
      assert info.algorithm == "ECDSA P-256"
    end

    test "CA cert is readable from system volume on all nodes", %{cluster: cluster} do
      for node_name <- [:node1, :node2, :node3] do
        assert_eventually timeout: 10_000 do
          case PeerCluster.rpc(
                 cluster,
                 node_name,
                 NeonFS.Core.SystemVolume,
                 :read,
                 ["/tls/ca.crt"]
               ) do
            {:ok, pem} when is_binary(pem) -> pem =~ "BEGIN CERTIFICATE"
            _ -> false
          end
        end
      end
    end

    test "each node has a locally stored certificate after init/join", %{cluster: cluster} do
      for node_name <- [:node1, :node2, :node3] do
        assert {:ok, _cert} =
                 PeerCluster.rpc(cluster, node_name, NeonFS.Transport.TLS, :read_local_cert, [])
      end
    end

    test "each node's certificate has a unique serial number", %{cluster: cluster} do
      serials =
        for node_name <- [:node1, :node2, :node3] do
          {:ok, cert} =
            PeerCluster.rpc(cluster, node_name, NeonFS.Transport.TLS, :read_local_cert, [])

          info =
            PeerCluster.rpc(cluster, node_name, NeonFS.Transport.TLS, :certificate_info, [cert])

          info.serial
        end

      assert length(Enum.uniq(serials)) == 3
    end

    test "serial numbers increase monotonically", %{cluster: cluster} do
      serials =
        for node_name <- [:node1, :node2, :node3] do
          {:ok, cert} =
            PeerCluster.rpc(cluster, node_name, NeonFS.Transport.TLS, :read_local_cert, [])

          info =
            PeerCluster.rpc(cluster, node_name, NeonFS.Transport.TLS, :certificate_info, [cert])

          info.serial
        end

      assert serials == Enum.sort(serials)
      assert Enum.at(serials, 0) < Enum.at(serials, 1)
      assert Enum.at(serials, 1) < Enum.at(serials, 2)
    end

    test "certificate subject contains the node name", %{cluster: cluster} do
      for node_name <- [:node1, :node2, :node3] do
        {:ok, cert} =
          PeerCluster.rpc(cluster, node_name, NeonFS.Transport.TLS, :read_local_cert, [])

        info =
          PeerCluster.rpc(cluster, node_name, NeonFS.Transport.TLS, :certificate_info, [cert])

        node_info = PeerCluster.get_node!(cluster, node_name)
        node_str = Atom.to_string(node_info.node)

        assert info.subject =~ node_str
      end
    end

    test "all nodes cache the same CA certificate", %{cluster: cluster} do
      ca_pems =
        for node_name <- [:node1, :node2, :node3] do
          {:ok, cert} =
            PeerCluster.rpc(cluster, node_name, NeonFS.Transport.TLS, :read_local_ca_cert, [])

          PeerCluster.rpc(cluster, node_name, NeonFS.Transport.TLS, :encode_cert, [cert])
        end

      assert length(Enum.uniq(ca_pems)) == 1
    end

    test "node certificates are signed by the cluster CA", %{cluster: cluster} do
      {:ok, ca_cert} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Transport.TLS, :read_local_ca_cert, [])

      ca_info =
        PeerCluster.rpc(cluster, :node1, NeonFS.Transport.TLS, :certificate_info, [ca_cert])

      for node_name <- [:node1, :node2, :node3] do
        {:ok, node_cert} =
          PeerCluster.rpc(cluster, node_name, NeonFS.Transport.TLS, :read_local_cert, [])

        node_info =
          PeerCluster.rpc(cluster, node_name, NeonFS.Transport.TLS, :certificate_info, [
            node_cert
          ])

        # Node cert's issuer should match the CA's subject
        assert node_info.issuer == ca_info.subject
      end
    end
  end

  describe "cross-node CSR signing" do
    test "any core node can sign a CSR", %{cluster: cluster} do
      # Generate a CSR on node1 but sign it on node2
      node_key = PeerCluster.rpc(cluster, :node1, NeonFS.Transport.TLS, :generate_node_key, [])

      csr =
        PeerCluster.rpc(cluster, :node1, NeonFS.Transport.TLS, :create_csr, [
          node_key,
          "extra-node"
        ])

      assert {:ok, node_cert, ca_cert} =
               PeerCluster.rpc(
                 cluster,
                 :node2,
                 NeonFS.Core.CertificateAuthority,
                 :sign_node_csr,
                 [csr, "extra-node.example.com"]
               )

      node_info =
        PeerCluster.rpc(cluster, :node1, NeonFS.Transport.TLS, :certificate_info, [node_cert])

      ca_info =
        PeerCluster.rpc(cluster, :node1, NeonFS.Transport.TLS, :certificate_info, [ca_cert])

      assert node_info.subject =~ "extra-node"
      assert node_info.issuer == ca_info.subject
    end
  end

  describe "certificate revocation" do
    test "revocation updates CRL visible to all nodes", %{cluster: cluster} do
      # Issue an extra cert so we can revoke it
      node_key = PeerCluster.rpc(cluster, :node1, NeonFS.Transport.TLS, :generate_node_key, [])

      csr =
        PeerCluster.rpc(cluster, :node1, NeonFS.Transport.TLS, :create_csr, [
          node_key,
          "revoke-target"
        ])

      {:ok, cert, _ca} =
        PeerCluster.rpc(
          cluster,
          :node1,
          NeonFS.Core.CertificateAuthority,
          :sign_node_csr,
          [csr, "revoke-target.example.com"]
        )

      cert_info =
        PeerCluster.rpc(cluster, :node1, NeonFS.Transport.TLS, :certificate_info, [cert])

      serial = cert_info.serial

      # Revoke on node1
      assert :ok =
               PeerCluster.rpc(
                 cluster,
                 :node1,
                 NeonFS.Core.CertificateAuthority,
                 :revoke_certificate,
                 [serial]
               )

      # Verify CRL is visible on node2
      assert_eventually timeout: 10_000 do
        case PeerCluster.rpc(
               cluster,
               :node2,
               NeonFS.Core.CertificateAuthority,
               :is_revoked?,
               [serial]
             ) do
          {:ok, true} -> true
          _ -> false
        end
      end
    end

    test "revocation reflected in ca list output", %{cluster: cluster} do
      # Issue and revoke a cert via the handler
      node_key = PeerCluster.rpc(cluster, :node1, NeonFS.Transport.TLS, :generate_node_key, [])

      csr =
        PeerCluster.rpc(cluster, :node1, NeonFS.Transport.TLS, :create_csr, [
          node_key,
          "list-target"
        ])

      {:ok, _cert, _ca} =
        PeerCluster.rpc(
          cluster,
          :node1,
          NeonFS.Core.CertificateAuthority,
          :sign_node_csr,
          [csr, "list-target.example.com"]
        )

      # Revoke by node name via handler
      assert {:ok, _result} =
               PeerCluster.rpc(
                 cluster,
                 :node1,
                 NeonFS.CLI.Handler,
                 :handle_ca_revoke,
                 ["list-target"]
               )

      # Verify via ca list from any node
      assert_eventually timeout: 10_000 do
        case PeerCluster.rpc(cluster, :node2, NeonFS.CLI.Handler, :handle_ca_list, []) do
          {:ok, certs} ->
            Enum.any?(certs, fn c -> c.status == "revoked" and c.node_name =~ "list-target" end)

          _ ->
            false
        end
      end
    end
  end

  describe "node failure resilience" do
    @describetag cluster_mode: :per_test

    setup %{cluster: cluster} do
      :ok = init_multi_node_cluster(cluster)
      %{}
    end

    test "CA operations survive single node failure", %{cluster: cluster} do
      # Stop node3
      :ok = PeerCluster.stop_node(cluster, :node3)

      node3_info = PeerCluster.get_node!(cluster, :node3)

      assert_eventually timeout: 5_000 do
        nodes = PeerCluster.rpc(cluster, :node1, Node, :list, [])
        node3_info.node not in nodes
      end

      # CA info should still work on surviving nodes
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_ca_info, []) do
          {:ok, info} -> is_binary(info.subject)
          _ -> false
        end
      end

      # Signing should still work on surviving nodes
      assert_eventually timeout: 60_000 do
        node_key =
          PeerCluster.rpc(cluster, :node2, NeonFS.Transport.TLS, :generate_node_key, [])

        csr =
          PeerCluster.rpc(cluster, :node2, NeonFS.Transport.TLS, :create_csr, [
            node_key,
            "survivor-node"
          ])

        case PeerCluster.rpc(
               cluster,
               :node2,
               NeonFS.Core.CertificateAuthority,
               :sign_node_csr,
               [csr, "survivor-node.example.com"]
             ) do
          {:ok, _cert, _ca} -> true
          _ -> false
        end
      end
    end
  end
end
