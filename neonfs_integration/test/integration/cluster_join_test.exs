defmodule NeonFS.Integration.ClusterJoinTest do
  @moduledoc """
  Integration tests for the full cluster init → invite → join flow.

  Tests both the HTTP-based join path (production flow) and the RPC-based
  join path (used by other integration tests).
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Cluster.InviteRedemption
  alias NeonFS.Transport.TLS

  @moduletag timeout: 120_000
  @moduletag cluster_mode: :per_test

  describe "HTTP-based cluster join" do
    @describetag nodes: 2
    @describetag metrics_port: 19_100

    test "node joins cluster via HTTP invite redemption", %{cluster: cluster} do
      # Step 1: Init cluster on node1
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["http-join-test"])

      :ok = wait_for_cluster_stable(cluster)

      # Step 2: Create invite token
      {:ok, %{"token" => token}} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_invite, [3600])

      # Step 3: Ensure :inets is started on node2 (needed for :httpc)
      {:ok, _} =
        PeerCluster.rpc(cluster, :node2, :application, :ensure_all_started, [:inets])

      # Step 4: Join node2 via HTTP — the full production flow
      node1_info = PeerCluster.get_node!(cluster, :node1)
      via_address = "localhost:#{node1_info.metrics_port}"

      {:ok, join_result} =
        PeerCluster.rpc(cluster, :node2, NeonFS.CLI.Handler, :join_cluster, [
          token,
          via_address
        ])

      # Verify join succeeded
      assert is_binary(join_result["cluster_id"])
      assert join_result["cluster_name"] == "http-join-test"
      assert is_binary(join_result["node_id"])

      # Verify node2 has cluster state
      assert PeerCluster.rpc(cluster, :node2, NeonFS.Cluster.State, :exists?, []) == true

      # Verify node2 can see node1 via distribution
      node1_atom = node1_info.node
      node2_connected = PeerCluster.rpc(cluster, :node2, Node, :list, [])
      assert node1_atom in node2_connected
    end

    test "join fails with invalid token", %{cluster: cluster} do
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["bad-token-test"])

      :ok = wait_for_cluster_stable(cluster)

      {:ok, _} =
        PeerCluster.rpc(cluster, :node2, :application, :ensure_all_started, [:inets])

      node1_info = PeerCluster.get_node!(cluster, :node1)
      bad_token = "nfs_inv_badtoken1234_9999999999_invalidsig00000"

      result =
        PeerCluster.rpc(cluster, :node2, NeonFS.CLI.Handler, :join_cluster, [
          bad_token,
          "localhost:#{node1_info.metrics_port}"
        ])

      assert {:error, _} = result
    end

    test "join fails when via node is unreachable", %{cluster: cluster} do
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["unreachable-test"])

      :ok = wait_for_cluster_stable(cluster)

      {:ok, _} =
        PeerCluster.rpc(cluster, :node2, :application, :ensure_all_started, [:inets])

      {:ok, %{"token" => token}} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_invite, [3600])

      result =
        PeerCluster.rpc(cluster, :node2, NeonFS.CLI.Handler, :join_cluster, [
          token,
          "localhost:19999"
        ])

      assert {:error, _} = result
    end
  end

  describe "RPC-based cluster join" do
    @describetag nodes: 2

    test "node joins cluster via direct RPC", %{cluster: cluster} do
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["rpc-join-test"])

      :ok = wait_for_cluster_stable(cluster)

      {:ok, %{"token" => token}} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_invite, [3600])

      node1_atom = PeerCluster.get_node!(cluster, :node1) |> Map.get(:node)

      {:ok, state} =
        PeerCluster.rpc(cluster, :node2, NeonFS.Cluster.Join, :join_cluster_rpc, [
          token,
          node1_atom
        ])

      assert state.cluster_name == "rpc-join-test"
      assert PeerCluster.rpc(cluster, :node2, NeonFS.Cluster.State, :exists?, []) == true
    end
  end

  describe "invite redemption crypto" do
    @describetag nodes: 1
    @describetag metrics_port: 19_200

    test "token proof is verified and response is encrypted", %{cluster: cluster} do
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["crypto-test"])

      :ok = wait_for_cluster_stable(cluster)

      {:ok, %{"token" => token}} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_invite, [3600])

      # Generate a CSR locally (crypto is pure, doesn't need to run on a peer)
      key = TLS.generate_node_key()
      csr_obj = TLS.create_csr(key, "test_node@localhost")
      csr = TLS.encode_csr(csr_obj)

      # Parse token components
      ["nfs", "inv", random, expiry, _sig] = String.split(token, "_")

      # Compute HMAC proof
      proof = :crypto.mac(:hmac, :sha256, token, csr) |> Base.encode64()

      params = %{
        "csr_pem" => csr,
        "token_random" => random,
        "token_expiry" => expiry,
        "proof" => proof,
        "node_name" => "test_node@localhost"
      }

      {:ok, encrypted_blob} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Cluster.InviteRedemption, :redeem, [params])

      # Verify the blob is encrypted (not plain JSON)
      assert is_binary(encrypted_blob)
      assert byte_size(encrypted_blob) > 28

      # Decrypt using the token
      {:ok, credentials} =
        InviteRedemption.decrypt_response(encrypted_blob, token)

      assert is_binary(credentials["ca_cert_pem"])
      assert is_binary(credentials["node_cert_pem"])
      assert is_binary(credentials["cookie"])
      assert is_binary(credentials["via_node"])

      assert credentials["ca_cert_pem"] =~ "BEGIN CERTIFICATE"
      assert credentials["node_cert_pem"] =~ "BEGIN CERTIFICATE"
    end
  end
end
