defmodule NeonFS.Integration.ClusterCARotateTLSTest do
  @moduledoc """
  A CA rotation on a cluster whose distribution actually runs over TLS,
  asserting the thing an operator cares about: that the cluster still
  handshakes afterwards.

  `cluster_ca_rotate_test.exs` covers what the rotation leaves on every node's
  disk, and `tls_dist_material_test.exs` covers that a node's material is what
  its next handshake uses — but between them the end-to-end property is
  *inferred*. Those peers run plain distribution, so nothing there could
  observe a rotation that left a node unable to validate its peers.

  Two details are what make the assertion here mean anything, and both were
  arrived at by watching it pass when it should not have:

  - **The peers present cluster-signed certificates, not the harness's.**
    `NeonFS.TestSupport.ClusterCase` brings the join's credentials into effect
    (`PeerCluster.activate_cluster_credentials!/2`); without that the handshake
    is authenticated by the CA the harness minted, and would keep succeeding
    after a rotation had broken the cluster's own.

  - **Each probe restarts distribution first.** A rewritten `cacertfile`
    reaches the *listening* side only when the distribution PEM cache's
    validator fires, every `ssl_pem_cache_clean` — 120 s by default, and
    `:ssl.clear_pem_cache/0` does not shorten it there: what the listener
    bound at start is what it keeps using. Measured here, with a node given a
    superseded trust store and the cache cleared: it went on completing
    handshakes at 15, 30, 45, 60, 75, 90 and 105 seconds, and only failed at
    120. A probe that skips the restart therefore reports on the material the
    listener booted with — which after a rotation is exactly the material the
    rotation replaced, so it passes whatever the rotation did. With the
    restart, the same strand fails at once.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.TestSupport.PeerCluster
  alias NeonFS.Transport.TLS

  @moduletag timeout: 300_000
  @moduletag nodes: 2
  @moduletag cluster_mode: :per_test
  @moduletag dist: :tls
  @moduletag :integration

  setup %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "ca-rotate-tls-test")
    :ok
  end

  describe "a rotated TLS cluster" do
    test "completes a fresh handshake between two of its nodes", %{cluster: cluster} do
      # What the peers present, before anything is asserted about rotating it.
      # If this were the harness's own certificate the rest of the test would
      # pass whatever the rotation did to the cluster's.
      for node_name <- node_names(cluster) do
        assert read_tls(cluster, node_name, "ssl_dist.conf") =~ "node.crt",
               "#{node_name} presents its pre-cluster certificate, not the cluster's"

        assert issuer(read_tls(cluster, node_name, "node.crt")) ==
                 subject(read_tls(cluster, node_name, "ca.crt")),
               "#{node_name}'s certificate is not signed by the cluster CA"
      end

      # Baseline: the cluster handshakes *before* the rotation. Without it the
      # test cannot tell a rotation that broke trust from a cluster that never
      # had it.
      assert fresh_handshake(cluster) == :pong

      pre_rotation_ca = read_tls(cluster, :node1, "ca.crt")

      assert {:ok, %{rotated: true}} =
               PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_ca_rotate, [
                 %{"no-wait" => true}
               ])

      # Every node moved off the CA it started on — the precondition for the
      # handshake below being about the *new* anchor.
      for node_name <- node_names(cluster) do
        refute read_tls(cluster, node_name, "ca_bundle.crt") =~ pre_rotation_ca,
               "#{node_name} still trusts the CA the rotation replaced"

        refute File.exists?(tls_path(cluster, node_name, "incoming-ca.crt")),
               "#{node_name} still holds a staged incoming CA after finalize"
      end

      assert fresh_handshake(cluster) == :pong,
             "the rotated cluster cannot complete a new handshake between its nodes"

      # And the connection carries real traffic, not just a completed
      # handshake.
      assert {:ok, _status} =
               PeerCluster.rpc(cluster, :node2, NeonFS.CLI.Handler, :cluster_status, [])
    end

    # The negative half. A passing handshake only means something if a node the
    # rotation failed to reach would fail one — which is the state a
    # `--finalize` that could not promote every node leaves behind, so it is
    # worth reproducing rather than assuming.
    test "a node left trusting only the superseded CA cannot handshake", %{cluster: cluster} do
      pre_rotation_bundle = read_tls(cluster, :node2, "ca_bundle.crt")

      assert {:ok, %{rotated: true}} =
               PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_ca_rotate, [
                 %{"no-wait" => true}
               ])

      File.write!(tls_path(cluster, :node2, "ca_bundle.crt"), pre_rotation_bundle)

      assert fresh_handshake(cluster) == :pang,
             "node2 validated a certificate signed by a CA it does not trust"
    end
  end

  # A handshake that reads the material on disk. Restarting distribution is
  # what releases the listener's hold on the trust store it booted with (see
  # the moduledoc); disconnecting first is what makes the ping a new handshake
  # rather than a report on an established connection.
  defp fresh_handshake(cluster) do
    for node_info <- cluster.nodes do
      :ok = :peer.call(node_info.peer, :ssl, :clear_pem_cache, [])
      :ok = :peer.call(node_info.peer, NeonFS.TLSDistConfig, :restart_distribution, [])
      :ok = :peer.call(node_info.peer, NeonFS.TLSDistConfig, :await_distribution, [10_000])
    end

    node1 = PeerCluster.get_node!(cluster, :node1).node

    PeerCluster.rpc(cluster, :node2, :erlang, :disconnect_node, [node1])
    PeerCluster.rpc(cluster, :node2, :net_adm, :ping, [node1])
  end

  # The peers share the runner's filesystem, so their TLS directories are
  # readable directly — and directly is the point: what is on disk is what the
  # listener reads.
  defp read_tls(cluster, node_name, file), do: File.read!(tls_path(cluster, node_name, file))

  defp tls_path(cluster, node_name, file) do
    Path.join([PeerCluster.get_node!(cluster, node_name).data_dir, "tls", file])
  end

  defp node_names(cluster), do: Enum.map(cluster.nodes, & &1.name)

  defp issuer(pem),
    do: pem |> TLS.decode_cert!() |> X509.Certificate.issuer() |> X509.RDNSequence.to_string()

  defp subject(pem),
    do: pem |> TLS.decode_cert!() |> X509.Certificate.subject() |> X509.RDNSequence.to_string()
end
