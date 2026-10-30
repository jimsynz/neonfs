defmodule NeonFS.Integration.TLSPeerClusterTest do
  @moduledoc """
  A peer cluster running distribution over TLS, which is how a real cluster
  runs and how the CLI reaches one.

  The controller stays on plain distribution and drives the peers over the
  standard-I/O control channel `:peer.start/1` already provides, so the
  seventy existing integration files are untouched — `PeerCluster.rpc/6`
  dispatches on the cluster's own `dist` flag rather than every caller
  learning a second API.
  """

  use ExUnit.Case, async: false

  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 180_000
  @moduletag :integration

  setup do
    cluster = PeerCluster.start_cluster!(2, dist: :tls, enable_ra: false)
    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)
    {:ok, cluster: cluster}
  end

  test "peers run distribution over TLS", %{cluster: cluster} do
    # `-proto_dist inet_tls` is what the harness passes; asking the peer
    # what it is actually running is what proves it took effect rather
    # than the flag merely being present on the command line.
    assert PeerCluster.rpc(cluster, :node1, :init, :get_argument, [:proto_dist]) ==
             {:ok, [[~c"inet_tls"]]}
  end

  test "rpc/6 reaches a TLS peer without the controller joining the mesh", %{cluster: cluster} do
    assert PeerCluster.rpc(cluster, :node1, :erlang, :node, []) ==
             PeerCluster.get_node!(cluster, :node1).node

    # The controller could not have completed a TLS handshake with the
    # peer, so if this works it is the alternative control channel doing
    # it — which is the whole point of the arrangement.
    refute PeerCluster.get_node!(cluster, :node1).node in Node.list()
  end

  test "each peer has its own certificate from the cluster's CA", %{cluster: cluster} do
    for node_name <- [:node1, :node2] do
      tls_dir = Path.join(PeerCluster.get_node!(cluster, node_name).data_dir, "tls")

      for file <- ["node-local.crt", "node-local.key", "ca_bundle.crt", "ssl_dist.conf"] do
        assert File.exists?(Path.join(tls_dir, file)),
               "#{node_name} is missing #{file}"
      end
    end
  end

  # The triple the CLI looks for, which the cluster CA does not issue — it
  # issues node certificates, so pointing the CLI at a peer's directory
  # turns TLS on and then fails to find an identity.
  test "the cluster issues the CLI its own identity", %{cluster: cluster} do
    cli_dir = PeerCluster.cli_tls_dir(cluster)

    for file <- ["local-ca.crt", "cli.crt", "cli.key", "ssl_dist.conf"] do
      assert File.exists?(Path.join(cli_dir, file)), "the CLI material is missing #{file}"
    end
  end

  # Asking the peer what *it* sees answers the same question from a vantage
  # point that can answer it, so these no longer have to refuse. The
  # controller's own view stays empty either way — it never joins the mesh.
  test "helpers that probe distribution ask the peers instead", %{cluster: cluster} do
    node1 = PeerCluster.get_node!(cluster, :node1).node
    node2 = PeerCluster.get_node!(cluster, :node2).node

    assert :ok = PeerCluster.connect_nodes(cluster)

    # The peers see each other; the controller sees neither, because it never
    # joins their mesh. Both halves matter: the first is the view the plain
    # path gets from the controller, and the second is why it cannot.
    assert node2 in PeerCluster.visible_nodes(cluster, :node1)
    assert node1 in PeerCluster.visible_nodes(cluster, :node2)
    refute node1 in Node.list()
    refute node2 in Node.list()
  end

  # `restart_node/3` still refuses, and still has to: it respawns the VM and
  # then wires it back into the mesh from the controller, which cannot
  # complete a TLS peer's handshake. Naming the limit at the call site beats
  # surfacing later as a mystery `:nodedown`.
  test "restart_node/3 refuses rather than misleading", %{cluster: cluster} do
    assert_raise ArgumentError, ~r/not supported on a TLS cluster/, fn ->
      PeerCluster.restart_node(cluster, :node1)
    end
  end

  test "a plain cluster is unaffected" do
    cluster = PeerCluster.start_cluster!(1, enable_ra: false)
    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    assert cluster.dist == :plain
    assert PeerCluster.rpc(cluster, :node1, :init, :get_argument, [:proto_dist]) == :error
  end
end
