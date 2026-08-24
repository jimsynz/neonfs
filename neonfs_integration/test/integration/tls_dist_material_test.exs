defmodule NeonFS.Integration.TLSDistMaterialTest do
  @moduledoc """
  What a running distribution listener does when the TLS material under it
  is replaced on disk.

  Two docstrings in `NeonFS.TLSDistConfig` used to assert that it does
  nothing — that `:ssl_dist` reads `cacertfile` and `certs_keys` once at
  listener start, so a rotated CA bundle or a renewed node certificate has
  no effect until the listener restarts, and that the `cluster ca rotate`
  orchestrator therefore had to drive a rolling restart. Both the CA
  rotation and the unattended certificate renewal were built on that claim,
  and neither had a test. This file is the test.

  What is cached once is the *option list* from `ssl_dist.conf`, and its
  entries are file **paths**. Replacing the file at one of those paths is
  picked up by the next handshake — after the distribution PEM cache's
  validator notices the mtime moved, which it does every
  `ssl_pem_cache_clean` milliseconds: **120 s** by default, and not
  settable from the command line.

  That interval is why the cluster has four peers rather than two. Both
  slow probes are set up in `setup_all`, against separate pairs, so the
  module waits out one interval instead of one per probe. Each test then
  polls its own pair; whichever runs first pays for both.
  """

  use ExUnit.Case, async: false

  alias NeonFS.TestSupport.{PeerCluster, PeerTLS}
  alias NeonFS.Transport.TLS

  @moduletag timeout: 420_000
  @moduletag :integration

  # Comfortably past the 120 s `ssl_pem_cache_clean` default, so a failure
  # means the file was never re-read rather than that we were impatient.
  @reread_deadline_ms 240_000

  # No NeonFS applications: `NeonFS.Client.Connection` re-dials its bootstrap
  # peers on a timer, and a reconnect landing between the disconnect and the
  # probe turns "the handshake was refused" into "someone else's handshake
  # succeeded first". The subject here is OTP's distribution layer, so bare
  # TLS peers are both sufficient and steadier.
  setup_all do
    cluster = PeerCluster.start_cluster!(4, dist: :tls, enable_ra: false, applications: [])
    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    # Both baselines first. They prove the pairs could talk before anything
    # was broken — without them a probe cannot tell a re-read from a peer
    # that never worked — and they are also what puts each file in the PEM
    # cache, which is what the validator later invalidates.
    assert :pong = ping(cluster, :node1, :node2)
    assert :pong = ping(cluster, :node3, :node4)

    break_trust_store(cluster, :node1)
    break_node_cert(cluster, :node4)

    {:ok, cluster: cluster}
  end

  test "the listener caches the option list, and every certificate in it is a path", %{
    cluster: cluster
  } do
    for role <- [:server, :client] do
      [{^role, opts}] = PeerCluster.rpc(cluster, :node1, :ets, :lookup, [:ssl_dist_opts, role])

      [%{certfile: certfile, keyfile: keyfile}] = Keyword.fetch!(opts, :certs_keys)

      assert Path.basename(to_string(certfile)) == "node-local.crt"
      assert Path.basename(to_string(keyfile)) == "node-local.key"
      assert Path.basename(to_string(Keyword.fetch!(opts, :cacertfile))) == "ca_bundle.crt"
    end
  end

  # The half `cluster ca rotate` rests on: it distributes a rewritten
  # `ca_bundle.crt` and nothing else, so if the listener held the trust
  # store it started with, the rotation would reach no running node.
  test "a new handshake validates against the trust store on disk", %{cluster: cluster} do
    assert wait_until_ping(cluster, :node1, :node2, :pang),
           "node1 went on completing handshakes against a trust store it no " <>
             "longer has on disk"
  end

  # The half `NeonFS.Transport.CertRenewal` rests on: it writes a renewed
  # `node.crt` and returns, so if the listener held the certificate it
  # started with, a renewed node would go on presenting the expired one
  # until something restarted it.
  test "a new handshake presents the certificate on disk", %{cluster: cluster} do
    assert wait_until_ping(cluster, :node3, :node4, :pang),
           "node4 was still presenting the certificate it booted with"
  end

  # Replaces the node's trust store with a CA that signed nothing here. Its
  # own certificate is untouched, so its peer still accepts it — the only
  # thing that changed is who this node is willing to believe.
  defp break_trust_store(cluster, node_name) do
    {foreign_ca, _key} = PeerTLS.create_ca("foreign-trust-store")
    File.write!(tls_path(cluster, node_name, "ca_bundle.crt"), TLS.encode_cert(foreign_ca))
  end

  # Replaces the node's own certificate and key with a pair from a CA
  # nobody here trusts, leaving its trust store alone so it still accepts
  # the node probing it.
  defp break_node_cert(cluster, node_name) do
    scratch = Path.join(cluster.data_dir, "foreign-tls-#{node_name}")
    peer = PeerCluster.get_node!(cluster, node_name)

    PeerTLS.write_node_material(scratch, peer.node, PeerTLS.create_ca("foreign-node-cert"), 1)

    for file <- ["node-local.crt", "node-local.key"] do
      File.cp!(Path.join(scratch, file), tls_path(cluster, node_name, file))
    end
  end

  defp tls_path(cluster, node_name, file) do
    Path.join([PeerCluster.get_node!(cluster, node_name).data_dir, "tls", file])
  end

  # Probed between two peers rather than from the controller, which cannot
  # complete a TLS peer's handshake at all. Disconnecting first is what
  # makes it a new handshake instead of a report on the established one.
  defp ping(cluster, from, to) do
    peer = PeerCluster.get_node!(cluster, to).node
    PeerCluster.rpc(cluster, from, :erlang, :disconnect_node, [peer])
    PeerCluster.rpc(cluster, from, :net_adm, :ping, [peer])
  end

  defp wait_until_ping(cluster, from, to, expected) do
    deadline = System.monotonic_time(:millisecond) + @reread_deadline_ms
    do_wait_until_ping(cluster, from, to, expected, deadline)
  end

  defp do_wait_until_ping(cluster, from, to, expected, deadline) do
    cond do
      ping(cluster, from, to) == expected ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(1_000)
        do_wait_until_ping(cluster, from, to, expected, deadline)
    end
  end
end
