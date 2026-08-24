defmodule NeonFS.Integration.ClusterCARotateTest do
  @moduledoc """
  End-to-end peer-cluster integration test for `cluster ca rotate`
  Sibling to `force_reset_test.exs`.

  Exercises the full orchestrator flow on a 3-node cluster:

  1. Stage an incoming CA in the system volume.
  2. Distribute it to every node's `incoming-ca.crt`, so the dual-CA
     bundle trusts both anchors before any certificate chains to the
     new one.
  3. Walk every BEAM-connected node, generating a fresh keypair and
     reissuing its `node.crt` against the staged CA.
  4. (`--no-wait` path) finalize immediately — promote the staged CA
     to active on every node and discard the old one. Without
     `--no-wait`, the orchestrator stops at `pending-finalize` so the
     operator can let the dual-CA window settle before running
     `--finalize`.

  The assertions are about each node's TLS directory, not just about
  what the orchestrator returned. A rotation that reports success while
  leaving every node's trust store holding the CA it rotated away from
  has not rotated anything, and asserting only on the reply cannot tell
  the two apart. `tls_dist_material_test.exs` is the other half: that
  what a node has on disk is what its next handshake uses.

  Uses `cluster_mode: :per_test` because rotation rewrites every
  node's TLS material — sharing the cluster across tests would mean
  later tests run against a post-rotation state.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.TestSupport.{CARotateRPC, PeerCluster}
  alias NeonFS.Transport.TLS

  @moduletag timeout: 300_000
  @moduletag nodes: 3
  @moduletag cluster_mode: :per_test
  @moduletag :integration

  setup %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "ca-rotate-test")
    :ok = configure_ca_rotate_rpc_stub(cluster)

    {:ok, pre_rotation_cas: read_from_every_node(cluster, "ca.crt")}
  end

  describe "cluster ca rotate (--no-wait)" do
    test "reissues every node's cert, distributes bundle, finalizes",
         %{cluster: cluster, pre_rotation_cas: pre_rotation_cas} do
      pre_fingerprints = collect_node_cert_fingerprints(cluster)

      assert {:ok,
              %{
                rotated: true,
                old_fingerprint: old_fp,
                fingerprint: new_fp
              }} =
               PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_ca_rotate, [
                 %{"no-wait" => true}
               ])

      assert is_binary(old_fp)
      assert is_binary(new_fp)
      refute new_fp == old_fp

      post_fingerprints = collect_node_cert_fingerprints(cluster)

      for {node_name, pre_fp} <- pre_fingerprints do
        post_fp = Map.fetch!(post_fingerprints, node_name)

        refute post_fp == pre_fp,
               "node #{inspect(node_name)} should have a freshly-issued cert after rotation " <>
                 "(fingerprint unchanged: #{pre_fp})"
      end

      # What the rotation was for: every node now trusts the promoted CA
      # and nothing else, and the staged anchor is gone. Before the
      # orchestrator distributed anything to the nodes themselves, none of
      # this was true — the reply said `rotated: true` while every node's
      # bundle still held only the CA being rotated away from.
      for {node_name, pre_ca} <- pre_rotation_cas do
        assert fingerprint(pre_ca) == old_fp

        active_ca = read_tls(cluster, node_name, "ca.crt")
        assert fingerprint(active_ca) == new_fp

        refute File.exists?(tls_path(cluster, node_name, "incoming-ca.crt")),
               "#{node_name} still has a staged incoming CA after finalize"

        bundle = read_tls(cluster, node_name, "ca_bundle.crt")
        assert bundle =~ active_ca

        refute bundle =~ pre_ca,
               "#{node_name} still trusts the CA the rotation superseded"
      end

      # Audit log lives per-node. The orchestrator runs on node1, so
      # node1 is where every event was recorded.
      events =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.AuditLog, :query, [
          [event_type: ca_rotate_event_types()]
        ])

      event_types = events |> Enum.map(& &1.event_type) |> MapSet.new()

      for required <- [
            :cluster_ca_rotate_started,
            :cluster_ca_rotate_node_completed,
            :cluster_ca_rotate_finalized
          ] do
        assert required in event_types,
               "expected audit event #{inspect(required)}, got #{inspect(event_types)}"
      end

      logged_nodes =
        events
        |> Enum.filter(&(&1.event_type == :cluster_ca_rotate_node_completed))
        |> Enum.map(& &1.details[:node])
        |> MapSet.new()

      cluster_node_atoms =
        for node_name <- node_names(cluster) do
          PeerCluster.get_node!(cluster, node_name).node |> Atom.to_string()
        end

      for node_atom <- cluster_node_atoms do
        assert node_atom in logged_nodes,
               "expected node_completed audit event for #{node_atom}, got #{inspect(logged_nodes)}"
      end
    end
  end

  describe "cluster ca rotate (default — pending finalize)" do
    test "stages + distributes but stops at pending_finalize until --finalize",
         %{cluster: cluster, pre_rotation_cas: pre_rotation_cas} do
      assert {:ok,
              %{
                rotated: false,
                pending_finalize: true,
                grace_window_seconds: grace_seconds
              }} =
               PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_ca_rotate, [%{}])

      assert is_integer(grace_seconds) and grace_seconds > 0

      # The grace window is only a window if both anchors are trusted for
      # its duration, on every node. Staging the incoming CA in the system
      # volume alone does not do that: the trust store a listener reads is
      # the node's own `ca_bundle.crt`.
      for {node_name, pre_ca} <- pre_rotation_cas do
        incoming_ca = read_tls(cluster, node_name, "incoming-ca.crt")
        bundle = read_tls(cluster, node_name, "ca_bundle.crt")

        assert bundle =~ pre_ca, "#{node_name} stopped trusting the outgoing CA mid-rotation"
        assert bundle =~ incoming_ca, "#{node_name} does not trust the incoming CA"

        # And the certificate it was reissued chains to the anchor that
        # just arrived, not to the one being retired.
        assert issuer(read_tls(cluster, node_name, "node.crt")) == subject(incoming_ca)
      end

      # Status reports the rotation as in-progress with both an active
      # and an incoming CA visible.
      assert {:ok, %{rotation_in_progress: true, active: %{}, incoming: %{}}} =
               PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_ca_rotate, [
                 %{"status" => true}
               ])

      # Operator runs --finalize after the grace window.
      assert {:ok, %{finalized: true}} =
               PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_ca_rotate, [
                 %{"finalize" => true}
               ])

      # Status post-finalize: no incoming CA staged any more.
      assert {:ok, %{rotation_in_progress: false, incoming: nil}} =
               PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :handle_ca_rotate, [
                 %{"status" => true}
               ])

      # …and the nodes agree, which is the only place it matters.
      for {node_name, pre_ca} <- pre_rotation_cas do
        refute File.exists?(tls_path(cluster, node_name, "incoming-ca.crt"))
        refute read_tls(cluster, node_name, "ca_bundle.crt") =~ pre_ca
      end
    end
  end

  defp ca_rotate_event_types do
    [
      :cluster_ca_rotate_started,
      :cluster_ca_rotate_node_completed,
      :cluster_ca_rotate_finalized,
      :cluster_ca_rotate_failed,
      :cluster_ca_rotate_aborted
    ]
  end

  defp collect_node_cert_fingerprints(cluster) do
    for node_name <- node_names(cluster), into: %{} do
      {:ok, cert} = PeerCluster.rpc(cluster, node_name, TLS, :read_local_cert, [])

      fingerprint =
        cert
        |> X509.Certificate.to_der()
        |> then(&:crypto.hash(:sha256, &1))
        |> Base.encode16(case: :lower)

      {node_name, fingerprint}
    end
  end

  defp read_from_every_node(cluster, file) do
    for node_name <- node_names(cluster), into: %{} do
      {node_name, read_tls(cluster, node_name, file)}
    end
  end

  # The peers share the runner's filesystem, so their TLS directories are
  # readable directly — no RPC needed, and none wanted: the point is what
  # is on disk for the listener to read.
  defp read_tls(cluster, node_name, file), do: File.read!(tls_path(cluster, node_name, file))

  defp tls_path(cluster, node_name, file) do
    Path.join([PeerCluster.get_node!(cluster, node_name).data_dir, "tls", file])
  end

  defp fingerprint(pem) do
    pem
    |> TLS.decode_cert!()
    |> X509.Certificate.to_der()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp issuer(pem),
    do: pem |> TLS.decode_cert!() |> X509.Certificate.issuer() |> X509.RDNSequence.to_string()

  defp subject(pem),
    do: pem |> TLS.decode_cert!() |> X509.Certificate.subject() |> X509.RDNSequence.to_string()

  defp node_names(cluster) do
    Enum.map(cluster.nodes, & &1.name)
  end

  defp configure_ca_rotate_rpc_stub(cluster) do
    runner_node = Node.self()

    for node_name <- node_names(cluster) do
      :ok =
        PeerCluster.rpc(cluster, node_name, Application, :put_env, [
          :neonfs_core,
          :ca_rotate_rpc_mod,
          CARotateRPC
        ])

      :ok =
        PeerCluster.rpc(cluster, node_name, Application, :put_env, [
          :neonfs_test_support,
          :ca_rotate_skip_node,
          runner_node
        ])
    end

    :ok
  end
end
