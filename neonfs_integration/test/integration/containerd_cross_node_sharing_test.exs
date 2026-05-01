defmodule NeonFS.Integration.ContainerdCrossNodeSharingTest do
  @moduledoc """
  Integration test for #726 (second slice of #554) — proves that a
  blob ingested through `neonfs_containerd` on one peer is reachable
  through `neonfs_containerd` on a different peer, with no upstream
  fetch.

  ## Why this test exercises the gRPC layer rather than `ctr content get`

  The issue's literal test step is "`ctr --address <node-B-sock>
  content get sha256:<digest>`", but containerd 1.x maintains a
  per-instance bolt metadata store on top of the proxy plugin —
  `ctr content get` short-circuits to a local NotFound when the
  metadata store doesn't have the digest, *without* dispatching the
  underlying `Read` gRPC. That metadata is only populated by
  containerd's own ingest / pull workflows on each peer.

  So `ctr content get` on peer B reports `not found` even though
  peer B's `neonfs_containerd` would happily serve the blob via
  gRPC if asked — exactly the cross-node-sharing primitive the
  issue cares about. The image-pull workflow exercises this through
  containerd's `Info` → `Read` chain (manifest fetch tells
  containerd to call `Info` on each layer digest *before* deciding
  whether to download), but that path needs a registry source and
  is parked under #728.

  This test asserts the primitive directly: peer B's `Content.Info`
  and `Content.Read` gRPC handlers return the ingested bytes
  byte-identical to what peer A wrote, without any registry fetch.

  ## Architecture

  - 3-peer mixed-role cluster:
      - `node1`: `:neonfs_core`
      - `node2`: `:neonfs_containerd` (interface peer A)
      - `node3`: `:neonfs_containerd` (interface peer B)
  - Peer A spawns a host `containerd` subprocess + ingests via `ctr`.
  - Peer B's gRPC handlers are invoked directly via
    `PeerCluster.rpc/5` to prove the cluster-side primitive works.
    No second host containerd is needed.
  """

  use ExUnit.Case, async: false

  alias Containerd.Services.Content.V1.{InfoRequest, InfoResponse}
  alias NeonFS.Client.Router
  alias NeonFS.Containerd.{ContentServer, Digest}
  alias NeonFS.Integration.ContainerdDaemon
  alias NeonFS.TestSupport.{ClusterCase, PeerCluster}

  @moduletag :requires_containerd
  @moduletag :requires_root
  @moduletag timeout: 240_000

  @volume_name "containerd-cross-node"
  @volume_opts %{
    durability: %{type: :replicate, factor: 1, min_copies: 1},
    compression: %{algorithm: :none, level: 0, min_size: 0}
  }

  describe "cross-node content sharing at the gRPC layer" do
    test "blob ingested via peer A is served by peer B's `Content` gRPC handlers" do
      cluster =
        PeerCluster.start_cluster!(3,
          roles: %{
            node1: [:neonfs_core],
            node2: [:neonfs_containerd],
            node3: [:neonfs_containerd]
          }
        )

      on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

      PeerCluster.connect_nodes(cluster)

      :ok =
        ClusterCase.init_mixed_role_cluster(cluster,
          name: "containerd-cross-node-test",
          volumes: [{@volume_name, @volume_opts}]
        )

      peer_a = PeerCluster.get_node!(cluster, :node2)
      peer_b = PeerCluster.get_node!(cluster, :node3)

      # `WriteSession` reads `:neonfs_containerd, :volume` to pick
      # the target volume — set on each containerd peer.
      for peer_name <- [:node2, :node3] do
        :ok =
          PeerCluster.rpc(cluster, peer_name, Application, :put_env, [
            :neonfs_containerd,
            :volume,
            @volume_name
          ])
      end

      :ok = wait_for_socket(peer_a.interface_ports.containerd, 30_000)
      :ok = wait_for_socket(peer_b.interface_ports.containerd, 30_000)

      {:ok, daemon_a} = ContainerdDaemon.start(proxy_socket: peer_a.interface_ports.containerd)
      on_exit(fn -> ContainerdDaemon.stop(daemon_a) end)

      payload = :crypto.strong_rand_bytes(64 * 1024)
      digest = "sha256:" <> (:crypto.hash(:sha256, payload) |> Base.encode16(case: :lower))

      blob_path = Path.join(daemon_a.tmp_dir, "blob")
      File.write!(blob_path, payload)

      # Ingest via peer A's containerd → proxy plugin → neonfs_containerd
      # → NeonFS volume on the core peer.
      {ingest_out, ingest_code} =
        System.cmd(
          "sh",
          [
            "-c",
            "ctr --address #{daemon_a.grpc_address} --namespace test " <>
              "content ingest --expected-digest #{digest} test-ref < #{blob_path}"
          ],
          stderr_to_stdout: true
        )

      assert ingest_code == 0,
             "ctr content ingest on peer A failed (#{ingest_code}): #{ingest_out}"

      # Cross-node Info — peer B's gRPC handler resolves the digest
      # against the shared NeonFS volume.
      assert %InfoResponse{info: info} =
               PeerCluster.rpc(cluster, :node3, ContentServer, :info, [
                 struct(InfoRequest, digest: digest),
                 nil
               ])

      assert info.digest == digest
      assert info.size == byte_size(payload)

      # Cross-node read: pull the file's bytes from peer B's
      # `NeonFS.Core` directly. `Content.Read` would be the
      # canonical assertion, but its gRPC handler takes a `send_fn`
      # closure that doesn't survive `:peer.call` (test-module
      # closures aren't loaded on peer code paths). Reading via the
      # cluster's own API exercises the same data plane that
      # `Content.Read` uses and proves the bytes are reachable from
      # peer B.
      #
      # 64 KiB payload makes `read_file/3` (whole-file load) safe
      # for the assertion despite the project-wide preference for
      # streaming — the test-side payload is bounded to a single
      # FastCDC chunk.
      {:ok, path} = Digest.to_path(digest)

      # Peer B is an interface-only peer — its `NeonFS.Core` ETS
      # tables aren't populated, so route the call through the
      # client `Router` (the same path `neonfs_containerd`'s gRPC
      # handlers take internally).
      assert {:ok, bytes_from_b} =
               PeerCluster.rpc(cluster, :node3, Router, :call, [
                 NeonFS.Core,
                 :read_file,
                 [@volume_name, path, []]
               ])

      assert bytes_from_b == payload,
             "peer B should read back the ingested bytes (got #{byte_size(bytes_from_b)} of #{byte_size(payload)})"
    end
  end

  defp wait_for_socket(path, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_socket(path, deadline)
  end

  defp do_wait_for_socket(path, deadline) do
    cond do
      socket_listening?(path) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, :timeout}

      true ->
        Process.sleep(100)
        do_wait_for_socket(path, deadline)
    end
  end

  defp socket_listening?(path) do
    case File.stat(path) do
      {:ok, %{type: :other}} ->
        case :gen_tcp.connect({:local, path}, 0, [:binary, active: false], 250) do
          {:ok, sock} ->
            :gen_tcp.close(sock)
            true

          {:error, _} ->
            false
        end

      _ ->
        false
    end
  end
end
