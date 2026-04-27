defmodule NeonFS.Integration.NFSv3BeamCrossNodeReadTest do
  @moduledoc """
  Cross-node integration test for the BEAM NFSv3 read stack (sub-issue
  #588 of #533). Builds on the local-only smoke test (#587) by
  separating the NFS interface peer from the core peers that hold the
  file's chunks — the streaming RPC path through
  `NeonFS.Client.ChunkReader.read_file_stream/3` is the subject under
  test.

  ## Cluster shape

    * `node1`: `:neonfs_core` (Ra member + chunk holder).
    * `node2`: `:neonfs_nfs` (no core role; runs the BEAM NFSv3 stack
      and fetches chunks back through the TLS data plane).

  The 1-core / 1-interface shape is the smallest configuration that
  exercises the cross-node streaming path observably. The issue body
  (#588) describes a 2-core variant; collapsing to 1 core keeps
  `init_mixed_role_cluster`'s pool wiring straightforward — the
  helper currently provisions a TLS pool to the first core only, so
  a 2-core variant would need ad-hoc `ensure_pool` plumbing for the
  second core. The streaming-RPC path itself is node-shape-agnostic.

  ## Coverage

    * Volume creation + multi-chunk write (≥ 16 MiB) via `Router.call`
      so chunks land on the core peer, not on the interface peer.
    * `Handler.handle_call(READ, …)` driven directly on the interface
      peer against the live `NFSv3Backend` round-trips byte-identical
      bytes.
    * Conn-pool-checkout telemetry on the interface peer confirms at
      least one data-plane fetch happened against the core peer.
  """

  use ExUnit.Case, async: false

  alias NeonFS.NFS.{Filehandle, InodeTable}
  alias NeonFS.NFS.IntegrationTest.BeamReadTestHooks
  alias NeonFS.TestSupport.{ClusterCase, PeerCluster}
  alias NFSServer.NFSv3.{Handler, Types}
  alias NFSServer.RPC.Auth
  alias NFSServer.XDR

  @moduletag timeout: 300_000
  @moduletag :integration
  @moduletag :nfs

  @auth %Auth.None{}
  @chunk_max 1_048_576
  @payload_size 16 * @chunk_max

  test "cross-node READ on an NFS interface peer pulls chunks via ChunkReader" do
    # 2-peer cluster: 1 core (chunk holder), 1 NFS interface. The
    # streaming RPC path is observable end-to-end with a single core
    # peer — `init_mixed_role_cluster` wires a TLS pool from the
    # interface to the (sole) core, and durability=1 keeps quorum
    # writes happy with a single replica. A 3-peer cluster with two
    # cores is conceptually closer to production, but the helper
    # currently only opens a pool to the first core; the second
    # core's chunks would need extra `ensure_pool` plumbing that's
    # adjacent to the test's actual subject. The streaming path is
    # node-shape-agnostic.
    cluster =
      PeerCluster.start_cluster!(2,
        roles: %{
          node1: [:neonfs_core],
          node2: [:neonfs_nfs]
        }
      )

    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    PeerCluster.connect_nodes(cluster)
    :ok = ClusterCase.init_mixed_role_cluster(cluster, name: "nfsv3-cross-node-read")

    volume = "nfsv3-xnode-vol-#{System.unique_integer([:positive])}"
    file_name = "stream.bin"
    file_path = "/" <> file_name

    # Stamp the cutover flag on the interface peer per the contract
    # `#286` will gate on. Not load-bearing here yet.
    :ok =
      PeerCluster.rpc(cluster, :node2, Application, :put_env, [
        :neonfs_nfs,
        :handler_stack,
        :beam
      ])

    on_exit(fn ->
      try do
        PeerCluster.rpc(cluster, :node2, Application, :delete_env, [
          :neonfs_nfs,
          :handler_stack
        ])
      catch
        _, _ -> :ok
      end
    end)

    # Disable compression so chunks bypass the
    # `needs_server_processing?` fallback in `ChunkReader` and ride
    # the TLS data plane (Router.data_call) — that's what conn-pool
    # checkout telemetry actually observes. The architecturally-
    # equivalent compressed path uses Erlang distribution RPC and
    # wouldn't fire the conn-pool events. Durability=1 because
    # there's only one core peer.
    volume_opts = %{
      compression: %{algorithm: :none, level: 0, min_size: 0},
      durability: %{type: :replicate, factor: 1, min_copies: 1}
    }

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [volume, volume_opts])

    assert_eventually(
      fn ->
        match?(
          {:ok, _},
          PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [volume])
        )
      end,
      timeout: 10_000
    )

    {:ok, vol_struct} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [volume])

    payload = :crypto.strong_rand_bytes(@payload_size)

    # Write from the core peer so chunks land in its drive store.
    # The interface peer (node2) has no core role and no local
    # chunks — its READ must traverse the data plane.
    {:ok, _meta} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core, :write_file_streamed, [
        volume,
        file_path,
        [payload]
      ])

    assert_eventually(
      fn ->
        match?(
          {:ok, _},
          PeerCluster.rpc(cluster, :node1, NeonFS.Core, :get_file_meta, [volume, file_path])
        )
      end,
      timeout: 10_000
    )

    # Boot InodeTable on the interface peer unlinked.
    # `GenServer.start/3` (no link) so the table survives the
    # rpc-handler exit.
    case PeerCluster.rpc(cluster, :node2, GenServer, :start, [
           InodeTable,
           [],
           [name: InodeTable]
         ]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    {:ok, root_inode} =
      PeerCluster.rpc(cluster, :node2, InodeTable, :allocate_inode, [volume, "/"])

    {:ok, vol_id_bin} =
      PeerCluster.rpc(cluster, :node2, Filehandle, :volume_uuid_to_binary, [vol_struct.id])

    root_fh =
      PeerCluster.rpc(cluster, :node2, Filehandle, :encode, [vol_id_bin, root_inode])

    # Wire up conn-pool-checkout telemetry on the interface peer so
    # we can assert the data plane was actually hit. Counters live
    # in a named ETS table on the interface peer; the test reads
    # them back via RPC.
    :ok = PeerCluster.rpc(cluster, :node2, BeamReadTestHooks, :init_checkout_table, [])

    :ok =
      PeerCluster.rpc(cluster, :node2, :telemetry, :attach, [
        "nfsv3-xnode-read-test",
        [:neonfs, :transport, :conn_pool, :checkout],
        &BeamReadTestHooks.record_checkout/4,
        nil
      ])

    on_exit(fn ->
      try do
        PeerCluster.rpc(cluster, :node2, :telemetry, :detach, ["nfsv3-xnode-read-test"])
      catch
        _, _ -> :ok
      end
    end)

    ctx = %{call: nil, nfs_v3_backend: NeonFS.NFS.NFSv3Backend}

    # LOOKUP "stream.bin" so we have its filehandle.
    {:ok, lookup_body} =
      PeerCluster.rpc(cluster, :node2, Handler, :handle_call, [
        3,
        Types.encode_diropargs3({root_fh, file_name}),
        @auth,
        ctx
      ])

    {:ok, :ok, rest} = Types.decode_nfsstat3(lookup_body)
    {:ok, file_fh, _rest} = Types.decode_fhandle3(rest)

    # READ chunked across the file. Each per-call body must respect
    # the `count` cap and reassembled bytes must equal `payload`.
    reassembled =
      0
      |> Stream.iterate(&(&1 + @chunk_max))
      |> Stream.take_while(&(&1 < @payload_size))
      |> Enum.map(fn offset ->
        args =
          Types.encode_fhandle3(file_fh) <>
            XDR.encode_uhyper(offset) <>
            XDR.encode_uint(@chunk_max)

        {:ok, body} =
          PeerCluster.rpc(cluster, :node2, Handler, :handle_call, [6, args, @auth, ctx])

        flat = IO.iodata_to_binary(body)

        {:ok, :ok, rest} = Types.decode_nfsstat3(flat)
        {:ok, _attr, rest} = Types.decode_post_op_attr(rest)
        {:ok, _count, rest} = XDR.decode_uint(rest)
        {:ok, _eof, rest} = XDR.decode_bool(rest)
        {:ok, data, <<>>} = XDR.decode_var_opaque(rest)
        data
      end)
      |> IO.iodata_to_binary()

    assert byte_size(reassembled) == @payload_size
    assert reassembled == payload

    # Telemetry assertion: at least one conn-pool checkout fired on
    # the interface peer. The interface peer has no core role, so any
    # successful chunk fetch is necessarily cross-node — the recorded
    # checkouts are the architectural proof that the data plane was
    # exercised (versus the Erlang-RPC fallback).
    peers = PeerCluster.rpc(cluster, :node2, BeamReadTestHooks, :checkout_peers, [])

    assert peers != [],
           "expected at least one conn-pool checkout on the interface peer; got #{inspect(peers)}"
  end

  defp assert_eventually(fun, opts) do
    deadline = System.monotonic_time(:millisecond) + Keyword.fetch!(opts, :timeout)
    do_assert_eventually(fun, deadline)
  end

  defp do_assert_eventually(fun, deadline) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) > deadline do
        flunk("assert_eventually timeout")
      else
        Process.sleep(100)
        do_assert_eventually(fun, deadline)
      end
    end
  end
end
