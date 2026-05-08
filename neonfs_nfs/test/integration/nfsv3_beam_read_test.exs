defmodule NeonFS.Integration.NFSv3BeamReadTest do
  @moduledoc """
  Local-only end-to-end smoke test of the BEAM-native NFSv3 read stack
  (sub-issue #587 of #533, itself part of #284 / #113).

  Boots a 3-node cluster, writes a multi-chunk file via `NeonFS.Core`,
  then drives `NFSServer.NFSv3.Handler.handle_call/4` directly on
  `:node1` with the live `NeonFS.NFS.NFSv3Backend`. No socket, no
  `ExportManager` — production wiring through ExportManager is tracked
  under #286 and lands separately. This is the foundation slice; the
  cross-node-via-`ChunkReader` path (#588) and peak-RSS bound (#589)
  build on the same harness.

  ## What's exercised

    * `NULL` (proc 0) — sanity
    * `GETATTR` on the volume root (decodes to `:dir`)
    * `LOOKUP` of the file's basename (returns a child filehandle whose
      `fileid` matches what `InodeTable.allocate_inode/2` would mint)
    * `READDIRPLUS` on the volume root (returns the file with correct
      `fileid`, `name`, and post-op attrs)
    * `READ` chunked across the file (one `READ` per chunk-size offset),
      reassembled bytes round-trip byte-for-byte against the source

  ## Out of scope (later slices)

    * Cross-node reads via `ChunkReader` streaming path — #588 uses a
      mixed-role peer cluster so the harness shape differs.
    * Peak-RSS memory bound — #589, separate slice for tuning headroom.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.NFS.{Filehandle, InodeTable}
  alias NeonFS.NFS.IntegrationTest.BeamReadTestHooks
  alias NeonFS.TestSupport.PeerCluster
  alias NFSServer.NFSv3.{Handler, Types}
  alias NFSServer.RPC.Auth
  alias NFSServer.XDR

  @moduletag timeout: 300_000
  @moduletag :integration
  @moduletag :nfs
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  # AUTH_NONE — accepted by the BEAM stack today; AUTH_SYS-style
  # checks live further inside the backend.
  @auth %Auth.None{}

  # FastCDC default `max` chunk size. Choosing a 16-MiB payload (16
  # whole chunks) keeps the test multi-chunk without ballooning the
  # peer-cluster runtime.
  @chunk_max 1_048_576
  @payload_size 16 * @chunk_max

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "nfsv3-beam-read")
    :ok
  end

  test "BEAM NFSv3 stack: write + GETATTR + LOOKUP + READDIRPLUS + READ", %{cluster: cluster} do
    volume = "nfsv3-beam-vol-#{System.unique_integer([:positive])}"
    file_name = "stream.bin"
    file_path = "/" <> file_name

    # Stamp the cutover flag on `:node1` for the test's lifetime. The
    # flag is not yet load-bearing for the procedure path this test
    # uses, but it's the contract `#286` will gate on.
    :ok =
      PeerCluster.rpc(cluster, :node1, Application, :put_env, [
        :neonfs_nfs,
        :handler_stack,
        :beam
      ])

    # node1 IS a core node here, so route `core_call/3` through a local
    # `apply/3` rather than the Router/Discovery stack (which is wired
    # for *non-core* interface peers reaching out to core). `&:erlang.apply/3`
    # is a built-in capture, so it crosses node boundaries cleanly.
    :ok =
      PeerCluster.rpc(cluster, :node1, Application, :put_env, [
        :neonfs_nfs,
        :core_call_fn,
        &:erlang.apply/3
      ])

    # Same story for the streaming reads: ChunkReader is the non-core-
    # peer's data-plane shim. When the BEAM stack runs on a core peer
    # (the smoke-test setup), short-circuit straight to
    # `NeonFS.Core.read_file_stream/3`. The cross-node-via-ChunkReader
    # variant is the explicit subject of #588.
    :ok =
      PeerCluster.rpc(cluster, :node1, Application, :put_env, [
        :neonfs_nfs,
        :read_file_stream_fn,
        &BeamReadTestHooks.local_read_file_stream/4
      ])

    on_exit(fn ->
      try do
        PeerCluster.rpc(cluster, :node1, Application, :delete_env, [
          :neonfs_nfs,
          :handler_stack
        ])

        PeerCluster.rpc(cluster, :node1, Application, :delete_env, [
          :neonfs_nfs,
          :core_call_fn
        ])

        PeerCluster.rpc(cluster, :node1, Application, :delete_env, [
          :neonfs_nfs,
          :read_file_stream_fn
        ])
      catch
        # Cluster may already be torn down by the time on_exit fires.
        _, _ -> :ok
      end
    end)

    # 1. Volume + multi-chunk file. Pin `replicate:1` because in this
    # cluster only `node1`'s drive is in the bootstrap layer
    # (`Cluster.Init.do_init_cluster/1` registers local drives via
    # #890; joining nodes' drives don't get registered there) — so
    # `Volume.MetadataWriter`'s lazy provisioning sees just one drive
    # and can't satisfy the default `replicate:3, min_copies:2`.
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        volume,
        %{"durability" => "replicate:1"}
      ])

    assert_eventually timeout: 10_000 do
      case PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [volume]) do
        {:ok, _} -> true
        _ -> false
      end
    end

    {:ok, vol_struct} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [volume])

    payload = :crypto.strong_rand_bytes(@payload_size)

    {:ok, _meta} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core, :write_file_streamed, [
        volume,
        file_path,
        [payload]
      ])

    # Quorum-write replicates asynchronously across the cluster; the
    # subsequent quorum-read must observe the new file. Poll for it
    # rather than racing.
    assert_eventually timeout: 10_000 do
      match?(
        {:ok, _},
        PeerCluster.rpc(cluster, :node1, NeonFS.Core, :get_file_meta, [volume, file_path])
      )
    end

    # 2. InodeTable on `:node1` — not in the core supervisor; boot it
    # explicitly per #587. Use `GenServer.start/3` (no link) so the
    # table survives the rpc-handler's exit; name registration lets
    # later rpc calls find it by module name.
    case PeerCluster.rpc(cluster, :node1, GenServer, :start, [
           InodeTable,
           [],
           [name: InodeTable]
         ]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    # Allocate the volume-specific root inode. `InodeTable.root_inode/0`
    # returns 1 — the *global* boot root with `vol_name=nil` — which the
    # backend can't resolve to a real volume. The volume-scoped root has
    # a hash-derived inode and `(volume, "/")` in the ETS mapping.
    {:ok, root_inode} =
      PeerCluster.rpc(cluster, :node1, InodeTable, :allocate_inode, [volume, "/"])

    {:ok, vol_id_bin} =
      PeerCluster.rpc(cluster, :node1, Filehandle, :volume_uuid_to_binary, [vol_struct.id])

    root_fh =
      PeerCluster.rpc(cluster, :node1, Filehandle, :encode, [vol_id_bin, root_inode])

    ctx = %{call: nil, nfs_v3_backend: NeonFS.NFS.NFSv3Backend}

    # 3. NULL.
    assert {:ok, <<>>} = call_proc(cluster, 0, <<>>, ctx)

    # 4. GETATTR on the volume root.
    {:ok, getattr_body} = call_proc(cluster, 1, Types.encode_fhandle3(root_fh), ctx)
    {:ok, :ok, rest} = Types.decode_nfsstat3(getattr_body)
    {:ok, root_attr, <<>>} = Types.decode_fattr3(rest)
    assert root_attr.type == :dir, "GETATTR on root must return :dir, got #{inspect(root_attr)}"

    # 5. LOOKUP the file by basename.
    {:ok, lookup_body} =
      call_proc(cluster, 3, Types.encode_diropargs3({root_fh, file_name}), ctx)

    {:ok, :ok, rest} = Types.decode_nfsstat3(lookup_body)
    {:ok, file_fh, _rest} = Types.decode_fhandle3(rest)

    # The fileid embedded in `file_fh` must match what
    # `InodeTable.allocate_inode/2` would mint for `(volume, file_path)`.
    {:ok, expected_inode} =
      PeerCluster.rpc(cluster, :node1, InodeTable, :allocate_inode, [volume, file_path])

    {:ok, decoded} = Filehandle.decode(file_fh)
    assert decoded.fileid == expected_inode

    # 6. READDIRPLUS on the volume root — file appears with right name + fileid.
    rdplus_args =
      Types.encode_fhandle3(root_fh) <>
        XDR.encode_uhyper(0) <>
        Types.encode_cookieverf3(<<0::64>>) <>
        XDR.encode_uint(8192) <>
        XDR.encode_uint(32_768)

    {:ok, rdplus_body} = call_proc(cluster, 17, rdplus_args, ctx)
    flat = IO.iodata_to_binary(rdplus_body)
    {:ok, :ok, rest} = Types.decode_nfsstat3(flat)
    {:ok, _dir_attr, rest} = Types.decode_post_op_attr(rest)
    {:ok, _verf, rest} = Types.decode_cookieverf3(rest)
    {:ok, entries, _rest} = decode_readdirplus_chain(rest)

    assert Enum.any?(entries, fn {fileid, name, _cookie, _attr, _fh} ->
             name == file_name and fileid == expected_inode
           end),
           "READDIRPLUS did not include #{file_name}: #{inspect(entries)}"

    # 7. READ chunked across the file. Reassembled bytes must equal `payload`.
    reassembled =
      0
      |> Stream.iterate(&(&1 + @chunk_max))
      |> Stream.take_while(&(&1 < @payload_size))
      |> Enum.map(fn offset ->
        args =
          Types.encode_fhandle3(file_fh) <>
            XDR.encode_uhyper(offset) <>
            XDR.encode_uint(@chunk_max)

        {:ok, body} = call_proc(cluster, 6, args, ctx)
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
    assert reassembled == payload, "READ-reassembled bytes did not match the source payload"
  end

  defp call_proc(cluster, proc, args, ctx) do
    PeerCluster.rpc(cluster, :node1, Handler, :handle_call, [proc, args, @auth, ctx])
  end

  # Same shape as the helper in `NFSServer.NFSv3.HandlerTest` —
  # READDIRPLUS entries carry `(fileid, name, cookie, post_op_attr,
  # post_op_fh3)`.
  defp decode_readdirplus_chain(binary), do: do_decode_readdirplus(binary, [])

  defp do_decode_readdirplus(binary, acc) do
    with {:ok, true, rest} <- XDR.decode_bool(binary),
         {:ok, fileid, rest} <- XDR.decode_uhyper(rest),
         {:ok, name, rest} <- Types.decode_filename3(rest),
         {:ok, cookie, rest} <- XDR.decode_uhyper(rest),
         {:ok, attr, rest} <- Types.decode_post_op_attr(rest),
         {:ok, fh, rest} <- Types.decode_post_op_fh3(rest) do
      do_decode_readdirplus(rest, [{fileid, name, cookie, attr, fh} | acc])
    else
      {:ok, false, rest} -> {:ok, Enum.reverse(acc), rest}
      err -> err
    end
  end
end
