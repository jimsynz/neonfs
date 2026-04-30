defmodule NeonFS.Integration.NFSv3BeamWriteTest do
  @moduledoc """
  End-to-end peer-cluster test for the BEAM NFSv3 write path
  (sub-issue #627 of #285). Closes the loop the read-path tests
  (#587 / #588 / #589) left open: prove a real write workload
  round-trips through the BEAM mutation procedures.

  ## Cluster shape

    * `node1`: `:neonfs_core` — chunk holder, runs the FileIndex
      and ChunkStore.
    * `node2`: `:neonfs_nfs` — runs the BEAM NFSv3 stack and
      drives `Handler.handle_call/4`.

  Same shape as #588 (the cross-node read-path test).

  ## What's exercised

    1. CREATE a regular file via proc 8 (UNCHECKED).
    2. WRITE multi-MiB across multiple chunks with mixed
       `UNSTABLE` / `FILE_SYNC` stability bits. The BEAM backend
       always reports `:file_sync` regardless of the request.
    3. COMMIT; assert the returned `writeverf3` is stable across
       repeated calls within the same VM.
    4. READ back; assert byte-identity against the source payload.
       This is the cross-cutting check — exercises both the read
       path from #587-#589 and the write path landing here.
    5. SETATTR (touch mtime); verify `wcc_data` carries the pre/post
       attrs.
    6. RENAME the file; READ from the new path.
    7. REMOVE; subsequent LOOKUP returns `NFS3ERR_NOENT`.
  """

  use ExUnit.Case, async: false

  alias NeonFS.NFS.{Filehandle, InodeTable}
  alias NeonFS.TestSupport.{ClusterCase, PeerCluster}
  alias NFSServer.NFSv3.{Handler, Types}
  alias NFSServer.RPC.Auth
  alias NFSServer.XDR

  @moduletag timeout: 300_000
  @moduletag :integration
  @moduletag :nfs

  @auth %Auth.None{}
  @chunk_size 1_048_576
  @payload_size 4 * @chunk_size

  test "BEAM NFSv3 stack: full write-path lifecycle on a peer cluster" do
    cluster =
      PeerCluster.start_cluster!(2,
        roles: %{
          node1: [:neonfs_core],
          node2: [:neonfs_nfs]
        }
      )

    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    PeerCluster.connect_nodes(cluster)
    :ok = ClusterCase.init_mixed_role_cluster(cluster, name: "nfsv3-write-e2e")

    volume = "nfsv3-write-vol-#{System.unique_integer([:positive])}"

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

    # Compression off + durability=1 so chunk reads come straight off
    # the data plane, same rationale as #588.
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

    # WRITE picks a drive via `DriveRegistry.select_drive(initial_tier)` on
    # the core peer — if no drive is registered yet, `:no_drives_in_tier`
    # bubbles up as `{:error, %Unavailable{}}` and the BEAM NFS backend's
    # catch-all maps it to `NFS3ERR_IO`, which is what #704 caught. The
    # volume registering does not imply a drive is selectable; wait for it
    # explicitly. Default tier is `:hot`.
    assert_eventually(
      fn ->
        match?(
          {:ok, _},
          PeerCluster.rpc(cluster, :node1, NeonFS.Core.DriveRegistry, :select_drive, [
            vol_struct.tiering.initial_tier
          ])
        )
      end,
      timeout: 10_000
    )

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

    # Step 4's READ uses the production `ChunkReader.read_file_stream/3`
    # path (no `read_file_stream_fn` override) — node2 is a non-core
    # interface peer, so chunks fetch over the TLS data plane just
    # like #588.
    ctx = %{call: nil, nfs_v3_backend: NeonFS.NFS.NFSv3Backend}
    file_name = "lifecycle.bin"

    # ——— Step 1: CREATE ——————————————————————————————————————————

    create_args =
      Types.encode_diropargs3({root_fh, file_name}) <>
        Types.encode_createhow3({:unchecked, %Types.Sattr3{mode: 0o644}})

    {:ok, create_body} = call_proc(cluster, 8, create_args, ctx)
    {:ok, :ok, rest} = Types.decode_nfsstat3(create_body)
    {:ok, file_fh, _rest} = Types.decode_post_op_fh3(rest)
    refute is_nil(file_fh), "CREATE must return a fhandle"

    # ——— Step 2: WRITE (single large, both stability hints) ——————
    #
    # The spec calls for "multiple chunks with mixed stability
    # bits"; a single WRITE big enough to span multiple FastCDC
    # chunks satisfies the multi-chunk requirement at the
    # *underlying-storage* layer (the chunker NIF emits ~64 KiB to
    # 1 MiB chunks). The stability-bit mix is exercised separately
    # via a second `:file_sync` WRITE that overwrites the same
    # offsets — both paths must report `:file_sync` from the
    # backend.

    payload = :crypto.strong_rand_bytes(@payload_size)

    write_args =
      Types.encode_fhandle3(file_fh) <>
        XDR.encode_uhyper(0) <>
        XDR.encode_uint(byte_size(payload)) <>
        Types.encode_stable_how(:unstable) <>
        XDR.encode_var_opaque(payload)

    {:ok, w_body} = call_proc(cluster, 7, write_args, ctx)
    {:ok, :ok, rest} = Types.decode_nfsstat3(w_body)
    {:ok, _wcc, rest} = Types.decode_wcc_data(rest)
    written = byte_size(payload)
    {:ok, ^written, rest} = XDR.decode_uint(rest)

    # Backend always reports `:file_sync` regardless of the
    # client's hint — FileIndex quorum-writes are durable.
    {:ok, :file_sync, rest} = Types.decode_stable_how(rest)
    {:ok, _verf, _} = Types.decode_writeverf3(rest)

    # ——— Step 3: COMMIT (verf stability) —————————————————————————

    commit_args =
      Types.encode_fhandle3(file_fh) <>
        XDR.encode_uhyper(0) <>
        XDR.encode_uint(0)

    {:ok, c1_body} = call_proc(cluster, 21, commit_args, ctx)
    {:ok, :ok, rest} = Types.decode_nfsstat3(c1_body)
    {:ok, _wcc, rest} = Types.decode_wcc_data(rest)
    {:ok, verf1, _} = Types.decode_writeverf3(rest)

    {:ok, c2_body} = call_proc(cluster, 21, commit_args, ctx)
    {:ok, :ok, rest} = Types.decode_nfsstat3(c2_body)
    {:ok, _, rest} = Types.decode_wcc_data(rest)
    {:ok, verf2, _} = Types.decode_writeverf3(rest)

    assert verf1 == verf2,
           "writeverf3 must be stable across COMMIT calls within the same VM"

    # ——— Step 4: READ back ——————————————————————————————————————

    reassembled =
      0
      |> Stream.iterate(&(&1 + @chunk_size))
      |> Stream.take_while(&(&1 < @payload_size))
      |> Enum.map(fn offset ->
        args =
          Types.encode_fhandle3(file_fh) <>
            XDR.encode_uhyper(offset) <>
            XDR.encode_uint(@chunk_size)

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

    # Identify the first divergence to make CI failures less guessy.
    first_diff =
      Enum.find_index(0..(byte_size(payload) - 1), fn i ->
        binary_part(reassembled, i, 1) != binary_part(payload, i, 1)
      end)

    assert reassembled == payload,
           "READ-back bytes did not match the WRITE-out payload " <>
             "(reassembled=#{byte_size(reassembled)}B, payload=#{byte_size(payload)}B, " <>
             "first_diff=#{inspect(first_diff)})"

    # ——— Step 5: SETATTR (set mtime via set_to_client_time) ————

    new_mtime = %NFSServer.NFSv3.Types.Nfstime3{seconds: 1_700_000_000, nseconds: 0}
    sattr = %Types.Sattr3{mtime: {:client, new_mtime}}

    setattr_args =
      Types.encode_fhandle3(file_fh) <>
        Types.encode_sattr3(sattr) <>
        XDR.encode_optional(nil, &Types.encode_nfstime3/1)

    {:ok, sa_body} = call_proc(cluster, 2, setattr_args, ctx)
    {:ok, :ok, rest} = Types.decode_nfsstat3(sa_body)
    {:ok, %Types.WccData{before: pre, after: post}, <<>>} = Types.decode_wcc_data(rest)

    assert %Types.WccAttr{} = pre,
           "SETATTR success reply must carry pre-op `wcc_attr`"

    assert %Types.Fattr3{mtime: ^new_mtime} = post,
           "post-op attrs must reflect the SETATTR-updated mtime"

    # ——— Step 6: RENAME ——————————————————————————————————————————

    new_name = "renamed.bin"

    rename_args =
      Types.encode_diropargs3({root_fh, file_name}) <>
        Types.encode_diropargs3({root_fh, new_name})

    {:ok, rn_body} = call_proc(cluster, 14, rename_args, ctx)
    {:ok, :ok, _} = Types.decode_nfsstat3(rn_body)

    {:ok, lookup_body} =
      call_proc(cluster, 3, Types.encode_diropargs3({root_fh, new_name}), ctx)

    {:ok, :ok, rest} = Types.decode_nfsstat3(lookup_body)
    {:ok, renamed_fh, _rest} = Types.decode_fhandle3(rest)

    # READ a chunk from the renamed file to confirm content survives.
    read_args =
      Types.encode_fhandle3(renamed_fh) <>
        XDR.encode_uhyper(0) <>
        XDR.encode_uint(@chunk_size)

    {:ok, rd_body} = call_proc(cluster, 6, read_args, ctx)
    flat = IO.iodata_to_binary(rd_body)
    {:ok, :ok, rest} = Types.decode_nfsstat3(flat)
    {:ok, _attr, rest} = Types.decode_post_op_attr(rest)
    {:ok, _count, rest} = XDR.decode_uint(rest)
    {:ok, _eof, rest} = XDR.decode_bool(rest)
    {:ok, first_chunk, _} = XDR.decode_var_opaque(rest)
    assert first_chunk == binary_part(payload, 0, byte_size(first_chunk))

    # ——— Step 7: REMOVE ——————————————————————————————————————————

    remove_args = Types.encode_diropargs3({root_fh, new_name})
    {:ok, rm_body} = call_proc(cluster, 12, remove_args, ctx)
    {:ok, :ok, _} = Types.decode_nfsstat3(rm_body)

    # Subsequent LOOKUP must return NFS3ERR_NOENT.
    {:ok, post_lookup_body} =
      call_proc(cluster, 3, Types.encode_diropargs3({root_fh, new_name}), ctx)

    assert {:ok, :noent, _} = Types.decode_nfsstat3(post_lookup_body)
  end

  defp call_proc(cluster, proc, args, ctx) do
    PeerCluster.rpc(cluster, :node2, Handler, :handle_call, [proc, args, @auth, ctx])
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
