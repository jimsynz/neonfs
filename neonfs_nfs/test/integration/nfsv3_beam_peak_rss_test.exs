defmodule NeonFS.Integration.NFSv3BeamPeakRSSTest do
  @moduledoc """
  Peak-`:processes_used` bound for the BEAM NFSv3 read path
  (sub-issue #589 of #533). The cross-node read test (#588) proves
  correctness; this test proves the streaming property the rest of
  the codebase has invariant-asserted (cf. #534, #499).

  ## What's exercised

    * 16-MiB file (16 × 1 MiB chunks) on a 1-core / 1-interface
      mixed-role cluster — same shape as #588 so the harness
      assumptions are stable.
    * `READ` loop covering the whole file, one per chunk-cap window.
      `:erlang.memory(:processes_used)` sampled before each call;
      peak − baseline must stay under `4 × chunk_max_bytes` (4 MiB).
      The 4× multiplier covers two in-flight chunks plus the OTP
      `:ssl_gen_statem` term per the analysis in #534.

  Tagged `:peak_rss` so CI can exclude it cheaply if the bound proves
  platform-sensitive.

  ## Why `:processes_used` rather than `:total`

  Same rationale as `streaming_upload_peak_rss_test.exs` in
  `neonfs_s3`: `:total` is dominated by the BEAM's binary heap
  (refc binaries from the chunker / TLS frames), and major GC of
  the binary heap is what bounds `memory(:binary)`, not the
  pipeline's actual working set. `:processes_used` captures only
  bytes actively used in process heaps — the streaming pipeline's
  retained state.
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
  @moduletag :peak_rss

  @auth %Auth.None{}

  # FastCDC default `max` chunk size from
  # `neonfs_client/native/neonfs_chunker/src/chunking.rs`.
  @chunk_max 1_048_576
  @payload_size 16 * @chunk_max

  # Bound on `:processes_used` delta during the READ loop. 4 × chunk
  # size — see the moduledoc and #589's tuning notes.
  @peak_bound_bytes 4 * @chunk_max

  test "READ loop on the interface peer keeps process heap under 4× chunk size" do
    cluster =
      PeerCluster.start_cluster!(2,
        roles: %{
          node1: [:neonfs_core],
          node2: [:neonfs_nfs]
        }
      )

    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    PeerCluster.connect_nodes(cluster)
    :ok = ClusterCase.init_mixed_role_cluster(cluster, name: "nfsv3-peak-rss")

    volume = "nfsv3-peak-rss-vol-#{System.unique_integer([:positive])}"
    file_name = "stream.bin"
    file_path = "/" <> file_name

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

    # Compression off + durability=1 so chunks ride the TLS data
    # plane rather than the Erlang-RPC fallback (see #588 for the
    # `needs_server_processing?` rationale).
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

    ctx = %{call: nil, nfs_v3_backend: NeonFS.NFS.NFSv3Backend}

    {:ok, lookup_body} =
      PeerCluster.rpc(cluster, :node2, Handler, :handle_call, [
        3,
        Types.encode_diropargs3({root_fh, file_name}),
        @auth,
        ctx
      ])

    {:ok, :ok, rest} = Types.decode_nfsstat3(lookup_body)
    {:ok, file_fh, _rest} = Types.decode_fhandle3(rest)

    # GC node2 to drop transient heap noise from the cluster setup
    # before sampling the baseline.
    PeerCluster.rpc(cluster, :node2, :erlang, :garbage_collect, [])
    baseline = PeerCluster.rpc(cluster, :node2, :erlang, :memory, [:processes_used])

    {peak, reassembled} =
      0
      |> Stream.iterate(&(&1 + @chunk_max))
      |> Stream.take_while(&(&1 < @payload_size))
      |> Enum.reduce({baseline, []}, fn offset, {peak_acc, parts_acc} ->
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

        sample = PeerCluster.rpc(cluster, :node2, :erlang, :memory, [:processes_used])
        {max(peak_acc, sample), [data | parts_acc]}
      end)

    reassembled = parts_to_binary(reassembled)

    # Sanity: the read actually round-trips. A peak-RSS bound on a
    # broken read path would be vacuously satisfiable.
    assert byte_size(reassembled) == @payload_size
    assert reassembled == payload

    delta = peak - baseline

    assert delta < @peak_bound_bytes,
           "peak `:processes_used` delta on the interface peer was " <>
             "#{format_bytes(delta)} (baseline=#{format_bytes(baseline)}, " <>
             "peak=#{format_bytes(peak)}) — bound is #{format_bytes(@peak_bound_bytes)}. " <>
             "Use `NeonFS.S3.IntegrationTest.ProcessMemoryProfile` to attribute the growth."
  end

  defp parts_to_binary(parts), do: parts |> Enum.reverse() |> IO.iodata_to_binary()

  defp format_bytes(bytes) when bytes >= 1_048_576,
    do: "#{Float.round(bytes / 1_048_576, 2)} MiB"

  defp format_bytes(bytes) when bytes >= 1024, do: "#{Float.round(bytes / 1024, 1)} KiB"
  defp format_bytes(bytes), do: "#{bytes} B"

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
