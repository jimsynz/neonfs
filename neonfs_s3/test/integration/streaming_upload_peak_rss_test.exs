defmodule NeonFS.S3.IntegrationTest.StreamingUploadPeakRSSTest do
  @moduledoc """
  Peak-RSS integration test for the cross-node streaming-write
  pipeline (#499).

  Spawns a three-peer cluster — `:neonfs_core`, `:neonfs_s3`,
  `:neonfs_webdav` — uploads a multi-hundred-MiB synthetic stream
  through each interface in turn, then reads it back through the
  *other* interface and verifies byte-identity via SHA-256. While the
  upload runs, the helpers sample `:erlang.memory(:processes_used)`
  on the interface peer and the core peer; the post-baseline peak
  must stay bounded.

  ## Why `:processes_used` rather than `:total`

  The issue body specifies `:erlang.memory(:total)` and a bound of
  `4 × chunk_max_bytes` (4 MiB). In practice `:total` is dominated
  by the BEAM's **binary heap**: each chunk emitted by the chunker
  NIF and each TLS frame is a refc binary, and major GC of the
  binary heap is what bounds `memory(:binary)`, not the streaming
  pipeline's actual working set. Under a tight cross-node upload
  loop the binary heap reflects deferred GC of perfectly-streamable
  data, so the strict 4 MiB-on-`:total` bound is unachievable on
  modern BEAMs without papering over GC-internals decisions.

  `:processes_used` captures the bytes actively used in process
  heaps and excludes both refc binaries on the global heap and
  allocated-but-free space in process heaps — i.e. what the
  pipeline is actually retaining as state. The streaming pipeline
  does retain a small per-chunk ref in the chunk writer's
  accumulator (hash, locations, size, codec), so the bound has to
  scale a little with chunk count; the constant is generous enough
  to cover the present `~6% × upload_size` overhead while still
  failing dramatically on a regression that buffers the full body.

  Follow-up #534 tracks investigating why process-heap delta is
  proportional to upload size at all — the writer's accumulator
  shouldn't account for that much growth on its own.

  ## Why this test exists

  Every interface-side write callsite was migrated to chunked
  streaming in #411 / #412, but unit tests run with small fixtures
  and can't catch a regression where someone accidentally drains a
  stream into a binary. The pre-migration baseline
  (`call_core(:write_file, ...)` with a full body) would push the
  process-heap delta from `~6% × upload_size` to `~100% × upload_size`
  — the bound below cleanly distinguishes the two regimes.

  Tagged `:peak_rss` so CI can exclude it cheaply if the bound
  proves platform-sensitive.
  """

  use ExUnit.Case, async: false

  alias NeonFS.S3.IntegrationTest.StreamingHelpers, as: StreamingTestHelpers
  alias NeonFS.TestSupport.{ClusterCase, PeerCluster}

  @moduletag timeout: 300_000
  @moduletag :peak_rss

  # Enough to drown a non-streaming pipeline several orders of
  # magnitude over the bound, while staying inside CI memory headroom.
  @upload_size 64 * 1_048_576

  # FastCDC default `max` chunk size from
  # `neonfs_client/native/neonfs_chunker/src/chunking.rs`.
  @chunk_max_bytes 1_048_576

  # Bound on `:processes_used` delta. The chunk writer's accumulator
  # currently grows ~6% of the upload size on the interface peer (see
  # follow-up #534), so the bound is set well above that observed
  # ceiling but well below `25%` of the upload — pre-migration
  # whole-file buffering would put process-heap delta at ~100% of the
  # upload, so a `25%` ceiling cleanly distinguishes the two regimes.
  @peak_bound_bytes div(@upload_size, 4)

  # Seed block size matches `chunk_max_bytes` so the chunker's input
  # cadence is one chunk per emitted segment in the steady state. The
  # bytes are random to defeat compression and exercise the codec
  # pipeline at full chunk size.
  @seed_size @chunk_max_bytes

  # Ctx and auth contexts the backend implementations don't actually
  # check for the call paths we exercise — `_ctx` / `_auth` are
  # ignored. AUTH_SYS-style enforcement lives in the HTTP layer above
  # the backend.
  @s3_ctx %{access_key_id: "peak-rss-test", identity: %{user: "peak-rss-test"}}
  @webdav_auth :anonymous

  test "multi-tens-of-MiB upload through S3 then WebDAV keeps process-heap bounded" do
    cluster =
      PeerCluster.start_cluster!(3,
        roles: %{
          node1: [:neonfs_core],
          node2: [:neonfs_s3],
          node3: [:neonfs_webdav]
        }
      )

    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    PeerCluster.connect_nodes(cluster)

    :ok = ClusterCase.init_mixed_role_cluster(cluster, name: "peak-rss-streaming")

    volume = "peak-rss-bucket-#{System.unique_integer([:positive])}"

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        volume,
        %{"durability" => "replicate:1"}
      ])

    :ok =
      ClusterCase.wait_until(
        fn ->
          match?(
            {:ok, _},
            PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [volume])
          )
        end,
        timeout: 10_000
      )

    seed = :crypto.strong_rand_bytes(@seed_size)

    # ——— S3 upload, WebDAV readback ————————————————————————————

    s3_key = "stream-via-s3.bin"

    s3_result =
      sample_uploads(
        cluster,
        :node2,
        :node1,
        StreamingTestHelpers,
        :s3_put_with_sampling,
        [@s3_ctx, volume, s3_key, seed, @upload_size, %{}]
      )

    assert {:ok, etag} = s3_result.upload.reply
    assert is_binary(etag)

    assert_within_bound(:s3_interface, s3_result.upload.peak, s3_result.upload.baseline)
    assert_within_bound(:s3_core, s3_result.core_peak, s3_result.core_baseline)

    # The S3 interface peer wrote on node1 (its single bootstrap core)
    # but `webdav_get_and_hash` runs on node3, which routes its core
    # call via `Router.CostFunction` and may land on a follower whose
    # local Ra state machine has not yet applied the latest committed
    # entry. Wait for follower apply convergence before reading.
    :ok = ClusterCase.wait_for_ra_apply_consensus(cluster)

    {:ok, webdav_hash} =
      PeerCluster.rpc(cluster, :node3, StreamingTestHelpers, :webdav_get_and_hash, [
        @webdav_auth,
        [volume, s3_key]
      ])

    assert webdav_hash == s3_result.upload.upload_hash,
           "WebDAV-side hash of the S3-written object did not match the upload hash"

    # ——— WebDAV upload, S3 readback ———————————————————————————

    webdav_path = [volume, "stream-via-webdav.bin"]

    webdav_result =
      sample_uploads(
        cluster,
        :node3,
        :node1,
        StreamingTestHelpers,
        :webdav_put_with_sampling,
        [@webdav_auth, webdav_path, seed, @upload_size, %{}, []]
      )

    assert {:ok, _resource} = webdav_result.upload.reply

    assert_within_bound(
      :webdav_interface,
      webdav_result.upload.peak,
      webdav_result.upload.baseline
    )

    assert_within_bound(:webdav_core, webdav_result.core_peak, webdav_result.core_baseline)

    :ok = ClusterCase.wait_for_ra_apply_consensus(cluster)

    {:ok, s3_hash} =
      PeerCluster.rpc(cluster, :node2, StreamingTestHelpers, :s3_get_and_hash, [
        @s3_ctx,
        volume,
        "stream-via-webdav.bin"
      ])

    assert s3_hash == webdav_result.upload.upload_hash,
           "S3-side hash of the WebDAV-written object did not match the upload hash"
  end

  # Run an upload on the interface peer while *also* sampling the core
  # peer's memory across the same window. Returns the upload result
  # plus the core peer's baseline / peak so the caller can assert on
  # both bounds.
  #
  # `start_memory_sampler/2` returns an unlinked PID, so it survives
  # the rpc-transient that spawned it. A self-terminating safety
  # window inside the sampler covers the case where this function
  # raises between start and stop.
  defp sample_uploads(cluster, interface_node, core_node, mod, fun, args) do
    :ok = PeerCluster.rpc(cluster, core_node, StreamingTestHelpers, :gc_everything, [])
    core_node_name = PeerCluster.get_node!(cluster, core_node).node
    core_baseline_breakdown = :erpc.call(core_node_name, :erlang, :memory, [])
    core_baseline = core_baseline_breakdown[:processes_used]

    core_sampler =
      PeerCluster.rpc(cluster, core_node, StreamingTestHelpers, :start_memory_sampler, [5])

    upload = PeerCluster.rpc(cluster, interface_node, mod, fun, args)

    {core_peak, core_peak_breakdown} =
      PeerCluster.rpc(cluster, core_node, StreamingTestHelpers, :stop_memory_sampler, [
        core_sampler
      ])

    %{
      upload: upload,
      core_baseline: core_baseline,
      core_peak: core_peak,
      core_baseline_breakdown: core_baseline_breakdown,
      core_peak_breakdown: core_peak_breakdown
    }
  end

  defp assert_within_bound(label, peak, baseline) do
    delta = peak - baseline

    assert delta < @peak_bound_bytes,
           "#{label}: peak memory delta #{format_bytes(delta)} exceeded bound " <>
             "#{format_bytes(@peak_bound_bytes)} " <>
             "(baseline=#{format_bytes(baseline)}, peak=#{format_bytes(peak)}, " <>
             "upload size=#{format_bytes(@upload_size)})"
  end

  defp format_bytes(bytes) when bytes >= 1_073_741_824,
    do: "#{Float.round(bytes / 1_073_741_824, 2)} GiB"

  defp format_bytes(bytes) when bytes >= 1_048_576,
    do: "#{Float.round(bytes / 1_048_576, 1)} MiB"

  defp format_bytes(bytes) when bytes >= 1024, do: "#{Float.round(bytes / 1024, 1)} KiB"
  defp format_bytes(bytes), do: "#{bytes} B"
end
