defmodule NeonFS.S3.PeakRSSTest do
  @moduledoc """
  Peak memory regression test for the S3 multipart upload path (#490).

  Uploads #{8} parts of 64 MiB through `NeonFS.S3.Backend.upload_part_stream/6`
  and `complete_multipart_upload/5` and asserts that BEAM `:erlang.memory(:total)`
  never grows by more than `2 × part_size` (128 MiB) above the post-setup
  baseline. The pre-migration code (#411) buffered every staged part in
  `complete_multipart_upload` before writing the assembled file, pushing
  peak memory to the full upload size (8 × 64 MiB = 512 MiB); this test
  fails on that baseline.

  The test mocks `:ship_chunks_fn` and `:commit_refs_fn` with streaming-
  friendly fakes that consume chunks without retaining bytes, so the
  measurement reflects the S3 backend's in-process allocations rather than
  the mock's. A real cluster is not required.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Core.FileMeta
  alias NeonFS.S3.Backend
  alias NeonFS.S3.MultipartStore
  alias NeonFS.S3.Test.MockCore

  @ctx %{access_key_id: "test-key", identity: %{user: "test-key"}}

  @bucket "rss-bucket"
  @key "big.bin"

  @part_size 64 * 1024 * 1024
  @num_parts 8
  @source_chunk_size 65_536

  @peak_bound_bytes 2 * @part_size

  @sample_interval_ms 5

  @moduletag :peak_rss

  setup do
    MockCore.setup()
    {:ok, _volume} = MockCore.create_volume(@bucket)
    :ok = MockCore.add_credential("test-key", "test-secret")

    Application.put_env(:neonfs_s3, :core_call_fn, fn function, args ->
      apply(MockCore, function, args)
    end)

    Application.put_env(:neonfs_s3, :ship_chunks_fn, fn _bucket, _key, body ->
      size = drain_and_count(body)
      hash = :crypto.strong_rand_bytes(32)

      ref = %{
        hash: hash,
        locations: [%{node: node(), drive_id: "default", tier: :hot}],
        size: size,
        codec: %{compression: :none, crypto: nil, original_size: size}
      }

      {:ok, [ref]}
    end)

    Application.put_env(:neonfs_s3, :commit_refs_fn, fn _bucket, _key, refs, _opts ->
      total = refs |> Enum.map(& &1.size) |> Enum.sum()
      now = DateTime.utc_now()

      meta = %FileMeta{
        id: "rss-test-file",
        volume_id: "rss-test-volume",
        path: "/" <> @key,
        chunks: [],
        size: total,
        content_type: "application/octet-stream",
        mode: 0o644,
        uid: 0,
        gid: 0,
        created_at: now,
        modified_at: now,
        accessed_at: now,
        changed_at: now,
        version: 1
      }

      {:ok, meta}
    end)

    start_supervised!(MultipartStore)

    on_exit(fn ->
      Application.delete_env(:neonfs_s3, :core_call_fn)
      Application.delete_env(:neonfs_s3, :ship_chunks_fn)
      Application.delete_env(:neonfs_s3, :commit_refs_fn)
    end)

    :ok
  end

  test "peak memory during multipart upload stays below #{div(@peak_bound_bytes, 1_048_576)} MiB" do
    {:ok, upload_id} = Backend.create_multipart_upload(@ctx, @bucket, @key, %{})

    :erlang.garbage_collect()
    baseline = :erlang.memory(:total)
    sampler = start_memory_sampler(@sample_interval_ms)

    for part_number <- 1..@num_parts do
      stream = part_stream(@part_size)

      {:ok, etag} =
        Backend.upload_part_stream(@ctx, @bucket, @key, upload_id, part_number, stream)

      assert is_binary(etag)
    end

    {:ok, _result} =
      Backend.complete_multipart_upload(@ctx, @bucket, @key, upload_id, [])

    peak = stop_memory_sampler(sampler)

    delta = peak - baseline

    assert delta < @peak_bound_bytes,
           "peak memory delta #{format_bytes(delta)} exceeded bound " <>
             "#{format_bytes(@peak_bound_bytes)} " <>
             "(baseline=#{format_bytes(baseline)}, peak=#{format_bytes(peak)}, " <>
             "total uploaded=#{format_bytes(@num_parts * @part_size)})"
  end

  defp part_stream(size) when size > 0 do
    chunk = :crypto.strong_rand_bytes(@source_chunk_size)

    Stream.unfold(size, fn
      0 ->
        nil

      remaining when remaining >= @source_chunk_size ->
        {chunk, remaining - @source_chunk_size}

      remaining ->
        {binary_part(chunk, 0, remaining), 0}
    end)
  end

  defp drain_and_count(body) when is_binary(body), do: byte_size(body)

  defp drain_and_count(body) do
    Enum.reduce(body, 0, fn segment, acc -> acc + IO.iodata_length(segment) end)
  end

  defp start_memory_sampler(interval_ms) do
    parent = self()

    pid =
      spawn_link(fn ->
        monitor_ref = Process.monitor(parent)
        sample_loop(0, interval_ms, monitor_ref, parent)
      end)

    pid
  end

  defp sample_loop(peak, interval_ms, monitor_ref, parent) do
    receive do
      {:DOWN, ^monitor_ref, :process, _, _} ->
        :ok

      {:report, reply_to} ->
        send(reply_to, {:peak, peak})
    after
      interval_ms ->
        mem = :erlang.memory(:total)
        sample_loop(max(peak, mem), interval_ms, monitor_ref, parent)
    end
  end

  defp stop_memory_sampler(pid) do
    send(pid, {:report, self()})

    receive do
      {:peak, peak} -> peak
    after
      5_000 -> raise "memory sampler did not respond within 5s"
    end
  end

  defp format_bytes(bytes) do
    cond do
      bytes >= 1_048_576 -> "#{Float.round(bytes / 1_048_576, 1)} MiB"
      bytes >= 1024 -> "#{Float.round(bytes / 1024, 1)} KiB"
      true -> "#{bytes} B"
    end
  end
end
