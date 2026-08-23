defmodule NeonFS.Block.WriteWindowBenchTest do
  @moduledoc """
  What the write window changes, measured where it changes it: guest writes
  in, metadata commits out.

  The transport is stubbed, deliberately. A commit's real cost is a
  consensus round whose latency belongs to the cluster, and mixing it in
  here would measure the cluster rather than the coalescing. What this
  reports is how many commits a workload produces and how much the chunk
  layer moves to serve it — the two numbers the window exists to reduce,
  and the ones that stay comparable across machines.

  Guest IOPS and flush latency against a real device are the rig's, with
  `fio`. They are the confirmation; this is the mechanism.

  Not run by default — tagged `:benchmark`. Run with:

      mix test test/neon_fs/block/write_window_bench_test.exs --include benchmark
  """

  use ExUnit.Case, async: false

  alias NeonFS.Block.{Device, WriteWindow}

  @moduletag :benchmark
  @moduletag timeout: 300_000

  @block 4096
  @chunk 131_072
  @size 512 * @chunk
  @writes 2_000

  setup do
    on_exit(fn ->
      for key <- [
            :core_call_fn,
            :write_chunks_fn,
            :fetch_chunk_fn,
            :write_window_bytes,
            :write_window_ms
          ] do
        Application.delete_env(:neonfs_block, key)
      end
    end)

    :ok
  end

  test "sequential 4 KiB writes: commits and amplification, window off and on" do
    rows =
      for {label, cap} <- [{"off", 0}, {"1 MiB", 1_048_576}] do
        Application.put_env(:neonfs_block, :write_window_bytes, cap)
        # Long enough that the timer never fires mid-run: this measures the
        # byte cap and the flush, not the clock.
        Application.put_env(:neonfs_block, :write_window_ms, 300_000)
        measure(label)
      end

    report(rows)

    [off, on] = rows

    # The whole claim, as an assertion rather than a printed number: the
    # window turns many commits into few, and many rewritten extents into
    # about as many as the writes actually covered.
    assert on.commits < div(off.commits, 10)
    assert on.amplification < off.amplification / 10
  end

  defp measure(label) do
    {:ok, counter} = Agent.start_link(fn -> %{commits: 0, extents: 0, chunk_bytes: 0} end)
    stub(counter)

    {:ok, opened} = Device.open("vol:/dev.img")
    window = start_supervised!({WriteWindow, opened}, id: {:window, label})
    device = Map.put(opened, :window, window)

    started = System.monotonic_time()

    for n <- 0..(@writes - 1) do
      :ok = Device.write(device, n * @block, :binary.copy(<<1>>, @block))
    end

    :ok = WriteWindow.flush(window)
    elapsed = System.monotonic_time() - started

    counts = Agent.get(counter, & &1)
    Agent.stop(counter)

    Map.merge(counts, %{
      label: label,
      writes: @writes,
      amplification: counts.chunk_bytes / (@writes * @block),
      writes_per_second:
        @writes / (System.convert_time_unit(elapsed, :native, :microsecond) / 1_000_000)
    })
  end

  defp report(rows) do
    IO.puts("""

    == block write window: #{@writes} sequential 4 KiB writes, #{div(@chunk, 1024)} KiB extents ==
    window        commits    extents    amplification   writes/s (stubbed transport)\
    """)

    for row <- rows do
      IO.puts(
        "#{pad(row.label, 14)}#{pad(row.commits, 11)}#{pad(row.extents, 11)}" <>
          "#{pad("#{Float.round(row.amplification, 1)}x", 16)}" <>
          "#{Float.round(row.writes_per_second, 0)}"
      )
    end

    IO.puts("")
  end

  defp pad(value, width), do: String.pad_trailing(to_string(value), width)

  defp stub(counter) do
    Application.put_env(:neonfs_block, :core_call_fn, fn _module, function, args ->
      core_reply(counter, function, args)
    end)

    Application.put_env(:neonfs_block, :write_chunks_fn, fn _volume, chunks ->
      Agent.update(counter, fn c ->
        %{c | chunk_bytes: c.chunk_bytes + Enum.sum(Enum.map(chunks, &byte_size/1))}
      end)

      {:ok, Enum.map(chunks, &chunk_ref/1)}
    end)

    Application.put_env(:neonfs_block, :fetch_chunk_fn, fn _volume, ref, _opts ->
      {:ok, :binary.copy(<<0>>, ref.width)}
    end)
  end

  defp core_reply(_counter, :open_device, _args) do
    {:ok,
     %{
       id: "device-id",
       size: @size,
       chunk_bytes: @chunk,
       epoch: 0,
       logical_block_bytes: @block,
       physical_block_bytes: @block
     }}
  end

  defp core_reply(_counter, :read_refs, [_volume, _path, offset, length]) do
    first = div(offset, @chunk)
    last = div(offset + length - 1, @chunk)

    extents =
      Enum.map(first..last, fn index ->
        extent_start = index * @chunk

        %{
          index: index,
          width: min(@chunk, @size - extent_start),
          read_start: max(offset, extent_start) - extent_start,
          read_length: min(offset + length, extent_start + @chunk) - max(offset, extent_start),
          target: :hole,
          hash: nil,
          locations: [],
          compression: :none,
          encrypted: false
        }
      end)

    {:ok, %{chunk_bytes: @chunk, size: @size, extents: extents}}
  end

  defp core_reply(counter, :commit_written, [_volume, _path, extents, _opts]) do
    Agent.update(counter, fn c ->
      %{c | commits: c.commits + 1, extents: c.extents + length(extents)}
    end)

    {:ok, %{chunks_published: length(extents)}}
  end

  defp core_reply(_counter, _other, _args), do: :ok

  defp chunk_ref(data) do
    %{
      hash: :crypto.hash(:sha256, data),
      locations: [%{node: node(), drive_id: "default", tier: :hot}],
      size: byte_size(data),
      codec: %{compression: :none, crypto: nil, original_size: byte_size(data)}
    }
  end
end
