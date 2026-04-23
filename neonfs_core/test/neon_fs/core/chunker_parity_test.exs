defmodule NeonFS.Core.ChunkerParityTest do
  @moduledoc """
  Property test that `NeonFS.Client.Chunker.Native` (the new client-
  side chunker added in #449) produces byte-for-byte identical output
  to `NeonFS.Core.Blob.Native` for the same input.

  Content-addressed deduplication depends on both chunkers agreeing
  on `(hash, offset, size)` tuples — if they drift, interface-side
  streaming writes (#450/#299) would break dedup against chunks
  written through the co-located path.

  The test lives in `neonfs_core` rather than `neonfs_client` because
  it needs both modules, and `neonfs_client` doesn't depend on
  `neonfs_core` (only the reverse). Keeping the cross-package check
  here avoids a test-only dep loop.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NeonFS.Client.Chunker.Native, as: ClientChunker
  alias NeonFS.Core.Blob.Native, as: CoreChunker

  describe "compute_hash/1 parity" do
    property "matches on arbitrary binaries" do
      check all(data <- StreamData.binary(max_length: 8 * 1024)) do
        assert ClientChunker.compute_hash(data) == CoreChunker.compute_hash(data)
      end
    end
  end

  describe "auto_chunk_strategy/1 parity" do
    property "matches on arbitrary sizes" do
      check all(len <- StreamData.integer(0..(4 * 1024 * 1024))) do
        assert ClientChunker.auto_chunk_strategy(len) == CoreChunker.auto_chunk_strategy(len)
      end
    end
  end

  describe "stateless chunk_data/3 parity" do
    property "fixed strategy produces identical chunks" do
      check all(
              data <- StreamData.binary(min_length: 0, max_length: 256 * 1024),
              size <- StreamData.integer(1..(32 * 1024))
            ) do
        {:ok, client_chunks} = ClientChunker.chunk_data(data, "fixed", size)
        {:ok, core_chunks} = CoreChunker.chunk_data(data, "fixed", size)

        assert strip_data(client_chunks) == strip_data(core_chunks)
      end
    end

    property "single strategy produces identical chunks" do
      check all(data <- StreamData.binary(max_length: 256 * 1024)) do
        {:ok, client_chunks} = ClientChunker.chunk_data(data, "single", 0)
        {:ok, core_chunks} = CoreChunker.chunk_data(data, "single", 0)

        assert strip_data(client_chunks) == strip_data(core_chunks)
      end
    end

    property "auto strategy produces identical chunks across size classes" do
      check all(data <- StreamData.binary(max_length: 1_500_000)) do
        {:ok, client_chunks} = ClientChunker.chunk_data(data, "auto", byte_size(data))
        {:ok, core_chunks} = CoreChunker.chunk_data(data, "auto", byte_size(data))

        assert strip_data(client_chunks) == strip_data(core_chunks)
      end
    end
  end

  describe "stateful chunker parity with stateless" do
    property "incremental feed+finish matches one-shot chunk_data for fixed strategy" do
      check all(
              data <- StreamData.binary(max_length: 256 * 1024),
              chunk_feed_size <- StreamData.integer(1..8192),
              cdc_size <- StreamData.integer(64..16_384)
            ) do
        # Interface callers stream bytes in via chunker_feed/2. The
        # final chunk list must match the stateless chunk_data/3
        # output for the same input.
        {:ok, chunker} = ClientChunker.chunker_init("fixed", cdc_size)

        streaming_chunks =
          data
          |> stream_in(chunk_feed_size)
          |> Enum.flat_map(&ClientChunker.chunker_feed(chunker, &1))
          |> Kernel.++(ClientChunker.chunker_finish(chunker))

        {:ok, one_shot_chunks} = ClientChunker.chunk_data(data, "fixed", cdc_size)

        assert strip_data(streaming_chunks) == strip_data(one_shot_chunks)
      end
    end

    property "stateful chunker matches NeonFS.Core.Blob.Native stateful chunker" do
      check all(
              data <- StreamData.binary(max_length: 512 * 1024),
              feed_size <- StreamData.integer(1..16_384)
            ) do
        {:ok, client} = ClientChunker.chunker_init("fixed", 16_384)
        {:ok, core} = CoreChunker.chunker_init("fixed", 16_384)

        # Drive both chunkers with the same input stream. Because each
        # slice is produced identically the emitted chunks should
        # agree slice-for-slice, as should the final flush.
        client_chunks =
          data
          |> stream_in(feed_size)
          |> Enum.flat_map(&ClientChunker.chunker_feed(client, &1))
          |> Kernel.++(ClientChunker.chunker_finish(client))

        core_chunks =
          data
          |> stream_in(feed_size)
          |> Enum.flat_map(&CoreChunker.chunker_feed(core, &1))
          |> Kernel.++(CoreChunker.chunker_finish(core))

        assert strip_data(client_chunks) == strip_data(core_chunks)
      end
    end
  end

  # Compare `(hash, offset, size)` rather than `(data, hash, offset,
  # size)` — the data-binary is redundant with hash+size and comparing
  # it bloats failure output without adding coverage.
  defp strip_data(chunks) do
    Enum.map(chunks, fn {_data, hash, offset, size} -> {hash, offset, size} end)
  end

  defp stream_in(<<>>, _size), do: []

  defp stream_in(data, size) when byte_size(data) <= size, do: [data]

  defp stream_in(data, size) do
    <<head::binary-size(size), rest::binary>> = data
    [head | stream_in(rest, size)]
  end
end
