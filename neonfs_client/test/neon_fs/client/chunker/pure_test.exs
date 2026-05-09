defmodule NeonFS.Client.Chunker.PureTest do
  use ExUnit.Case, async: true

  alias NeonFS.Client.Chunker.{Native, Pure}

  @opts %{min: 64 * 1024, avg: 256 * 1024, max: 1024 * 1024}

  describe "cut/2" do
    test "returns {0, size} when source is shorter than min" do
      assert Pure.cut(<<>>, @opts) == {0, 0}
      assert Pure.cut(:crypto.strong_rand_bytes(63 * 1024), @opts) == {0, 63 * 1024}
    end

    test "returns the same boundary as the Rust NIF on a representative random input" do
      data = :crypto.strong_rand_bytes(2 * 1024 * 1024)
      {_pure_hash, pure_end} = Pure.cut(data, @opts)

      {:ok, native_chunks} = Native.nif_chunk_data(data, "fastcdc", @opts.avg)
      {_data, _hash, native_offset, native_size} = hd(native_chunks)

      # The first native chunk's `length` is exactly the first cut.
      assert pure_end == native_offset + native_size
    end
  end

  describe "chunks/2 — parity with the Rust NIF" do
    @small_corpus_size 256 * 1024
    @medium_corpus_size 2 * 1024 * 1024

    test "produces identical (offset, length) lists on a 256 KiB random input" do
      assert_parity(:crypto.strong_rand_bytes(@small_corpus_size))
    end

    test "produces identical (offset, length) lists on a 2 MiB random input" do
      assert_parity(:crypto.strong_rand_bytes(@medium_corpus_size))
    end

    test "handles all-zero input the same way as the Rust NIF" do
      assert_parity(<<0::size(@medium_corpus_size * 8)>>)
    end

    test "handles a small mostly-uniform input (last-chunk path)" do
      # Slightly above min_size so we exit the cut_short loop and hit
      # the final-chunk fallthrough case.
      assert_parity(<<0xAA::size(80 * 1024 * 8)>>)
    end

    defp assert_parity(data) do
      pure = Pure.chunks(data, @opts)
      {:ok, native} = Native.nif_chunk_data(data, "fastcdc", @opts.avg)

      pure_pairs = Enum.map(pure, fn {_hash, offset, length} -> {offset, length} end)

      native_pairs =
        Enum.map(native, fn {_data, _hash, offset, length} -> {offset, length} end)

      assert pure_pairs == native_pairs,
             "chunk boundaries diverged — pure=#{inspect(Enum.take(pure_pairs, 5))}, " <>
               "native=#{inspect(Enum.take(native_pairs, 5))}"
    end
  end
end
