defmodule NeonFS.Client.Chunker.NativeTest do
  @moduledoc """
  Smoke tests for the client-side chunker NIF (#449).

  Cross-package parity against `NeonFS.Core.Blob.Native` lives in
  `neonfs_core/test/neon_fs/core/chunker_parity_test.exs` because
  `neonfs_client` doesn't depend on `neonfs_core`; that suite proves
  the two chunkers emit identical `(hash, offset, size)` output.

  The tests here just confirm the NIF loaded and the five functions
  exposed round-trip sensibly — any deeper invariants are covered by
  the parity suite.
  """

  use ExUnit.Case, async: true

  alias NeonFS.Client.Chunker.Native

  describe "compute_hash/1" do
    test "returns a 32-byte SHA-256 hash" do
      hash = Native.compute_hash("hello world")
      assert byte_size(hash) == 32
      # Spot-check the known SHA-256 of "hello world"
      assert Base.encode16(hash, case: :lower) ==
               "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    end

    test "distinct inputs produce distinct hashes" do
      refute Native.compute_hash("a") == Native.compute_hash("b")
    end
  end

  describe "auto_chunk_strategy/1" do
    test "small data uses single" do
      assert Native.auto_chunk_strategy(1_000) == {"single", 0}
    end

    test "medium data uses fixed 256KB" do
      assert Native.auto_chunk_strategy(100_000) == {"fixed", 262_144}
    end

    test "large data uses fastcdc 256KB avg" do
      assert Native.auto_chunk_strategy(2_000_000) == {"fastcdc", 262_144}
    end
  end

  describe "chunk_data/3" do
    test "fixed strategy splits into equal-sized chunks" do
      data = :binary.copy(<<0>>, 1024)
      {:ok, chunks} = Native.chunk_data(data, "fixed", 256)

      assert length(chunks) == 4

      for {chunk_data, hash, offset, size} <- chunks do
        assert byte_size(chunk_data) == 256
        assert byte_size(hash) == 32
        assert rem(offset, 256) == 0
        assert size == 256
      end
    end

    test "single strategy emits one chunk" do
      data = :crypto.strong_rand_bytes(4096)
      {:ok, [{chunk_data, hash, 0, size}]} = Native.chunk_data(data, "single", 0)
      assert chunk_data == data
      assert size == byte_size(data)
      assert hash == Native.compute_hash(data)
    end

    test "rejects unknown strategy" do
      assert {:error, msg} = Native.chunk_data("x", "bogus", 0)
      assert msg =~ "invalid strategy"
    end

    test "fixed strategy rejects zero chunk size" do
      assert {:error, msg} = Native.chunk_data("x", "fixed", 0)
      assert msg =~ "non-zero"
    end
  end

  describe "incremental chunker" do
    test "feed + finish round-trip matches stateless chunk_data for fixed strategy" do
      data = :crypto.strong_rand_bytes(4096)

      {:ok, chunker} = Native.chunker_init("fixed", 256)

      streaming =
        data
        |> split_every(123)
        |> Enum.flat_map(&Native.chunker_feed(chunker, &1))
        |> Kernel.++(Native.chunker_finish(chunker))

      {:ok, one_shot} = Native.chunk_data(data, "fixed", 256)

      assert strip_data(streaming) == strip_data(one_shot)
    end

    test "feed emits complete chunks and buffers the rest" do
      {:ok, chunker} = Native.chunker_init("fixed", 10)

      assert Native.chunker_feed(chunker, "abcde") == []
      [{first_data, _hash, 0, 10}] = Native.chunker_feed(chunker, "fghij_tail")

      assert first_data == "abcdefghij"

      [{tail_data, _hash, 10, 5}] = Native.chunker_finish(chunker)
      assert tail_data == "_tail"
    end

    test "rejects unknown strategy" do
      assert {:error, _msg} = Native.chunker_init("bogus", 0)
    end
  end

  defp strip_data(chunks) do
    Enum.map(chunks, fn {_data, hash, offset, size} -> {hash, offset, size} end)
  end

  defp split_every(<<>>, _size), do: []
  defp split_every(data, size) when byte_size(data) <= size, do: [data]

  defp split_every(data, size) do
    <<head::binary-size(size), rest::binary>> = data
    [head | split_every(rest, size)]
  end
end
