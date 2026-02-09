defmodule NeonFS.Core.Blob.NativeErasureTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Blob.Native

  describe "erasure_encode/2" do
    test "encodes data shards and returns correct number of parity shards" do
      data = [<<1, 2, 3, 4>>, <<5, 6, 7, 8>>, <<9, 10, 11, 12>>]
      assert {:ok, parity} = Native.erasure_encode(data, 2)
      assert length(parity) == 2
      assert Enum.all?(parity, &(byte_size(&1) == 4))
    end

    test "single data shard with parity" do
      data = [<<42, 43, 44, 45>>]
      assert {:ok, parity} = Native.erasure_encode(data, 2)
      assert length(parity) == 2
    end

    test "returns error for empty data shards" do
      assert {:error, reason} = Native.erasure_encode([], 2)
      assert reason =~ "no data shards"
    end

    test "returns error for zero parity count" do
      assert {:error, reason} = Native.erasure_encode([<<1, 2>>], 0)
      assert reason =~ "parity count must be > 0"
    end

    test "returns error for unequal shard sizes" do
      data = [<<1, 2, 3>>, <<4, 5>>]
      assert {:error, reason} = Native.erasure_encode(data, 1)
      assert reason =~ "not equal"
    end
  end

  describe "erasure_decode/4" do
    test "reconstructs missing data shards" do
      data = [<<10, 20, 30, 40>>, <<50, 60, 70, 80>>, <<90, 100, 110, 120>>]
      {:ok, parity} = Native.erasure_encode(data, 2)

      # Drop shard 1 (data) — keep shards 0, 2, and first parity (index 3)
      available = [{0, Enum.at(data, 0)}, {2, Enum.at(data, 2)}, {3, Enum.at(parity, 0)}]
      assert {:ok, recovered} = Native.erasure_decode(available, 3, 2, 4)
      assert recovered == data
    end

    test "reconstructs when all data shards are missing using only parity" do
      data = [<<42, 43, 44, 45>>]
      {:ok, parity} = Native.erasure_encode(data, 2)

      # Only parity shards available
      available = [{1, Enum.at(parity, 0)}, {2, Enum.at(parity, 1)}]
      assert {:ok, recovered} = Native.erasure_decode(available, 1, 2, 4)
      assert recovered == data
    end

    test "roundtrip with larger shards" do
      shard_size = 1024
      data = for _i <- 0..4, do: :crypto.strong_rand_bytes(shard_size)

      {:ok, parity} = Native.erasure_encode(data, 3)
      assert length(parity) == 3

      # Drop 3 data shards (indices 0, 1, 2), keep 2 data + 3 parity = 5 >= 5
      parity_tuples =
        parity
        |> Enum.with_index(5)
        |> Enum.map(fn {shard, idx} -> {idx, shard} end)

      available = [{3, Enum.at(data, 3)}, {4, Enum.at(data, 4)}] ++ parity_tuples

      assert {:ok, recovered} = Native.erasure_decode(available, 5, 3, shard_size)
      assert recovered == data
    end

    test "returns error for insufficient shards" do
      assert {:error, reason} = Native.erasure_decode([{0, <<1, 2>>}], 3, 2, 2)
      assert reason =~ "insufficient"
    end

    test "returns error for zero data count" do
      assert {:error, reason} = Native.erasure_decode([], 0, 2, 4)
      assert reason =~ "data shard count must be > 0"
    end

    test "returns error for zero parity count" do
      assert {:error, reason} = Native.erasure_decode([{0, <<1>>}], 1, 0, 1)
      assert reason =~ "parity count must be > 0"
    end

    test "returns error for index out of range" do
      assert {:error, reason} = Native.erasure_decode([{10, <<1, 2>>}, {11, <<3, 4>>}], 2, 1, 2)
      assert reason =~ "out of range"
    end

    test "returns error for shard size mismatch" do
      assert {:error, reason} = Native.erasure_decode([{0, <<1, 2, 3>>}, {1, <<4, 5>>}], 2, 1, 3)
      assert reason =~ "does not match"
    end
  end

  describe "encode/decode roundtrip" do
    test "survives maximum shard loss" do
      # 10 data + 4 parity, lose exactly 4 shards
      shard_size = 64
      data = for _i <- 0..9, do: :crypto.strong_rand_bytes(shard_size)
      {:ok, parity} = Native.erasure_encode(data, 4)

      # Keep first 6 data shards + all 4 parity shards = 10 >= 10 data_count
      available =
        (data |> Enum.take(6) |> Enum.with_index() |> Enum.map(fn {s, i} -> {i, s} end)) ++
          (parity |> Enum.with_index(10) |> Enum.map(fn {s, i} -> {i, s} end))

      assert {:ok, recovered} = Native.erasure_decode(available, 10, 4, shard_size)
      assert recovered == data
    end
  end
end
