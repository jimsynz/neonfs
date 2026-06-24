defmodule NeonFS.Core.BlobStorePropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NeonFS.Core.Blob.Native
  alias NeonFS.Core.BlobStore

  @moduletag :tmp_dir

  # Each property runs ~100 synchronous compress/encrypt writes. These finish
  # in seconds locally but can exceed ExUnit's default 60s per-test timeout on
  # a heavily contended CI runner, flaking the suite (#1394). Raise the
  # per-test budget — the assertions are about correctness, not latency.
  @moduletag timeout: 120_000

  # The default 5s `GenServer.call` timeout is too tight on a heavily contended
  # CI runner (#1132), so give every call generous headroom too.
  @call_timeout 30_000

  setup %{tmp_dir: tmp_dir} do
    File.mkdir_p!(tmp_dir)

    server = :"blob_prop_#{System.unique_integer([:positive, :monotonic])}"
    drives = [%{id: "default", path: tmp_dir, tier: :hot, capacity: 100_000_000}]

    {:ok, _pid} =
      start_supervised({BlobStore, drives: drives, prefix_depth: 2, name: server})

    on_exit(fn -> File.rm_rf!(tmp_dir) end)

    %{server: server}
  end

  describe "hashing" do
    property "compute_hash is deterministic for identical inputs" do
      check all(data <- StreamData.binary(min_length: 0, max_length: 65_536), max_runs: 200) do
        hash_a = Native.compute_hash(data)
        hash_b = Native.compute_hash(data)

        assert hash_a == hash_b
        assert byte_size(hash_a) == 32
      end
    end

    property "different data produces different hashes" do
      check all(
              data_a <- StreamData.binary(min_length: 1, max_length: 65_536),
              data_b <- StreamData.binary(min_length: 1, max_length: 65_536),
              data_a != data_b,
              max_runs: 200
            ) do
        hash_a = Native.compute_hash(data_a)
        hash_b = Native.compute_hash(data_b)

        assert hash_a != hash_b
      end
    end
  end

  describe "zstd compression" do
    property "compress then decompress round-trip preserves data", %{server: server} do
      check all(data <- StreamData.binary(min_length: 0, max_length: 65_536), max_runs: 100) do
        {:ok, hash, _info} =
          write_chunk(server, data, compression: "zstd", compression_level: 3)

        {:ok, result} =
          read_chunk(server, hash, decompress: true, verify: true, compression: "zstd:3")

        assert result == data
      end
    end

    property "compressed size has bounded overhead", %{server: server} do
      check all(data <- StreamData.binary(min_length: 0, max_length: 65_536), max_runs: 100) do
        {:ok, _hash, info} =
          write_chunk(server, data, compression: "zstd", compression_level: 3)

        # Zstd worst-case expansion: ~original + original/128 + frame overhead.
        # Use a generous bound to avoid flaky tests.
        max_overhead = max(128, div(info.original_size, 8))
        assert info.stored_size <= info.original_size + max_overhead
      end
    end
  end

  describe "encryption" do
    property "encrypt then decrypt round-trip preserves data", %{server: server} do
      check all(
              data <- StreamData.binary(min_length: 0, max_length: 65_536),
              key <- StreamData.binary(length: 32),
              nonce <- StreamData.binary(length: 12),
              max_runs: 100
            ) do
        {:ok, hash, _info} = write_chunk(server, data, key: key, nonce: nonce)

        {:ok, result} = read_chunk(server, hash, key: key, nonce: nonce)

        assert result == data
      end
    end

    property "different keys produce different ciphertext", %{server: server} do
      check all(
              data <- StreamData.binary(min_length: 1, max_length: 1024),
              key_a <- StreamData.binary(length: 32),
              key_b <- StreamData.binary(length: 32),
              nonce <- StreamData.binary(length: 12),
              key_a != key_b,
              max_runs: 100
            ) do
        # Write with key_a and the same nonce. Since encrypted chunks live
        # at a nonce-derived codec suffix on disk (issue #270), writing
        # with key_b + same nonce overwrites the file in place. The test
        # verifies that the resulting ciphertext really did change by
        # attempting to decrypt with the old key — it must now fail.
        {:ok, hash, _} = write_chunk(server, data, key: key_a, nonce: nonce)

        assert {:ok, ^data} = read_chunk(server, hash, key: key_a, nonce: nonce)

        {:ok, ^hash, _} = write_chunk(server, data, key: key_b, nonce: nonce)

        # After the rewrite with key_b the on-disk ciphertext is different.
        # Decrypting with key_a must now fail authentication.
        assert {:error, reason} = read_chunk(server, hash, key: key_a, nonce: nonce)

        assert reason =~ "authentication failed" or reason =~ "encryption error"

        assert {:ok, ^data} = read_chunk(server, hash, key: key_b, nonce: nonce)
      end
    end
  end

  defp write_chunk(server, data, opts) do
    BlobStore.write_chunk(
      data,
      "default",
      "hot",
      Keyword.merge([server: server, timeout: @call_timeout], opts)
    )
  end

  defp read_chunk(server, hash, opts) do
    BlobStore.read_chunk_with_options(
      hash,
      "default",
      "hot",
      Keyword.merge([server: server, timeout: @call_timeout], opts)
    )
  end
end
