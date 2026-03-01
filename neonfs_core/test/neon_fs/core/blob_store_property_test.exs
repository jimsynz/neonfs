defmodule NeonFS.Core.BlobStorePropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NeonFS.Core.Blob.Native
  alias NeonFS.Core.BlobStore

  @moduletag :tmp_dir

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
          BlobStore.write_chunk(data, "default", "hot",
            compression: "zstd",
            compression_level: 3,
            server: server
          )

        {:ok, result} =
          BlobStore.read_chunk_with_options(hash, "default", "hot",
            decompress: true,
            verify: true,
            server: server
          )

        assert result == data
      end
    end

    property "compressed size has bounded overhead", %{server: server} do
      check all(data <- StreamData.binary(min_length: 0, max_length: 65_536), max_runs: 100) do
        {:ok, _hash, info} =
          BlobStore.write_chunk(data, "default", "hot",
            compression: "zstd",
            compression_level: 3,
            server: server
          )

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
        {:ok, hash, _info} =
          BlobStore.write_chunk(data, "default", "hot",
            key: key,
            nonce: nonce,
            server: server
          )

        {:ok, result} =
          BlobStore.read_chunk_with_options(hash, "default", "hot",
            key: key,
            nonce: nonce,
            server: server
          )

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
        # Write with key_a, read raw (encrypted) data
        {:ok, hash, _} =
          BlobStore.write_chunk(data, "default", "hot",
            key: key_a,
            nonce: nonce,
            server: server
          )

        {:ok, encrypted_a} =
          BlobStore.read_chunk(hash, "default", tier: "hot", server: server)

        # Write same data with key_b (same hash → overwrites on disk)
        {:ok, ^hash, _} =
          BlobStore.write_chunk(data, "default", "hot",
            key: key_b,
            nonce: nonce,
            server: server
          )

        {:ok, encrypted_b} =
          BlobStore.read_chunk(hash, "default", tier: "hot", server: server)

        assert encrypted_a != encrypted_b
      end
    end
  end
end
