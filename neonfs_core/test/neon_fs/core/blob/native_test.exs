defmodule NeonFS.Core.Blob.NativeTest do
  use ExUnit.Case

  alias NeonFS.Core.Blob.Native

  describe "add/2" do
    test "adds two positive integers" do
      assert Native.add(1, 2) == 3
    end

    test "adds negative integers" do
      assert Native.add(-5, 3) == -2
    end

    test "adds zero" do
      assert Native.add(0, 42) == 42
    end
  end

  describe "compute_hash/1" do
    # SHA-256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    @empty_hash_hex "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    # SHA-256("hello world") = b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
    @hello_world_hash_hex "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

    test "returns 32-byte binary" do
      hash = Native.compute_hash("test")
      assert is_binary(hash)
      assert byte_size(hash) == 32
    end

    test "computes correct hash for empty string" do
      hash = Native.compute_hash("")
      assert Base.encode16(hash, case: :lower) == @empty_hash_hex
    end

    test "computes correct hash for hello world" do
      hash = Native.compute_hash("hello world")
      assert Base.encode16(hash, case: :lower) == @hello_world_hash_hex
    end

    test "matches Elixir :crypto.hash/2" do
      data = "some test data for hashing"
      nif_hash = Native.compute_hash(data)
      erlang_hash = :crypto.hash(:sha256, data)
      assert nif_hash == erlang_hash
    end

    test "handles empty binary" do
      hash = Native.compute_hash(<<>>)
      assert is_binary(hash)
      assert byte_size(hash) == 32
    end

    test "handles large binary (1MB+)" do
      # Create 1MB of data
      large_data = :binary.copy(<<0>>, 1_048_576)
      hash = Native.compute_hash(large_data)
      assert byte_size(hash) == 32
      # Verify against Erlang's crypto
      assert hash == :crypto.hash(:sha256, large_data)
    end

    test "produces different hashes for different inputs" do
      hash1 = Native.compute_hash("hello")
      hash2 = Native.compute_hash("world")
      assert hash1 != hash2
    end

    test "produces same hash for same input (deterministic)" do
      data = "deterministic test"
      hash1 = Native.compute_hash(data)
      hash2 = Native.compute_hash(data)
      assert hash1 == hash2
    end

    test "handles binary with null bytes" do
      data = <<0, 1, 2, 0, 3, 4, 0>>
      hash = Native.compute_hash(data)
      assert hash == :crypto.hash(:sha256, data)
    end

    test "handles unicode binary" do
      data = "hello in Japanese: "
      hash = Native.compute_hash(data)
      assert hash == :crypto.hash(:sha256, data)
    end
  end

  describe "store_open/2" do
    setup do
      # Create a temporary directory for each test
      tmp_dir = Path.join(System.tmp_dir!(), "neonfs_test_#{:rand.uniform(1_000_000)}")
      on_exit(fn -> File.rm_rf!(tmp_dir) end)
      {:ok, tmp_dir: tmp_dir}
    end

    test "opens a store at new directory", %{tmp_dir: tmp_dir} do
      assert {:ok, store} = Native.store_open(tmp_dir, 2)
      assert is_reference(store)
    end

    test "opens a store at existing directory", %{tmp_dir: tmp_dir} do
      File.mkdir_p!(tmp_dir)
      assert {:ok, store} = Native.store_open(tmp_dir, 2)
      assert is_reference(store)
    end

    test "creates base directory if not exists", %{tmp_dir: tmp_dir} do
      refute File.exists?(tmp_dir)
      assert {:ok, _store} = Native.store_open(tmp_dir, 2)
      assert File.exists?(tmp_dir)
    end
  end

  describe "store operations" do
    setup do
      tmp_dir = Path.join(System.tmp_dir!(), "neonfs_store_test_#{:rand.uniform(1_000_000)}")
      {:ok, store} = Native.store_open(tmp_dir, 2)
      on_exit(fn -> File.rm_rf!(tmp_dir) end)
      {:ok, store: store, tmp_dir: tmp_dir}
    end

    test "write then read returns same data", %{store: store} do
      data = "hello world"
      hash = Native.compute_hash(data)

      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")
      assert {:ok, read_data} = Native.store_read_chunk(store, hash, "hot")
      assert read_data == data
    end

    test "read nonexistent chunk returns error", %{store: store} do
      hash = Native.compute_hash("nonexistent")
      assert {:error, reason} = Native.store_read_chunk(store, hash, "hot")
      assert reason =~ "not found"
    end

    test "delete removes chunk from disk", %{store: store} do
      data = "to be deleted"
      hash = Native.compute_hash(data)

      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")
      assert {:ok, true} = Native.store_chunk_exists(store, hash, "hot")

      assert {:ok, _} = Native.store_delete_chunk(store, hash, "hot")
      assert {:ok, false} = Native.store_chunk_exists(store, hash, "hot")
    end

    test "delete nonexistent chunk returns error", %{store: store} do
      hash = Native.compute_hash("nonexistent")
      assert {:error, reason} = Native.store_delete_chunk(store, hash, "hot")
      assert reason =~ "not found"
    end

    test "chunk_exists returns correct boolean", %{store: store} do
      data = "existence test"
      hash = Native.compute_hash(data)

      assert {:ok, false} = Native.store_chunk_exists(store, hash, "hot")

      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")

      assert {:ok, true} = Native.store_chunk_exists(store, hash, "hot")
    end

    test "handles different tiers independently", %{store: store} do
      data = "tier test data"
      hash = Native.compute_hash(data)

      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")

      assert {:ok, true} = Native.store_chunk_exists(store, hash, "hot")
      assert {:ok, false} = Native.store_chunk_exists(store, hash, "warm")
      assert {:ok, false} = Native.store_chunk_exists(store, hash, "cold")
    end

    test "write is idempotent", %{store: store} do
      data = "idempotent data"
      hash = Native.compute_hash(data)

      # Write twice
      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")
      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")

      # Should still read correctly
      assert {:ok, read_data} = Native.store_read_chunk(store, hash, "hot")
      assert read_data == data
    end

    test "handles large chunk (1MB)", %{store: store} do
      data = :binary.copy(<<42>>, 1_048_576)
      hash = Native.compute_hash(data)

      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")
      assert {:ok, read_data} = Native.store_read_chunk(store, hash, "hot")
      assert read_data == data
    end

    test "handles empty chunk", %{store: store} do
      data = ""
      hash = Native.compute_hash(data)

      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")
      assert {:ok, read_data} = Native.store_read_chunk(store, hash, "hot")
      assert read_data == data
    end

    test "handles binary with null bytes", %{store: store} do
      data = <<0, 1, 2, 0, 3, 4, 0>>
      hash = Native.compute_hash(data)

      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")
      assert {:ok, read_data} = Native.store_read_chunk(store, hash, "hot")
      assert read_data == data
    end

    test "supports all tiers", %{store: store} do
      for tier <- ["hot", "warm", "cold"] do
        data = "data for #{tier}"
        hash = Native.compute_hash(data)

        assert {:ok, _} = Native.store_write_chunk(store, hash, data, tier)
        assert {:ok, read_data} = Native.store_read_chunk(store, hash, tier)
        assert read_data == data
      end
    end

    test "invalid tier returns error", %{store: store} do
      data = "test"
      hash = Native.compute_hash(data)

      assert {:error, reason} = Native.store_write_chunk(store, hash, data, "invalid")
      assert reason =~ "invalid tier"
    end

    test "invalid hash length returns error", %{store: store} do
      data = "test"
      invalid_hash = "not32bytes"

      assert {:error, reason} = Native.store_write_chunk(store, invalid_hash, data, "hot")
      assert reason =~ "invalid hash length"
    end
  end

  describe "chunk verification" do
    setup do
      tmp_dir = Path.join(System.tmp_dir!(), "neonfs_verify_test_#{:rand.uniform(1_000_000)}")
      {:ok, store} = Native.store_open(tmp_dir, 2)
      on_exit(fn -> File.rm_rf!(tmp_dir) end)
      {:ok, store: store, tmp_dir: tmp_dir}
    end

    test "read with verify=true on valid chunk succeeds", %{store: store} do
      data = "valid data for verification"
      hash = Native.compute_hash(data)

      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")
      assert {:ok, read_data} = Native.store_read_chunk_verified(store, hash, "hot", true)
      assert read_data == data
    end

    test "read with verify=false on valid chunk succeeds", %{store: store} do
      data = "valid data no verification"
      hash = Native.compute_hash(data)

      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")
      assert {:ok, read_data} = Native.store_read_chunk_verified(store, hash, "hot", false)
      assert read_data == data
    end

    test "read with verify=true on corrupt chunk fails", %{store: store, tmp_dir: tmp_dir} do
      data = "original data"
      hash = Native.compute_hash(data)

      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")

      # Manually corrupt the chunk file
      hash_hex = Base.encode16(hash, case: :lower)
      prefix1 = String.slice(hash_hex, 0, 2)
      prefix2 = String.slice(hash_hex, 2, 2)
      chunk_path = Path.join([tmp_dir, "blobs", "hot", prefix1, prefix2, hash_hex])
      File.write!(chunk_path, "corrupted data")

      # Read with verification should fail
      assert {:error, reason} = Native.store_read_chunk_verified(store, hash, "hot", true)
      assert reason =~ "corrupt chunk"
    end

    test "read with verify=false on corrupt chunk returns corrupt data", %{
      store: store,
      tmp_dir: tmp_dir
    } do
      data = "original data"
      hash = Native.compute_hash(data)

      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")

      # Manually corrupt the chunk file
      hash_hex = Base.encode16(hash, case: :lower)
      prefix1 = String.slice(hash_hex, 0, 2)
      prefix2 = String.slice(hash_hex, 2, 2)
      chunk_path = Path.join([tmp_dir, "blobs", "hot", prefix1, prefix2, hash_hex])
      corrupt_data = "corrupted data"
      File.write!(chunk_path, corrupt_data)

      # Read without verification returns corrupt data without error
      assert {:ok, read_data} = Native.store_read_chunk_verified(store, hash, "hot", false)
      assert read_data == corrupt_data
      assert read_data != data
    end
  end

  describe "resource cleanup" do
    test "store can be garbage collected" do
      tmp_dir = Path.join(System.tmp_dir!(), "neonfs_gc_test_#{:rand.uniform(1_000_000)}")

      # Open store in a function so it goes out of scope
      {:ok, _store} = Native.store_open(tmp_dir, 2)

      # Force garbage collection
      :erlang.garbage_collect()

      # If we got here without crash, cleanup is working
      File.rm_rf!(tmp_dir)
    end
  end

  describe "chunk compression" do
    setup do
      tmp_dir = Path.join(System.tmp_dir!(), "neonfs_compress_test_#{:rand.uniform(1_000_000)}")
      {:ok, store} = Native.store_open(tmp_dir, 2)
      on_exit(fn -> File.rm_rf!(tmp_dir) end)
      {:ok, store: store, tmp_dir: tmp_dir}
    end

    test "write with compression=none returns same size", %{store: store} do
      data = String.duplicate("hello world ", 100)
      hash = Native.compute_hash(data)

      assert {:ok, {original_size, stored_size, compression}} =
               Native.store_write_chunk_compressed(store, hash, data, "hot", "none", 0)

      assert original_size == byte_size(data)
      assert stored_size == original_size
      assert compression == "none"
    end

    test "write with zstd compression reduces size for compressible data", %{store: store} do
      # Highly compressible data
      data = String.duplicate("hello world ", 1000)
      hash = Native.compute_hash(data)

      assert {:ok, {original_size, stored_size, compression}} =
               Native.store_write_chunk_compressed(store, hash, data, "hot", "zstd", 3)

      assert original_size == byte_size(data)
      assert stored_size < original_size
      assert compression == "zstd:3"
    end

    test "read compressed chunk with decompress=true returns original data", %{store: store} do
      data = String.duplicate("hello world ", 1000)
      hash = Native.compute_hash(data)

      assert {:ok, _} =
               Native.store_write_chunk_compressed(store, hash, data, "hot", "zstd", 3)

      assert {:ok, read_data} =
               Native.store_read_chunk_with_options(store, hash, "hot", false, true)

      assert read_data == data
    end

    test "read compressed chunk with decompress=false returns compressed data", %{store: store} do
      data = String.duplicate("hello world ", 1000)
      hash = Native.compute_hash(data)

      assert {:ok, {_orig, stored_size, _comp}} =
               Native.store_write_chunk_compressed(store, hash, data, "hot", "zstd", 3)

      assert {:ok, read_data} =
               Native.store_read_chunk_with_options(store, hash, "hot", false, false)

      # Should return compressed data
      assert byte_size(read_data) == stored_size
      assert read_data != data
    end

    test "read uncompressed chunk with decompress=true returns original", %{store: store} do
      data = "simple uncompressed data"
      hash = Native.compute_hash(data)

      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")

      # Reading with decompress=true on uncompressed data should fail
      # because zstd decoder expects a valid zstd frame
      assert {:error, _reason} =
               Native.store_read_chunk_with_options(store, hash, "hot", false, true)
    end

    test "read with verify=true and decompress=true verifies original data", %{store: store} do
      data = String.duplicate("verify compressed ", 1000)
      hash = Native.compute_hash(data)

      assert {:ok, _} =
               Native.store_write_chunk_compressed(store, hash, data, "hot", "zstd", 3)

      assert {:ok, read_data} =
               Native.store_read_chunk_with_options(store, hash, "hot", true, true)

      assert read_data == data
    end

    test "read with verify=true on corrupt compressed data fails", %{
      store: store,
      tmp_dir: tmp_dir
    } do
      data = String.duplicate("will be corrupted ", 1000)
      hash = Native.compute_hash(data)

      assert {:ok, _} =
               Native.store_write_chunk_compressed(store, hash, data, "hot", "zstd", 3)

      # Corrupt the file (but keep it as valid zstd)
      hash_hex = Base.encode16(hash, case: :lower)
      prefix1 = String.slice(hash_hex, 0, 2)
      prefix2 = String.slice(hash_hex, 2, 2)
      chunk_path = Path.join([tmp_dir, "blobs", "hot", prefix1, prefix2, hash_hex])

      # Write different compressed data
      other_data = String.duplicate("different data ", 1000)

      {:ok, _other_compressed} =
        Native.store_write_chunk_compressed(
          store,
          Native.compute_hash(other_data),
          other_data,
          "warm",
          "zstd",
          3
        )

      {:ok, wrong_compressed_data} =
        Native.store_read_chunk_with_options(
          store,
          Native.compute_hash(other_data),
          "warm",
          false,
          false
        )

      File.write!(chunk_path, wrong_compressed_data)

      # Verification should fail because decompressed data doesn't match expected hash
      assert {:error, reason} =
               Native.store_read_chunk_with_options(store, hash, "hot", true, true)

      assert reason =~ "corrupt chunk"
    end

    test "different compression levels produce different stored sizes", %{store: store} do
      # Semi-compressible data (not too random, not too repetitive)
      data = Enum.map_join(0..10_000, fn i -> <<rem(i, 256)>> end)

      hash1 = Native.compute_hash(data <> "1")
      hash9 = Native.compute_hash(data <> "9")

      assert {:ok, {_, stored_level1, _}} =
               Native.store_write_chunk_compressed(store, hash1, data <> "1", "hot", "zstd", 1)

      assert {:ok, {_, stored_level9, _}} =
               Native.store_write_chunk_compressed(store, hash9, data <> "9", "warm", "zstd", 9)

      # Higher level should produce equal or smaller size
      assert stored_level9 <= stored_level1
    end

    test "empty chunk can be compressed", %{store: store} do
      data = ""
      hash = Native.compute_hash(data)

      assert {:ok, {original_size, stored_size, compression}} =
               Native.store_write_chunk_compressed(store, hash, data, "hot", "zstd", 3)

      assert original_size == 0
      # zstd still creates a small frame for empty data
      assert stored_size > 0
      assert compression == "zstd:3"

      # Should decompress back to empty
      assert {:ok, read_data} =
               Native.store_read_chunk_with_options(store, hash, "hot", false, true)

      assert read_data == data
    end

    test "invalid compression type returns error", %{store: store} do
      data = "test"
      hash = Native.compute_hash(data)

      assert {:error, reason} =
               Native.store_write_chunk_compressed(store, hash, data, "hot", "invalid", 3)

      assert reason =~ "invalid compression"
    end
  end
end
