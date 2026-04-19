defmodule NeonFS.Core.Blob.NativeTest do
  use ExUnit.Case

  alias NeonFS.Core.Blob.Native

  @moduletag :tmp_dir

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
    setup %{tmp_dir: tmp_dir} do
      # Create a temporary directory for each test
      store_dir = Path.join(tmp_dir, "store_open")
      File.rm_rf!(store_dir)
      on_exit(fn -> File.rm_rf!(store_dir) end)
      {:ok, tmp_dir: store_dir}
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
    setup %{tmp_dir: tmp_dir} do
      store_dir = Path.join(tmp_dir, "store_ops")
      {:ok, store} = Native.store_open(store_dir, 2)
      on_exit(fn -> File.rm_rf!(store_dir) end)
      {:ok, store: store, tmp_dir: store_dir}
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
    setup %{tmp_dir: tmp_dir} do
      store_dir = Path.join(tmp_dir, "verify")
      {:ok, store} = Native.store_open(store_dir, 2)
      on_exit(fn -> File.rm_rf!(store_dir) end)
      {:ok, store: store, tmp_dir: store_dir}
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
    @tag :tmp_dir
    test "store can be garbage collected", %{tmp_dir: tmp_dir} do
      store_dir = Path.join(tmp_dir, "gc")

      # Open store in a function so it goes out of scope
      {:ok, _store} = Native.store_open(store_dir, 2)

      # Force garbage collection
      :erlang.garbage_collect()

      # If we got here without crash, cleanup is working
      File.rm_rf!(store_dir)
    end
  end

  describe "chunk compression" do
    setup %{tmp_dir: tmp_dir} do
      store_dir = Path.join(tmp_dir, "compress")
      {:ok, store} = Native.store_open(store_dir, 2)
      on_exit(fn -> File.rm_rf!(store_dir) end)
      {:ok, store: store, tmp_dir: store_dir}
    end

    test "write with compression=none returns same size", %{store: store} do
      data = String.duplicate("hello world ", 100)
      hash = Native.compute_hash(data)

      assert {:ok, {original_size, stored_size, compression, _, _}} =
               Native.store_write_chunk_compressed(
                 store,
                 hash,
                 data,
                 "hot",
                 "none",
                 0,
                 <<>>,
                 <<>>
               )

      assert original_size == byte_size(data)
      assert stored_size == original_size
      assert compression == "none"
    end

    test "write with zstd compression reduces size for compressible data", %{store: store} do
      # Highly compressible data
      data = String.duplicate("hello world ", 1000)
      hash = Native.compute_hash(data)

      assert {:ok, {original_size, stored_size, compression, _, _}} =
               Native.store_write_chunk_compressed(
                 store,
                 hash,
                 data,
                 "hot",
                 "zstd",
                 3,
                 <<>>,
                 <<>>
               )

      assert original_size == byte_size(data)
      assert stored_size < original_size
      assert compression == "zstd:3"
    end

    test "read compressed chunk with decompress=true returns original data", %{store: store} do
      data = String.duplicate("hello world ", 1000)
      hash = Native.compute_hash(data)

      assert {:ok, _} =
               Native.store_write_chunk_compressed(
                 store,
                 hash,
                 data,
                 "hot",
                 "zstd",
                 3,
                 <<>>,
                 <<>>
               )

      assert {:ok, read_data} =
               Native.store_read_chunk_with_options(store, hash, "hot", false, true, <<>>, <<>>)

      assert read_data == data
    end

    test "read compressed chunk with decompress=false returns compressed data", %{store: store} do
      data = String.duplicate("hello world ", 1000)
      hash = Native.compute_hash(data)

      assert {:ok, {_orig, stored_size, _comp, _, _}} =
               Native.store_write_chunk_compressed(
                 store,
                 hash,
                 data,
                 "hot",
                 "zstd",
                 3,
                 <<>>,
                 <<>>
               )

      assert {:ok, read_data} =
               Native.store_read_chunk_with_options(store, hash, "hot", false, false, <<>>, <<>>)

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
               Native.store_read_chunk_with_options(store, hash, "hot", false, true, <<>>, <<>>)
    end

    test "read with verify=true and decompress=true verifies original data", %{store: store} do
      data = String.duplicate("verify compressed ", 1000)
      hash = Native.compute_hash(data)

      assert {:ok, _} =
               Native.store_write_chunk_compressed(
                 store,
                 hash,
                 data,
                 "hot",
                 "zstd",
                 3,
                 <<>>,
                 <<>>
               )

      assert {:ok, read_data} =
               Native.store_read_chunk_with_options(store, hash, "hot", true, true, <<>>, <<>>)

      assert read_data == data
    end

    test "read with verify=true on corrupt compressed data fails", %{
      store: store,
      tmp_dir: tmp_dir
    } do
      data = String.duplicate("will be corrupted ", 1000)
      hash = Native.compute_hash(data)

      assert {:ok, _} =
               Native.store_write_chunk_compressed(
                 store,
                 hash,
                 data,
                 "hot",
                 "zstd",
                 3,
                 <<>>,
                 <<>>
               )

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
          3,
          <<>>,
          <<>>
        )

      {:ok, wrong_compressed_data} =
        Native.store_read_chunk_with_options(
          store,
          Native.compute_hash(other_data),
          "warm",
          false,
          false,
          <<>>,
          <<>>
        )

      File.write!(chunk_path, wrong_compressed_data)

      # Verification should fail because decompressed data doesn't match expected hash
      assert {:error, reason} =
               Native.store_read_chunk_with_options(store, hash, "hot", true, true, <<>>, <<>>)

      assert reason =~ "corrupt chunk"
    end

    test "different compression levels produce different stored sizes", %{store: store} do
      # Semi-compressible data (not too random, not too repetitive)
      data = Enum.map_join(0..10_000, fn i -> <<rem(i, 256)>> end)

      hash1 = Native.compute_hash(data <> "1")
      hash9 = Native.compute_hash(data <> "9")

      assert {:ok, {_, stored_level1, _, _, _}} =
               Native.store_write_chunk_compressed(
                 store,
                 hash1,
                 data <> "1",
                 "hot",
                 "zstd",
                 1,
                 <<>>,
                 <<>>
               )

      assert {:ok, {_, stored_level9, _, _, _}} =
               Native.store_write_chunk_compressed(
                 store,
                 hash9,
                 data <> "9",
                 "warm",
                 "zstd",
                 9,
                 <<>>,
                 <<>>
               )

      # Higher level should produce equal or smaller size
      assert stored_level9 <= stored_level1
    end

    test "empty chunk can be compressed", %{store: store} do
      data = ""
      hash = Native.compute_hash(data)

      assert {:ok, {original_size, stored_size, compression, _, _}} =
               Native.store_write_chunk_compressed(
                 store,
                 hash,
                 data,
                 "hot",
                 "zstd",
                 3,
                 <<>>,
                 <<>>
               )

      assert original_size == 0
      # zstd still creates a small frame for empty data
      assert stored_size > 0
      assert compression == "zstd:3"

      # Should decompress back to empty
      assert {:ok, read_data} =
               Native.store_read_chunk_with_options(store, hash, "hot", false, true, <<>>, <<>>)

      assert read_data == data
    end

    test "invalid compression type returns error", %{store: store} do
      data = "test"
      hash = Native.compute_hash(data)

      assert {:error, reason} =
               Native.store_write_chunk_compressed(
                 store,
                 hash,
                 data,
                 "hot",
                 "invalid",
                 3,
                 <<>>,
                 <<>>
               )

      assert reason =~ "invalid compression"
    end
  end

  describe "auto_chunk_strategy/1" do
    test "returns single for small data" do
      assert {"single", 0} = Native.auto_chunk_strategy(0)
      assert {"single", 0} = Native.auto_chunk_strategy(1000)
      # 64KB - 1 = 65535
      assert {"single", 0} = Native.auto_chunk_strategy(65_535)
    end

    test "returns fixed for medium data" do
      # 64KB = 65536
      assert {"fixed", 262_144} = Native.auto_chunk_strategy(65_536)
      assert {"fixed", 262_144} = Native.auto_chunk_strategy(500_000)
      # 1MB - 1 = 1048575
      assert {"fixed", 262_144} = Native.auto_chunk_strategy(1_048_575)
    end

    test "returns fastcdc for large data" do
      # 1MB = 1048576
      assert {"fastcdc", 262_144} = Native.auto_chunk_strategy(1_048_576)
      assert {"fastcdc", 262_144} = Native.auto_chunk_strategy(10_000_000)
    end
  end

  describe "chunk_data/3" do
    test "returns empty list for empty data" do
      assert {:ok, []} = Native.chunk_data("", "single", 0)
      assert {:ok, []} = Native.chunk_data("", "auto", 0)
    end

    test "single strategy produces one chunk" do
      data = "hello world, this is test data"

      assert {:ok, [{chunk_data, hash, offset, size}]} =
               Native.chunk_data(data, "single", 0)

      assert chunk_data == data
      assert hash == Native.compute_hash(data)
      assert offset == 0
      assert size == byte_size(data)
    end

    test "fixed strategy splits data into chunks" do
      # 1000 bytes with 300 byte chunks = 4 chunks
      data = :binary.copy(<<0>>, 1000)

      assert {:ok, chunks} = Native.chunk_data(data, "fixed", 300)
      assert length(chunks) == 4

      # Check sizes
      [{_, _, _, size1}, {_, _, _, size2}, {_, _, _, size3}, {_, _, _, size4}] = chunks
      assert size1 == 300
      assert size2 == 300
      assert size3 == 300
      assert size4 == 100

      # Check offsets
      [{_, _, offset1, _}, {_, _, offset2, _}, {_, _, offset3, _}, {_, _, offset4, _}] = chunks
      assert offset1 == 0
      assert offset2 == 300
      assert offset3 == 600
      assert offset4 == 900

      # Verify reconstruction
      reconstructed = Enum.map_join(chunks, fn {d, _, _, _} -> d end)

      assert reconstructed == data
    end

    test "chunk hashes are correct" do
      data = :binary.copy(<<42>>, 1000)

      assert {:ok, chunks} = Native.chunk_data(data, "fixed", 300)

      for {chunk_data, hash, _, _} <- chunks do
        assert hash == Native.compute_hash(chunk_data)
      end
    end

    test "auto strategy selects appropriate strategy based on size" do
      # Small data (< 64KB) -> single chunk
      small_data = String.duplicate("x", 1000)
      assert {:ok, chunks} = Native.chunk_data(small_data, "auto", byte_size(small_data))
      assert length(chunks) == 1

      # Medium data (64KB - 1MB) -> fixed chunks
      medium_data = :binary.copy(<<0>>, 100_000)
      assert {:ok, chunks} = Native.chunk_data(medium_data, "auto", byte_size(medium_data))
      # With 256KB chunks, 100KB should be single chunk
      assert length(chunks) == 1

      # Medium data that gets split
      medium_data2 = :binary.copy(<<0>>, 500_000)
      assert {:ok, chunks} = Native.chunk_data(medium_data2, "auto", byte_size(medium_data2))
      # 500KB with 256KB chunks = 2 chunks
      assert length(chunks) == 2
    end

    test "fastcdc strategy produces variable-size chunks" do
      # Create 2MB of semi-random data
      data = Enum.map_join(0..2_097_151, fn i -> <<rem(i, 256)>> end)

      assert {:ok, chunks} = Native.chunk_data(data, "fastcdc", 262_144)

      # Should have multiple chunks
      assert length(chunks) > 1

      # Verify reconstruction
      reconstructed = Enum.map_join(chunks, fn {d, _, _, _} -> d end)

      assert reconstructed == data

      # Verify offsets are contiguous
      expected_offset =
        Enum.reduce(chunks, 0, fn {_, _, offset, size}, expected ->
          assert offset == expected
          expected + size
        end)

      assert expected_offset == byte_size(data)
    end

    test "invalid strategy returns error" do
      data = "test"

      assert {:error, reason} = Native.chunk_data(data, "invalid", 0)
      assert reason =~ "invalid strategy"
    end

    test "fixed strategy with zero size returns error" do
      data = "test"

      assert {:error, reason} = Native.chunk_data(data, "fixed", 0)
      assert reason =~ "non-zero"
    end

    test "handles large binary data" do
      # 1MB of data
      data = :binary.copy(<<99>>, 1_048_576)

      assert {:ok, chunks} = Native.chunk_data(data, "fixed", 262_144)

      # 1MB / 256KB = 4 chunks
      assert length(chunks) == 4

      # Verify all hashes are valid
      for {chunk_data, hash, _, _} <- chunks do
        assert byte_size(hash) == 32
        assert hash == :crypto.hash(:sha256, chunk_data)
      end
    end

    test "handles binary with null bytes" do
      data = <<0, 1, 2, 0, 3, 4, 0, 0, 0>>

      assert {:ok, [{chunk_data, hash, offset, size}]} =
               Native.chunk_data(data, "single", 0)

      assert chunk_data == data
      assert hash == :crypto.hash(:sha256, data)
      assert offset == 0
      assert size == 9
    end
  end

  describe "incremental chunker" do
    test "init rejects unknown strategy" do
      assert {:error, _reason} = Native.chunker_init("bogus", 0)
    end

    test "init rejects fixed strategy with zero size" do
      assert {:error, _reason} = Native.chunker_init("fixed", 0)
    end

    test "single strategy buffers until finish" do
      {:ok, chunker} = Native.chunker_init("single", 0)
      assert [] = Native.chunker_feed(chunker, "hello ")
      assert [] = Native.chunker_feed(chunker, "world")

      assert [{data, hash, 0, 11}] = Native.chunker_finish(chunker)
      assert data == "hello world"
      assert hash == :crypto.hash(:sha256, "hello world")
    end

    test "fixed strategy emits chunks at the size threshold" do
      {:ok, chunker} = Native.chunker_init("fixed", 4)

      assert [] = Native.chunker_feed(chunker, "ab")
      assert [{"abcd", _, 0, 4}] = Native.chunker_feed(chunker, "cd")
      assert [{"efgh", _, 4, 4}, {"ijkl", _, 8, 4}] = Native.chunker_feed(chunker, "efghijkl")
      assert [] = Native.chunker_feed(chunker, "mn")
      assert [{"mn", _, 12, 2}] = Native.chunker_finish(chunker)
    end

    test "incremental output equals batch output for the same input" do
      data = :crypto.strong_rand_bytes(50_000)

      {:ok, batch} = Native.chunk_data(data, "fixed", 256)
      {:ok, chunker} = Native.chunker_init("fixed", 256)

      emitted =
        data
        |> chunk_binary(1024)
        |> Enum.flat_map(&Native.chunker_feed(chunker, &1))
        |> Kernel.++(Native.chunker_finish(chunker))

      assert emitted == batch
    end

    test "fastcdc incremental output equals batch output across many slice sizes" do
      data = :crypto.strong_rand_bytes(8 * 1024)

      {:ok, batch} = Native.chunk_data(data, "fastcdc", 512)

      for slice_size <- [1, 64, 512, 1024, 4096] do
        {:ok, chunker} = Native.chunker_init("fastcdc", 512)

        emitted =
          data
          |> chunk_binary(slice_size)
          |> Enum.flat_map(&Native.chunker_feed(chunker, &1))
          |> Kernel.++(Native.chunker_finish(chunker))

        assert emitted == batch, "incremental != batch for slice_size=#{slice_size}"
      end
    end

    test "finish on empty chunker returns empty list" do
      {:ok, chunker} = Native.chunker_init("single", 0)
      assert [] = Native.chunker_finish(chunker)
    end

    test "chunker can be reused after finish, offset continues" do
      {:ok, chunker} = Native.chunker_init("fixed", 4)

      assert [{"abcd", _, 0, 4}] = Native.chunker_feed(chunker, "abcd")
      assert [] = Native.chunker_finish(chunker)

      assert [{"wxyz", _, 4, 4}] = Native.chunker_feed(chunker, "wxyz")
    end
  end

  defp chunk_binary(<<>>, _size), do: []

  defp chunk_binary(data, size) do
    case data do
      <<head::binary-size(size), rest::binary>> -> [head | chunk_binary(rest, size)]
      tail -> [tail]
    end
  end

  describe "chunk tier migration" do
    setup %{tmp_dir: tmp_dir} do
      store_dir = Path.join(tmp_dir, "migrate")
      {:ok, store} = Native.store_open(store_dir, 2)
      on_exit(fn -> File.rm_rf!(store_dir) end)
      {:ok, store: store}
    end

    test "migrates chunk from hot to cold", %{store: store} do
      data = "data to migrate hot to cold"
      hash = Native.compute_hash(data)

      # Write to hot tier
      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")
      assert {:ok, true} = Native.store_chunk_exists(store, hash, "hot")
      assert {:ok, false} = Native.store_chunk_exists(store, hash, "cold")

      # Migrate to cold tier
      assert {:ok, _} = Native.store_migrate_chunk(store, hash, "hot", "cold")

      # Verify: chunk now in cold, not in hot
      assert {:ok, false} = Native.store_chunk_exists(store, hash, "hot")
      assert {:ok, true} = Native.store_chunk_exists(store, hash, "cold")

      # Verify data integrity
      assert {:ok, read_data} = Native.store_read_chunk(store, hash, "cold")
      assert read_data == data
    end

    test "migrates chunk from cold to hot", %{store: store} do
      data = "data to migrate cold to hot"
      hash = Native.compute_hash(data)

      # Write to cold tier
      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "cold")

      # Migrate to hot tier
      assert {:ok, _} = Native.store_migrate_chunk(store, hash, "cold", "hot")

      # Verify: chunk now in hot, not in cold
      assert {:ok, true} = Native.store_chunk_exists(store, hash, "hot")
      assert {:ok, false} = Native.store_chunk_exists(store, hash, "cold")

      # Verify data integrity
      assert {:ok, read_data} = Native.store_read_chunk(store, hash, "hot")
      assert read_data == data
    end

    test "migrates chunk from warm to hot", %{store: store} do
      data = "data to migrate warm to hot"
      hash = Native.compute_hash(data)

      # Write to warm tier
      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "warm")

      # Migrate to hot tier
      assert {:ok, _} = Native.store_migrate_chunk(store, hash, "warm", "hot")

      # Verify: chunk now in hot, not in warm
      assert {:ok, true} = Native.store_chunk_exists(store, hash, "hot")
      assert {:ok, false} = Native.store_chunk_exists(store, hash, "warm")
    end

    test "migrating to same tier is a no-op", %{store: store} do
      data = "no-op migration"
      hash = Native.compute_hash(data)

      # Write to hot tier
      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")

      # "Migrate" to same tier
      assert {:ok, _} = Native.store_migrate_chunk(store, hash, "hot", "hot")

      # Verify: still in hot tier
      assert {:ok, true} = Native.store_chunk_exists(store, hash, "hot")
      assert {:ok, read_data} = Native.store_read_chunk(store, hash, "hot")
      assert read_data == data
    end

    test "migration of nonexistent chunk returns error", %{store: store} do
      hash = Native.compute_hash("nonexistent")

      assert {:error, reason} = Native.store_migrate_chunk(store, hash, "hot", "cold")
      assert reason =~ "not found"
    end

    test "migration preserves data integrity", %{store: store} do
      # Use larger data to ensure integrity is important
      data = :binary.copy(<<42>>, 100_000)
      hash = Native.compute_hash(data)

      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")

      # Migrate through all tiers
      assert {:ok, _} = Native.store_migrate_chunk(store, hash, "hot", "warm")
      assert {:ok, read1} = Native.store_read_chunk(store, hash, "warm")
      assert read1 == data

      assert {:ok, _} = Native.store_migrate_chunk(store, hash, "warm", "cold")
      assert {:ok, read2} = Native.store_read_chunk(store, hash, "cold")
      assert read2 == data

      assert {:ok, _} = Native.store_migrate_chunk(store, hash, "cold", "hot")
      assert {:ok, read3} = Native.store_read_chunk(store, hash, "hot")
      assert read3 == data

      # Verify hash still matches
      assert hash == Native.compute_hash(read3)
    end

    test "handles empty chunk migration", %{store: store} do
      data = ""
      hash = Native.compute_hash(data)

      assert {:ok, _} = Native.store_write_chunk(store, hash, data, "hot")
      assert {:ok, _} = Native.store_migrate_chunk(store, hash, "hot", "cold")

      assert {:ok, false} = Native.store_chunk_exists(store, hash, "hot")
      assert {:ok, true} = Native.store_chunk_exists(store, hash, "cold")
      assert {:ok, read_data} = Native.store_read_chunk(store, hash, "cold")
      assert read_data == data
    end
  end
end
