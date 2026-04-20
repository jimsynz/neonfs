defmodule NeonFS.Core.Blob.NativeEncryptionTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Blob.Native

  @moduletag :tmp_dir

  @test_key :crypto.strong_rand_bytes(32)
  @test_nonce :crypto.strong_rand_bytes(12)

  setup %{tmp_dir: tmp_dir} do
    store_dir = Path.join(tmp_dir, "enc_test")
    {:ok, store} = Native.store_open(store_dir, 2)
    on_exit(fn -> File.rm_rf!(store_dir) end)
    {:ok, store: store}
  end

  describe "encrypted write and read round-trip" do
    test "write with encryption, read with decryption returns original data", %{store: store} do
      data = "hello encrypted NIF world"
      hash = Native.compute_hash(data)
      key = @test_key
      nonce = @test_nonce

      assert {:ok, {original_size, stored_size, _comp, enc_algo, enc_nonce}} =
               Native.store_write_chunk_compressed(
                 store,
                 hash,
                 data,
                 "hot",
                 "none",
                 0,
                 key,
                 nonce
               )

      assert original_size == byte_size(data)
      # Encrypted data is plaintext + 16 byte auth tag
      assert stored_size == byte_size(data) + 16
      assert enc_algo == "aes-256-gcm"
      assert enc_nonce == nonce

      assert {:ok, read_data} =
               Native.store_read_chunk_with_options(
                 store,
                 hash,
                 "hot",
                 false,
                 false,
                 {"", 0, key, nonce}
               )

      assert read_data == data
    end

    test "write with compression + encryption round-trip", %{store: store} do
      data = String.duplicate("compress and encrypt me ", 500)
      hash = Native.compute_hash(data)
      key = @test_key
      nonce = @test_nonce

      assert {:ok, {original_size, stored_size, comp, enc_algo, _nonce}} =
               Native.store_write_chunk_compressed(
                 store,
                 hash,
                 data,
                 "hot",
                 "zstd",
                 3,
                 key,
                 nonce
               )

      assert original_size == byte_size(data)
      # Compressed+encrypted should be smaller than original (compression helps)
      assert stored_size < original_size
      assert comp == "zstd:3"
      assert enc_algo == "aes-256-gcm"

      # Read with decryption + decompression + verification
      assert {:ok, read_data} =
               Native.store_read_chunk_with_options(
                 store,
                 hash,
                 "hot",
                 true,
                 true,
                 {"zstd", 3, key, nonce}
               )

      assert read_data == data
    end

    test "read with wrong key returns error", %{store: store} do
      data = "secret data"
      hash = Native.compute_hash(data)
      key = @test_key
      nonce = @test_nonce

      assert {:ok, _} =
               Native.store_write_chunk_compressed(
                 store,
                 hash,
                 data,
                 "hot",
                 "none",
                 0,
                 key,
                 nonce
               )

      wrong_key = :crypto.strong_rand_bytes(32)

      assert {:error, reason} =
               Native.store_read_chunk_with_options(
                 store,
                 hash,
                 "hot",
                 false,
                 false,
                 {"", 0, wrong_key, nonce}
               )

      assert reason =~ "authentication failed"
    end

    test "empty binary key/nonce means no encryption", %{store: store} do
      data = "not encrypted"
      hash = Native.compute_hash(data)

      assert {:ok, {_orig, stored_size, _comp, enc_algo, enc_nonce}} =
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

      # No encryption overhead
      assert stored_size == byte_size(data)
      assert enc_algo == ""
      assert enc_nonce == <<>>

      # Read without encryption
      assert {:ok, read_data} =
               Native.store_read_chunk_with_options(
                 store,
                 hash,
                 "hot",
                 false,
                 false,
                 {"", 0, <<>>, <<>>}
               )

      assert read_data == data
    end
  end

  describe "store_reencrypt_chunk" do
    test "re-encrypts chunk with new key", %{store: store} do
      data = "data to reencrypt via NIF"
      hash = Native.compute_hash(data)
      old_key = :crypto.strong_rand_bytes(32)
      old_nonce = :crypto.strong_rand_bytes(12)
      new_key = :crypto.strong_rand_bytes(32)
      new_nonce = :crypto.strong_rand_bytes(12)

      # Write with old key
      assert {:ok, _} =
               Native.store_write_chunk_compressed(
                 store,
                 hash,
                 data,
                 "hot",
                 "none",
                 0,
                 old_key,
                 old_nonce
               )

      # Re-encrypt
      assert {:ok, stored_size} =
               Native.store_reencrypt_chunk(
                 store,
                 hash,
                 "hot",
                 {"", 0},
                 {old_key, old_nonce},
                 {new_key, new_nonce}
               )

      assert stored_size == byte_size(data) + 16

      # Old key should fail — the old-suffix path no longer exists
      assert {:error, _} =
               Native.store_read_chunk_with_options(
                 store,
                 hash,
                 "hot",
                 false,
                 false,
                 {"", 0, old_key, old_nonce}
               )

      # New key should work
      assert {:ok, read_data} =
               Native.store_read_chunk_with_options(
                 store,
                 hash,
                 "hot",
                 false,
                 false,
                 {"", 0, new_key, new_nonce}
               )

      assert read_data == data
    end

    test "re-encrypt preserves compression", %{store: store} do
      data = String.duplicate("reencrypt with compression ", 500)
      hash = Native.compute_hash(data)
      old_key = :crypto.strong_rand_bytes(32)
      old_nonce = :crypto.strong_rand_bytes(12)
      new_key = :crypto.strong_rand_bytes(32)
      new_nonce = :crypto.strong_rand_bytes(12)

      # Write compressed + encrypted
      assert {:ok, _} =
               Native.store_write_chunk_compressed(
                 store,
                 hash,
                 data,
                 "hot",
                 "zstd",
                 3,
                 old_key,
                 old_nonce
               )

      # Re-encrypt
      assert {:ok, _} =
               Native.store_reencrypt_chunk(
                 store,
                 hash,
                 "hot",
                 {"zstd", 3},
                 {old_key, old_nonce},
                 {new_key, new_nonce}
               )

      # Read with new key + decompress + verify
      assert {:ok, read_data} =
               Native.store_read_chunk_with_options(
                 store,
                 hash,
                 "hot",
                 true,
                 true,
                 {"zstd", 3, new_key, new_nonce}
               )

      assert read_data == data
    end
  end

  describe "error cases" do
    test "invalid key size returns error", %{store: store} do
      data = "test"
      hash = Native.compute_hash(data)
      bad_key = :crypto.strong_rand_bytes(16)
      nonce = @test_nonce

      assert {:error, reason} =
               Native.store_write_chunk_compressed(
                 store,
                 hash,
                 data,
                 "hot",
                 "none",
                 0,
                 bad_key,
                 nonce
               )

      assert reason =~ "invalid key size"
    end

    test "invalid nonce size returns error", %{store: store} do
      data = "test"
      hash = Native.compute_hash(data)
      key = @test_key
      bad_nonce = :crypto.strong_rand_bytes(8)

      assert {:error, reason} =
               Native.store_write_chunk_compressed(
                 store,
                 hash,
                 data,
                 "hot",
                 "none",
                 0,
                 key,
                 bad_nonce
               )

      assert reason =~ "invalid nonce size"
    end
  end
end
