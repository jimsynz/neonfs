defmodule NeonFS.Core.ChunkCryptoTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.ChunkCrypto

  describe "new/1" do
    test "creates with defaults and required fields" do
      nonce = :crypto.strong_rand_bytes(12)
      crypto = ChunkCrypto.new(nonce: nonce, key_version: 1)

      assert crypto.algorithm == :aes_256_gcm
      assert crypto.nonce == nonce
      assert crypto.key_version == 1
    end

    test "allows overriding algorithm" do
      nonce = :crypto.strong_rand_bytes(12)
      crypto = ChunkCrypto.new(algorithm: :aes_256_gcm, nonce: nonce, key_version: 3)

      assert crypto.algorithm == :aes_256_gcm
      assert crypto.key_version == 3
    end

    test "raises on missing nonce" do
      assert_raise KeyError, ~r/nonce/, fn ->
        ChunkCrypto.new(key_version: 1)
      end
    end

    test "raises on missing key_version" do
      assert_raise KeyError, ~r/key_version/, fn ->
        ChunkCrypto.new(nonce: :crypto.strong_rand_bytes(12))
      end
    end
  end

  describe "validate/1" do
    test "accepts valid entry" do
      crypto = ChunkCrypto.new(nonce: :crypto.strong_rand_bytes(12), key_version: 1)
      assert :ok = ChunkCrypto.validate(crypto)
    end

    test "rejects invalid algorithm" do
      crypto = %ChunkCrypto{
        algorithm: :chacha20,
        nonce: :crypto.strong_rand_bytes(12),
        key_version: 1
      }

      assert {:error, "algorithm must be :aes_256_gcm" <> _} = ChunkCrypto.validate(crypto)
    end

    test "rejects nonce with wrong size" do
      crypto = %ChunkCrypto{algorithm: :aes_256_gcm, nonce: <<1, 2, 3>>, key_version: 1}
      assert {:error, "nonce must be a 12-byte binary"} = ChunkCrypto.validate(crypto)
    end

    test "rejects nil nonce" do
      crypto = %ChunkCrypto{algorithm: :aes_256_gcm, nonce: nil, key_version: 1}
      assert {:error, "nonce must be a 12-byte binary"} = ChunkCrypto.validate(crypto)
    end

    test "rejects zero key_version" do
      crypto = %ChunkCrypto{
        algorithm: :aes_256_gcm,
        nonce: :crypto.strong_rand_bytes(12),
        key_version: 0
      }

      assert {:error, "key_version must be a positive integer"} = ChunkCrypto.validate(crypto)
    end

    test "rejects negative key_version" do
      crypto = %ChunkCrypto{
        algorithm: :aes_256_gcm,
        nonce: :crypto.strong_rand_bytes(12),
        key_version: -1
      }

      assert {:error, "key_version must be a positive integer"} = ChunkCrypto.validate(crypto)
    end

    test "accepts high key_version" do
      crypto = ChunkCrypto.new(nonce: :crypto.strong_rand_bytes(12), key_version: 9999)
      assert :ok = ChunkCrypto.validate(crypto)
    end
  end
end
