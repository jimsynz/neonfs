defmodule NeonFS.Core.BackupCryptoTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.BackupCrypto

  describe "seal/1 + open/2" do
    test "round-trips the content key under the right passphrase" do
      {:ok, key, envelope} = BackupCrypto.seal("correct horse battery staple")
      assert byte_size(key) == 32
      assert {:ok, ^key} = BackupCrypto.open(envelope, "correct horse battery staple")
    end

    test "the envelope is JSON-safe (base64 fields, integer params)" do
      {:ok, _key, envelope} = BackupCrypto.seal("pw")
      assert is_binary(envelope["kdf_salt"])
      assert is_binary(envelope["wrapped_key"])
      assert {:ok, _} = Base.decode64(envelope["kdf_salt"])
      assert {:ok, _} = Base.decode64(envelope["wrapped_key"])
      assert is_integer(envelope["kdf_iters"])
      assert envelope["cipher"] == "AES-256-GCM"
    end

    test "a wrong passphrase fails with :bad_passphrase" do
      {:ok, _key, envelope} = BackupCrypto.seal("right")
      assert {:error, :bad_passphrase} = BackupCrypto.open(envelope, "wrong")
    end

    test "each seal uses a fresh salt + content key" do
      {:ok, key1, env1} = BackupCrypto.seal("pw")
      {:ok, key2, env2} = BackupCrypto.seal("pw")
      assert key1 != key2
      assert env1["kdf_salt"] != env2["kdf_salt"]
    end

    test "a tampered wrapped key fails with :bad_passphrase" do
      {:ok, _key, envelope} = BackupCrypto.seal("pw")
      {:ok, wrapped} = Base.decode64(envelope["wrapped_key"])
      <<first, rest::binary>> = wrapped

      tampered = %{
        envelope
        | "wrapped_key" => Base.encode64(<<Bitwise.bxor(first, 1), rest::binary>>)
      }

      assert {:error, :bad_passphrase} = BackupCrypto.open(tampered, "pw")
    end

    test "a structurally invalid envelope fails with :malformed_envelope" do
      assert {:error, :malformed_envelope} = BackupCrypto.open(%{}, "pw")

      assert {:error, :malformed_envelope} =
               BackupCrypto.open(
                 %{"kdf_salt" => "!!notb64", "wrapped_key" => "x", "kdf_iters" => 1},
                 "pw"
               )
    end
  end

  describe "frame sizing" do
    test "encrypted_size adds nonce+tag overhead per frame" do
      frame = BackupCrypto.frame_size()
      overhead = BackupCrypto.frame_overhead()

      assert BackupCrypto.encrypted_size(0) == 0
      assert BackupCrypto.encrypted_size(10) == 10 + overhead
      assert BackupCrypto.encrypted_size(frame) == frame + overhead
      assert BackupCrypto.encrypted_size(frame + 1) == frame + 1 + 2 * overhead
    end

    test "frame_count is ceil(size / frame_size)" do
      frame = BackupCrypto.frame_size()
      assert BackupCrypto.frame_count(0) == 0
      assert BackupCrypto.frame_count(1) == 1
      assert BackupCrypto.frame_count(frame) == 1
      assert BackupCrypto.frame_count(frame + 1) == 2
      assert BackupCrypto.frame_count(3 * frame) == 3
    end

    test "frame_plaintext_len is full for all but the last frame" do
      frame = BackupCrypto.frame_size()
      size = frame + 100
      n = BackupCrypto.frame_count(size)
      assert BackupCrypto.frame_plaintext_len(size, 0, n) == frame
      assert BackupCrypto.frame_plaintext_len(size, 1, n) == 100
    end
  end

  describe "encrypt_frame/4 + decrypt_frame/4" do
    setup do
      {:ok, key, _env} = BackupCrypto.seal("pw")
      {:ok, key: key}
    end

    test "round-trips a frame", %{key: key} do
      frame = BackupCrypto.encrypt_frame("hello world", key, "/f.txt", 0)
      assert {:ok, "hello world"} = BackupCrypto.decrypt_frame(frame, key, "/f.txt", 0)
    end

    test "layout is nonce(12) + ciphertext + tag(16)", %{key: key} do
      frame = BackupCrypto.encrypt_frame("abc", key, "/f.txt", 0)
      assert byte_size(frame) == 12 + 3 + 16
    end

    test "a wrong key fails", %{key: key} do
      {:ok, other, _} = BackupCrypto.seal("other")
      frame = BackupCrypto.encrypt_frame("data", key, "/f.txt", 0)
      assert {:error, :corrupt_frame} = BackupCrypto.decrypt_frame(frame, other, "/f.txt", 0)
    end

    test "a swapped frame index fails (AAD binds the index)", %{key: key} do
      frame = BackupCrypto.encrypt_frame("data", key, "/f.txt", 0)
      assert {:error, :corrupt_frame} = BackupCrypto.decrypt_frame(frame, key, "/f.txt", 1)
    end

    test "a swapped path fails (AAD binds the path)", %{key: key} do
      frame = BackupCrypto.encrypt_frame("data", key, "/a.txt", 0)
      assert {:error, :corrupt_frame} = BackupCrypto.decrypt_frame(frame, key, "/b.txt", 0)
    end

    test "a corrupted ciphertext byte fails", %{key: key} do
      <<head::binary-size(13), byte, tail::binary>> =
        BackupCrypto.encrypt_frame("data", key, "/f.txt", 0)

      corrupted = <<head::binary, Bitwise.bxor(byte, 1), tail::binary>>
      assert {:error, :corrupt_frame} = BackupCrypto.decrypt_frame(corrupted, key, "/f.txt", 0)
    end
  end
end
