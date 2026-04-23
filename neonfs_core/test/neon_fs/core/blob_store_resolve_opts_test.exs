defmodule NeonFS.Core.BlobStoreResolveOptsTest do
  @moduledoc """
  Unit tests for `NeonFS.Core.BlobStore.resolve_put_chunk_opts/1`
  covering the encryption-propagation half of #451 (tracked as #470).

  The compression branch was landed in #469 and exercised through the
  handler integration tests in
  `neonfs_client/test/neon_fs/transport/handler_test.exs`. These tests
  focus on the encryption branch — they require real Ra + KeyManager
  setup, so the module runs `async: false` alongside other encrypted-
  volume tests that share the same infrastructure shape.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{
    BlobStore,
    KeyManager,
    RaServer,
    VolumeEncryption,
    VolumeRegistry
  }

  @moduletag :tmp_dir

  @test_master_key :crypto.strong_rand_bytes(32) |> Base.encode64()

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_drive_registry()
    start_blob_store()
    start_volume_registry()

    write_cluster_json(tmp_dir, @test_master_key)
    start_ra()
    :ok = RaServer.init_cluster()

    on_exit(fn -> cleanup_test_dirs() end)

    :ok
  end

  describe "resolve_put_chunk_opts/1" do
    test "returns [] for unknown volume" do
      assert BlobStore.resolve_put_chunk_opts("does-not-exist") == []
    end

    test "returns [] for a volume with no compression and no encryption" do
      {:ok, volume} =
        VolumeRegistry.create(unique_vol_name(), compression: %{algorithm: :none})

      assert BlobStore.resolve_put_chunk_opts(volume.id) == []
    end

    test "returns compression opts for a zstd-compressed plaintext volume" do
      {:ok, volume} =
        VolumeRegistry.create(unique_vol_name(), compression: %{algorithm: :zstd, level: 5})

      assert BlobStore.resolve_put_chunk_opts(volume.id) ==
               [compression: "zstd", compression_level: 5]
    end

    test "returns encryption opts for an encrypted, uncompressed volume" do
      {:ok, volume} =
        VolumeRegistry.create(unique_vol_name(),
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :none}
        )

      {:ok, _version} = KeyManager.setup_volume_encryption(volume.id)

      opts = BlobStore.resolve_put_chunk_opts(volume.id)

      assert is_list(opts)
      assert byte_size(Keyword.fetch!(opts, :key)) == 32
      assert byte_size(Keyword.fetch!(opts, :nonce)) == 12
      refute Keyword.has_key?(opts, :compression)
      refute Keyword.has_key?(opts, :compression_level)
    end

    test "returns both compression and encryption opts for an encrypted + compressed volume" do
      {:ok, volume} =
        VolumeRegistry.create(unique_vol_name(),
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :zstd, level: 3}
        )

      {:ok, _version} = KeyManager.setup_volume_encryption(volume.id)

      opts = BlobStore.resolve_put_chunk_opts(volume.id)

      assert Keyword.fetch!(opts, :compression) == "zstd"
      assert Keyword.fetch!(opts, :compression_level) == 3
      assert byte_size(Keyword.fetch!(opts, :key)) == 32
      assert byte_size(Keyword.fetch!(opts, :nonce)) == 12
    end

    test "emits a fresh nonce per call for the same encrypted volume" do
      {:ok, volume} =
        VolumeRegistry.create(unique_vol_name(),
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :none}
        )

      {:ok, _version} = KeyManager.setup_volume_encryption(volume.id)

      opts_a = BlobStore.resolve_put_chunk_opts(volume.id)
      opts_b = BlobStore.resolve_put_chunk_opts(volume.id)

      assert Keyword.fetch!(opts_a, :key) == Keyword.fetch!(opts_b, :key)
      assert Keyword.fetch!(opts_a, :nonce) != Keyword.fetch!(opts_b, :nonce)
    end

    test "returns {:error, _} when the encrypted volume has no key material" do
      # An encrypted volume whose `setup_volume_encryption` has never
      # run has no key stored for `current_key_version` — KeyManager
      # returns `{:error, :key_not_found}` (or similar). The resolver
      # must fail closed rather than drop back to plaintext.
      {:ok, volume} =
        VolumeRegistry.create(unique_vol_name(),
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :none}
        )

      assert {:error, _reason} = BlobStore.resolve_put_chunk_opts(volume.id)
    end
  end

  describe "round-trip through BlobStore.write_chunk/4 with resolved opts" do
    setup do
      {:ok, volume} =
        VolumeRegistry.create(unique_vol_name(),
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :none}
        )

      {:ok, _version} = KeyManager.setup_volume_encryption(volume.id)

      {:ok, enc_volume: volume}
    end

    test "stores chunk encrypted and reads it back as plaintext", %{enc_volume: volume} do
      plaintext = "encrypted put_chunk payload"
      opts = BlobStore.resolve_put_chunk_opts(volume.id)

      assert {:ok, hash, info} =
               BlobStore.write_chunk(plaintext, "default", "hot", opts)

      # Stored payload is plaintext bytes + 16-byte GCM auth tag.
      assert info.original_size == byte_size(plaintext)
      assert info.stored_size == byte_size(plaintext) + 16

      # Round-trip via the codec-aware read path: supplying the same
      # key/nonce decrypts back to the original plaintext.
      assert {:ok, ^plaintext} =
               BlobStore.read_chunk_with_options(hash, "default", "hot",
                 decompress: false,
                 key: Keyword.fetch!(opts, :key),
                 nonce: Keyword.fetch!(opts, :nonce)
               )
    end

    test "compressed + encrypted volume applies both on write", %{enc_volume: _default} do
      {:ok, volume} =
        VolumeRegistry.create(unique_vol_name(),
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :zstd, level: 3}
        )

      {:ok, _version} = KeyManager.setup_volume_encryption(volume.id)

      plaintext = String.duplicate("highly compressible payload ", 200)
      opts = BlobStore.resolve_put_chunk_opts(volume.id)

      assert {:ok, hash, info} = BlobStore.write_chunk(plaintext, "default", "hot", opts)

      # The NIF pipeline is compress-then-encrypt. Compressed size is
      # smaller than plaintext; the GCM tag then adds 16 bytes. We only
      # assert the result is smaller than plaintext rather than a precise
      # figure, because zstd output is implementation-sensitive.
      assert info.stored_size < byte_size(plaintext)
      assert info.compression == "zstd:3"

      assert {:ok, ^plaintext} =
               BlobStore.read_chunk_with_options(hash, "default", "hot",
                 decompress: true,
                 compression: "zstd:3",
                 key: Keyword.fetch!(opts, :key),
                 nonce: Keyword.fetch!(opts, :nonce)
               )
    end
  end

  defp unique_vol_name, do: "vol-resolve-#{:rand.uniform(999_999_999)}"
end
