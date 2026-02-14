defmodule NeonFS.Integration.EncryptionTest do
  @moduledoc """
  Phase 6 integration tests for volume encryption.

  Tests the full encryption lifecycle:
  - Write to encrypted volume and read back (round-trip)
  - Verify raw chunks are not plaintext (ciphertext check)
  - Unencrypted and encrypted volumes coexist without interference
  - Encrypted erasure-coded volume (encrypt + EC compose correctly)
  - CLI handler volume creation with encryption flag
  """
  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Core.KeyManager
  alias NeonFS.Core.VolumeEncryption
  alias NeonFS.Core.VolumeRegistry

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 1

  describe "encrypted volume round-trip" do
    test "create encrypted volume, write file, read back", %{cluster: cluster} do
      :ok = init_encrypted_cluster(cluster)

      test_data = :crypto.strong_rand_bytes(4096)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "enc-volume",
          "/secret.bin",
          test_data
        ])

      assert file.size == byte_size(test_data)
      assert file.chunks != [] or file.stripes != []

      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "enc-volume",
          "/secret.bin"
        ])

      assert read_data == test_data
    end

    test "raw encrypted chunk is not plaintext", %{cluster: cluster} do
      :ok = init_encrypted_cluster(cluster)

      # Use recognisable pattern data so we can detect if it's stored in plaintext
      test_data = String.duplicate("PLAINTEXT_MARKER_DATA_", 200)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "enc-volume",
          "/plaincheck.bin",
          test_data
        ])

      # Get chunk hash from file metadata
      [chunk_hash | _] = file.chunks

      # Read raw chunk from BlobStore without decryption
      {:ok, raw_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.BlobStore, :read_chunk, [
          chunk_hash,
          "default",
          [tier: "hot", verify: false, decompress: false]
        ])

      # Raw data should NOT contain our plaintext marker
      refute String.contains?(raw_data, "PLAINTEXT_MARKER_DATA_"),
             "Encrypted chunk contains plaintext data — encryption may not be working"
    end
  end

  describe "mixed encryption modes" do
    test "unencrypted and encrypted volumes coexist", %{cluster: cluster} do
      :ok = init_mixed_encryption_cluster(cluster)

      plain_data = :crypto.strong_rand_bytes(2048)
      secret_data = :crypto.strong_rand_bytes(2048)

      # Write to both volumes
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "plain-volume",
          "/plain.bin",
          plain_data
        ])

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "enc-volume",
          "/secret.bin",
          secret_data
        ])

      # Read from both
      {:ok, read_plain} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "plain-volume",
          "/plain.bin"
        ])

      {:ok, read_secret} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "enc-volume",
          "/secret.bin"
        ])

      assert read_plain == plain_data
      assert read_secret == secret_data
    end
  end

  describe "encrypted erasure-coded volume" do
    test "write and read with encryption + erasure coding", %{cluster: cluster} do
      :ok = init_encrypted_erasure_cluster(cluster)

      test_data = :crypto.strong_rand_bytes(4096)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "enc-ec-volume",
          "/ec-secret.bin",
          test_data
        ])

      assert is_list(file.stripes)
      assert file.stripes != []

      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "enc-ec-volume",
          "/ec-secret.bin"
        ])

      assert read_data == test_data
    end

    test "degraded read with decryption succeeds", %{cluster: cluster} do
      :ok = init_encrypted_erasure_cluster(cluster)

      test_data = :crypto.strong_rand_bytes(4096)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "enc-ec-volume",
          "/degrade-enc.bin",
          test_data
        ])

      # Delete one chunk to force degraded read
      [%{stripe_id: sid} | _] = file.stripes

      {:ok, stripe} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.StripeIndex, :get, [sid])

      [first_hash | _] = stripe.chunks
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.ChunkIndex, :delete, [first_hash])

      # Degraded read should still succeed with decryption
      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "enc-ec-volume",
          "/degrade-enc.bin"
        ])

      assert read_data == test_data
    end
  end

  describe "CLI handler" do
    test "create volume with --encryption server-side via handler", %{cluster: cluster} do
      init_cluster_base(cluster)

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "cli-enc-vol",
          %{"encryption" => %{"mode" => "server_side"}}
        ])

      assert volume.encryption.mode in [:server_side, "server_side"]
    end

    test "volume info shows encryption status", %{cluster: cluster} do
      :ok = init_encrypted_cluster(cluster)

      {:ok, volume_info} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :get_volume, ["enc-volume"])

      assert volume_info[:encryption][:mode] in [:server_side, "server_side"]
      assert is_integer(volume_info[:encryption][:current_key_version])
    end
  end

  # ─── Helpers ──────────────────────────────────────────────────────────

  defp init_cluster_base(cluster) do
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["enc-test"])

    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_status, []) do
            {:ok, _} -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )
  end

  defp init_encrypted_cluster(cluster) do
    init_cluster_base(cluster)

    # Create encrypted volume directly via VolumeRegistry to set compression: none
    # This avoids compression+encryption interaction issues in round-trip tests
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :create, [
        "enc-volume",
        [
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :none}
        ]
      ])

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :get_by_name, [
        "enc-volume"
      ])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, KeyManager, :setup_volume_encryption, [
        volume.id
      ])

    :ok
  end

  defp init_mixed_encryption_cluster(cluster) do
    init_cluster_base(cluster)

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "plain-volume",
        %{}
      ])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :create, [
        "enc-volume",
        [
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :none}
        ]
      ])

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :get_by_name, [
        "enc-volume"
      ])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, KeyManager, :setup_volume_encryption, [
        volume.id
      ])

    :ok
  end

  defp init_encrypted_erasure_cluster(cluster) do
    init_cluster_base(cluster)

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :create, [
        "enc-ec-volume",
        [
          durability: %{type: :erasure, data_chunks: 2, parity_chunks: 1},
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :none}
        ]
      ])

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :get_by_name, [
        "enc-ec-volume"
      ])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, KeyManager, :setup_volume_encryption, [
        volume.id
      ])

    :ok
  end
end
