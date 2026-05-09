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
  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.KeyManager
  alias NeonFS.Core.VolumeEncryption
  alias NeonFS.Core.VolumeRegistry

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_single_node_cluster(cluster, name: "enc-test")

    # Encrypted volume (no compression — avoids compression+encryption interaction)
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :create, [
        "enc-volume",
        [
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :none}
        ]
      ])

    {:ok, enc_vol} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :get_by_name, ["enc-volume"])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, KeyManager, :setup_volume_encryption, [enc_vol.id])

    # Plain volume for mixed-mode test
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "plain-volume",
        %{}
      ])

    # Encrypted + erasure-coded volume
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :create, [
        "enc-ec-volume",
        [
          durability: %{type: :erasure, data_chunks: 2, parity_chunks: 1},
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :none}
        ]
      ])

    {:ok, enc_ec_vol} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :get_by_name, ["enc-ec-volume"])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, KeyManager, :setup_volume_encryption, [enc_ec_vol.id])

    %{}
  end

  describe "encrypted volume round-trip" do
    test "create encrypted volume, write file, read back", %{cluster: cluster} do
      test_data = :crypto.strong_rand_bytes(4096)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
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
      # Use recognisable pattern data so we can detect if it's stored in plaintext
      test_data = String.duplicate("PLAINTEXT_MARKER_DATA_", 200)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "enc-volume",
          "/plaincheck.bin",
          test_data
        ])

      # Get chunk hash from file metadata
      [chunk_hash | _] = file.chunks

      # Locate the raw on-disk file directly. Going through BlobStore.read_chunk
      # with no codec info can't find the chunk now that encrypted writes have
      # a nonce-derived codec suffix (#270) — and supplying the nonce would
      # trigger decryption. For this assertion we just want to compare raw
      # bytes on disk against the plaintext marker.
      hex = Base.encode16(chunk_hash, case: :lower)
      prefix1 = String.slice(hex, 0, 2)
      prefix2 = String.slice(hex, 2, 2)

      node1 = PeerCluster.get_node!(cluster, :node1).node

      drives =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.DriveRegistry, :drives_for_node, [
          node1
        ])

      chunk_paths =
        Enum.flat_map(drives, fn drive ->
          drive_chunk_dir = Path.join([drive.path, "blobs", "hot", prefix1, prefix2])

          PeerCluster.rpc(cluster, :node1, Path, :wildcard, [
            Path.join(drive_chunk_dir, "#{hex}.*")
          ])
        end)

      assert chunk_paths != [],
             "encrypted chunk #{hex} not found under any registered drive on node1"

      [chunk_path | _] = chunk_paths
      raw_data = PeerCluster.rpc(cluster, :node1, File, :read!, [chunk_path])

      # Raw data should NOT contain our plaintext marker
      refute String.contains?(raw_data, "PLAINTEXT_MARKER_DATA_"),
             "Encrypted chunk contains plaintext data — encryption may not be working"
    end
  end

  describe "mixed encryption modes" do
    test "unencrypted and encrypted volumes coexist", %{cluster: cluster} do
      plain_data = :crypto.strong_rand_bytes(2048)
      secret_data = :crypto.strong_rand_bytes(2048)

      # Write to both volumes
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "plain-volume",
          "/plain.bin",
          plain_data
        ])

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
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
    @tag :pending_903
    test "write and read with encryption + erasure coding", %{cluster: cluster} do
      test_data = :crypto.strong_rand_bytes(4096)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
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

    @tag :pending_903
    test "degraded read with decryption succeeds", %{cluster: cluster} do
      test_data = :crypto.strong_rand_bytes(4096)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "enc-ec-volume",
          "/degrade-enc.bin",
          test_data
        ])

      # Delete one chunk to force degraded read
      [%{stripe_id: sid} | _] = file.stripes

      {:ok, stripe} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.StripeIndex, :get, [
          file.volume_id,
          sid
        ])

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
      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "cli-enc-vol",
          %{"encryption" => %{"mode" => "server_side"}}
        ])

      assert volume.encryption.mode in [:server_side, "server_side"]
    end

    test "volume info shows encryption status", %{cluster: cluster} do
      {:ok, volume_info} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :get_volume, ["enc-volume"])

      assert volume_info[:encryption][:mode] in [:server_side, "server_side"]
      assert is_integer(volume_info[:encryption][:current_key_version])
    end
  end
end
