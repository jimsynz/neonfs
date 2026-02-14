defmodule NeonFS.Integration.KeyRotationTest do
  @moduledoc """
  Phase 6 integration tests for key rotation.

  Tests the full key rotation lifecycle:
  - Write file with key v1, rotate to v2, read back (mixed versions)
  - Rotation status reporting
  - Complete rotation — all chunks at new version
  """
  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Core.KeyManager
  alias NeonFS.Core.VolumeEncryption
  alias NeonFS.Core.VolumeRegistry

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 1

  describe "key rotation lifecycle" do
    test "write with v1, rotate to v2, read back succeeds", %{cluster: cluster} do
      :ok = init_rotation_cluster(cluster)

      # Write file with initial key (v1)
      test_data = :crypto.strong_rand_bytes(4096)

      {:ok, _file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "rotation-volume",
          "/v1-file.bin",
          test_data
        ])

      # Verify current key version is 1
      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, VolumeRegistry, :get_by_name, [
          "rotation-volume"
        ])

      assert volume.encryption.current_key_version == 1

      # Start key rotation
      {:ok, rotation} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.KeyRotation, :start_rotation, [
          volume.id
        ])

      assert rotation.from_version == 1
      assert rotation.to_version == 2

      # Read back should still work (mixed key versions)
      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "rotation-volume",
          "/v1-file.bin"
        ])

      assert read_data == test_data
    end

    test "rotation status reports progress", %{cluster: cluster} do
      :ok = init_rotation_cluster(cluster)

      # Write a file so there's something to rotate
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "rotation-volume",
          "/status-test.bin",
          :crypto.strong_rand_bytes(4096)
        ])

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, VolumeRegistry, :get_by_name, [
          "rotation-volume"
        ])

      # Start rotation
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.KeyRotation, :start_rotation, [
          volume.id
        ])

      # Check rotation status
      {:ok, status} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.KeyRotation, :rotation_status, [
          volume.id
        ])

      assert is_map(status)
      assert Map.has_key?(status, :from_version)
      assert Map.has_key?(status, :to_version)
      assert status.from_version == 1
      assert status.to_version == 2
    end

    test "complete rotation — data still readable after rotation finishes", %{cluster: cluster} do
      :ok = init_rotation_cluster(cluster)

      test_data = :crypto.strong_rand_bytes(4096)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "rotation-volume",
          "/rotate-complete.bin",
          test_data
        ])

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, VolumeRegistry, :get_by_name, [
          "rotation-volume"
        ])

      # Start rotation — this should re-encrypt all chunks
      {:ok, rotation} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.KeyRotation, :start_rotation, [
          volume.id
        ])

      assert rotation.total_chunks > 0

      # Wait for rotation to complete (the worker processes chunks asynchronously)
      assert_eventually timeout: 30_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.Core.KeyRotation, :rotation_status, [
               volume.id
             ]) do
          {:error, :no_rotation} ->
            # Rotation completed and was cleaned up
            true

          {:ok, %{progress: %{total_chunks: total, migrated: migrated}}} ->
            migrated >= total

          _ ->
            false
        end
      end

      # Verify data still reads correctly after rotation
      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "rotation-volume",
          "/rotate-complete.bin"
        ])

      assert read_data == test_data

      # Verify chunks have been re-encrypted to v2
      Enum.each(file.chunks, fn chunk_hash ->
        case PeerCluster.rpc(cluster, :node1, NeonFS.Core.ChunkIndex, :get, [chunk_hash]) do
          {:ok, chunk_meta} when chunk_meta.crypto != nil ->
            assert chunk_meta.crypto.key_version == 2,
                   "Chunk should be at key version 2 after rotation, " <>
                     "got v#{chunk_meta.crypto.key_version}"

          _ ->
            :ok
        end
      end)
    end
  end

  describe "CLI handler key rotation" do
    test "rotate_volume_key via handler", %{cluster: cluster} do
      :ok = init_rotation_cluster(cluster)

      # Write a file first
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "rotation-volume",
          "/cli-rotate.bin",
          :crypto.strong_rand_bytes(1024)
        ])

      # Start rotation via CLI handler
      {:ok, result} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :rotate_volume_key, [
          "rotation-volume"
        ])

      assert Map.has_key?(result, :from_version)
      assert Map.has_key?(result, :to_version)
      assert result.from_version == 1
      assert result.to_version == 2
    end

    test "rotation_status via handler", %{cluster: cluster} do
      :ok = init_rotation_cluster(cluster)

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "rotation-volume",
          "/cli-status.bin",
          :crypto.strong_rand_bytes(1024)
        ])

      # Start rotation
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :rotate_volume_key, [
          "rotation-volume"
        ])

      # Query status via handler
      {:ok, status} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :rotation_status, [
          "rotation-volume"
        ])

      assert is_map(status)
      assert Map.has_key?(status, :from_version)
      assert Map.has_key?(status, :to_version)
    end
  end

  describe "rotation edge cases" do
    test "cannot rotate unencrypted volume", %{cluster: cluster} do
      init_cluster_base(cluster)

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "plain-volume",
          %{}
        ])

      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.KeyRotation, :start_rotation, [
          volume.id
        ])

      assert {:error, :not_encrypted} = result
    end
  end

  # ─── Helpers ──────────────────────────────────────────────────────────

  defp init_cluster_base(cluster) do
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["rotation-test"])

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

  defp init_rotation_cluster(cluster) do
    init_cluster_base(cluster)

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :create, [
        "rotation-volume",
        [
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :none}
        ]
      ])

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :get_by_name, [
        "rotation-volume"
      ])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, KeyManager, :setup_volume_encryption, [
        volume.id
      ])

    :ok
  end
end
