defmodule NeonFS.Integration.SingleNodeTest do
  @moduledoc """
  Single-node integration tests for core NeonFS functionality.

  These tests verify fundamental operations work correctly on a single node
  before testing distributed scenarios. They cover:
  - File read/write operations
  - Partial file reads
  - Directory structure
  - Large file chunking
  - Volume statistics
  - DETS persistence across restarts
  """
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 120_000
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    init_cluster_with_volume(cluster)
    %{}
  end

  describe "file operations" do
    test "write and read file", %{cluster: cluster} do
      # Write test data
      test_data = :crypto.strong_rand_bytes(100 * 1024)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "test-volume",
          "/test.bin",
          test_data
        ])

      assert file.size == byte_size(test_data)
      assert file.chunks != []

      # Read data back
      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "test-volume",
          "/test.bin"
        ])

      assert read_data == test_data
    end

    test "partial file reads work correctly", %{cluster: cluster} do
      # Write test data
      test_data = :crypto.strong_rand_bytes(1024)

      {:ok, _file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "test-volume",
          "/partial.bin",
          test_data
        ])

      # Read full file
      {:ok, full_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "test-volume",
          "/partial.bin"
        ])

      assert full_data == test_data

      # Read first 100 bytes
      {:ok, first_100} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file_partial, [
          "test-volume",
          "/partial.bin",
          0,
          100
        ])

      assert first_100 == binary_part(test_data, 0, 100)

      # Read middle 100 bytes
      {:ok, middle_100} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file_partial, [
          "test-volume",
          "/partial.bin",
          100,
          100
        ])

      assert middle_100 == binary_part(test_data, 100, 100)

      # Read last 100 bytes
      {:ok, last_100} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file_partial, [
          "test-volume",
          "/partial.bin",
          924,
          100
        ])

      assert last_100 == binary_part(test_data, 924, 100)
    end

    test "multiple files in directory structure", %{cluster: cluster} do
      # Write multiple files including subdirectories
      files = [
        {"/file1.txt", "Hello World"},
        {"/file2.txt", "Test Data"},
        {"/docs/readme.md", "# README"},
        {"/docs/guide.md", "# Guide"}
      ]

      for {path, content} <- files do
        {:ok, _file} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
            "test-volume",
            path,
            content
          ])
      end

      # Verify root directory listing
      {:ok, root_files} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :list_dir, [
          "test-volume",
          "/"
        ])

      root_names = Map.keys(root_files)
      assert "file1.txt" in root_names
      assert "file2.txt" in root_names

      # Verify subdirectory listing
      {:ok, docs_files} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :list_dir, [
          "test-volume",
          "/docs"
        ])

      docs_names = Map.keys(docs_files)
      assert "readme.md" in docs_names
      assert "guide.md" in docs_names
    end

    test "large file chunking and reassembly", %{cluster: cluster} do
      # Write large file (> 1MB to trigger chunking)
      test_data = :crypto.strong_rand_bytes(2 * 1024 * 1024)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "test-volume",
          "/large.bin",
          test_data
        ])

      # Verify file was chunked
      assert length(file.chunks) > 1, "Expected file to be split into multiple chunks"

      # Read back and verify
      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "test-volume",
          "/large.bin"
        ])

      assert read_data == test_data
      assert byte_size(read_data) == byte_size(test_data)
    end
  end

  describe "volume statistics" do
    test "volume statistics are tracked correctly", %{cluster: cluster} do
      # Capture volume stats before writing
      {:ok, volume_before} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :get_volume, ["test-volume"])

      assert is_integer(volume_before[:logical_size])
      assert is_integer(volume_before[:physical_size])
      assert is_integer(volume_before[:chunk_count])

      # Write some data
      test_data = :crypto.strong_rand_bytes(10 * 1024)

      {:ok, _file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "test-volume",
          "/stats_test.bin",
          test_data
        ])

      # Check volume info includes updated statistics
      {:ok, volume_after} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :get_volume, ["test-volume"])

      assert is_integer(volume_after[:logical_size])
      assert is_integer(volume_after[:physical_size])
      assert is_integer(volume_after[:chunk_count])

      # Stats should have increased after write
      assert volume_after[:logical_size] > volume_before[:logical_size]
      assert volume_after[:chunk_count] > volume_before[:chunk_count]
    end
  end

  describe "persistence" do
    @describetag cluster_mode: :per_test

    setup %{cluster: cluster} do
      init_cluster_with_volume(cluster)
      %{}
    end

    test "data persists across application restart", %{cluster: cluster} do
      # Write test data
      test_data = :crypto.strong_rand_bytes(50 * 1024)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "test-volume",
          "/persist.bin",
          test_data
        ])

      original_file_id = file.id

      # Trigger persistence snapshot before restart
      :ok = PeerCluster.rpc(cluster, :node1, NeonFS.Core.Persistence, :snapshot_now, [])

      # Stop and restart core application on the node
      :ok = PeerCluster.rpc(cluster, :node1, Application, :stop, [:neonfs_core])

      # Wait for app to fully stop
      :ok =
        wait_until(
          fn ->
            apps = PeerCluster.rpc(cluster, :node1, Application, :started_applications, [])
            app_names = Enum.map(apps, fn {name, _, _} -> name end)
            :neonfs_core not in app_names
          end,
          timeout: 5_000
        )

      # Restart the application
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, Application, :ensure_all_started, [:neonfs_core])

      # Wait for persistence to restore data
      assert_eventually timeout: 10_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
               "test-volume"
             ]) do
          {:ok, _} -> true
          _ -> false
        end
      end

      # Verify data was restored
      {:ok, restored_file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :get_file, [
          "test-volume",
          "/persist.bin"
        ])

      assert restored_file.id == original_file_id

      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "test-volume",
          "/persist.bin"
        ])

      assert read_data == test_data
    end
  end

  defp init_cluster_with_volume(cluster) do
    {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])

    # Wait for cluster to be ready
    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_status, []) do
            {:ok, _status} -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )

    # Create a test volume
    {:ok, _volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "test-volume",
        %{}
      ])

    :ok
  end
end
