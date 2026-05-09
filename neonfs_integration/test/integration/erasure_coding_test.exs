defmodule NeonFS.Integration.ErasureCodingTest do
  @moduledoc """
  Phase 4 integration tests for erasure coding.

  Tests the full erasure coding lifecycle:
  - Write to erasure-coded volume and read back
  - Degraded read (missing chunks within parity tolerance)
  - Critical failure (too many missing chunks)
  - Stripe repair after chunk loss
  - GC cleanup of erasure-coded files

  - Mixed cluster with both replicated and erasure-coded volumes
  - CLI handler volume creation with erasure durability
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_single_node_cluster(cluster, name: "ec-test")

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "ec-volume",
        %{"durability" => "erasure:2:1"}
      ])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "rep-volume",
        %{"durability" => "replicate:1"}
      ])

    %{}
  end

  describe "erasure write and read" do
    @tag :pending_903
    test "write file to erasure volume and read back", %{cluster: cluster} do
      test_data = :crypto.strong_rand_bytes(4096)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "ec-volume",
          "/basic.bin",
          test_data
        ])

      assert is_list(file.stripes)
      assert file.stripes != []

      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "ec-volume",
          "/basic.bin"
        ])

      assert read_data == test_data
    end

    @tag :pending_903
    test "small file on erasure volume (single partial stripe)", %{cluster: cluster} do
      # Small file — only one partial stripe with 2+1 config
      test_data = :crypto.strong_rand_bytes(512)

      {:ok, _file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "ec-volume",
          "/small.bin",
          test_data
        ])

      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "ec-volume",
          "/small.bin"
        ])

      assert read_data == test_data
      assert byte_size(read_data) == 512
    end

    @tag :pending_903
    test "large file spanning multiple stripes", %{cluster: cluster} do
      # Large file: will span multiple stripes
      test_data = :crypto.strong_rand_bytes(100 * 1024)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "ec-volume",
          "/large.bin",
          test_data
        ])

      assert file.stripes != []

      # Full read
      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "ec-volume",
          "/large.bin"
        ])

      assert read_data == test_data

      # Partial reads at various offsets
      {:ok, partial} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file_partial, [
          "ec-volume",
          "/large.bin",
          1000,
          2000
        ])

      assert partial == binary_part(test_data, 1000, 2000)
    end
  end

  describe "degraded read" do
    @tag :pending_903
    test "read succeeds with one chunk missing (within parity tolerance)", %{cluster: cluster} do
      test_data = :crypto.strong_rand_bytes(4096)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "ec-volume",
          "/degrade.bin",
          test_data
        ])

      # Get stripe and delete one data chunk
      [%{stripe_id: sid} | _] = file.stripes

      {:ok, stripe} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.StripeIndex, :get, [file.volume_id, sid])

      [first_hash | _] = stripe.chunks
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.ChunkIndex, :delete, [first_hash])

      # Read should still succeed via degraded reconstruction
      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "ec-volume",
          "/degrade.bin"
        ])

      assert read_data == test_data
    end

    @tag :pending_903
    test "read fails when too many chunks missing", %{cluster: cluster} do
      test_data = :crypto.strong_rand_bytes(4096)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "ec-volume",
          "/critical.bin",
          test_data
        ])

      # Delete 2 chunks (> parity_chunks=1)
      [%{stripe_id: sid} | _] = file.stripes

      {:ok, stripe} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.StripeIndex, :get, [file.volume_id, sid])

      stripe.chunks
      |> Enum.take(2)
      |> Enum.each(fn hash ->
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ChunkIndex, :delete, [hash])
      end)

      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "ec-volume",
          "/critical.bin"
        ])

      assert {:error, %{class: :unavailable}} = result
    end
  end

  describe "stripe repair" do
    @tag :pending_903
    test "repair restores degraded stripe", %{cluster: cluster} do
      test_data = :crypto.strong_rand_bytes(4096)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "ec-volume",
          "/repair.bin",
          test_data
        ])

      [%{stripe_id: sid} | _] = file.stripes

      {:ok, stripe} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.StripeIndex, :get, [file.volume_id, sid])

      # Delete one chunk
      [first_hash | _] = stripe.chunks
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.ChunkIndex, :delete, [first_hash])

      # Scan should detect degraded
      degraded =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.StripeRepair, :scan_stripes, [])

      assert Enum.any?(degraded, fn {_vol, id, state, _} ->
               id == sid and state == :degraded
             end)

      # Repair
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.StripeRepair, :repair_stripe, [
          file.volume_id,
          sid
        ])

      assert :ok = result
    end
  end

  describe "garbage collection" do
    @describetag cluster_mode: :per_test

    setup %{cluster: cluster} do
      :ok = init_single_node_cluster(cluster, name: "gc-ec-test")

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "gc-ec-volume",
          %{"durability" => "erasure:2:1"}
        ])

      %{}
    end

    @tag :pending_903
    test "GC cleans up erasure-coded file chunks and stripes", %{cluster: cluster} do
      test_data = :crypto.strong_rand_bytes(4096)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "gc-ec-volume",
          "/gc-test.bin",
          test_data
        ])

      [%{stripe_id: sid} | _] = file.stripes

      {:ok, stripe} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.StripeIndex, :get, [
          file.volume_id,
          sid
        ])

      chunk_hashes = stripe.chunks

      # Delete the file
      PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :delete_file, [
        "gc-ec-volume",
        "/gc-test.bin"
      ])

      # Run GC
      {:ok, gc_result} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.GarbageCollector, :collect, [])

      assert gc_result.chunks_deleted > 0
      assert gc_result.stripes_deleted > 0

      # Verify chunks are gone
      Enum.each(chunk_hashes, fn hash ->
        result =
          PeerCluster.rpc(cluster, :node1, NeonFS.Core.ChunkIndex, :get, [
            file.volume_id,
            hash
          ])

        assert {:error, :not_found} = result
      end)

      # Verify stripe metadata is gone
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.StripeIndex, :get, [file.volume_id, sid])

      assert {:error, :not_found} = result
    end
  end

  describe "mixed cluster" do
    @tag :pending_903
    test "replicated and erasure volumes coexist", %{cluster: cluster} do
      rep_data = :crypto.strong_rand_bytes(2048)
      ec_data = :crypto.strong_rand_bytes(2048)

      # Write to both volumes
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "rep-volume",
          "/rep.bin",
          rep_data
        ])

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "ec-volume",
          "/ec.bin",
          ec_data
        ])

      # Read from both
      {:ok, read_rep} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "rep-volume",
          "/rep.bin"
        ])

      {:ok, read_ec} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "ec-volume",
          "/ec.bin"
        ])

      assert read_rep == rep_data
      assert read_ec == ec_data
    end
  end

  describe "CLI handler" do
    test "create volume with erasure durability string", %{cluster: cluster} do
      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "cli-ec-vol",
          %{"durability" => "erasure:4:2"}
        ])

      assert volume.durability == %{type: :erasure, data_chunks: 4, parity_chunks: 2}
      assert volume.durability_display == "erasure:4+2 (1.50x overhead)"
    end
  end
end
