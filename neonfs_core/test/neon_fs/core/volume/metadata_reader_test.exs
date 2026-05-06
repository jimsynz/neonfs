defmodule NeonFS.Core.Volume.MetadataReaderTest do
  use ExUnit.Case, async: true

  alias NeonFS.Cluster.State, as: ClusterState
  alias NeonFS.Core.Volume.MetadataReader
  alias NeonFS.Core.Volume.RootSegment

  describe "get/4" do
    test "returns {:ok, value} when the index tree NIF returns a binary" do
      assert {:ok, "value-bytes"} =
               MetadataReader.get("vol-1", :file_index, "the-key", build_opts())
    end

    test "returns {:error, :not_found} when the index tree NIF returns nil" do
      opts = build_opts(index_tree_get: fn _store, _root, _tier, _key -> {:ok, nil} end)

      assert {:error, :not_found} =
               MetadataReader.get("vol-1", :chunk_index, "missing", opts)
    end

    test "passes the correct index tree root to the NIF" do
      tree_root = <<1::256>>

      segment =
        sample_segment(
          index_roots: %{
            file_index: tree_root,
            chunk_index: nil,
            stripe_index: nil
          }
        )

      table = :ets.new(:nif_calls, [:public, :duplicate_bag])

      capture_get = fn _store, root_hash, _tier, _key ->
        :ets.insert(table, {:root, root_hash})
        {:ok, "v"}
      end

      opts =
        build_opts(
          root_chunk_reader: const_chunk_reader(segment),
          index_tree_get: capture_get
        )

      assert {:ok, "v"} = MetadataReader.get("vol-1", :file_index, "k", opts)
      assert [{:root, ^tree_root}] = :ets.lookup(table, :root)
    end

    test "passes <<>> for an empty index tree root" do
      segment =
        sample_segment(index_roots: %{file_index: nil, chunk_index: nil, stripe_index: nil})

      table = :ets.new(:nif_calls, [:public, :duplicate_bag])

      capture_get = fn _store, root_hash, _tier, _key ->
        :ets.insert(table, {:root, root_hash})
        {:ok, nil}
      end

      opts =
        build_opts(
          root_chunk_reader: const_chunk_reader(segment),
          index_tree_get: capture_get
        )

      assert {:error, :not_found} =
               MetadataReader.get("vol-1", :stripe_index, "k", opts)

      assert [{:root, <<>>}] = :ets.lookup(table, :root)
    end

    test "surfaces cluster_state_unavailable" do
      opts = build_opts(cluster_state_loader: fn -> {:error, :not_loaded} end)

      assert {:error, {:cluster_state_unavailable, :not_loaded}} =
               MetadataReader.get("vol-1", :file_index, "k", opts)
    end

    test "surfaces bootstrap_query_failed" do
      opts = build_opts(bootstrap_lookup: fn _ -> {:error, :ra_timeout} end)

      assert {:error, {:bootstrap_query_failed, :ra_timeout}} =
               MetadataReader.get("vol-1", :file_index, "k", opts)
    end

    test "returns :not_found when the volume is not in the bootstrap layer" do
      opts = build_opts(bootstrap_lookup: fn _ -> {:error, :not_found} end)

      assert {:error, :not_found} =
               MetadataReader.get("vol-missing", :file_index, "k", opts)
    end

    test "surfaces root_chunk_unreachable" do
      opts =
        build_opts(root_chunk_reader: fn _entry, _opts -> {:error, :io_error} end)

      assert {:error, {:root_chunk_unreachable, :io_error}} =
               MetadataReader.get("vol-1", :file_index, "k", opts)
    end

    test "surfaces malformed_root_segment when decode fails" do
      opts =
        build_opts(root_chunk_reader: fn _entry, _opts -> {:ok, <<0, 1, 2, 3>>} end)

      assert {:error, {:malformed_root_segment, _}} =
               MetadataReader.get("vol-1", :file_index, "k", opts)
    end

    test "surfaces cluster_mismatch when segment cluster_id differs" do
      foreign_segment = sample_segment(cluster_id: "clust-other")

      opts = build_opts(root_chunk_reader: const_chunk_reader(foreign_segment))

      assert {:error, {:cluster_mismatch, expected: "clust-test", actual: "clust-other"}} =
               MetadataReader.get("vol-1", :file_index, "k", opts)
    end

    test "surfaces index_tree_read_failed for a NIF error" do
      opts =
        build_opts(
          index_tree_get: fn _store, _root, _tier, _key -> {:error, "missing chunk: …"} end
        )

      assert {:error, {:index_tree_read_failed, _}} =
               MetadataReader.get("vol-1", :file_index, "k", opts)
    end
  end

  describe "range/5" do
    test "returns the NIF's entries on success" do
      entries = [{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}]

      opts =
        build_opts(index_tree_range: fn _store, _root, _tier, _start, _end -> {:ok, entries} end)

      assert {:ok, ^entries} = MetadataReader.range("vol-1", :file_index, "", "", opts)
    end

    test "surfaces NIF errors as index_tree_read_failed" do
      opts =
        build_opts(
          index_tree_range: fn _store, _root, _tier, _start, _end ->
            {:error, "decode error"}
          end
        )

      assert {:error, {:index_tree_read_failed, _}} =
               MetadataReader.range("vol-1", :file_index, "", "", opts)
    end
  end

  ## Helpers

  defp sample_segment(overrides \\ []) do
    base =
      RootSegment.new(
        volume_id: "vol-1",
        volume_name: "vol1",
        cluster_id: "clust-test",
        cluster_name: "test-cluster",
        durability: %{type: :replicate, factor: 1, min_copies: 1}
      )

    Enum.reduce(overrides, base, fn {k, v}, seg -> Map.put(seg, k, v) end)
  end

  defp sample_cluster_state do
    %ClusterState{
      cluster_id: "clust-test",
      cluster_name: "test-cluster",
      created_at: DateTime.utc_now(),
      master_key: <<0::256>>,
      this_node: node()
    }
  end

  defp sample_root_entry do
    %{
      volume_id: "vol-1",
      root_chunk_hash: <<99::256>>,
      drive_locations: [%{node: node(), drive_id: "drv-local"}],
      durability_cache: %{type: :replicate, factor: 1, min_copies: 1},
      updated_at: DateTime.utc_now()
    }
  end

  defp const_chunk_reader(segment) do
    fn _entry, _opts -> {:ok, RootSegment.encode(segment)} end
  end

  defp build_opts(extra \\ []) do
    defaults = [
      cluster_state_loader: fn -> {:ok, sample_cluster_state()} end,
      bootstrap_lookup: fn _vol -> {:ok, sample_root_entry()} end,
      root_chunk_reader: const_chunk_reader(sample_segment()),
      store_handle: :stub_store_handle,
      index_tree_get: fn _store, _root, _tier, _key -> {:ok, "value-bytes"} end,
      index_tree_range: fn _store, _root, _tier, _start, _end -> {:ok, []} end
    ]

    Keyword.merge(defaults, extra)
  end
end
