defmodule NeonFS.Core.Volume.MetadataWriterTest do
  use ExUnit.Case, async: true

  alias NeonFS.Cluster.State, as: ClusterState
  alias NeonFS.Core.Volume.MetadataWriter
  alias NeonFS.Core.Volume.RootSegment

  describe "put/5" do
    test "calls the put NIF, encodes a fresh segment, replicates, and updates the bootstrap entry" do
      capture = build_capture()

      opts = build_opts(capture: capture)

      assert {:ok, "new-root-hash"} =
               MetadataWriter.put("vol-1", :file_index, "k", "v", opts)

      [{op, store, root, _tier, key, value}] = :ets.lookup(capture.tree_calls, :put)
      assert op == :put
      assert store == :stub_store_handle
      # The current tree root for :file_index is `nil` in the sample
      # segment → MetadataWriter normalises to `<<>>` for the NIF.
      assert root == <<>>
      assert key == "k"
      assert value == "v"

      assert [{:replicate, encoded}] = :ets.lookup(capture.replicator_calls, :replicate)
      assert {:ok, segment} = RootSegment.decode(encoded)
      assert segment.index_roots.file_index == "new-tree-root"

      assert [{:bootstrap, command}] = :ets.lookup(capture.bootstrap_calls, :bootstrap)
      {:update_volume_root, "vol-1", payload} = command
      assert payload.root_chunk_hash == "new-root-hash"
      assert payload.durability_cache == segment.durability
    end

    test "advances the HLC and bumps last_written_by_neonfs_version" do
      capture = build_capture()
      opts = build_opts(capture: capture)

      original_segment = sample_segment()

      assert {:ok, _} = MetadataWriter.put("vol-1", :file_index, "k", "v", opts)

      [{:replicate, encoded}] = :ets.lookup(capture.replicator_calls, :replicate)
      assert {:ok, new_segment} = RootSegment.decode(encoded)

      # HLC advanced (counter or wall increased).
      assert new_segment.hlc != original_segment.hlc

      # last_written_by_neonfs_version is the running app's version,
      # so just check it's a binary and present.
      assert is_binary(new_segment.last_written_by_neonfs_version)
    end
  end

  describe "delete/4" do
    test "calls the delete NIF and commits a new segment" do
      capture = build_capture()
      opts = build_opts(capture: capture)

      assert {:ok, "new-root-hash"} =
               MetadataWriter.delete("vol-1", :file_index, "k", opts)

      [{:delete, _store, _root, _tier, "k"}] = :ets.lookup(capture.tree_calls, :delete)
    end
  end

  describe "purge_tombstones/4" do
    test "errors with an empty index tree (current root is nil)" do
      capture = build_capture()
      opts = build_opts(capture: capture)

      assert {:error, {:index_tree_write_failed, _}} =
               MetadataWriter.purge_tombstones("vol-1", :file_index, 1_000, opts)
    end

    test "calls the purge NIF when the index tree has a root" do
      capture = build_capture()

      segment_with_root =
        sample_segment(
          index_roots: %{
            file_index: <<99::256>>,
            chunk_index: nil,
            stripe_index: nil
          }
        )

      opts =
        build_opts(
          capture: capture,
          root_chunk_reader: const_chunk_reader(segment_with_root)
        )

      assert {:ok, "new-root-hash"} =
               MetadataWriter.purge_tombstones("vol-1", :file_index, 1_000, opts)

      [{:purge, _store, root, _tier, before_nanos}] =
        :ets.lookup(capture.tree_calls, :purge)

      assert root == <<99::256>>
      assert before_nanos == 1_000
    end
  end

  describe "error surfacing" do
    test "passes through bootstrap_query_failed from the read path" do
      capture = build_capture()

      opts =
        build_opts(
          capture: capture,
          bootstrap_lookup: fn _ -> {:error, :ra_timeout} end
        )

      assert {:error, {:bootstrap_query_failed, :ra_timeout}} =
               MetadataWriter.put("vol-1", :file_index, "k", "v", opts)
    end

    test "surfaces index_tree_write_failed when the NIF errors" do
      capture = build_capture()

      opts =
        build_opts(
          capture: capture,
          index_tree_put: fn _store, _root, _tier, _key, _value ->
            {:error, "missing chunk"}
          end
        )

      assert {:error, {:index_tree_write_failed, "missing chunk"}} =
               MetadataWriter.put("vol-1", :file_index, "k", "v", opts)
    end

    test "surfaces insufficient_replicas when the chunk replicator can't meet quorum" do
      capture = build_capture()

      failing_replicator = %{
        write_chunk: fn _data, _drives, _opts ->
          {:error, :insufficient_replicas, %{successful: [], failed: [], needed: 2}}
        end
      }

      opts =
        build_opts(
          capture: capture,
          chunk_replicator: stub_replicator(failing_replicator)
        )

      assert {:error, :insufficient_replicas, %{needed: 2}} =
               MetadataWriter.put("vol-1", :file_index, "k", "v", opts)
    end

    test "surfaces bootstrap_update_failed when Ra rejects the command" do
      capture = build_capture()

      opts =
        build_opts(
          capture: capture,
          bootstrap_registrar: fn _ -> {:error, :ra_timeout} end
        )

      assert {:error, {:bootstrap_update_failed, :ra_timeout}} =
               MetadataWriter.put("vol-1", :file_index, "k", "v", opts)
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
        durability: %{type: :replicate, factor: 2, min_copies: 1}
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
      durability_cache: %{type: :replicate, factor: 2, min_copies: 1},
      updated_at: DateTime.utc_now()
    }
  end

  defp sample_drives do
    [
      %{
        drive_id: "drv-local",
        node: node(),
        cluster_id: "clust-test",
        on_disk_format_version: 1,
        registered_at: DateTime.utc_now()
      }
    ]
  end

  defp const_chunk_reader(segment) do
    fn _entry, _opts -> {:ok, RootSegment.encode(segment)} end
  end

  defp build_capture do
    %{
      tree_calls: :ets.new(:tree_calls, [:public, :duplicate_bag]),
      replicator_calls: :ets.new(:replicator_calls, [:public, :duplicate_bag]),
      bootstrap_calls: :ets.new(:bootstrap_calls, [:public, :duplicate_bag])
    }
  end

  defp build_opts(extra) do
    capture = Keyword.fetch!(extra, :capture)
    extra = Keyword.delete(extra, :capture)

    capturing_replicator = %{
      write_chunk: fn data, _drives, _opts ->
        :ets.insert(capture.replicator_calls, {:replicate, data})
        {:ok, "new-root-hash", %{successful: ["drv-local"], failed: []}}
      end
    }

    capturing_registrar = fn command ->
      :ets.insert(capture.bootstrap_calls, {:bootstrap, command})
      {:ok, :updated}
    end

    capturing_put = fn store, root, tier, key, value ->
      :ets.insert(capture.tree_calls, {:put, store, root, tier, key, value})
      {:ok, "new-tree-root"}
    end

    capturing_delete = fn store, root, tier, key ->
      :ets.insert(capture.tree_calls, {:delete, store, root, tier, key})
      {:ok, "new-tree-root"}
    end

    capturing_purge = fn store, root, tier, before_nanos ->
      :ets.insert(capture.tree_calls, {:purge, store, root, tier, before_nanos})
      {:ok, "new-tree-root"}
    end

    defaults = [
      cluster_state_loader: fn -> {:ok, sample_cluster_state()} end,
      bootstrap_lookup: fn _vol -> {:ok, sample_root_entry()} end,
      root_chunk_reader: const_chunk_reader(sample_segment()),
      drive_lister: fn -> {:ok, sample_drives()} end,
      store_handle: :stub_store_handle,
      chunk_replicator: stub_replicator(capturing_replicator),
      bootstrap_registrar: capturing_registrar,
      index_tree_put: capturing_put,
      index_tree_delete: capturing_delete,
      index_tree_purge_tombstones: capturing_purge
    ]

    Keyword.merge(defaults, extra)
  end

  defp stub_replicator(%{write_chunk: write_chunk_fn}) do
    name =
      String.to_atom(
        "Elixir.NeonFS.Core.Volume.MetadataWriterTest.Replicator#{:erlang.unique_integer([:positive])}"
      )

    pt_key = {name, :write_chunk}
    :persistent_term.put(pt_key, write_chunk_fn)

    Module.create(
      name,
      quote do
        @pt_key unquote(Macro.escape(pt_key))
        def write_chunk(data, drives, opts) do
          fun = :persistent_term.get(@pt_key)
          fun.(data, drives, opts)
        end
      end,
      Macro.Env.location(__ENV__)
    )

    name
  end
end
