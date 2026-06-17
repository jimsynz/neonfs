defmodule NeonFS.Core.Volume.MetadataWriterTest do
  use ExUnit.Case, async: true

  alias NeonFS.Cluster.State, as: ClusterState
  alias NeonFS.Core.Volume.MetadataWriter
  alias NeonFS.Core.Volume.RootSegment
  alias NeonFS.Error.QuorumUnavailable

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
      {:cas_update_volume_root, "vol-1", _shard, expected_prev, payload} = command
      assert is_binary(expected_prev)
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

    test "replicates every copy-on-write index-tree node chunk the write produced (#903)" do
      capture = build_capture()

      node_chunks = [{"node-hash-a", "node-bytes-a"}, {"node-hash-b", "node-bytes-b"}]

      recording_replicator = %{
        write_chunk: fn data, drives, opts ->
          :ets.insert(capture.replicator_calls, {:replicate, {data, drives, opts}})
          {:ok, "new-root-hash", %{successful: ["drv-local"], failed: []}}
        end
      }

      opts =
        build_opts(
          capture: capture,
          chunk_replicator: stub_replicator(recording_replicator),
          index_tree_put: fn store, root, tier, key, value ->
            :ets.insert(capture.tree_calls, {:put, store, root, tier, key, value})
            {:ok, {"new-tree-root", node_chunks}}
          end
        )

      assert {:ok, "new-root-hash"} = MetadataWriter.put("vol-1", :file_index, "k", "v", opts)

      replicated =
        capture.replicator_calls
        |> :ets.lookup(:replicate)
        |> Enum.map(fn {:replicate, {data, drives, write_opts}} -> {data, drives, write_opts} end)

      replicated_data = Enum.map(replicated, fn {data, _drives, _opts} -> data end)

      # Both node chunks AND the root segment are replicated, each
      # through the same drive set and min_copies quorum.
      assert "node-bytes-a" in replicated_data
      assert "node-bytes-b" in replicated_data
      assert length(replicated) == 3

      expected_drive_ids = Enum.map(sample_drives(), & &1.drive_id)

      for {_data, drives, write_opts} <- replicated do
        assert Enum.map(drives, & &1.drive_id) == expected_drive_ids
        assert Keyword.fetch!(write_opts, :min_copies) == 1
      end
    end

    test "aborts the write without flipping the bootstrap pointer when a node chunk can't reach quorum" do
      capture = build_capture()

      failing_node_replicator = %{
        write_chunk: fn _data, _drives, _opts ->
          {:error,
           QuorumUnavailable.exception(
             operation: :write_chunk,
             required: 2,
             available: 0,
             successful: [],
             failed: []
           )}
        end
      }

      opts =
        build_opts(
          capture: capture,
          chunk_replicator: stub_replicator(failing_node_replicator),
          index_tree_put: fn store, root, tier, key, value ->
            :ets.insert(capture.tree_calls, {:put, store, root, tier, key, value})
            {:ok, {"new-tree-root", [{"node-hash-a", "node-bytes-a"}]}}
          end
        )

      assert {:error, %QuorumUnavailable{}} =
               MetadataWriter.put("vol-1", :file_index, "k", "v", opts)

      assert [] = :ets.lookup(capture.bootstrap_calls, :bootstrap)
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

  describe "apply_batch/3" do
    test "applies every mutation but commits a single CAS (#1295)" do
      capture = build_capture()
      opts = build_opts(capture: capture)

      mutations = [
        {:put, :file_index, "k1", "v1"},
        {:put, :file_index, "k2", "v2"},
        {:delete, :file_index, "k3"}
      ]

      # All keys hash to the same shard at count 1, so they commit
      # together as one shard batch → `%{shard => root}`.
      assert {:ok, roots} = MetadataWriter.apply_batch("vol-1", mutations, opts)
      assert map_size(roots) == 1

      # Three tree ops applied...
      assert length(:ets.lookup(capture.tree_calls, :put)) == 2
      assert length(:ets.lookup(capture.tree_calls, :delete)) == 1

      # ...but only one bootstrap CAS / root flip.
      assert length(:ets.lookup(capture.bootstrap_calls, :bootstrap)) == 1
    end

    test "threads the tree root forward across mutations" do
      capture = build_capture()
      opts = build_opts(capture: capture)

      mutations = [
        {:put, :file_index, "k1", "v1"},
        {:put, :file_index, "k2", "v2"}
      ]

      assert {:ok, _} = MetadataWriter.apply_batch("vol-1", mutations, opts)

      roots =
        capture.tree_calls
        |> :ets.lookup(:put)
        |> Enum.map(fn {:put, _store, root, _tier, _key, _value} -> root end)

      # The second put builds on the first put's new tree root.
      assert "new-tree-root" in roots
    end

    test "an empty mutation list is a no-op with no CAS" do
      capture = build_capture()
      opts = build_opts(capture: capture)

      assert {:ok, _root} = MetadataWriter.apply_batch("vol-1", [], opts)

      assert :ets.lookup(capture.tree_calls, :put) == []
      assert :ets.lookup(capture.bootstrap_calls, :bootstrap) == []
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
          bootstrap_lookup: fn _, _shard -> {:error, :ra_timeout} end
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

    test "surfaces QuorumUnavailable when the chunk replicator can't meet quorum" do
      capture = build_capture()

      failing_replicator = %{
        write_chunk: fn _data, _drives, _opts ->
          {:error,
           QuorumUnavailable.exception(
             operation: :write_chunk,
             required: 2,
             available: 0,
             successful: [],
             failed: []
           )}
        end
      }

      opts =
        build_opts(
          capture: capture,
          chunk_replicator: stub_replicator(failing_replicator)
        )

      assert {:error, %QuorumUnavailable{required: 2}} =
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

  describe "CAS retry on :stale_pointer" do
    test "retries the whole flow on stale pointer and succeeds when the second CAS wins" do
      capture = build_capture()
      counter = :counters.new(1, [])

      flaky_registrar = fn command ->
        :ets.insert(capture.bootstrap_calls, {:bootstrap, command})
        :counters.add(counter, 1, 1)
        n = :counters.get(counter, 1)

        case n do
          1 ->
            {:error, {:stale_pointer, expected: "old", actual: "newer"}}

          _ ->
            {:ok, :updated}
        end
      end

      opts = build_opts(capture: capture, bootstrap_registrar: flaky_registrar)

      assert {:ok, "new-root-hash"} =
               MetadataWriter.put("vol-1", :file_index, "k", "v", opts)

      # Two bootstrap commands submitted: the first stale, the second
      # successful. Each was a CAS variant.
      bootstrap_calls = :ets.lookup(capture.bootstrap_calls, :bootstrap)
      assert length(bootstrap_calls) == 2

      Enum.each(bootstrap_calls, fn {_, cmd} ->
        assert match?({:cas_update_volume_root, "vol-1", _shard, _, _}, cmd)
      end)
    end

    test "exhausts retries and returns cas_retries_exhausted" do
      capture = build_capture()

      always_stale = fn command ->
        :ets.insert(capture.bootstrap_calls, {:bootstrap, command})
        {:error, {:stale_pointer, expected: "x", actual: "y"}}
      end

      opts =
        build_opts(
          capture: capture,
          bootstrap_registrar: always_stale,
          cas_retries: 1
        )

      assert {:error, {:cas_retries_exhausted, _}} =
               MetadataWriter.put("vol-1", :file_index, "k", "v", opts)

      # Initial attempt + 1 retry = 2 bootstrap submissions.
      assert length(:ets.lookup(capture.bootstrap_calls, :bootstrap)) == 2
    end
  end

  describe "remote fallback (#1045)" do
    test "re-dispatches the write to a node holding the root when there is no local replica" do
      capture = build_capture()
      parent = self()
      remote = %{node: :remote@host, drive_id: "drv-remote"}

      opts =
        build_opts(
          capture: capture,
          root_chunk_reader: fn _entry, _opts -> {:error, {:no_local_replica, [remote]}} end,
          remote_caller: fn node, fn_name, args ->
            send(parent, {:remote, node, fn_name, length(args)})
            {:ok, "remote-root-hash"}
          end
        )

      assert {:ok, "remote-root-hash"} = MetadataWriter.put("vol-1", :file_index, "k", "v", opts)
      assert_received {:remote, :remote@host, :put, 5}
    end

    test "does not re-dispatch on a successful local write" do
      capture = build_capture()
      parent = self()

      opts =
        build_opts(
          capture: capture,
          remote_caller: fn node, _fn, _args -> send(parent, {:remote, node}) && {:ok, "x"} end
        )

      assert {:ok, "new-root-hash"} = MetadataWriter.put("vol-1", :file_index, "k", "v", opts)
      refute_received {:remote, _}
    end

    test "does not re-dispatch when already remote-dispatched (no recursion)" do
      capture = build_capture()
      parent = self()
      remote = %{node: :remote@host, drive_id: "drv-remote"}

      opts =
        build_opts(
          capture: capture,
          __remote_dispatched: true,
          root_chunk_reader: fn _entry, _opts -> {:error, {:no_local_replica, [remote]}} end,
          remote_caller: fn node, _fn, _args -> send(parent, {:remote, node}) && {:ok, "x"} end
        )

      assert {:error, {:root_chunk_unreachable, {:no_local_replica, _}}} =
               MetadataWriter.put("vol-1", :file_index, "k", "v", opts)

      refute_received {:remote, _}
    end

    test "does not re-dispatch errors other than no_local_replica" do
      capture = build_capture()
      parent = self()

      opts =
        build_opts(
          capture: capture,
          root_chunk_reader: fn _entry, _opts -> {:error, :some_other_failure} end,
          remote_caller: fn node, _fn, _args -> send(parent, {:remote, node}) && {:ok, "x"} end
        )

      assert {:error, _} = MetadataWriter.put("vol-1", :file_index, "k", "v", opts)
      refute_received {:remote, _}
    end

    test "tries the next candidate node when the first remote attempt fails" do
      capture = build_capture()
      parent = self()

      locations = [
        %{node: :remote1@host, drive_id: "d1"},
        %{node: :remote2@host, drive_id: "d2"}
      ]

      opts =
        build_opts(
          capture: capture,
          root_chunk_reader: fn _entry, _opts -> {:error, {:no_local_replica, locations}} end,
          remote_caller: fn
            :remote1@host, _fn, _args ->
              send(parent, {:remote, :remote1@host})
              {:error, :unreachable}

            :remote2@host, _fn, _args ->
              send(parent, {:remote, :remote2@host})
              {:ok, "second-node-hash"}
          end
        )

      assert {:ok, "second-node-hash"} = MetadataWriter.put("vol-1", :file_index, "k", "v", opts)
      assert_received {:remote, :remote1@host}
      assert_received {:remote, :remote2@host}
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

    # The NIFs return `{new_root, written_nodes}`; the default mocks
    # write no copy-on-write nodes (empty list) so only the root
    # segment replicates. The dedicated fan-out test below injects a
    # non-empty `written_nodes` list.
    capturing_put = fn store, root, tier, key, value ->
      :ets.insert(capture.tree_calls, {:put, store, root, tier, key, value})
      {:ok, {"new-tree-root", []}}
    end

    capturing_delete = fn store, root, tier, key ->
      :ets.insert(capture.tree_calls, {:delete, store, root, tier, key})
      {:ok, {"new-tree-root", []}}
    end

    capturing_purge = fn store, root, tier, before_nanos ->
      :ets.insert(capture.tree_calls, {:purge, store, root, tier, before_nanos})
      {:ok, {"new-tree-root", []}}
    end

    defaults = [
      cluster_state_loader: fn -> {:ok, sample_cluster_state()} end,
      bootstrap_lookup: fn _vol, _shard -> {:ok, sample_root_entry()} end,
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
