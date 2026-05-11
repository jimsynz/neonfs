defmodule NeonFS.Core.Volume.MetadataReaderTest do
  use ExUnit.Case, async: true

  alias NeonFS.Cluster.State, as: ClusterState
  alias NeonFS.Core.Volume.MetadataReader
  alias NeonFS.Core.Volume.MetadataValue
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

  describe "typed wrappers" do
    test "get_file_meta/3 decodes an ETF-encoded value" do
      sample = %{path: "/a/b", size: 42, owner_uid: 1000}
      opts = build_opts_returning(sample)

      assert {:ok, ^sample} = MetadataReader.get_file_meta("vol-1", "file-id", opts)
    end

    test "get_chunk_meta/3 decodes an ETF-encoded value" do
      sample = %{hash: <<1, 2, 3>>, size: 1024, codec: %{compression: :none}}
      opts = build_opts_returning(sample)

      assert {:ok, ^sample} = MetadataReader.get_chunk_meta("vol-1", <<1, 2, 3>>, opts)
    end

    test "get_stripe/3 decodes an ETF-encoded value" do
      sample = %{stripe_id: "s-1", file_id: "f-1", chunks: [<<1>>, <<2>>]}
      opts = build_opts_returning(sample)

      assert {:ok, ^sample} = MetadataReader.get_stripe("vol-1", "s-1", opts)
    end

    test "typed wrappers surface :not_found from the underlying generic get" do
      opts = build_opts(index_tree_get: fn _, _, _, _ -> {:ok, nil} end)

      assert {:error, :not_found} = MetadataReader.get_file_meta("vol-1", "missing", opts)
    end

    test "typed wrappers surface :malformed_value when the bytes don't decode" do
      opts =
        build_opts(index_tree_get: fn _, _, _, _ -> {:ok, <<0, 1, 2, 3>>} end)

      assert {:error, {:malformed_value, _}} =
               MetadataReader.get_file_meta("vol-1", "f-1", opts)
    end
  end

  describe "list_dir/3" do
    test "uses a path-prefix range and decodes each entry" do
      entries = [
        {"/dir/alpha", MetadataValue.encode(%{name: "alpha", size: 10})},
        {"/dir/beta", MetadataValue.encode(%{name: "beta", size: 20})}
      ]

      table = :ets.new(:nif_calls, [:public, :duplicate_bag])

      capture_range = fn _store, _root, _tier, start_key, end_key ->
        :ets.insert(table, {:bounds, start_key, end_key})
        {:ok, entries}
      end

      opts = build_opts(index_tree_range: capture_range)

      assert {:ok, decoded} = MetadataReader.list_dir("vol-1", "/dir", opts)

      assert decoded == [
               {"/dir/alpha", %{name: "alpha", size: 10}},
               {"/dir/beta", %{name: "beta", size: 20}}
             ]

      # Range bounds are `[/dir/, /dir0)` — the `0` is the byte after `/`.
      assert [{:bounds, "/dir/", "/dir0"}] = :ets.lookup(table, :bounds)
    end

    test "empty parent_path means full range" do
      table = :ets.new(:nif_calls, [:public, :duplicate_bag])

      capture_range = fn _store, _root, _tier, start_key, end_key ->
        :ets.insert(table, {:bounds, start_key, end_key})
        {:ok, []}
      end

      opts = build_opts(index_tree_range: capture_range)
      assert {:ok, []} = MetadataReader.list_dir("vol-1", "", opts)
      assert [{:bounds, <<>>, <<>>}] = :ets.lookup(table, :bounds)
    end

    test "stops at the first malformed entry" do
      entries = [
        {"/dir/ok", MetadataValue.encode(%{ok: true})},
        {"/dir/bad", <<0, 1, 2, 3>>}
      ]

      opts =
        build_opts(index_tree_range: fn _, _, _, _, _ -> {:ok, entries} end)

      assert {:error, {:malformed_value, _}} = MetadataReader.list_dir("vol-1", "/dir", opts)
    end
  end

  describe ":at_root override" do
    test "substitutes the bootstrap entry's root_chunk_hash with the supplied one" do
      snapshot_hash = <<7::256>>
      live_hash = <<99::256>>

      live_entry = %{
        volume_id: "vol-1",
        root_chunk_hash: live_hash,
        drive_locations: [%{node: node(), drive_id: "drv-local"}],
        durability_cache: %{type: :replicate, factor: 1, min_copies: 1},
        updated_at: DateTime.utc_now()
      }

      table = :ets.new(:reader_calls, [:public, :duplicate_bag])

      capture_reader = fn root_entry, _opts ->
        :ets.insert(table, {:read, root_entry.root_chunk_hash})
        {:ok, RootSegment.encode(sample_segment())}
      end

      opts =
        build_opts(
          bootstrap_lookup: fn _ -> {:ok, live_entry} end,
          root_chunk_reader: capture_reader,
          at_root: snapshot_hash
        )

      assert {:ok, "value-bytes"} =
               MetadataReader.get("vol-1", :file_index, "k", opts)

      assert [{:read, ^snapshot_hash}] = :ets.lookup(table, :read)
    end

    test "preserves drive_locations from the bootstrap entry" do
      remote_locations = [
        %{node: :remote_a@host, drive_id: "drv-a"},
        %{node: node(), drive_id: "drv-local"}
      ]

      live_entry = %{
        volume_id: "vol-1",
        root_chunk_hash: <<99::256>>,
        drive_locations: remote_locations,
        durability_cache: %{type: :replicate, factor: 2, min_copies: 1},
        updated_at: DateTime.utc_now()
      }

      table = :ets.new(:reader_entries, [:public, :duplicate_bag])

      capture_reader = fn root_entry, _opts ->
        :ets.insert(table, {:entry, root_entry})
        {:ok, RootSegment.encode(sample_segment())}
      end

      opts =
        build_opts(
          bootstrap_lookup: fn _ -> {:ok, live_entry} end,
          root_chunk_reader: capture_reader,
          at_root: <<42::256>>
        )

      assert {:ok, _} = MetadataReader.get("vol-1", :file_index, "k", opts)

      assert [{:entry, entry}] = :ets.lookup(table, :entry)
      assert entry.root_chunk_hash == <<42::256>>
      assert entry.drive_locations == remote_locations
      assert entry.durability_cache.factor == 2
    end

    test "cache key is segregated by the overridden root_chunk_hash" do
      snapshot_hash = <<7::256>>
      table = :ets.new(:cache_puts, [:public, :duplicate_bag])

      miss_cache = %{
        get: fn _v, _r, _k -> :miss end,
        put: fn v, r, k, val ->
          :ets.insert(table, {:put, v, r, k, val})
          :ok
        end
      }

      opts =
        build_opts(
          cache_module: stub_cache(miss_cache),
          index_tree_get: fn _, _, _, _ -> {:ok, "snap-value"} end,
          at_root: snapshot_hash
        )

      assert {:ok, "snap-value"} =
               MetadataReader.get("vol-1", :file_index, "k", opts)

      assert [{:put, "vol-1", ^snapshot_hash, {:file_index, :get, "k"}, "snap-value"}] =
               :ets.tab2list(table)
    end

    test "range/5 also honours :at_root" do
      snapshot_hash = <<7::256>>
      table = :ets.new(:range_reads, [:public, :duplicate_bag])

      capture_reader = fn root_entry, _opts ->
        :ets.insert(table, {:read, root_entry.root_chunk_hash})
        {:ok, RootSegment.encode(sample_segment())}
      end

      opts =
        build_opts(
          root_chunk_reader: capture_reader,
          at_root: snapshot_hash
        )

      assert {:ok, []} =
               MetadataReader.range("vol-1", :file_index, "", "", opts)

      assert [{:read, ^snapshot_hash}] = :ets.lookup(table, :read)
    end

    test "typed wrappers forward :at_root" do
      snapshot_hash = <<7::256>>
      table = :ets.new(:typed_reads, [:public, :duplicate_bag])

      capture_reader = fn root_entry, _opts ->
        :ets.insert(table, {:read, root_entry.root_chunk_hash})
        {:ok, RootSegment.encode(sample_segment())}
      end

      sample = %{path: "/a", size: 1, owner_uid: 0}

      opts =
        build_opts(
          root_chunk_reader: capture_reader,
          index_tree_get: fn _, _, _, _ -> {:ok, MetadataValue.encode(sample)} end,
          at_root: snapshot_hash
        )

      assert {:ok, ^sample} =
               MetadataReader.get_file_meta("vol-1", "file-1", opts)

      assert [{:read, ^snapshot_hash}] = :ets.lookup(table, :read)
    end

    test "list_dir forwards :at_root" do
      snapshot_hash = <<7::256>>
      table = :ets.new(:dir_reads, [:public, :duplicate_bag])

      capture_reader = fn root_entry, _opts ->
        :ets.insert(table, {:read, root_entry.root_chunk_hash})
        {:ok, RootSegment.encode(sample_segment())}
      end

      opts =
        build_opts(
          root_chunk_reader: capture_reader,
          at_root: snapshot_hash
        )

      assert {:ok, []} =
               MetadataReader.list_dir("vol-1", "/dir", opts)

      assert [{:read, ^snapshot_hash}] = :ets.lookup(table, :read)
    end

    test "remote dispatch threads :at_root through to the remote node" do
      snapshot_hash = <<7::256>>

      remote_entry = %{
        volume_id: "vol-1",
        root_chunk_hash: <<99::256>>,
        drive_locations: [%{node: :remote_a@host, drive_id: "drv-remote"}],
        durability_cache: %{type: :replicate, factor: 1, min_copies: 1},
        updated_at: DateTime.utc_now()
      }

      table = :ets.new(:remote_at_root, [:public, :duplicate_bag])

      remote_caller = fn _node, _fn_name, [_, _, _, dispatched_opts] = args ->
        :ets.insert(table, {:dispatched, args})
        assert Keyword.fetch!(dispatched_opts, :__remote_dispatched) == true
        {:ok, "remote-snap-value"}
      end

      opts =
        build_opts(
          bootstrap_lookup: fn _ -> {:ok, remote_entry} end,
          root_chunk_reader: fn _entry, _opts -> {:error, :io_error} end,
          remote_caller: remote_caller,
          at_root: snapshot_hash
        )

      assert {:ok, "remote-snap-value"} =
               MetadataReader.get("vol-1", :file_index, "k", opts)

      assert [{:dispatched, [_, _, _, dispatched_opts]}] = :ets.tab2list(table)
      assert Keyword.fetch!(dispatched_opts, :at_root) == snapshot_hash
    end
  end

  describe "cache integration" do
    test "cache hit short-circuits the bootstrap → segment → tree walk" do
      # Stub cache that pretends to have the value cached. The inner
      # `index_tree_get` should NOT be called.
      table = :ets.new(:hits, [:public, :duplicate_bag])

      hit_cache = %{
        get: fn _v, _r, _k ->
          :ets.insert(table, {:hit_returned, true})
          {:ok, "cached-value"}
        end,
        put: fn _v, _r, _k, _val -> :ok end
      }

      walked = :ets.new(:walked, [:public, :duplicate_bag])

      opts =
        build_opts(
          cache_module: stub_cache(hit_cache),
          index_tree_get: fn _, _, _, _ ->
            :ets.insert(walked, {:walked, true})
            {:ok, "walked-value"}
          end
        )

      assert {:ok, "cached-value"} = MetadataReader.get("vol-1", :file_index, "k", opts)

      assert :ets.tab2list(table) != []
      # The walk must have been short-circuited.
      assert :ets.tab2list(walked) == []
    end

    test "cache miss populates the cache after walking" do
      table = :ets.new(:puts, [:public, :duplicate_bag])

      miss_cache = %{
        get: fn _v, _r, _k -> :miss end,
        put: fn v, r, k, val ->
          :ets.insert(table, {:put, v, r, k, val})
          :ok
        end
      }

      opts =
        build_opts(
          cache_module: stub_cache(miss_cache),
          index_tree_get: fn _, _, _, _ -> {:ok, "fresh-value"} end
        )

      assert {:ok, "fresh-value"} = MetadataReader.get("vol-1", :file_index, "k", opts)

      assert [{:put, "vol-1", _root_hash, {:file_index, :get, "k"}, "fresh-value"}] =
               :ets.tab2list(table)
    end

    test "cache miss does NOT populate on :not_found" do
      table = :ets.new(:puts2, [:public, :duplicate_bag])

      miss_cache = %{
        get: fn _v, _r, _k -> :miss end,
        put: fn v, r, k, val ->
          :ets.insert(table, {:put, v, r, k, val})
          :ok
        end
      }

      opts =
        build_opts(
          cache_module: stub_cache(miss_cache),
          index_tree_get: fn _, _, _, _ -> {:ok, nil} end
        )

      assert {:error, :not_found} =
               MetadataReader.get("vol-1", :file_index, "missing", opts)

      assert :ets.tab2list(table) == []
    end

    test "range/5 also goes through the cache with a {:range, start, end} key" do
      entries = [{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}]
      table = :ets.new(:range_puts, [:public, :duplicate_bag])

      miss_cache = %{
        get: fn _v, _r, _k -> :miss end,
        put: fn v, r, k, val ->
          :ets.insert(table, {:put, v, r, k, val})
          :ok
        end
      }

      opts =
        build_opts(
          cache_module: stub_cache(miss_cache),
          index_tree_range: fn _, _, _, _, _ -> {:ok, entries} end
        )

      assert {:ok, ^entries} =
               MetadataReader.range("vol-1", :file_index, "a", "z", opts)

      assert [{:put, "vol-1", _root, {:file_index, :range, "a", "z"}, ^entries}] =
               :ets.tab2list(table)
    end
  end

  describe "cross-node fallback (#936)" do
    test "remote-dispatches when local read fails with root_chunk_unreachable" do
      remote_entry = %{
        volume_id: "vol-1",
        root_chunk_hash: <<99::256>>,
        drive_locations: [%{node: :remote_a@host, drive_id: "drv-remote"}],
        durability_cache: %{type: :replicate, factor: 1, min_copies: 1},
        updated_at: DateTime.utc_now()
      }

      table = :ets.new(:remote_calls, [:public, :duplicate_bag])

      remote_caller = fn node, fn_name, args ->
        :ets.insert(table, {:call, node, fn_name, args})
        {:ok, "remote-value"}
      end

      opts =
        build_opts(
          bootstrap_lookup: fn _ -> {:ok, remote_entry} end,
          root_chunk_reader: fn _entry, _opts -> {:error, :io_error} end,
          remote_caller: remote_caller
        )

      assert {:ok, "remote-value"} =
               MetadataReader.get("vol-1", :file_index, "k", opts)

      assert [{:call, :remote_a@host, :get, [_, _, _, dispatched_opts]}] =
               :ets.tab2list(table)

      assert Keyword.fetch!(dispatched_opts, :__remote_dispatched) == true
    end

    test "remote-dispatches when local read fails with no_local_replica" do
      remote_entry = %{
        volume_id: "vol-1",
        root_chunk_hash: <<99::256>>,
        drive_locations: [%{node: :remote_b@host, drive_id: "drv-r"}],
        durability_cache: %{type: :replicate, factor: 1, min_copies: 1},
        updated_at: DateTime.utc_now()
      }

      remote_caller = fn _node, :get, _args -> {:ok, "from-remote"} end

      opts =
        build_opts(
          bootstrap_lookup: fn _ -> {:ok, remote_entry} end,
          root_chunk_reader: fn _entry, _opts ->
            {:error, {:no_local_replica, [%{node: :remote_b@host, drive_id: "drv-r"}]}}
          end,
          remote_caller: remote_caller
        )

      assert {:ok, "from-remote"} =
               MetadataReader.get("vol-1", :file_index, "k", opts)
    end

    test "does not dispatch when local read returns :not_found" do
      table = :ets.new(:not_dispatched, [:public, :duplicate_bag])

      remote_caller = fn node, fn_name, args ->
        :ets.insert(table, {:call, node, fn_name, args})
        {:ok, "should-not-be-called"}
      end

      opts =
        build_opts(
          index_tree_get: fn _, _, _, _ -> {:ok, nil} end,
          remote_caller: remote_caller
        )

      assert {:error, :not_found} =
               MetadataReader.get("vol-1", :file_index, "missing", opts)

      assert :ets.tab2list(table) == []
    end

    test "does not dispatch when local read returns cluster_mismatch" do
      table = :ets.new(:not_dispatched_mismatch, [:public, :duplicate_bag])

      foreign_segment = sample_segment(cluster_id: "clust-other")

      opts =
        build_opts(
          root_chunk_reader: const_chunk_reader(foreign_segment),
          remote_caller: fn node, fn_name, args ->
            :ets.insert(table, {:call, node, fn_name, args})
            {:ok, "nope"}
          end
        )

      assert {:error, {:cluster_mismatch, _}} =
               MetadataReader.get("vol-1", :file_index, "k", opts)

      assert :ets.tab2list(table) == []
    end

    test "does not dispatch when local read returns malformed_root_segment" do
      opts =
        build_opts(
          root_chunk_reader: fn _entry, _opts -> {:ok, <<0, 1, 2, 3>>} end,
          remote_caller: fn _, _, _ -> raise "remote_caller must not be called" end
        )

      assert {:error, {:malformed_root_segment, _}} =
               MetadataReader.get("vol-1", :file_index, "k", opts)
    end

    test "does not dispatch when local read returns bootstrap_query_failed" do
      opts =
        build_opts(
          bootstrap_lookup: fn _ -> {:error, :ra_timeout} end,
          remote_caller: fn _, _, _ -> raise "remote_caller must not be called" end
        )

      assert {:error, {:bootstrap_query_failed, :ra_timeout}} =
               MetadataReader.get("vol-1", :file_index, "k", opts)
    end

    test "does not dispatch when caller flagged __remote_dispatched (no recursion)" do
      opts =
        build_opts(
          root_chunk_reader: fn _, _ -> {:error, :io_error} end,
          remote_caller: fn _, _, _ -> raise "remote_caller must not be called" end,
          __remote_dispatched: true
        )

      assert {:error, {:root_chunk_unreachable, :io_error}} =
               MetadataReader.get("vol-1", :file_index, "k", opts)
    end

    test "tries each remote node until one succeeds" do
      remote_entry = %{
        volume_id: "vol-1",
        root_chunk_hash: <<99::256>>,
        drive_locations: [
          %{node: :remote_a@host, drive_id: "drv-a"},
          %{node: :remote_b@host, drive_id: "drv-b"},
          %{node: :remote_c@host, drive_id: "drv-c"}
        ],
        durability_cache: %{type: :replicate, factor: 3, min_copies: 2},
        updated_at: DateTime.utc_now()
      }

      attempts = :ets.new(:attempts, [:public, :duplicate_bag])

      remote_caller = fn
        :remote_a@host, :get, _args ->
          :ets.insert(attempts, {:tried, :remote_a@host})
          {:error, {:rpc_failed, :nodedown}}

        :remote_b@host, :get, _args ->
          :ets.insert(attempts, {:tried, :remote_b@host})
          {:ok, "from-b"}

        :remote_c@host, :get, _args ->
          :ets.insert(attempts, {:tried, :remote_c@host})
          {:ok, "should-stop-at-b"}
      end

      opts =
        build_opts(
          bootstrap_lookup: fn _ -> {:ok, remote_entry} end,
          root_chunk_reader: fn _, _ -> {:error, :io_error} end,
          remote_caller: remote_caller
        )

      assert {:ok, "from-b"} = MetadataReader.get("vol-1", :file_index, "k", opts)

      tried = :ets.tab2list(attempts) |> Enum.map(fn {:tried, n} -> n end)
      assert :remote_a@host in tried
      assert :remote_b@host in tried
      refute :remote_c@host in tried
    end

    test "remote :not_found is authoritative — does not retry further nodes" do
      remote_entry = %{
        volume_id: "vol-1",
        root_chunk_hash: <<99::256>>,
        drive_locations: [
          %{node: :remote_a@host, drive_id: "drv-a"},
          %{node: :remote_b@host, drive_id: "drv-b"}
        ],
        durability_cache: %{type: :replicate, factor: 2, min_copies: 1},
        updated_at: DateTime.utc_now()
      }

      attempts = :ets.new(:auth_attempts, [:public, :duplicate_bag])

      remote_caller = fn node, :get, _args ->
        :ets.insert(attempts, {:tried, node})
        {:error, :not_found}
      end

      opts =
        build_opts(
          bootstrap_lookup: fn _ -> {:ok, remote_entry} end,
          root_chunk_reader: fn _, _ -> {:error, :io_error} end,
          remote_caller: remote_caller
        )

      assert {:error, :not_found} =
               MetadataReader.get("vol-1", :file_index, "k", opts)

      tried = :ets.tab2list(attempts) |> Enum.map(fn {:tried, n} -> n end)
      assert tried == [:remote_a@host]
    end

    test "collapses to :all_replicas_failed when every remote also fails" do
      remote_entry = %{
        volume_id: "vol-1",
        root_chunk_hash: <<99::256>>,
        drive_locations: [
          %{node: :remote_a@host, drive_id: "drv-a"},
          %{node: :remote_b@host, drive_id: "drv-b"}
        ],
        durability_cache: %{type: :replicate, factor: 2, min_copies: 1},
        updated_at: DateTime.utc_now()
      }

      remote_caller = fn _node, :get, _args -> {:error, {:rpc_failed, :nodedown}} end

      opts =
        build_opts(
          bootstrap_lookup: fn _ -> {:ok, remote_entry} end,
          root_chunk_reader: fn _, _ ->
            {:error, {:no_local_replica, [%{node: :remote_a@host, drive_id: "drv-a"}]}}
          end,
          remote_caller: remote_caller
        )

      assert {:error, :all_replicas_failed} =
               MetadataReader.get("vol-1", :file_index, "k", opts)
    end

    test "range/5 has the same fallback behaviour" do
      remote_entry = %{
        volume_id: "vol-1",
        root_chunk_hash: <<99::256>>,
        drive_locations: [%{node: :remote_a@host, drive_id: "drv-a"}],
        durability_cache: %{type: :replicate, factor: 1, min_copies: 1},
        updated_at: DateTime.utc_now()
      }

      remote_caller = fn :remote_a@host, :range, _args ->
        {:ok, [{<<"k1">>, <<"v1">>}]}
      end

      opts =
        build_opts(
          bootstrap_lookup: fn _ -> {:ok, remote_entry} end,
          root_chunk_reader: fn _, _ -> {:error, :io_error} end,
          remote_caller: remote_caller
        )

      assert {:ok, [{<<"k1">>, <<"v1">>}]} =
               MetadataReader.range("vol-1", :file_index, "", "", opts)
    end

    test "no remote candidates surfaces the original local error" do
      single_node_entry = %{
        volume_id: "vol-1",
        root_chunk_hash: <<99::256>>,
        drive_locations: [%{node: node(), drive_id: "drv-local"}],
        durability_cache: %{type: :replicate, factor: 1, min_copies: 1},
        updated_at: DateTime.utc_now()
      }

      opts =
        build_opts(
          bootstrap_lookup: fn _ -> {:ok, single_node_entry} end,
          root_chunk_reader: fn _, _ -> {:error, :io_error} end,
          remote_caller: fn _, _, _ -> raise "no remotes — must not be called" end
        )

      assert {:error, {:root_chunk_unreachable, :io_error}} =
               MetadataReader.get("vol-1", :file_index, "k", opts)
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

  defp build_opts_returning(value) do
    bytes = MetadataValue.encode(value)
    build_opts(index_tree_get: fn _, _, _, _ -> {:ok, bytes} end)
  end

  defp build_opts(extra \\ []) do
    defaults = [
      cluster_state_loader: fn -> {:ok, sample_cluster_state()} end,
      bootstrap_lookup: fn _vol -> {:ok, sample_root_entry()} end,
      root_chunk_reader: const_chunk_reader(sample_segment()),
      store_handle: :stub_store_handle,
      index_tree_get: fn _store, _root, _tier, _key -> {:ok, "value-bytes"} end,
      index_tree_range: fn _store, _root, _tier, _start, _end -> {:ok, []} end,
      cache_module: __MODULE__.NoopCache
    ]

    Keyword.merge(defaults, extra)
  end

  defmodule NoopCache do
    @moduledoc false
    def get(_v, _r, _k), do: :miss
    def put(_v, _r, _k, _val), do: :ok
  end

  # Build a stub module that delegates to per-test fns held in
  # :persistent_term so we can pass closures cleanly without runtime
  # macros. Re-uses one module per test by hashing the get fn.
  defp stub_cache(%{get: get_fn, put: put_fn}) do
    name =
      String.to_atom(
        "Elixir.NeonFS.Core.Volume.MetadataReaderTest.StubCache#{:erlang.unique_integer([:positive])}"
      )

    pt_key = {name, :fns}
    :persistent_term.put(pt_key, {get_fn, put_fn})

    Module.create(
      name,
      quote do
        @pt_key unquote(Macro.escape(pt_key))
        def get(v, r, k) do
          {gf, _} = :persistent_term.get(@pt_key)
          gf.(v, r, k)
        end

        def put(v, r, k, val) do
          {_, pf} = :persistent_term.get(@pt_key)
          pf.(v, r, k, val)
        end
      end,
      Macro.Env.location(__ENV__)
    )

    name
  end
end
