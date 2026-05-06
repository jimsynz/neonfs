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
