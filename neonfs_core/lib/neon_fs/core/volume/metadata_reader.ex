defmodule NeonFS.Core.Volume.MetadataReader do
  @moduledoc """
  Walks bootstrap layer (#779) → root segment (#780) → index tree
  (#781) for a per-volume metadata read.

  This module is the Elixir-side companion of the index tree read
  NIFs (#814). It exposes a generic `get/4` and `range/5` keyed by
  index kind (`:file_index` / `:chunk_index` / `:stripe_index`);
  per-type wrappers (`get_file_meta/2` etc.) and value decoding will
  layer on top in a follow-up sub-issue.

  The flow:

  1. Resolve `volume_id` to `{root_chunk_hash, drive_locations}` via
     the bootstrap layer.
  2. Read the root segment chunk from a local replica (any drive
     listed in `drive_locations` whose `node` matches `Node.self/0`).
  3. Decode via `Volume.RootSegment.decode/1`.
  4. Validate the segment's `cluster_id` matches the local cluster.
  5. Pull the right `index_root` hash out of the segment.
  6. Call the index tree read NIF for the actual key lookup or
     range walk.

  Cross-node read-repair (HLC-aware fallback when replicas diverge)
  is explicitly out of scope per #784 — the routing layer
  (`NeonFS.Client.Router`) is expected to land the call on a node
  with a local replica before this module runs.

  Every external dependency is injectable via opts so unit tests
  drive the function with deterministic stubs without spinning up a
  full cluster (same pattern as `Volume.Provisioner` from #810).
  """

  alias NeonFS.Cluster.State, as: ClusterState
  alias NeonFS.Core.Blob.Native
  alias NeonFS.Core.BlobStore
  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.Core.RaSupervisor
  alias NeonFS.Core.Volume.MetadataCache
  alias NeonFS.Core.Volume.MetadataValue
  alias NeonFS.Core.Volume.RootSegment

  @type index_kind :: :file_index | :chunk_index | :stripe_index
  @type read_error ::
          {:error, :not_found}
          | {:error, {:cluster_state_unavailable, term()}}
          | {:error, {:bootstrap_query_failed, term()}}
          | {:error, {:root_chunk_unreachable, term()}}
          | {:error, {:malformed_root_segment, term()}}
          | {:error, {:cluster_mismatch, expected: String.t(), actual: String.t()}}
          | {:error, {:no_local_replica, [%{node: node(), drive_id: String.t()}]}}
          | {:error, {:index_tree_read_failed, term()}}
          | {:error, {:store_handle_unavailable, term()}}
          | {:error, :all_replicas_failed}

  @doc """
  Look up an opaque value by `key` in the volume's `index_kind`
  index tree.

  Returns `{:ok, binary}` if the key is present and not tombstoned,
  `{:error, :not_found}` if absent or tombstoned, or another error
  variant for failures along the read path.

  When the local node has no replica drive holding the volume's
  metadata chunks (or the local read fails for a structural reason),
  the call is re-dispatched via Erlang distribution to a remote node
  that does — see "Cross-node fallback" below for details.

  ## Options

    * `:at_root` — read the tree as if the volume's bootstrap pointer
      had this `root_chunk_hash` instead of its live value. The
      bootstrap entry's `drive_locations` and `durability_cache` are
      reused — snapshots share storage with the live volume, so the
      same drives hold the historic root chunk. Used by snapshot
      consumers (multi-root GC, restore, export). The cache key
      already includes `root_chunk_hash`, so cache segregation falls
      out for free.
  """
  @spec get(volume_id :: binary(), index_kind(), key :: binary(), keyword()) ::
          {:ok, binary()} | read_error()
  def get(volume_id, index_kind, key, opts \\ [])
      when is_binary(volume_id) and is_atom(index_kind) and is_binary(key) do
    with_remote_fallback(
      volume_id,
      opts,
      fn -> do_local_get(volume_id, index_kind, key, opts) end,
      fn node, remote_opts ->
        remote_call(node, opts, :get, [
          volume_id,
          index_kind,
          key,
          remote_opts ++ forwardable_opts(opts)
        ])
      end
    )
  end

  @doc """
  Range scan `[start_key, end_key)` on the volume's `index_kind`
  index tree. `<<>>` on either side is open-ended.

  Returns `{:ok, [{key, value}]}` in ascending key order, with
  tombstones filtered out, or an error variant. Cross-node fallback
  semantics match `get/4`.
  """
  @spec range(
          volume_id :: binary(),
          index_kind(),
          start_key :: binary(),
          end_key :: binary(),
          keyword()
        ) :: {:ok, [{binary(), binary()}]} | read_error()
  def range(volume_id, index_kind, start_key, end_key, opts \\ [])
      when is_binary(volume_id) and is_atom(index_kind) and
             is_binary(start_key) and is_binary(end_key) do
    with_remote_fallback(
      volume_id,
      opts,
      fn -> do_local_range(volume_id, index_kind, start_key, end_key, opts) end,
      fn node, remote_opts ->
        remote_call(node, opts, :range, [
          volume_id,
          index_kind,
          start_key,
          end_key,
          remote_opts ++ forwardable_opts(opts)
        ])
      end
    )
  end

  defp do_local_get(volume_id, index_kind, key, opts) do
    with {:ok, segment, root_entry} <- resolve_segment(volume_id, opts) do
      tree_root = Map.fetch!(segment.index_roots, index_kind)
      cache_key = {index_kind, :get, key}

      cached_call(volume_id, root_entry.root_chunk_hash, cache_key, opts, fn ->
        do_index_tree_get(root_entry, tree_root, key, opts)
      end)
    end
  end

  defp do_local_range(volume_id, index_kind, start_key, end_key, opts) do
    with {:ok, segment, root_entry} <- resolve_segment(volume_id, opts) do
      tree_root = Map.fetch!(segment.index_roots, index_kind)
      cache_key = {index_kind, :range, start_key, end_key}

      cached_call(volume_id, root_entry.root_chunk_hash, cache_key, opts, fn ->
        do_index_tree_range(root_entry, tree_root, start_key, end_key, opts)
      end)
    end
  end

  @doc """
  Look up a `FileMeta` by `file_id` in the volume's `:file_index`.

  Returns the decoded struct, `{:error, :not_found}`, or any other
  error from `get/4` plus `{:malformed_value, _}` if the stored
  bytes don't decode as ETF.
  """
  @spec get_file_meta(volume_id :: binary(), file_id :: binary(), keyword()) ::
          {:ok, term()} | read_error() | MetadataValue.decode_error()
  def get_file_meta(volume_id, file_id, opts \\ []) do
    typed_get(volume_id, :file_index, file_id, opts)
  end

  @doc """
  Look up a `ChunkMeta` by content hash in the volume's
  `:chunk_index`.
  """
  @spec get_chunk_meta(volume_id :: binary(), chunk_hash :: binary(), keyword()) ::
          {:ok, term()} | read_error() | MetadataValue.decode_error()
  def get_chunk_meta(volume_id, chunk_hash, opts \\ []) do
    typed_get(volume_id, :chunk_index, chunk_hash, opts)
  end

  @doc """
  Look up a `Stripe` (per-file erasure-coded stripe metadata) by
  stripe id in the volume's `:stripe_index`.
  """
  @spec get_stripe(volume_id :: binary(), stripe_id :: binary(), keyword()) ::
          {:ok, term()} | read_error() | MetadataValue.decode_error()
  def get_stripe(volume_id, stripe_id, opts \\ []) do
    typed_get(volume_id, :stripe_index, stripe_id, opts)
  end

  @doc """
  Walk the directory entries under `parent_path` from the volume's
  `:file_index`. Encodes the path as a key prefix and uses
  `range/5` to enumerate entries in sort order.

  Decoded values come back as a list of `{key, value}` pairs where
  each value has been ETF-decoded.
  """
  @spec list_dir(volume_id :: binary(), parent_path :: binary(), keyword()) ::
          {:ok, [{binary(), term()}]} | read_error() | MetadataValue.decode_error()
  def list_dir(volume_id, parent_path, opts \\ []) when is_binary(parent_path) do
    {start_key, end_key} = dir_range_keys(parent_path)

    with {:ok, raw_entries} <- range(volume_id, :file_index, start_key, end_key, opts) do
      decode_entries(raw_entries)
    end
  end

  @doc """
  List every node chunk hash reachable from the volume's three
  index trees (`file_index`, `chunk_index`, `stripe_index`) — both
  internal-page chunks and leaf-page chunks. Used by the
  anti-entropy runner (#955) so the per-volume reconciliation pass
  enumerates index-tree pages as well as data chunks, catching
  tree-page divergence between replicas that the read-path
  cross-node fallback (#947) would otherwise leave undetected.

  Returns `{:ok, [hash, ...]}` with hashes from all three trees
  unioned (deduped — internal pages shared across trees are
  reported once). Empty trees contribute zero hashes.

  ## Options

  Same options as `get/4` / `range/5` for testability (each opt is
  passed through to `resolve_segment_for_write/2` and the NIF
  wrapper).
  """
  @spec list_referenced_chunks(volume_id :: binary(), keyword()) ::
          {:ok, [binary()]} | read_error()
  def list_referenced_chunks(volume_id, opts \\ []) when is_binary(volume_id) do
    with {:ok, segment, root_entry} <- resolve_segment(volume_id, opts),
         {:ok, store_handle} <- resolve_store_handle(root_entry, opts) do
      collect_tree_chunks(store_handle, segment, opts)
    end
  end

  defp collect_tree_chunks(store_handle, segment, opts) do
    nif = Keyword.get(opts, :index_tree_list, &default_index_tree_list/3)

    [:file_index, :chunk_index, :stripe_index]
    |> Enum.reduce_while({:ok, MapSet.new()}, fn kind, {:ok, acc} ->
      tree_root = Map.fetch!(segment.index_roots, kind)

      case nif.(store_handle, tree_root_or_empty(tree_root), "hot") do
        {:ok, hashes} when is_list(hashes) ->
          {:cont, {:ok, Enum.into(hashes, acc)}}

        {:error, reason} ->
          {:halt, {:error, {:index_tree_read_failed, reason}}}
      end
    end)
    |> case do
      {:ok, set} -> {:ok, MapSet.to_list(set)}
      {:error, _} = err -> err
    end
  end

  defp default_index_tree_list(store, root_hash, tier) do
    Native.index_tree_list_referenced_chunks(store, root_hash, tier)
  end

  ## Internals

  # Wraps a cache miss + populate around the actual read function.
  # Callers stay clean — `get/4` and `range/5` just supply the key
  # tuple and a closure over the work. `:cache_module` opt lets
  # tests inject a stub.
  defp cached_call(volume_id, root_chunk_hash, cache_key, opts, fun) do
    cache_module = Keyword.get(opts, :cache_module, MetadataCache)

    case cache_module.get(volume_id, root_chunk_hash, cache_key) do
      {:ok, value} ->
        {:ok, value}

      :miss ->
        case fun.() do
          {:ok, value} ->
            cache_module.put(volume_id, root_chunk_hash, cache_key, value)
            {:ok, value}

          # `:not_found` and other errors are not cached — they may
          # legitimately become hits on the next read after a write.
          other ->
            other
        end
    end
  end

  defp typed_get(volume_id, kind, key, opts) do
    with {:ok, bytes} <- get(volume_id, kind, key, opts) do
      MetadataValue.decode(bytes)
    end
  end

  defp decode_entries(raw_entries) do
    Enum.reduce_while(raw_entries, {:ok, []}, fn {key, bytes}, {:ok, acc} ->
      case MetadataValue.decode(bytes) do
        {:ok, value} -> {:cont, {:ok, [{key, value} | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, _} = err -> err
    end
  end

  # Directory key prefix: parent_path with a trailing separator.
  # Range is `[parent_path/, parent_path0)` — the `0` byte is the
  # smallest byte greater than `/`, capturing every key prefixed by
  # `parent_path/`. Empty parent_path means "root", which uses an
  # empty prefix → full range.
  defp dir_range_keys(""), do: {<<>>, <<>>}

  defp dir_range_keys(parent_path) do
    prefix = ensure_trailing_slash(parent_path)
    {prefix, byte_after_prefix(prefix)}
  end

  defp ensure_trailing_slash(path) do
    if String.ends_with?(path, "/"), do: path, else: path <> "/"
  end

  defp byte_after_prefix(prefix) do
    # `/` is 0x2F. The smallest byte > `/` is `0` (0x30) — append it
    # to make a key strictly greater than every key starting with
    # `prefix`.
    base = String.trim_trailing(prefix, "/")
    base <> "0"
  end

  ## Resolve helpers (shared by get/range)

  @doc """
  Public `resolve_segment` helper for the write path
  (`Volume.MetadataWriter`, #785). Returns
  `{:ok, segment, root_entry}` so the writer can both inspect the
  current segment and use the root entry's `drive_locations` to
  pick replicas for the new chunk.

  Same contract as the internal helper used by `get/4` and
  `range/5` — see those for the failure modes.
  """
  @spec resolve_segment_for_write(volume_id :: binary(), keyword()) ::
          {:ok, RootSegment.t(), MetadataStateMachine.volume_root_entry()} | read_error()
  def resolve_segment_for_write(volume_id, opts) when is_binary(volume_id) do
    resolve_segment(volume_id, opts)
  end

  defp resolve_segment(volume_id, opts) do
    cluster_state_loader = Keyword.get(opts, :cluster_state_loader, &default_cluster_loader/0)
    bootstrap_lookup = Keyword.get(opts, :bootstrap_lookup, &default_bootstrap_lookup/1)
    root_chunk_reader = Keyword.get(opts, :root_chunk_reader, &default_root_chunk_reader/2)
    at_root = Keyword.get(opts, :at_root)

    with {:ok, cluster_state} <- load_cluster_state(cluster_state_loader),
         {:ok, bootstrap_entry} <- bootstrap_query(bootstrap_lookup, volume_id),
         root_entry = apply_at_root_override(bootstrap_entry, at_root),
         {:ok, chunk_bytes} <- read_root_chunk(root_chunk_reader, root_entry),
         {:ok, segment} <- decode_segment(chunk_bytes),
         :ok <- check_cluster(segment, cluster_state) do
      {:ok, segment, root_entry}
    end
  end

  defp apply_at_root_override(entry, nil), do: entry

  defp apply_at_root_override(entry, hash) when is_binary(hash),
    do: %{entry | root_chunk_hash: hash}

  # Opts that should travel with a cross-node dispatch so the remote
  # node walks the same logical read. `__remote_dispatched` is added
  # by `try_remote_nodes/3`; everything else here is a caller-visible
  # read option that affects which tree gets walked.
  defp forwardable_opts(opts), do: Keyword.take(opts, [:at_root])

  defp default_cluster_loader, do: ClusterState.load()

  # The volume root entry is the bootstrap pointer for every per-volume
  # metadata read. It advances on every metadata write via Ra's
  # `:cas_update_volume_root` command, which returns success once a
  # quorum has the entry in their log + the leader has applied it
  # locally. Followers apply asynchronously after the next heartbeat,
  # so a `local_query` here on a follower can return the pre-write
  # pointer for up to a heartbeat after the writer's call returned —
  # i.e. a read on any node immediately after a successful write
  # against another node may walk the *old* segment and miss the
  # newly-written file (#935 / #936).
  #
  # `consistent_query` round-trips through the leader, which has
  # applied by the time it answers. One leader round-trip per
  # bootstrap-pointer read in exchange for read-after-write
  # linearisability.
  defp default_bootstrap_lookup(volume_id) do
    case RaSupervisor.query(&MetadataStateMachine.get_volume_root(&1, volume_id)) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, entry} when is_map(entry) -> {:ok, entry}
      {:error, _} = err -> err
    end
  end

  defp default_root_chunk_reader(root_entry, opts) do
    drive_lister = Keyword.get(opts, :drive_lister, &default_drive_lister/0)
    chunk_reader = Keyword.get(opts, :chunk_reader, &default_chunk_reader/3)

    with {:ok, all_drives} <- drive_lister.(),
         {:ok, drive} <- pick_local_replica(root_entry, all_drives) do
      chunk_reader.(root_entry.root_chunk_hash, drive.drive_id, "hot")
    end
  end

  defp default_drive_lister do
    case RaSupervisor.local_query(&MetadataStateMachine.get_drives/1) do
      {:ok, drives_map} when is_map(drives_map) -> {:ok, Map.values(drives_map)}
      other -> other
    end
  end

  defp default_chunk_reader(hash, drive_id, tier) do
    BlobStore.read_chunk(hash, drive_id, tier: tier)
  end

  defp load_cluster_state(loader) do
    case loader.() do
      {:ok, %ClusterState{} = state} -> {:ok, state}
      {:error, reason} -> {:error, {:cluster_state_unavailable, reason}}
      other -> {:error, {:cluster_state_unavailable, other}}
    end
  end

  defp bootstrap_query(lookup, volume_id) do
    case lookup.(volume_id) do
      {:ok, entry} -> {:ok, entry}
      {:error, :not_found} = err -> err
      {:error, reason} -> {:error, {:bootstrap_query_failed, reason}}
      other -> {:error, {:bootstrap_query_failed, other}}
    end
  end

  defp read_root_chunk(reader, root_entry) when is_function(reader, 2) do
    case reader.(root_entry, []) do
      {:ok, bytes} when is_binary(bytes) -> {:ok, bytes}
      {:error, reason} -> {:error, {:root_chunk_unreachable, reason}}
      other -> {:error, {:root_chunk_unreachable, other}}
    end
  end

  defp pick_local_replica(root_entry, all_drives) do
    # `drive_id` alone is not unique across nodes — two nodes may
    # both expose a `default` drive. Key by `{node, drive_id}` to
    # match the bootstrap drives table's v14 schema.
    by_key = Map.new(all_drives, &{{&1.node, &1.drive_id}, &1})
    locals = Enum.filter(root_entry.drive_locations, &(&1.node == node()))

    found =
      Enum.find_value(locals, fn loc ->
        case Map.fetch(by_key, {loc.node, loc.drive_id}) do
          {:ok, drive} -> drive
          :error -> nil
        end
      end)

    case found do
      nil -> {:error, {:no_local_replica, root_entry.drive_locations}}
      drive -> {:ok, drive}
    end
  end

  defp decode_segment(bytes) do
    case RootSegment.decode(bytes) do
      {:ok, segment} -> {:ok, segment}
      {:error, reason} -> {:error, {:malformed_root_segment, reason}}
    end
  end

  defp check_cluster(segment, %ClusterState{cluster_id: expected}) do
    case RootSegment.validate_cluster(segment, expected) do
      :ok -> :ok
      {:error, mismatch} -> {:error, mismatch}
    end
  end

  defp do_index_tree_get(root_entry, tree_root, key, opts) do
    with {:ok, store_handle} <- resolve_store_handle(root_entry, opts) do
      nif_get = Keyword.get(opts, :index_tree_get, &default_index_tree_get/4)

      case nif_get.(store_handle, tree_root_or_empty(tree_root), "hot", key) do
        {:ok, nil} -> {:error, :not_found}
        {:ok, value} when is_binary(value) -> {:ok, value}
        {:error, reason} -> {:error, {:index_tree_read_failed, reason}}
      end
    end
  end

  defp do_index_tree_range(root_entry, tree_root, start_key, end_key, opts) do
    with {:ok, store_handle} <- resolve_store_handle(root_entry, opts) do
      nif_range = Keyword.get(opts, :index_tree_range, &default_index_tree_range/5)
      root_bytes = tree_root_or_empty(tree_root)

      case nif_range.(store_handle, root_bytes, "hot", start_key, end_key) do
        {:ok, entries} when is_list(entries) -> {:ok, entries}
        {:error, reason} -> {:error, {:index_tree_read_failed, reason}}
      end
    end
  end

  # Returns `{:ok, handle}` on success or
  # `{:error, {:no_local_replica, drive_locations}}` when the local
  # node has no drive in the volume's replica set (the read path
  # caller — `with_remote_fallback/4` — re-dispatches to a remote
  # node in that case).
  defp resolve_store_handle(root_entry, opts) do
    case Keyword.fetch(opts, :store_handle) do
      {:ok, handle} ->
        {:ok, handle}

      :error ->
        drive_lister = Keyword.get(opts, :drive_lister, &default_drive_lister/0)

        with {:ok, all_drives} <- drive_lister.(),
             {:ok, drive} <- pick_local_replica(root_entry, all_drives),
             {:ok, handle} <- BlobStore.get_store_handle(drive.drive_id) do
          {:ok, handle}
        else
          {:error, {:no_local_replica, _} = err} ->
            {:error, err}

          {:error, reason} ->
            {:error, {:store_handle_unavailable, reason}}

          other ->
            {:error, {:store_handle_unavailable, other}}
        end
    end
  end

  defp tree_root_or_empty(nil), do: <<>>
  defp tree_root_or_empty(hash) when is_binary(hash), do: hash

  defp default_index_tree_get(store, root_hash, tier, key) do
    Native.index_tree_get(store, root_hash, tier, key)
  end

  defp default_index_tree_range(store, root_hash, tier, start_key, end_key) do
    Native.index_tree_range(store, root_hash, tier, start_key, end_key)
  end

  ## Cross-node fallback
  #
  # Per-volume metadata writes only synchronously replicate to
  # `min_copies` of the volume's replica drives, and the index tree
  # itself is written by the writer to a single local drive (the
  # remaining replicas catch up via background replication / anti-
  # entropy). A reader landing on a node that doesn't yet have the
  # chunks therefore needs to dispatch the read to a node that does
  # — otherwise read-after-write surfaces as a phantom `:not_found`
  # against an authoritative bootstrap pointer (see #936).
  #
  # The fallback runs once: when the local attempt fails for any
  # reason other than an authoritative miss (`:not_found`,
  # `:cluster_*`, `:bootstrap_*`, `:malformed_*`), the call is
  # re-dispatched to a remote node from `root_entry.drive_locations`
  # with `__remote_dispatched: true`, which suppresses further
  # fallback on the remote side. The remote node sees the call as
  # a normal local read and either returns a result or surfaces an
  # error that the entry node treats as a per-node failure.

  defp with_remote_fallback(volume_id, opts, local_fun, remote_fun) do
    case local_fun.() do
      {:ok, _} = ok ->
        ok

      {:error, reason} = err ->
        if Keyword.get(opts, :__remote_dispatched, false) or not retryable_error?(reason) do
          err
        else
          remote_dispatch(volume_id, opts, err, remote_fun)
        end
    end
  end

  # `:not_found` is authoritative — the volume isn't provisioned, or
  # the index tree returned `nil` for the key. Bootstrap / cluster /
  # malformed-segment errors describe the volume's metadata state in
  # a way that won't differ on another node. Everything else
  # (chunk-fetch, store-handle, NIF I/O) is local and worth a remote
  # retry.
  defp retryable_error?(:not_found), do: false
  defp retryable_error?({:cluster_state_unavailable, _}), do: false
  defp retryable_error?({:cluster_mismatch, _}), do: false
  defp retryable_error?({:malformed_root_segment, _}), do: false
  defp retryable_error?({:malformed_value, _}), do: false
  defp retryable_error?({:bootstrap_query_failed, _}), do: false
  defp retryable_error?(_), do: true

  defp remote_dispatch(volume_id, opts, fallback_err, remote_fun) do
    bootstrap_lookup = Keyword.get(opts, :bootstrap_lookup, &default_bootstrap_lookup/1)

    case bootstrap_lookup.(volume_id) do
      {:ok, root_entry} ->
        candidates = candidate_remote_nodes(root_entry)
        try_remote_nodes(candidates, fallback_err, remote_fun)

      _ ->
        fallback_err
    end
  end

  defp candidate_remote_nodes(root_entry) do
    self_node = node()

    root_entry.drive_locations
    |> Enum.map(& &1.node)
    |> Enum.uniq()
    |> Enum.reject(&(&1 == self_node))
  end

  # Iterate remote nodes. A clean `{:ok, _}` or `{:error, :not_found}`
  # is authoritative — the remote node has the chunks and answered
  # for real. Anything else means that particular node also can't
  # serve the read, so try the next.
  defp try_remote_nodes([], fallback_err, _fun),
    do: maybe_collapse_to_all_replicas_failed(fallback_err)

  defp try_remote_nodes([node | rest], fallback_err, fun) do
    case fun.(node, __remote_dispatched: true) do
      {:ok, _} = ok -> ok
      {:error, :not_found} = err -> err
      {:error, _} -> try_remote_nodes(rest, fallback_err, fun)
    end
  end

  # If the local error was specifically about no local replica, and
  # every remote candidate also failed, surface the more informative
  # `:all_replicas_failed` rather than the original "this node has
  # no replica" — the actual problem is now systemic.
  defp maybe_collapse_to_all_replicas_failed({:error, {:no_local_replica, _}}),
    do: {:error, :all_replicas_failed}

  defp maybe_collapse_to_all_replicas_failed(
         {:error, {:root_chunk_unreachable, {:no_local_replica, _}}}
       ),
       do: {:error, :all_replicas_failed}

  defp maybe_collapse_to_all_replicas_failed(other), do: other

  # Production callers reach the remote node via Erlang distribution;
  # tests inject `:remote_caller` to drive the dispatch deterministically.
  # Stub callers that can't run a real `:rpc.call` short-circuit here.
  defp remote_call(target_node, opts, fn_name, args) do
    caller = Keyword.get(opts, :remote_caller, &default_remote_caller/3)
    caller.(target_node, fn_name, args)
  end

  defp default_remote_caller(node, fn_name, args) do
    case :rpc.call(node, __MODULE__, fn_name, args, 10_000) do
      {:badrpc, reason} -> {:error, {:rpc_failed, reason}}
      other -> other
    end
  end
end
