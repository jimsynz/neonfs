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

  @doc """
  Look up an opaque value by `key` in the volume's `index_kind`
  index tree.

  Returns `{:ok, binary}` if the key is present and not tombstoned,
  `{:error, :not_found}` if absent or tombstoned, or another error
  variant for failures along the read path.
  """
  @spec get(volume_id :: binary(), index_kind(), key :: binary(), keyword()) ::
          {:ok, binary()} | read_error()
  def get(volume_id, index_kind, key, opts \\ [])
      when is_binary(volume_id) and is_atom(index_kind) and is_binary(key) do
    with {:ok, segment, root_entry} <- resolve_segment(volume_id, opts) do
      tree_root = Map.fetch!(segment.index_roots, index_kind)
      do_index_tree_get(root_entry, tree_root, key, opts)
    end
  end

  @doc """
  Range scan `[start_key, end_key)` on the volume's `index_kind`
  index tree. `<<>>` on either side is open-ended.

  Returns `{:ok, [{key, value}]}` in ascending key order, with
  tombstones filtered out, or an error variant.
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
    with {:ok, segment, root_entry} <- resolve_segment(volume_id, opts) do
      tree_root = Map.fetch!(segment.index_roots, index_kind)
      do_index_tree_range(root_entry, tree_root, start_key, end_key, opts)
    end
  end

  ## Internals

  defp resolve_segment(volume_id, opts) do
    cluster_state_loader = Keyword.get(opts, :cluster_state_loader, &default_cluster_loader/0)
    bootstrap_lookup = Keyword.get(opts, :bootstrap_lookup, &default_bootstrap_lookup/1)
    root_chunk_reader = Keyword.get(opts, :root_chunk_reader, &default_root_chunk_reader/2)

    with {:ok, cluster_state} <- load_cluster_state(cluster_state_loader),
         {:ok, root_entry} <- bootstrap_query(bootstrap_lookup, volume_id),
         {:ok, chunk_bytes} <- read_root_chunk(root_chunk_reader, root_entry),
         {:ok, segment} <- decode_segment(chunk_bytes),
         :ok <- check_cluster(segment, cluster_state) do
      {:ok, segment, root_entry}
    end
  end

  defp default_cluster_loader, do: ClusterState.load()

  defp default_bootstrap_lookup(volume_id) do
    case RaSupervisor.local_query(&MetadataStateMachine.get_volume_root(&1, volume_id)) do
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
    by_id = Map.new(all_drives, &{&1.drive_id, &1})
    locals = Enum.filter(root_entry.drive_locations, &(&1.node == node()))

    found =
      Enum.find_value(locals, fn loc ->
        case Map.fetch(by_id, loc.drive_id) do
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
    nif_get = Keyword.get(opts, :index_tree_get, &default_index_tree_get/4)
    store_handle = pick_store_handle(root_entry, opts)

    case nif_get.(store_handle, tree_root_or_empty(tree_root), "hot", key) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, value} when is_binary(value) -> {:ok, value}
      {:error, reason} -> {:error, {:index_tree_read_failed, reason}}
    end
  end

  defp do_index_tree_range(root_entry, tree_root, start_key, end_key, opts) do
    nif_range = Keyword.get(opts, :index_tree_range, &default_index_tree_range/5)
    store_handle = pick_store_handle(root_entry, opts)
    root_bytes = tree_root_or_empty(tree_root)

    case nif_range.(store_handle, root_bytes, "hot", start_key, end_key) do
      {:ok, entries} when is_list(entries) -> {:ok, entries}
      {:error, reason} -> {:error, {:index_tree_read_failed, reason}}
    end
  end

  defp pick_store_handle(root_entry, opts) do
    Keyword.get_lazy(opts, :store_handle, fn ->
      drive_lister = Keyword.get(opts, :drive_lister, &default_drive_lister/0)

      with {:ok, all_drives} <- drive_lister.(),
           {:ok, drive} <- pick_local_replica(root_entry, all_drives),
           {:ok, handle} <- BlobStore.get_store_handle(drive.drive_id) do
        handle
      else
        _ ->
          raise ArgumentError,
                "MetadataReader could not resolve a local store handle for " <>
                  "volume #{inspect(root_entry.volume_id)}. Pass `:store_handle` " <>
                  "explicitly, or ensure the volume has a local replica."
      end
    end)
  end

  defp tree_root_or_empty(nil), do: <<>>
  defp tree_root_or_empty(hash) when is_binary(hash), do: hash

  defp default_index_tree_get(store, root_hash, tier, key) do
    Native.index_tree_get(store, root_hash, tier, key)
  end

  defp default_index_tree_range(store, root_hash, tier, start_key, end_key) do
    Native.index_tree_range(store, root_hash, tier, start_key, end_key)
  end
end
