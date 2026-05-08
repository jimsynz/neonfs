defmodule NeonFS.Core.Volume.MetadataWriter do
  @moduledoc """
  The mirror of `NeonFS.Core.Volume.MetadataReader` (#820) for the
  write path (#785).

  The flow for every write:

  1. Resolve `volume_id` to `{root_chunk_hash, drive_locations}`
     via the bootstrap layer (#779).
  2. Read + decode the current `RootSegment` (#780).
  3. Apply the update to the relevant index tree via the write
     NIFs (#828) — produces a new `tree_root_hash` (CoW).
  4. Advance the per-volume HLC (#782) and bump
     `last_written_by_neonfs_version`.
  5. Build a new `RootSegment` with the updated `index_roots[kind]`
     + advanced HLC.
  6. Encode + replicate via `Volume.ChunkReplicator` (#808).
  7. Submit `:update_volume_root` to Ra so the bootstrap pointer
     swaps to the new root.

  The bootstrap-event subscription on `MetadataCache` (#826)
  invalidates cached entries for the volume once the Ra command
  commits, so the next read goes through the full walk and picks
  up the new state.

  Per-volume serialisation is provided by the CAS variant of the Ra
  command (`:cas_update_volume_root`, #830). The writer threads the
  current root chunk hash as `expected_previous_hash`; if a
  concurrent writer has flipped the bootstrap pointer in the
  meantime, Ra rejects the update with
  `{:stale_pointer, expected:, actual:}` and the writer retries
  end-to-end (re-reading the now-newer segment, re-applying the
  tree op, replicating the new chunk, and CAS-ing again). The
  retry budget is configurable via `:cas_retries` (default 5).

  Each external dependency is injectable via opts so unit tests
  drive the function with deterministic stubs (same pattern as
  `Volume.Provisioner` from #810 and `MetadataReader` from #820).
  """

  alias NeonFS.Core.Blob.Native
  alias NeonFS.Core.BlobStore
  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.Core.RaSupervisor
  alias NeonFS.Core.Volume.{ChunkReplicator, HLC, MetadataReader, Provisioner, RootSegment}
  alias NeonFS.Core.VolumeRegistry

  @type index_kind :: :file_index | :chunk_index | :stripe_index
  @type write_error ::
          MetadataReader.read_error()
          | {:error, :insufficient_replicas, map()}
          | {:error, {:bootstrap_update_failed, term()}}
          | {:error, {:index_tree_write_failed, term()}}

  @doc """
  Insert or replace `key`'s value in the volume's `index_kind`
  index tree. Returns the `root_chunk_hash` the bootstrap layer
  now points at.
  """
  @spec put(binary(), index_kind(), binary(), binary(), keyword()) ::
          {:ok, binary()} | write_error()
  def put(volume_id, index_kind, key, value, opts \\ [])
      when is_binary(volume_id) and is_atom(index_kind) and is_binary(key) and is_binary(value) do
    apply_index_op(volume_id, index_kind, opts, fn store, current_tree_root ->
      nif_put = Keyword.get(opts, :index_tree_put, &Native.index_tree_put/5)
      nif_put.(store, current_tree_root, "hot", key, value)
    end)
  end

  @doc """
  Tombstone `key`. Even on a never-written tree this writes a
  tombstone so anti-entropy replicates the delete.
  """
  @spec delete(binary(), index_kind(), binary(), keyword()) ::
          {:ok, binary()} | write_error()
  def delete(volume_id, index_kind, key, opts \\ [])
      when is_binary(volume_id) and is_atom(index_kind) and is_binary(key) do
    apply_index_op(volume_id, index_kind, opts, fn store, current_tree_root ->
      nif_delete = Keyword.get(opts, :index_tree_delete, &Native.index_tree_delete/4)
      nif_delete.(store, current_tree_root, "hot", key)
    end)
  end

  @doc """
  Reap tombstones older than `before_unix_nanos`. Returns the new
  `root_chunk_hash`. Errors if the index is empty (nothing to purge).
  """
  @spec purge_tombstones(binary(), index_kind(), non_neg_integer(), keyword()) ::
          {:ok, binary()} | write_error()
  def purge_tombstones(volume_id, index_kind, before_unix_nanos, opts \\ [])
      when is_binary(volume_id) and is_atom(index_kind) and is_integer(before_unix_nanos) do
    apply_index_op(volume_id, index_kind, opts, fn store, current_tree_root ->
      nif_purge =
        Keyword.get(opts, :index_tree_purge_tombstones, &Native.index_tree_purge_tombstones/4)

      if current_tree_root in [nil, <<>>] do
        {:error, "cannot purge tombstones on an empty tree"}
      else
        nif_purge.(store, current_tree_root, "hot", before_unix_nanos)
      end
    end)
  end

  ## Internals

  @default_cas_retries 5

  # Walks the read path to resolve the current segment, applies the
  # caller's `tree_op` to produce a new tree root hash, then commits
  # the change end-to-end (build new segment, replicate, CAS the
  # bootstrap pointer). On a CAS conflict (`:stale_pointer`) the
  # whole flow retries — a concurrent writer flipped the pointer,
  # so we re-read and re-apply our op against their root.
  defp apply_index_op(volume_id, index_kind, opts, tree_op) do
    retries_left = Keyword.get(opts, :cas_retries, @default_cas_retries)
    do_apply_index_op(volume_id, index_kind, opts, tree_op, retries_left)
  end

  defp do_apply_index_op(_volume_id, _index_kind, _opts, _tree_op, retries_left)
       when retries_left < 0 do
    {:error, {:cas_retries_exhausted, %{}}}
  end

  defp do_apply_index_op(volume_id, index_kind, opts, tree_op, retries_left) do
    with {:ok, segment, root_entry} <-
           resolve_or_provision(volume_id, opts),
         current_tree_root = Map.fetch!(segment.index_roots, index_kind),
         store = pick_store_handle(root_entry, opts),
         {:ok, new_tree_root} <- run_tree_op(tree_op, store, current_tree_root),
         {advanced_segment, _ts} = advance_segment(segment, index_kind, new_tree_root),
         encoded = RootSegment.encode(advanced_segment),
         {:ok, replica_drives} <- pick_replica_drives(root_entry, opts),
         {:ok, new_root_chunk_hash} <-
           replicate_segment(encoded, replica_drives, advanced_segment.durability, opts) do
      case update_bootstrap(
             volume_id,
             root_entry.root_chunk_hash,
             new_root_chunk_hash,
             replica_drives,
             advanced_segment,
             opts
           ) do
        {:ok, _} ->
          {:ok, new_root_chunk_hash}

        {:error, {:bootstrap_update_failed, {:stale_pointer, _info}}} ->
          do_apply_index_op(volume_id, index_kind, opts, tree_op, retries_left - 1)

        {:error, _} = err ->
          err
      end
    end
  end

  # Looks up the volume's bootstrap entry. If missing — `VolumeRegistry`
  # skipped eager provisioning at create-time because the cluster
  # didn't have enough drives for the volume's durability — provision
  # the volume now, then retry the lookup. Production volumes that
  # were created before the cluster grew enough drives end up in this
  # state until their first metadata write.
  #
  # Returns the same `{:ok, segment, root_entry}` shape as
  # `MetadataReader.resolve_segment_for_write/2`. Provisioning errors
  # surface as `{:error, _}` / `{:error, _, _}`.
  defp resolve_or_provision(volume_id, opts) do
    case MetadataReader.resolve_segment_for_write(volume_id, opts) do
      {:ok, _segment, _root_entry} = ok ->
        ok

      {:error, :not_found} ->
        with {:ok, volume} <- fetch_volume(volume_id, opts),
             :ok <- provision_volume(volume, opts) do
          MetadataReader.resolve_segment_for_write(volume_id, opts)
        end

      {:error, _} = err ->
        err
    end
  end

  defp fetch_volume(volume_id, opts) do
    fetcher = Keyword.get(opts, :volume_fetcher, &VolumeRegistry.get/1)

    case fetcher.(volume_id) do
      {:ok, volume} -> {:ok, volume}
      {:error, _} = err -> err
    end
  end

  defp provision_volume(volume, opts) do
    provisioner = Keyword.get(opts, :provisioner, Provisioner)

    case provisioner.provision(volume) do
      {:ok, _root_chunk_hash} -> :ok
      {:error, _reason} = err -> err
      {:error, reason, info} -> {:error, {reason, info}}
    end
  end

  defp run_tree_op(tree_op, store, current_tree_root) do
    case tree_op.(store, normalise_tree_root(current_tree_root)) do
      {:ok, new_root} when is_binary(new_root) -> {:ok, new_root}
      {:error, reason} -> {:error, {:index_tree_write_failed, reason}}
      other -> {:error, {:index_tree_write_failed, other}}
    end
  end

  defp normalise_tree_root(nil), do: <<>>
  defp normalise_tree_root(hash) when is_binary(hash), do: hash

  defp advance_segment(%RootSegment{} = segment, index_kind, new_tree_root) do
    {timestamp, advanced} = HLC.now(segment)

    new_index_roots = Map.put(segment.index_roots, index_kind, new_tree_root)

    {%{advanced | index_roots: new_index_roots} |> RootSegment.touch(), timestamp}
  end

  defp pick_store_handle(root_entry, opts) do
    Keyword.get_lazy(opts, :store_handle, fn ->
      drive_lister = Keyword.get(opts, :drive_lister, &default_drive_lister/0)

      with {:ok, all_drives} <- drive_lister.(),
           drive when not is_nil(drive) <- pick_local_drive(root_entry, all_drives),
           {:ok, handle} <- BlobStore.get_store_handle(drive.drive_id) do
        handle
      else
        _ ->
          raise ArgumentError,
                "MetadataWriter could not resolve a local store handle for " <>
                  "volume #{inspect(root_entry.volume_id)}. Pass `:store_handle` " <>
                  "explicitly, or ensure the volume has a local replica."
      end
    end)
  end

  defp default_drive_lister do
    case RaSupervisor.local_query(&MetadataStateMachine.get_drives/1) do
      {:ok, drives_map} when is_map(drives_map) -> {:ok, Map.values(drives_map)}
      other -> other
    end
  end

  defp pick_local_drive(root_entry, all_drives) do
    by_id = Map.new(all_drives, &{&1.drive_id, &1})
    locals = Enum.filter(root_entry.drive_locations, &(&1.node == node()))

    Enum.find_value(locals, fn loc ->
      case Map.fetch(by_id, loc.drive_id) do
        {:ok, drive} -> drive
        :error -> nil
      end
    end)
  end

  defp pick_replica_drives(root_entry, opts) do
    drive_lister = Keyword.get(opts, :drive_lister, &default_drive_lister/0)

    with {:ok, all_drives} <- drive_lister.() do
      by_id = Map.new(all_drives, &{&1.drive_id, &1})

      drives =
        for %{drive_id: id} <- root_entry.drive_locations,
            drive = Map.get(by_id, id),
            not is_nil(drive),
            do: drive

      {:ok, drives}
    end
  end

  defp replicate_segment(encoded, replica_drives, durability, opts) do
    chunk_replicator = Keyword.get(opts, :chunk_replicator, ChunkReplicator)
    min_copies = min_copies(durability)

    case chunk_replicator.write_chunk(encoded, replica_drives,
           min_copies: min_copies,
           writer_fn: Keyword.get(opts, :writer_fn)
         ) do
      {:ok, hash, _summary} -> {:ok, hash}
      {:error, _, _} = err -> err
      {:error, _} = err -> err
    end
  end

  defp min_copies(%{type: :replicate, min_copies: m}), do: m
  defp min_copies(%{type: :erasure, data_chunks: d}), do: d

  defp update_bootstrap(
         volume_id,
         expected_previous_hash,
         new_root_chunk_hash,
         replica_drives,
         segment,
         opts
       ) do
    bootstrap_registrar = Keyword.get(opts, :bootstrap_registrar, &default_bootstrap_registrar/1)

    update_payload = %{
      root_chunk_hash: new_root_chunk_hash,
      drive_locations: Enum.map(replica_drives, &%{node: &1.node, drive_id: &1.drive_id}),
      durability_cache: segment.durability
    }

    command =
      {:cas_update_volume_root, volume_id, expected_previous_hash, update_payload}

    case bootstrap_registrar.(command) do
      :ok -> {:ok, :updated}
      {:ok, _} = ok -> ok
      {:error, reason} -> {:error, {:bootstrap_update_failed, reason}}
      other -> {:error, {:bootstrap_update_failed, other}}
    end
  end

  defp default_bootstrap_registrar(command) do
    case RaSupervisor.command(command) do
      {:ok, result, _leader} -> {:ok, result}
      {:error, _} = err -> err
      other -> {:error, other}
    end
  end
end
