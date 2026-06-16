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
  retry budget is configurable via `:cas_retries` (default 10),
  and conflicting retries back off with full jitter so a burst of
  concurrent writers to one volume decorrelates rather than
  re-colliding on every attempt (#1219 — the high-concurrency
  write-burst test exhausted an immediate-retry budget).

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
  @type mutation ::
          {:put, index_kind(), binary(), binary()}
          | {:delete, index_kind(), binary()}
  @type write_error ::
          MetadataReader.read_error()
          | {:error, Splode.Error.t()}
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
    with_remote_fallback(
      volume_id,
      opts,
      fn ->
        apply_index_op(volume_id, index_kind, opts, fn store, current_tree_root ->
          nif_put = Keyword.get(opts, :index_tree_put, &Native.index_tree_put/5)
          nif_put.(store, current_tree_root, "hot", key, value)
        end)
      end,
      fn node, remote_opts ->
        remote_call(node, opts, :put, [volume_id, index_kind, key, value, remote_opts])
      end
    )
  end

  @doc """
  Tombstone `key`. Even on a never-written tree this writes a
  tombstone so anti-entropy replicates the delete.
  """
  @spec delete(binary(), index_kind(), binary(), keyword()) ::
          {:ok, binary()} | write_error()
  def delete(volume_id, index_kind, key, opts \\ [])
      when is_binary(volume_id) and is_atom(index_kind) and is_binary(key) do
    with_remote_fallback(
      volume_id,
      opts,
      fn ->
        apply_index_op(volume_id, index_kind, opts, fn store, current_tree_root ->
          nif_delete = Keyword.get(opts, :index_tree_delete, &Native.index_tree_delete/4)
          nif_delete.(store, current_tree_root, "hot", key)
        end)
      end,
      fn node, remote_opts ->
        remote_call(node, opts, :delete, [volume_id, index_kind, key, remote_opts])
      end
    )
  end

  @doc """
  Apply a list of index-tree mutations as a **single commit** — one
  CoW tree rebuild across all touched index kinds, one metadata-chunk
  replication, one `:cas_update_volume_root` flip. Returns the
  `root_chunk_hash` the bootstrap layer now points at.

  This is the transaction primitive (#1295): a caller that would
  otherwise issue several `put/5` / `delete/4` calls — each its own
  root flip — collapses them into one consensus round. Mutations are
  applied in list order, threading the per-kind tree root forward, so
  later mutations observe earlier ones (last-write-wins for the same
  key). A stale-pointer CAS conflict retries the whole batch.

  `mutations` is a list of `{:put, kind, key, value}` /
  `{:delete, kind, key}`. An empty list is a no-op that returns the
  current root without flipping.
  """
  @spec apply_batch(binary(), [mutation()], keyword()) :: {:ok, binary()} | write_error()
  def apply_batch(volume_id, mutations, opts \\ [])
      when is_binary(volume_id) and is_list(mutations) do
    with_remote_fallback(
      volume_id,
      opts,
      fn -> local_apply_batch(volume_id, mutations, opts) end,
      fn node, remote_opts ->
        remote_call(node, opts, :apply_batch, [volume_id, mutations, remote_opts])
      end
    )
  end

  @doc """
  Reap tombstones older than `before_unix_nanos`. Returns the new
  `root_chunk_hash`. Errors if the index is empty (nothing to purge).
  """
  @spec purge_tombstones(binary(), index_kind(), non_neg_integer(), keyword()) ::
          {:ok, binary()} | write_error()
  def purge_tombstones(volume_id, index_kind, before_unix_nanos, opts \\ [])
      when is_binary(volume_id) and is_atom(index_kind) and is_integer(before_unix_nanos) do
    with_remote_fallback(
      volume_id,
      opts,
      fn -> local_purge_tombstones(volume_id, index_kind, before_unix_nanos, opts) end,
      fn node, remote_opts ->
        remote_call(node, opts, :purge_tombstones, [
          volume_id,
          index_kind,
          before_unix_nanos,
          remote_opts
        ])
      end
    )
  end

  defp local_purge_tombstones(volume_id, index_kind, before_unix_nanos, opts) do
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

  @doc """
  Update one of the per-volume background-job schedules
  (`:gc | :scrub | :anti_entropy`) in the volume's root segment.

  Same CAS-retry semantics as the index-op path: if the bootstrap
  pointer flips between read and write, the whole flow re-reads and
  re-applies. Index trees are not touched.
  """
  @spec update_schedule(binary(), :gc | :scrub | :anti_entropy, RootSegment.schedule(), keyword()) ::
          {:ok, binary()} | write_error()
  def update_schedule(volume_id, schedule_key, schedule, opts \\ [])
      when is_binary(volume_id) and schedule_key in [:gc, :scrub, :anti_entropy] and
             is_map(schedule) do
    with_remote_fallback(
      volume_id,
      opts,
      fn ->
        apply_segment_op(volume_id, opts, fn segment ->
          %{segment | schedules: Map.put(segment.schedules, schedule_key, schedule)}
        end)
      end,
      fn node, remote_opts ->
        remote_call(node, opts, :update_schedule, [volume_id, schedule_key, schedule, remote_opts])
      end
    )
  end

  ## Internals

  @default_cas_retries 30

  # CAS-conflict backoff bounds (#1219). Optimistic
  # `:cas_update_volume_root` conflicts retry end-to-end; retrying
  # immediately under a concurrent write burst just re-collides, so
  # sleep a jittered, exponentially-growing interval (capped) between
  # attempts to decorrelate the racing writers.
  @cas_backoff_base_ms 2
  @cas_backoff_max_ms 250
  @remote_write_timeout 30_000

  # A metadata write needs the volume's root segment, but it can only be read
  # and CAS-updated from a node that holds a replica of it. When the local node
  # has none, the resolve step fails with `{:no_local_replica, drive_locations}`
  # (often wrapped in `:root_chunk_unreachable`) *before* anything is committed,
  # and the candidate nodes are carried in that error. Re-dispatch the whole
  # operation to one of them — the read path has the same fallback (#1045).
  #
  # Correctness-only: this does not change the root segment's replica set; it
  # just routes the write to a node that can perform it (routing/placement is
  # tracked in #1046). `__remote_dispatched` stops the remote side recursing.
  # Only `:no_local_replica` triggers re-dispatch — every other error (CAS
  # exhaustion, not_found, malformed) is authoritative or already retried
  # locally, and is returned as-is.
  defp with_remote_fallback(_volume_id, opts, local_fun, remote_fun) do
    case local_fun.() do
      {:error, reason} = err ->
        with false <- Keyword.get(opts, :__remote_dispatched, false),
             {:ok, locations} <- no_local_replica_locations(reason),
             [_ | _] = nodes <- candidate_nodes(locations) do
          try_remote_nodes(nodes, err, remote_fun)
        else
          _ -> err
        end

      # `{:ok, _}` and structured write errors (e.g. the quorum
      # `%NeonFS.Error.QuorumUnavailable{}`) pass through: only the
      # pre-commit `:no_local_replica` case is re-dispatched.
      other ->
        other
    end
  end

  defp no_local_replica_locations({:root_chunk_unreachable, {:no_local_replica, locations}}),
    do: {:ok, locations}

  defp no_local_replica_locations({:no_local_replica, locations}), do: {:ok, locations}
  defp no_local_replica_locations(_), do: :error

  defp candidate_nodes(locations) do
    locations
    |> Enum.map(& &1.node)
    |> Enum.uniq()
    |> Enum.reject(&(&1 == node()))
  end

  defp try_remote_nodes([], err, _fun), do: err

  defp try_remote_nodes([node | rest], err, fun) do
    case fun.(node, __remote_dispatched: true) do
      {:ok, _} = ok -> ok
      {:error, _} -> try_remote_nodes(rest, err, fun)
    end
  end

  defp remote_call(node, opts, fn_name, args) do
    caller = Keyword.get(opts, :remote_caller, &default_remote_caller/3)
    caller.(node, fn_name, args)
  end

  defp default_remote_caller(node, fn_name, args) do
    case :rpc.call(node, __MODULE__, fn_name, args, @remote_write_timeout) do
      {:badrpc, reason} -> {:error, {:rpc_failed, reason}}
      other -> other
    end
  end

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
         {:ok, new_tree_root, written_nodes} <- run_tree_op(tree_op, store, current_tree_root),
         {advanced_segment, _ts} = advance_segment(segment, index_kind, new_tree_root),
         encoded = RootSegment.encode(advanced_segment),
         {:ok, replica_drives} <- pick_replica_drives(root_entry, opts),
         :ok <-
           replicate_tree_nodes(written_nodes, replica_drives, advanced_segment.durability, opts),
         {:ok, new_root_chunk_hash} <-
           replicate_metadata_chunk(encoded, replica_drives, advanced_segment.durability, opts) do
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
          cas_backoff(opts, retries_left)
          do_apply_index_op(volume_id, index_kind, opts, tree_op, retries_left - 1)

        {:error, _} = err ->
          err
      end
    end
  end

  # Batch variant of `apply_index_op/4`: applies a list of mutations
  # across (possibly several) index kinds against one resolved segment,
  # then a single segment build + replicate + CAS. Reuses the same
  # stale-pointer retry / backoff as the single-op path.
  defp local_apply_batch(volume_id, [], opts) do
    with {:ok, _segment, root_entry} <- resolve_or_provision(volume_id, opts) do
      {:ok, root_entry.root_chunk_hash}
    end
  end

  defp local_apply_batch(volume_id, mutations, opts) do
    retries_left = Keyword.get(opts, :cas_retries, @default_cas_retries)
    do_apply_batch(volume_id, mutations, opts, retries_left)
  end

  defp do_apply_batch(_volume_id, _mutations, _opts, retries_left) when retries_left < 0 do
    {:error, {:cas_retries_exhausted, %{}}}
  end

  defp do_apply_batch(volume_id, mutations, opts, retries_left) do
    with {:ok, segment, root_entry} <- resolve_or_provision(volume_id, opts),
         store = pick_store_handle(root_entry, opts),
         {:ok, updated_roots, written_nodes} <-
           run_batch_ops(mutations, store, segment.index_roots, opts),
         {advanced_segment, _ts} = advance_segment_multi(segment, updated_roots),
         encoded = RootSegment.encode(advanced_segment),
         {:ok, replica_drives} <- pick_replica_drives(root_entry, opts),
         :ok <-
           replicate_tree_nodes(written_nodes, replica_drives, advanced_segment.durability, opts),
         {:ok, new_root_chunk_hash} <-
           replicate_metadata_chunk(encoded, replica_drives, advanced_segment.durability, opts) do
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
          cas_backoff(opts, retries_left)
          do_apply_batch(volume_id, mutations, opts, retries_left - 1)

        {:error, _} = err ->
          err
      end
    end
  end

  # Apply each mutation against the working per-kind tree root, threading
  # the new root forward so later mutations build on earlier ones, and
  # accumulate every written CoW node for replication.
  defp run_batch_ops(mutations, store, initial_roots, opts) do
    nif_put = Keyword.get(opts, :index_tree_put, &Native.index_tree_put/5)
    nif_delete = Keyword.get(opts, :index_tree_delete, &Native.index_tree_delete/4)

    Enum.reduce_while(mutations, {:ok, initial_roots, []}, fn mutation, {:ok, roots, nodes} ->
      kind = elem(mutation, 1)
      current_tree_root = Map.fetch!(roots, kind)
      op = batch_tree_op(mutation, nif_put, nif_delete)

      case run_tree_op(op, store, current_tree_root) do
        {:ok, new_root, written} ->
          {:cont, {:ok, Map.put(roots, kind, new_root), nodes ++ written}}

        {:error, _} = err ->
          {:halt, err}
      end
    end)
  end

  defp batch_tree_op({:put, _kind, key, value}, nif_put, _nif_delete),
    do: fn store, root -> nif_put.(store, root, "hot", key, value) end

  defp batch_tree_op({:delete, _kind, key}, _nif_put, nif_delete),
    do: fn store, root -> nif_delete.(store, root, "hot", key) end

  defp advance_segment_multi(%RootSegment{} = segment, updated_roots) do
    {timestamp, advanced} = HLC.now(segment)
    {RootSegment.touch(%{advanced | index_roots: updated_roots}), timestamp}
  end

  # Same shape as `apply_index_op/4` but for changes that mutate the
  # segment header itself (schedules, future cluster-meta fields)
  # rather than an index tree. The CAS-retry / replicate / bootstrap-
  # update flow is identical past the segment build.
  defp apply_segment_op(volume_id, opts, segment_transform) do
    retries_left = Keyword.get(opts, :cas_retries, @default_cas_retries)
    do_apply_segment_op(volume_id, opts, segment_transform, retries_left)
  end

  defp do_apply_segment_op(_volume_id, _opts, _transform, retries_left) when retries_left < 0 do
    {:error, {:cas_retries_exhausted, %{}}}
  end

  defp do_apply_segment_op(volume_id, opts, transform, retries_left) do
    with {:ok, segment, root_entry} <- resolve_or_provision(volume_id, opts),
         updated_segment = RootSegment.touch(transform.(segment)),
         encoded = RootSegment.encode(updated_segment),
         {:ok, replica_drives} <- pick_replica_drives(root_entry, opts),
         {:ok, new_root_chunk_hash} <-
           replicate_metadata_chunk(encoded, replica_drives, updated_segment.durability, opts) do
      case update_bootstrap(
             volume_id,
             root_entry.root_chunk_hash,
             new_root_chunk_hash,
             replica_drives,
             updated_segment,
             opts
           ) do
        {:ok, _} ->
          {:ok, new_root_chunk_hash}

        {:error, {:bootstrap_update_failed, {:stale_pointer, _info}}} ->
          cas_backoff(opts, retries_left)
          do_apply_segment_op(volume_id, opts, transform, retries_left - 1)

        {:error, _} = err ->
          err
      end
    end
  end

  # Full-jitter exponential backoff between CAS conflicts. `attempt`
  # grows as `retries_left` shrinks, so each successive collision waits
  # a wider random window — capped — letting a burst of concurrent
  # writers to one volume spread out instead of re-colliding in lockstep.
  defp cas_backoff(opts, retries_left) do
    budget = Keyword.get(opts, :cas_retries, @default_cas_retries)
    attempt = max(0, budget - retries_left)
    ceiling = min(@cas_backoff_max_ms, @cas_backoff_base_ms * Integer.pow(2, attempt))
    Process.sleep(:rand.uniform(ceiling))
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
  # surface as `{:error, _}`.
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
    end
  end

  defp run_tree_op(tree_op, store, current_tree_root) do
    case tree_op.(store, normalise_tree_root(current_tree_root)) do
      {:ok, {new_root, written_nodes}} when is_binary(new_root) and is_list(written_nodes) ->
        {:ok, new_root, written_nodes}

      {:error, reason} ->
        {:error, {:index_tree_write_failed, reason}}

      other ->
        {:error, {:index_tree_write_failed, other}}
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
    by_key = Map.new(all_drives, &{{&1.node, &1.drive_id}, &1})
    locals = Enum.filter(root_entry.drive_locations, &(&1.node == node()))

    Enum.find_value(locals, fn loc ->
      case Map.fetch(by_key, {loc.node, loc.drive_id}) do
        {:ok, drive} -> drive
        :error -> nil
      end
    end)
  end

  defp pick_replica_drives(root_entry, opts) do
    drive_lister = Keyword.get(opts, :drive_lister, &default_drive_lister/0)

    with {:ok, all_drives} <- drive_lister.() do
      by_key = Map.new(all_drives, &{{&1.node, &1.drive_id}, &1})

      drives =
        for %{node: node, drive_id: id} <- root_entry.drive_locations,
            drive = Map.get(by_key, {node, id}),
            not is_nil(drive),
            do: drive

      {:ok, drives}
    end
  end

  # The copy-on-write index-tree nodes a write produced were written by
  # the NIF only to the single local store. Replicate each to the
  # volume's full metadata drive set with the same majority-wins quorum
  # (`min_copies`) the root segment uses, before the bootstrap pointer
  # flips — so any replica node can walk the tree immediately rather
  # than only after anti-entropy catches up (#903). A node chunk that
  # can't reach quorum aborts the whole write; the locally-written
  # chunks orphan to GC, exactly as a failed segment replication does.
  defp replicate_tree_nodes([], _replica_drives, _durability, _opts), do: :ok

  defp replicate_tree_nodes(written_nodes, replica_drives, durability, opts) do
    Enum.reduce_while(written_nodes, :ok, fn {_hash, bytes}, :ok ->
      case replicate_metadata_chunk(bytes, replica_drives, durability, opts) do
        {:ok, _hash} -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  defp replicate_metadata_chunk(encoded, replica_drives, durability, opts) do
    chunk_replicator = Keyword.get(opts, :chunk_replicator, ChunkReplicator)
    min_copies = min_copies(durability)

    write_opts = maybe_put_writer_fn([min_copies: min_copies], opts)

    case chunk_replicator.write_chunk(encoded, replica_drives, write_opts) do
      {:ok, hash, _summary} -> {:ok, hash}
      {:error, _} = err -> err
    end
  end

  # `Keyword.get(opts, :writer_fn)` returns `nil` when the caller
  # didn't supply a stub; passing `writer_fn: nil` straight through
  # to `ChunkReplicator.write_chunk/3` defeats its `Keyword.get_lazy`
  # default and crashes the per-drive `Task.async_stream` worker
  # with "expected a function, got: nil". Only thread `:writer_fn`
  # when the caller actually set one.
  defp maybe_put_writer_fn(write_opts, opts) do
    case Keyword.get(opts, :writer_fn) do
      nil -> write_opts
      fun when is_function(fun) -> Keyword.put(write_opts, :writer_fn, fun)
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
      # `:ra.process_command` wraps the state machine's reply in `{:ok,
      # Reply, Leader}` even when that reply is itself an error — so a
      # `:cas_update_volume_root` that the state machine *rejected* with
      # `{:error, {:stale_pointer, …}}` arrives here as
      # `{:ok, {:error, …}, leader}`. Unwrap the inner error and surface
      # it, otherwise a rejected CAS reads as success and the write is
      # silently dropped (#1260 — concurrent index-tree lost updates).
      {:ok, {:error, reason}, _leader} -> {:error, reason}
      {:ok, result, _leader} -> {:ok, result}
      {:error, _} = err -> err
      other -> {:error, other}
    end
  end
end
