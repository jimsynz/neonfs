defmodule NeonFS.Core.CommitChunks do
  @moduledoc """
  Commits a file whose chunk bytes have already been written to their
  replica nodes out-of-band — typically by an interface node that streamed
  an upload through `NeonFS.Client.ChunkWriter` directly to the data plane.

  The flow is the inverse half of `WriteOperation.write_file_streamed/4`:
  bytes are already on replica disks; this function only has to verify
  them, materialise `ChunkIndex` entries, and lay down the `FileIndex`
  entry under the same lock / share-mode / quorum-write semantics as a
  co-located streaming write.

  ## Assumptions

  The caller (e.g. `ChunkWriter`) knows the replica placement it used, so
  the set of `{hash => [location]}` pairs is supplied in `opts`.
  Validation for each chunk is a `Router.data_call(:has_chunk, …)` against
  the reported locations — if every location reports `:not_found` the
  chunk is considered missing.

  This is the *interface-side chunking* path described in the #408
  design decision — compression and encryption still happen on core in
  the `put_chunk` handler, so this commit path stores chunks with the
  codec shape the `put_chunk` handler chose. Volumes that rely on
  compression or encryption therefore require the `put_chunk` handler to
  propagate those options; that is tracked outside this module.
  """

  require Logger

  alias NeonFS.Client.Router
  alias NeonFS.Core.Authorise
  alias NeonFS.Core.BlobStore
  alias NeonFS.Core.ChunkIndex
  alias NeonFS.Core.ChunkMeta
  alias NeonFS.Core.FileIndex
  alias NeonFS.Core.FileMeta
  alias NeonFS.Core.LockManager
  alias NeonFS.Core.VolumeRegistry
  alias NeonFS.Core.WriteOperation
  alias NeonFS.Error.VolumeNotFound

  @type location :: %{
          required(:node) => node(),
          required(:drive_id) => String.t(),
          required(:tier) => :hot | :warm | :cold
        }

  @type opts :: [
          total_size: non_neg_integer(),
          locations: %{optional(binary()) => [location()]},
          uid: non_neg_integer(),
          gids: [non_neg_integer()],
          client_ref: term(),
          mode: non_neg_integer(),
          content_type: String.t(),
          metadata: map()
        ]

  @doc """
  Commits a pre-chunked file into the cluster.

  Parameters:

    * `volume_id` — internal volume id (not the user-facing name).
    * `path` — absolute file path inside the volume.
    * `chunk_hashes` — ordered list of chunk SHA-256 hashes (32-byte
      binaries). The order of this list is the file's byte order.
    * `opts` — see `t:opts/0`. `:total_size` and `:locations` are
      required; every hash in `chunk_hashes` must appear in `:locations`.

  Returns `{:ok, %FileMeta{}}` on success. Errors:

    * `{:error, {:missing_chunk, hash}}` — no reported location answered
      `:has_chunk` for that hash.
    * `{:error, :unknown_chunk_location, hash}` — the hash has no entry
      in the `:locations` map.
    * `{:error, term()}` — any other failure from the lock / authorisation
      / index layer.

  Aborts roll back any `active_write_refs` added in-flight, matching
  `WriteOperation.write_file_streamed/4`'s failure behaviour.
  """
  @spec commit(binary(), String.t(), [binary()], opts()) ::
          {:ok, FileMeta.t()} | {:error, term()}
  def commit(volume_id, path, chunk_hashes, opts \\ []) do
    Logger.metadata(component: :commit_chunks, volume_id: volume_id, file_path: path)

    write_id = WriteOperation.generate_write_id()
    total_size = Keyword.fetch!(opts, :total_size)
    locations_map = Keyword.fetch!(opts, :locations)
    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])
    client_ref = Keyword.get(opts, :client_ref)

    result =
      with {:ok, volume} <- get_volume(volume_id),
           :ok <- Authorise.check(uid, gids, :write, {:volume, volume_id}),
           :ok <- check_lock(volume_id, path, client_ref, opts),
           chunk_metas <- reconcile_chunks(chunk_hashes, locations_map, write_id),
           {:ok, reconciled} <- collect_reconciled(chunk_metas),
           {:ok, file_meta} <-
             create_file_metadata(volume.id, path, chunk_hashes, total_size, opts),
           :ok <- commit_chunk_refs(reconciled, write_id) do
        {:ok, file_meta}
      end

    case result do
      {:ok, _meta} = ok ->
        ok

      {:error, _reason} = err ->
        WriteOperation.abort_chunks(write_id)
        err
    end
  end

  # ─── Helpers ───────────────────────────────────────────────────────────

  defp get_volume(volume_id) do
    case VolumeRegistry.get(volume_id) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, VolumeNotFound.exception(volume_id: volume_id)}
    end
  end

  # Conservative full-file range — same constant as
  # WriteOperation.streamed_lock_range/0.
  @streamed_lock_length 9_223_372_036_854_775_807

  defp check_lock(_volume_id, _path, nil, _opts), do: :ok

  defp check_lock(volume_id, path, client_ref, opts) do
    lock_file_id = WriteOperation.lock_file_id(volume_id, path)
    range = {0, @streamed_lock_length}

    if Keyword.get(opts, :block_on_lock, false) do
      timeout = Keyword.get(opts, :block_on_lock_timeout, 5_000)
      LockManager.check_write_blocking(lock_file_id, client_ref, range, timeout: timeout)
    else
      LockManager.check_write(lock_file_id, client_ref, range)
    end
  end

  defp reconcile_chunks(chunk_hashes, locations_map, write_id) do
    Enum.map(chunk_hashes, fn hash ->
      reconcile_chunk(hash, Map.get(locations_map, hash), write_id)
    end)
  end

  defp reconcile_chunk(hash, nil, _write_id) do
    {:error, {:unknown_chunk_location, hash}}
  end

  defp reconcile_chunk(hash, locations, write_id) do
    case ChunkIndex.get(hash) do
      {:ok, existing} ->
        add_write_ref(existing, write_id, locations)

      {:error, :not_found} ->
        create_chunk_meta(hash, locations, write_id)
    end
  end

  defp add_write_ref(%ChunkMeta{} = existing, write_id, supplied_locations) do
    merged_locations = merge_locations(existing.locations, supplied_locations)

    case ChunkIndex.add_write_ref(existing.hash, write_id) do
      :ok ->
        updated =
          existing
          |> Map.put(:locations, merged_locations)
          |> Map.update!(:active_write_refs, &MapSet.put(&1, write_id))

        maybe_update_locations(updated, supplied_locations)

        {:ok, updated}

      {:error, _reason} = err ->
        err
    end
  end

  defp merge_locations(existing_locations, supplied_locations) do
    (existing_locations ++ supplied_locations) |> Enum.uniq()
  end

  defp maybe_update_locations(%ChunkMeta{hash: hash, locations: locations}, supplied) do
    # Only push locations back to the index when the caller gave us new
    # ones — avoids unnecessary Ra commands on the common "chunk already
    # fully registered" path.
    if Enum.any?(supplied, &(&1 not in locations)) do
      _ = ChunkIndex.update_locations(hash, locations)
    end

    :ok
  end

  defp create_chunk_meta(hash, locations, write_id) do
    case first_has_chunk(hash, locations) do
      {:ok, size} ->
        meta = %ChunkMeta{
          hash: hash,
          original_size: size,
          stored_size: size,
          compression: :none,
          crypto: nil,
          locations: Enum.uniq(locations),
          target_replicas: max(length(locations), 1),
          commit_state: :uncommitted,
          active_write_refs: MapSet.new([write_id]),
          stripe_id: nil,
          stripe_index: nil,
          created_at: DateTime.utc_now(),
          last_verified: nil
        }

        case ChunkIndex.put(meta) do
          :ok -> {:ok, meta}
          {:error, _reason} = err -> err
        end

      :missing ->
        {:error, {:missing_chunk, hash}}
    end
  end

  defp first_has_chunk(_hash, []), do: :missing

  defp first_has_chunk(hash, [location | rest]) do
    case probe_location(hash, location) do
      {:ok, size} -> {:ok, size}
      _ -> first_has_chunk(hash, rest)
    end
  end

  defp probe_location(hash, %{node: node}) when node == node() do
    case BlobStore.chunk_info(hash) do
      {:ok, _tier, size} -> {:ok, size}
      {:error, _} -> :missing
    end
  end

  defp probe_location(hash, %{node: node}) do
    case Router.data_call(node, :has_chunk, hash: hash) do
      {:ok, %{size: size}} -> {:ok, size}
      _ -> :missing
    end
  end

  defp collect_reconciled(results) do
    Enum.reduce_while(results, {:ok, []}, fn
      {:ok, meta}, {:ok, acc} -> {:cont, {:ok, [meta | acc]}}
      {:error, _} = err, _ -> {:halt, err}
    end)
    |> case do
      {:ok, metas} -> {:ok, Enum.reverse(metas)}
      err -> err
    end
  end

  defp create_file_metadata(volume_id, path, chunk_hashes, total_size, opts) do
    case FileIndex.get_by_path(volume_id, path) do
      {:ok, existing} ->
        FileIndex.delete(existing.id)

      {:error, :not_found} ->
        :ok
    end

    file_opts =
      [chunks: chunk_hashes, size: total_size]
      |> maybe_put(opts, :mode)
      |> maybe_put(opts, :content_type)
      |> maybe_put(opts, :metadata)
      |> maybe_put(opts, :uid)
      |> maybe_put(opts, :gids)

    file_meta = FileMeta.new(volume_id, path, file_opts)

    case FileIndex.create(file_meta) do
      {:ok, stored} -> {:ok, stored}
      {:error, _reason} = err -> err
    end
  end

  defp maybe_put(kw, opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> Keyword.put(kw, key, value)
      :error -> kw
    end
  end

  defp commit_chunk_refs(metas, write_id) do
    remove_results =
      Enum.map(metas, fn %ChunkMeta{hash: hash} ->
        ChunkIndex.remove_write_ref(hash, write_id)
      end)

    if Enum.all?(remove_results, &(&1 == :ok)) do
      commit_results =
        Enum.map(metas, fn %ChunkMeta{hash: hash} -> commit_if_ready(hash) end)

      if Enum.all?(commit_results, &acceptable_commit_result?/1) do
        :ok
      else
        {:error, :commit_failed}
      end
    else
      {:error, :commit_failed}
    end
  end

  defp commit_if_ready(hash) do
    case ChunkIndex.get(hash) do
      {:ok, %ChunkMeta{active_write_refs: refs}} ->
        if MapSet.size(refs) == 0 do
          ChunkIndex.commit(hash)
        else
          :ok
        end

      {:error, :not_found} ->
        :ok
    end
  end

  defp acceptable_commit_result?(:ok), do: true
  defp acceptable_commit_result?({:error, :has_active_writes}), do: true
  defp acceptable_commit_result?(_), do: false
end
