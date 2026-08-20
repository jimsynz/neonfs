defmodule NeonFS.Core.ChunkReconciler do
  @moduledoc """
  Verifies that chunks an interface node claims to have written are really on
  the disks it names, and materialises their `ChunkIndex` entries.

  This is the step that stands between a published map — a file's chunk list,
  or a block device's extent map — and data that is not there. A caller that
  streamed through `NeonFS.Client.ChunkWriter` knows the replica placement it
  used and supplies it as `{hash => [location]}`; this module checks each
  claim rather than trusting it, because the writer's report is the very thing
  in doubt when a chunk is missing.

  Validation per chunk is a `Router.data_call(:has_chunk, …)` against the
  reported locations, short-circuiting on the first that answers. A chunk
  every location reports `:not_found` for is missing, and the caller must not
  publish anything pointing at it.

  Shared by the file commit (`NeonFS.Core.CommitChunks`) and the block
  device's batched extent commit. It is one module rather than one per caller
  deliberately: two implementations of this check would drift, and the copy
  that drifted would be the one without the field history behind these
  clauses.

  Chunks are left `:uncommitted` with the caller's write ref held. Publishing
  them is the caller's job — for a file that is
  `FileIndex.create_committing_chunks/3` folding the commit into the same
  shard-CAS as the file entry.
  """

  alias NeonFS.Client.Router
  alias NeonFS.Core.BlobStore
  alias NeonFS.Core.ChunkIndex
  alias NeonFS.Core.ChunkMeta

  @type location :: ChunkMeta.location()
  @type codec :: %{optional(atom()) => term()}

  @doc """
  Reconcile `chunk_hashes` against the locations and codecs their writer
  reported, holding `write_id`'s reference on each.

  Returns the metadata in the order the hashes were given, or the first error
  — `{:unknown_chunk_location, hash}` for a hash the caller supplied no
  location for, `{:missing_chunk, hash}` for one no location will admit to
  holding.

  Failing on the first error rather than collecting them all is deliberate:
  the caller's next step is to abort the whole write, so a second missing
  chunk changes nothing about what happens next.
  """
  @spec reconcile(
          binary(),
          [binary()],
          %{binary() => [location()]},
          %{binary() => codec()},
          binary()
        ) ::
          {:ok, [ChunkMeta.t()]} | {:error, term()}
  def reconcile(volume_id, chunk_hashes, locations_map, chunk_codecs, write_id)
      when is_binary(volume_id) and is_list(chunk_hashes) and is_map(locations_map) and
             is_map(chunk_codecs) do
    chunk_hashes
    |> Enum.map(fn hash ->
      reconcile_chunk(
        volume_id,
        hash,
        Map.get(locations_map, hash),
        Map.get(chunk_codecs, hash, %{compression: :none, crypto: nil}),
        write_id
      )
    end)
    |> collect()
  end

  defp reconcile_chunk(_volume_id, hash, nil, _codec, _write_id) do
    {:error, {:unknown_chunk_location, hash}}
  end

  defp reconcile_chunk(volume_id, hash, locations, codec, write_id) do
    case ChunkIndex.get(volume_id, hash) do
      {:ok, existing} ->
        add_write_ref(existing, write_id, locations)

      {:error, :not_found} ->
        create_chunk_meta(volume_id, hash, locations, codec, write_id)
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

  defp create_chunk_meta(volume_id, hash, locations, codec, write_id) do
    case first_has_chunk(hash, locations) do
      {:ok, stored_size} ->
        meta = %ChunkMeta{
          volume_ids: MapSet.new([volume_id]),
          hash: hash,
          original_size: Map.get(codec, :original_size, stored_size),
          stored_size: stored_size,
          compression: Map.get(codec, :compression, :none),
          crypto: Map.get(codec, :crypto),
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

  defp collect(results) do
    Enum.reduce_while(results, {:ok, []}, fn
      {:ok, meta}, {:ok, acc} -> {:cont, {:ok, [meta | acc]}}
      {:error, _} = err, _ -> {:halt, err}
    end)
    |> case do
      {:ok, metas} -> {:ok, Enum.reverse(metas)}
      err -> err
    end
  end
end
