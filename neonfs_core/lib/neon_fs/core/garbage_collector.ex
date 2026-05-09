defmodule NeonFS.Core.GarbageCollector do
  @moduledoc """
  Mark-and-sweep garbage collector for NeonFS chunks and stripe metadata.

  The GC identifies committed chunks that are no longer referenced by any file
  and deletes them from both ChunkIndex and BlobStore. For erasure-coded
  volumes, the mark phase resolves file → stripes → chunk hashes to build
  the referenced set. Orphaned stripe metadata is cleaned up after chunk GC.

  Chunks with `active_write_refs` are never deleted (in-flight write protection).
  """

  alias NeonFS.Core.{
    BlobStore,
    ChunkIndex,
    ChunkMeta,
    FileIndex,
    StripeIndex
  }

  require Logger

  @type gc_result :: %{
          chunks_deleted: non_neg_integer(),
          stripes_deleted: non_neg_integer(),
          chunks_protected: non_neg_integer()
        }

  @doc """
  Runs a full garbage collection pass across all volumes.

  Equivalent to `collect([])`.
  """
  @spec collect() :: {:ok, gc_result()}
  def collect, do: collect([])

  @doc """
  Runs a garbage collection pass with options.

  1. Mark phase: builds the set of all chunk hashes referenced by files
  2. Sweep phase: deletes committed chunks not in the referenced set
  3. Stripe cleanup: deletes orphaned stripe metadata

  ## Options

  - `:volume_id` — when given, the entire pass (mark **and** sweep) is
    scoped to that volume. Files outside the volume are ignored, and
    only chunks/stripes whose `volume_id` matches are considered for
    deletion. Without this scoping the sweep would delete every other
    volume's chunks, since they aren't in the (volume-scoped)
    referenced set.

  Returns a summary map with counts of deleted chunks and stripes.
  """
  @spec collect(keyword()) :: {:ok, gc_result()}
  def collect(opts) when is_list(opts) do
    start_time = System.monotonic_time()
    volume_filter = Keyword.get(opts, :volume_id)

    files = list_files(opts)
    referenced_chunks = mark_referenced_chunks(files)
    referenced_stripes = mark_referenced_stripes(files)

    {chunks_deleted, chunks_protected} = sweep_chunks(referenced_chunks, volume_filter)
    stripes_deleted = sweep_stripes(referenced_stripes, volume_filter)

    duration = System.monotonic_time() - start_time

    result = %{
      chunks_deleted: chunks_deleted,
      stripes_deleted: stripes_deleted,
      chunks_protected: chunks_protected
    }

    emit_gc_telemetry(result, duration)

    {:ok, result}
  end

  # ─── File Listing ──────────────────────────────────────────────────────

  defp list_files(opts) do
    case Keyword.get(opts, :volume_id) do
      nil -> FileIndex.list_all()
      volume_id -> FileIndex.list_volume(volume_id)
    end
  end

  # ─── Mark Phase ────────────────────────────────────────────────────────

  defp mark_referenced_chunks(files) do
    Enum.reduce(files, MapSet.new(), &collect_file_chunks/2)
  end

  defp collect_file_chunks(file_meta, acc) do
    acc
    |> collect_direct_chunks(file_meta)
    |> collect_stripe_chunks(file_meta)
  end

  # Replicated files: chunks field contains hash list
  defp collect_direct_chunks(acc, %{chunks: chunks}) when is_list(chunks) do
    Enum.reduce(chunks, acc, &MapSet.put(&2, &1))
  end

  defp collect_direct_chunks(acc, _), do: acc

  # Erasure-coded files: stripes field → StripeIndex → chunk hashes
  defp collect_stripe_chunks(acc, %{stripes: stripes, volume_id: volume_id})
       when is_list(stripes) do
    Enum.reduce(stripes, acc, fn %{stripe_id: sid}, inner_acc ->
      case StripeIndex.get(volume_id, sid) do
        {:ok, stripe} ->
          Enum.reduce(stripe.chunks, inner_acc, &MapSet.put(&2, &1))

        {:error, :not_found} ->
          inner_acc
      end
    end)
  end

  defp collect_stripe_chunks(acc, _), do: acc

  defp mark_referenced_stripes(files) do
    Enum.reduce(files, MapSet.new(), &collect_file_stripe_ids/2)
  end

  defp collect_file_stripe_ids(%{stripes: stripes}, acc) when is_list(stripes) do
    Enum.reduce(stripes, acc, fn %{stripe_id: sid}, inner ->
      MapSet.put(inner, sid)
    end)
  end

  defp collect_file_stripe_ids(_, acc), do: acc

  # ─── Sweep Phase ───────────────────────────────────────────────────────

  defp sweep_chunks(referenced_set, volume_filter) do
    all_committed = list_committed_chunks(volume_filter)

    Enum.reduce(all_committed, {0, 0}, fn chunk_meta, {deleted, protected} ->
      cond do
        MapSet.member?(referenced_set, chunk_meta.hash) ->
          {deleted, protected}

        has_active_writes?(chunk_meta) ->
          {deleted, protected + 1}

        true ->
          sweep_unreferenced_chunk(chunk_meta, deleted, protected)
      end
    end)
  end

  defp sweep_unreferenced_chunk(chunk_meta, deleted, protected) do
    case delete_chunk(chunk_meta) do
      :ok -> {deleted + 1, protected}
      :error -> {deleted, protected}
    end
  end

  defp sweep_stripes(referenced_set, volume_filter) do
    StripeIndex.list_all()
    |> Enum.filter(&matches_volume?(&1, volume_filter))
    |> Enum.reduce(0, fn stripe, count ->
      if MapSet.member?(referenced_set, stripe.id) do
        count
      else
        StripeIndex.delete(stripe.id)
        count + 1
      end
    end)
  end

  defp matches_volume?(_chunk_or_stripe, nil), do: true
  defp matches_volume?(%{volume_id: volume_id}, volume_id), do: true
  defp matches_volume?(_, _), do: false

  # ─── Helpers ───────────────────────────────────────────────────────────

  defp list_committed_chunks(volume_filter) do
    :ets.foldl(
      fn
        {_hash, %ChunkMeta{commit_state: :committed} = meta}, acc ->
          if matches_volume?(meta, volume_filter), do: [meta | acc], else: acc

        _, acc ->
          acc
      end,
      [],
      :chunk_index
    )
  end

  defp has_active_writes?(chunk_meta) do
    MapSet.size(chunk_meta.active_write_refs) > 0
  end

  defp delete_chunk(chunk_meta) do
    case delete_chunk_from_storage(chunk_meta) do
      :ok ->
        ChunkIndex.delete(chunk_meta.hash)
        :ok

      :error ->
        :error
    end
  end

  defp delete_chunk_from_storage(%{locations: locations, hash: hash} = chunk_meta)
       when is_list(locations) and locations != [] do
    delete_opts = BlobStore.codec_opts_for_chunk(chunk_meta)

    results =
      Enum.map(locations, fn location ->
        drive_id = Map.get(location, :drive_id, "default")

        case BlobStore.delete_chunk(hash, drive_id, delete_opts) do
          {:ok, _bytes_freed} ->
            :ok

          {:error, reason} ->
            hex = Base.encode16(hash, case: :lower)

            Logger.warning(
              "GC failed to delete blob #{hex} from drive #{drive_id}: #{inspect(reason)}"
            )

            :error
        end
      end)

    if Enum.any?(results, &(&1 == :ok)), do: :ok, else: :error
  end

  defp delete_chunk_from_storage(%{hash: hash} = chunk_meta) do
    case delete_from_all_drives(hash, BlobStore.codec_opts_for_chunk(chunk_meta)) do
      :ok ->
        :ok

      :error ->
        hex = Base.encode16(hash, case: :lower)
        Logger.warning("GC failed to delete blob #{hex} from any drive")
        :error
    end
  end

  defp delete_from_all_drives(hash, delete_opts) do
    {:ok, drives} = BlobStore.list_drives()
    drive_ids = Map.keys(drives)

    results =
      Enum.map(drive_ids, fn drive_id ->
        case BlobStore.delete_chunk(hash, drive_id, delete_opts) do
          {:ok, _bytes_freed} -> :ok
          {:error, _} -> :error
        end
      end)

    if Enum.any?(results, &(&1 == :ok)), do: :ok, else: :error
  end

  defp emit_gc_telemetry(result, duration) do
    :telemetry.execute(
      [:neonfs, :garbage_collector, :collect],
      %{
        duration: duration,
        chunks_deleted: result.chunks_deleted,
        stripes_deleted: result.stripes_deleted,
        chunks_protected: result.chunks_protected
      },
      %{}
    )
  end
end
