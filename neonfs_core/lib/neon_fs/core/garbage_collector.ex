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
    MetadataStateMachine,
    RaSupervisor,
    StripeIndex
  }

  alias NeonFS.Core.Volume.{MetadataReader, MetadataValue}

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
     reachable from the live volume_root **and** every snapshot root.
  2. Sweep phase: deletes committed chunks not in the referenced set.
  3. Stripe cleanup: deletes orphaned stripe metadata.

  ## Options

  - `:volume_id` — when given, the entire pass (mark **and** sweep) is
    scoped to that volume. Files outside the volume are ignored, and
    only chunks/stripes whose `volume_id` matches are considered for
    deletion. Without this scoping the sweep would delete every other
    volume's chunks, since they aren't in the (volume-scoped)
    referenced set.
  - `:snapshot_enumerator` — `0`-arity function returning
    `[{volume_id, snapshot_entry}]`. Defaults to a Ra `local_query`
    against `MetadataStateMachine.get_all_snapshots/1` and a
    `volume_id`-scoped filter. Injectable for unit tests so the mark
    phase can be exercised without spinning up Ra.
  - `:metadata_reader` — module implementing the `:file_index` /
    `:stripe_index` read API used to walk a snapshot's tree.
    Defaults to `NeonFS.Core.Volume.MetadataReader`. Tests stub this
    to feed deterministic file/stripe entries.

  Returns a summary map with counts of deleted chunks and stripes.
  """
  @spec collect(keyword()) :: {:ok, gc_result()}
  def collect(opts) when is_list(opts) do
    start_time = System.monotonic_time()
    volume_filter = Keyword.get(opts, :volume_id)

    files = list_files(opts)
    live_chunks = mark_referenced_chunks(files)
    live_stripes = mark_referenced_stripes(files)

    {snapshot_chunks, snapshot_stripes} = mark_snapshot_references(volume_filter, opts)

    referenced_chunks = MapSet.union(live_chunks, snapshot_chunks)
    referenced_stripes = MapSet.union(live_stripes, snapshot_stripes)

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

  # ─── Multi-Root Mark (snapshots) ───────────────────────────────────────
  #
  # Each snapshot pins a `root_chunk_hash` — a frozen view of the
  # volume's index trees at the moment the snapshot was taken. After
  # the live root advances (files deleted, rewrites, etc.) the chunks
  # those files referenced are no longer reachable from the live root,
  # but they're still reachable from the snapshot's root. The mark
  # phase must therefore walk *every* snapshot's `:file_index` (and
  # follow stripes through that snapshot's `:stripe_index`) and add
  # all those chunk hashes to the live-root mark set.
  #
  # Chunks and inner tree pages are content-addressed, so shared
  # structure between the live root and a snapshot dedups for free in
  # the MapSet — cost is proportional to *distinct* chunks across
  # roots, not `snapshots × volume_size`.

  defp mark_snapshot_references(volume_filter, opts) do
    enumerator =
      Keyword.get(opts, :snapshot_enumerator, fn -> default_snapshot_enumerator(volume_filter) end)

    reader = Keyword.get(opts, :metadata_reader, MetadataReader)

    enumerator.()
    |> Enum.reduce({MapSet.new(), MapSet.new()}, fn {volume_id, snapshot}, {chunks, stripes} ->
      entries = walk_snapshot_files(reader, volume_id, snapshot.root_chunk_hash)

      chunks = add_snapshot_chunks(entries, reader, volume_id, snapshot.root_chunk_hash, chunks)
      stripes = add_snapshot_stripes(entries, stripes)

      {chunks, stripes}
    end)
  end

  defp default_snapshot_enumerator(nil) do
    case RaSupervisor.local_query(&MetadataStateMachine.get_all_snapshots/1) do
      {:ok, by_volume} when is_map(by_volume) ->
        Enum.flat_map(by_volume, fn {volume_id, snapshots_map} ->
          Enum.map(Map.values(snapshots_map), &{volume_id, &1})
        end)

      _ ->
        []
    end
  end

  defp default_snapshot_enumerator(volume_id) when is_binary(volume_id) do
    case RaSupervisor.local_query(&MetadataStateMachine.list_snapshots(&1, volume_id)) do
      {:ok, snapshots} when is_list(snapshots) ->
        Enum.map(snapshots, &{volume_id, &1})

      _ ->
        []
    end
  end

  defp walk_snapshot_files(reader, volume_id, root_chunk_hash) do
    case reader.range(volume_id, :file_index, <<>>, <<>>, at_root: root_chunk_hash) do
      {:ok, raw_entries} when is_list(raw_entries) ->
        Enum.flat_map(raw_entries, &decode_file_entry/1)

      _ ->
        []
    end
  end

  defp decode_file_entry({"file:" <> _, bytes}) when is_binary(bytes) do
    case MetadataValue.decode(bytes) do
      {:ok, map} when is_map(map) -> [map]
      _ -> []
    end
  end

  defp decode_file_entry(_), do: []

  defp add_snapshot_chunks(entries, reader, volume_id, root_chunk_hash, acc) do
    Enum.reduce(entries, acc, fn entry, set ->
      set
      |> add_direct_chunks(entry)
      |> add_stripe_chunks(entry, reader, volume_id, root_chunk_hash)
    end)
  end

  defp add_direct_chunks(set, %{chunks: chunks}) when is_list(chunks),
    do: Enum.reduce(chunks, set, &MapSet.put(&2, &1))

  defp add_direct_chunks(set, _), do: set

  defp add_stripe_chunks(set, %{stripes: stripes}, reader, volume_id, root_chunk_hash)
       when is_list(stripes) do
    Enum.reduce(stripes, set, fn stripe_ref, inner ->
      case stripe_ref_id(stripe_ref) do
        nil ->
          inner

        stripe_id ->
          add_chunks_from_stripe(inner, reader, volume_id, stripe_id, root_chunk_hash)
      end
    end)
  end

  defp add_stripe_chunks(set, _entry, _reader, _vol, _root), do: set

  defp stripe_ref_id(%{stripe_id: sid}) when is_binary(sid), do: sid
  defp stripe_ref_id(%{"stripe_id" => sid}) when is_binary(sid), do: sid
  defp stripe_ref_id(_), do: nil

  defp add_chunks_from_stripe(set, reader, volume_id, stripe_id, root_chunk_hash) do
    case reader.get_stripe(volume_id, "stripe:" <> stripe_id, at_root: root_chunk_hash) do
      {:ok, stripe_map} when is_map(stripe_map) ->
        case Map.get(stripe_map, :chunks) || Map.get(stripe_map, "chunks") do
          chunks when is_list(chunks) -> Enum.reduce(chunks, set, &MapSet.put(&2, &1))
          _ -> set
        end

      _ ->
        set
    end
  end

  defp add_snapshot_stripes(entries, acc) do
    Enum.reduce(entries, acc, &add_entry_stripe_ids/2)
  end

  defp add_entry_stripe_ids(entry, set) do
    case Map.get(entry, :stripes) || Map.get(entry, "stripes") do
      stripes when is_list(stripes) -> Enum.reduce(stripes, set, &put_stripe_ref/2)
      _ -> set
    end
  end

  defp put_stripe_ref(ref, set) do
    case stripe_ref_id(ref) do
      nil -> set
      sid -> MapSet.put(set, sid)
    end
  end

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

  defp matches_volume?(%ChunkMeta{volume_ids: volume_ids}, volume_id),
    do: MapSet.member?(volume_ids, volume_id)

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
