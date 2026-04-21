defmodule NeonFS.Core.PendingWriteLog do
  @moduledoc """
  Tracks in-flight streaming writes so chunks orphaned by a crash can
  be reclaimed immediately on the next startup, rather than waiting
  for a garbage-collection pass.

  A streaming write may place many chunks via
  `NeonFS.Core.WriteOperation.write_file_streamed/4` before its final
  metadata commit. If the core node crashes between the first
  successful `put_chunk` and `commit_chunks/2`, those chunks are
  orphaned. In-process `abort_chunks/1` cleans them up for graceful
  errors — this log closes the gap for crashes.

  ## Design

  - **Node-local**, not Ra-replicated. Orphaned-chunk recovery only
    concerns the node that wrote them, and per-chunk consensus is
    explicitly disallowed by the parent issue (#296).
  - Backed by a single **DETS** table under
    `NeonFS.Core.Persistence.meta_dir/0`, fsync'd after each update
    (`:dets.sync/1`). DETS (rather than ETS) because a crash between
    an ETS write and its periodic snapshot would lose the record we
    need to drive recovery.
  - One record per in-flight write, keyed by `write_id`:
    `%{write_id, volume_id, path, chunk_hashes (list), started_at}`.
  - Recovery pass (see `list_orphans/1`) returns records older than
    a grace window so stale writes can be aborted without racing
    recent in-flight ones on a just-started node.

  ## Lifecycle

      :ok = PendingWriteLog.open()  # app startup
      :ok = PendingWriteLog.open_write(write_id, volume_id, path)
      :ok = PendingWriteLog.record_chunk(write_id, chunk_hash)  # per chunk
      :ok = PendingWriteLog.clear(write_id)  # on commit or abort
      PendingWriteLog.list_orphans(300)  # startup recovery scan
  """

  alias NeonFS.Core.Persistence

  @table :pending_writes

  @typedoc "Per-write record stored in DETS."
  @type record :: %{
          write_id: binary(),
          volume_id: binary(),
          path: String.t(),
          chunk_hashes: [binary()],
          started_at: DateTime.t()
        }

  @doc """
  Open the DETS backing file. Called once at application startup.
  Idempotent — repeated calls reuse the already-open table.
  """
  @spec open(keyword()) :: :ok | {:error, term()}
  def open(opts \\ []) do
    meta_dir = Keyword.get(opts, :meta_dir, Persistence.meta_dir())
    File.mkdir_p!(meta_dir)
    dets_path = Path.join(meta_dir, "pending_writes.dets")

    case :dets.open_file(@table, file: to_charlist(dets_path), type: :set) do
      {:ok, @table} -> :ok
      {:error, _} = err -> err
    end
  end

  @doc """
  Close the DETS backing file. Called during orderly shutdown.
  """
  @spec close() :: :ok
  def close do
    case :dets.info(@table) do
      :undefined -> :ok
      _ -> :dets.close(@table)
    end
  end

  @doc """
  Record the start of an in-flight streaming write. Creates an empty
  record that `record_chunk/2` will extend as chunks are stored.

  If the log isn't open (e.g. tests that don't start
  `NeonFS.Core.PendingWriteRecovery`), this is a no-op — callers
  treat the log as best-effort. Orphan cleanup then falls back to
  the existing in-process `abort_chunks/1` path.
  """
  @spec open_write(binary(), binary(), String.t()) :: :ok | {:error, term()}
  def open_write(write_id, volume_id, path) do
    if_open(fn ->
      record = %{
        write_id: write_id,
        volume_id: volume_id,
        path: path,
        chunk_hashes: [],
        started_at: DateTime.utc_now()
      }

      with :ok <- :dets.insert(@table, {write_id, record}) do
        :dets.sync(@table)
      end
    end)
  end

  @doc """
  Append `chunk_hash` to the in-flight record for `write_id`. Called
  after each successful `put_chunk` in the streaming write path.

  If no open record exists (e.g. log was truncated mid-write, or the
  DETS table isn't open), this is a no-op.
  """
  @spec record_chunk(binary(), binary()) :: :ok | {:error, term()}
  def record_chunk(write_id, chunk_hash) do
    if_open(fn -> do_record_chunk(write_id, chunk_hash) end)
  end

  defp do_record_chunk(write_id, chunk_hash) do
    case :dets.lookup(@table, write_id) do
      [{^write_id, record}] -> append_chunk_hash(write_id, record, chunk_hash)
      [] -> :ok
    end
  end

  defp append_chunk_hash(write_id, record, chunk_hash) do
    updated = %{record | chunk_hashes: [chunk_hash | record.chunk_hashes]}

    with :ok <- :dets.insert(@table, {write_id, updated}) do
      :dets.sync(@table)
    end
  end

  @doc """
  Remove the record for `write_id`. Called on both `commit_chunks`
  (successful finalisation) and `abort_chunks` (in-process rollback).
  """
  @spec clear(binary()) :: :ok | {:error, term()}
  def clear(write_id) do
    if_open(fn ->
      with :ok <- :dets.delete(@table, write_id) do
        :dets.sync(@table)
      end
    end)
  end

  defp if_open(fun) do
    case :dets.info(@table) do
      :undefined -> :ok
      _ -> fun.()
    end
  end

  @doc """
  Fetch a single record for inspection. Primarily a test helper.
  """
  @spec get(binary()) :: {:ok, record()} | {:error, :not_found}
  def get(write_id) do
    case :dets.lookup(@table, write_id) do
      [{^write_id, record}] -> {:ok, record}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Return all records older than `grace_seconds`. The startup recovery
  pass uses this to find orphaned writes whose chunks should be
  cleaned up.
  """
  @spec list_orphans(non_neg_integer()) :: [record()]
  def list_orphans(grace_seconds) when grace_seconds >= 0 do
    cutoff = DateTime.utc_now() |> DateTime.add(-grace_seconds, :second)

    :dets.foldl(
      fn {_write_id, record}, acc ->
        if DateTime.compare(record.started_at, cutoff) == :lt do
          [record | acc]
        else
          acc
        end
      end,
      [],
      @table
    )
  end

  @doc """
  Return every record regardless of age. Primarily a test helper.
  """
  @spec list_all() :: [record()]
  def list_all do
    :dets.foldl(fn {_id, record}, acc -> [record | acc] end, [], @table)
  end
end
