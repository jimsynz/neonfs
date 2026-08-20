defmodule NeonFS.Core.PendingWriteRecovery do
  @moduledoc """
  Boot-time sweep that reclaims chunks orphaned by interrupted
  streaming writes.

  Supervised alongside the other core GenServers. On `init/1` it:

    1. Opens `NeonFS.Core.PendingWriteLog` (creating the DETS file if
       needed).
    2. Calls `PendingWriteLog.list_orphans/1` with the configured
       grace window to find writes that started more than
       `grace_seconds` ago and never committed or aborted.
    3. For each orphan, calls
       `NeonFS.Core.WriteOperation.reclaim_orphaned_chunks/3` with the
       hashes the record accumulated, then `clear/1`s the record.

  The reclamation goes by hash because it has to. `abort_chunks/1` — the
  in-process rollback — selects chunks by write-ref membership, and
  `active_write_refs` live only in local ETS and are never persisted, so after
  a restart every chunk re-warmed from the quorum store carries an empty ref
  set and matches no write id. A sweep driven that way deletes nothing, which
  is what this process used to do.

  A chunk a committed file references is not reclaimed: cluster-truth
  `commit_state` is the authority, the same rule the in-process abort applies.

  The grace window avoids racing a write that just started on a
  freshly-booted node. Default is 300 seconds (5 minutes); override
  per-process via `start_link(grace_seconds: N)` or globally via
  `config :neonfs_core, pending_write_grace_seconds: N`.

  After the initial sweep the process stays up as a passive owner of
  the DETS table and closes it in `terminate/2`.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{PendingWriteLog, WriteOperation}

  @default_grace_seconds 300

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    grace =
      Keyword.get(opts, :grace_seconds) ||
        Application.get_env(:neonfs_core, :pending_write_grace_seconds, @default_grace_seconds)

    case PendingWriteLog.open() do
      :ok ->
        {:ok, %{grace_seconds: grace}, {:continue, :recover}}

      {:error, reason} ->
        Logger.error("Could not open pending-write log", reason: inspect(reason))
        # Continue running — future writes can't be tracked, but the
        # existing GC pass still catches orphans eventually.
        {:ok, %{grace_seconds: grace}}
    end
  end

  @impl true
  def handle_continue(:recover, state) do
    recover_orphans(state.grace_seconds)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    PendingWriteLog.close()
    :ok
  rescue
    _ -> :ok
  end

  @doc """
  Trigger a recovery sweep on demand. Primarily a test hook — the
  supervised init already runs one pass at boot.
  """
  @spec sweep(non_neg_integer()) :: :ok
  def sweep(grace_seconds \\ @default_grace_seconds) do
    recover_orphans(grace_seconds)
  end

  defp recover_orphans(grace_seconds) do
    grace_seconds
    |> PendingWriteLog.list_orphans()
    |> Enum.each(&recover_orphan/1)

    :ok
  end

  # `chunks` counts deletions, not hashes in the record. The two differ when a
  # hash was already reclaimed, was committed by a concurrent write, or could
  # not be read — and reporting the record's length instead is how this process
  # claimed to reclaim chunks it had not touched.
  defp recover_orphan(record) do
    named = length(record.chunk_hashes)

    reclaimed =
      WriteOperation.reclaim_orphaned_chunks(
        record.volume_id,
        record.chunk_hashes,
        record.write_id
      )

    PendingWriteLog.clear(record.write_id)

    :telemetry.execute(
      [:neonfs, :write_operation, :orphan_recovered],
      %{chunks: reclaimed, chunks_named: named},
      %{write_id: record.write_id, volume_id: record.volume_id, path: record.path}
    )

    Logger.info("Reclaimed orphaned streaming write",
      write_id: record.write_id,
      volume_id: record.volume_id,
      file_path: record.path,
      chunks_reclaimed: reclaimed,
      chunks_named: named
    )
  end
end
