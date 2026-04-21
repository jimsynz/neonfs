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
       `NeonFS.Core.WriteOperation.abort_chunks/1` to remove the
       chunks and their `ChunkIndex` entries, then `clear/1`s the
       record.

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
    orphans = PendingWriteLog.list_orphans(grace_seconds)

    Enum.each(orphans, fn record ->
      WriteOperation.abort_chunks(record.write_id)
      PendingWriteLog.clear(record.write_id)

      :telemetry.execute(
        [:neonfs, :write_operation, :orphan_recovered],
        %{chunks: length(record.chunk_hashes)},
        %{write_id: record.write_id, volume_id: record.volume_id, path: record.path}
      )

      Logger.info("Reclaimed orphaned streaming write",
        write_id: record.write_id,
        volume_id: record.volume_id,
        file_path: record.path
      )
    end)

    :ok
  end
end
