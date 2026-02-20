defmodule NeonFS.Core.Persistence do
  @moduledoc """
  Coordinates metadata persistence for NeonFS Core configuration tables.

  Provides DETS-backed persistence with atomic write-then-move semantics to
  prevent corruption during shutdown. Periodically snapshots ETS tables to
  disk and restores them on startup.

  ## Persistence Strategy

  - **Startup**: Load DETS -> ETS for configuration tables
  - **Runtime**: Periodic snapshots every N seconds (default: 30s)
  - **Shutdown**: Immediate snapshot before termination
  - **Atomic Writes**: Write to `.tmp` file, sync, then rename

  ## Persisted Tables

  - VolumeRegistry: `:volumes_by_id`, `:volumes_by_name` -> `volume_registry_*.dets`

  ## Metadata Tables (NOT persisted here)

  ChunkIndex, FileIndex, and StripeIndex are now backed by the leaderless
  quorum-replicated BlobStore (Phase 5). They load from local BlobStore
  on startup via `load_from_local_store/0` — DETS snapshots are no longer
  needed for these tables.
  """

  use GenServer
  require Logger

  @type table_config :: %{
          ets_table: atom(),
          dets_path: String.t()
        }

  @default_meta_dir "/var/lib/neonfs/meta"
  @default_snapshot_interval_ms 30_000

  # Client API

  @doc """
  Starts the persistence GenServer.

  ## Options

  - `:meta_dir` - Directory for DETS files (default: #{@default_meta_dir})
  - `:snapshot_interval_ms` - Snapshot interval in milliseconds (default: #{@default_snapshot_interval_ms})
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Triggers an immediate snapshot of all metadata tables.
  """
  @spec snapshot_now() :: :ok | {:error, term()}
  def snapshot_now do
    GenServer.call(__MODULE__, :snapshot_now, :infinity)
  end

  @doc """
  Snapshots a single ETS table to a DETS file.

  This is a direct function (not a GenServer call) so it can be called
  from other GenServers' terminate callbacks even after Persistence
  has terminated.

  Uses atomic write-then-rename to prevent corruption.
  """
  @spec snapshot_table(atom(), String.t()) :: :ok | {:error, term()}
  def snapshot_table(ets_table, dets_path) do
    # Ensure parent directory exists
    dets_path |> Path.dirname() |> File.mkdir_p!()
    do_atomic_snapshot(ets_table, dets_path)
  end

  @doc """
  Returns the configured metadata directory.
  """
  @spec meta_dir() :: String.t()
  def meta_dir do
    Application.get_env(:neonfs_core, :meta_dir, @default_meta_dir)
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    # Trap exits so terminate/2 is called during supervisor shutdown
    Process.flag(:trap_exit, true)

    meta_dir = Keyword.get(opts, :meta_dir, @default_meta_dir)
    snapshot_interval_ms = Keyword.get(opts, :snapshot_interval_ms, @default_snapshot_interval_ms)

    # Ensure metadata directory exists
    File.mkdir_p!(meta_dir)

    # Clean up any leftover .tmp files from previous failed snapshots
    cleanup_temp_files(meta_dir)

    # Define table configurations
    # Only VolumeRegistry tables are persisted via DETS.
    # ChunkIndex, FileIndex, StripeIndex load from BlobStore on startup (Phase 5).
    tables = [
      %{ets_table: :volumes_by_id, dets_path: Path.join(meta_dir, "volume_registry_by_id.dets")},
      %{
        ets_table: :volumes_by_name,
        dets_path: Path.join(meta_dir, "volume_registry_by_name.dets")
      },
      %{ets_table: :volume_acls, dets_path: Path.join(meta_dir, "volume_acls.dets")},
      %{ets_table: :audit_log, dets_path: Path.join(meta_dir, "audit_log.dets")},
      %{ets_table: :neonfs_jobs, dets_path: Path.join(meta_dir, "jobs.dets")}
    ]

    state = %{tables: tables, snapshot_interval_ms: snapshot_interval_ms}

    # Use handle_continue to restore tables after other modules have started
    {:ok, state, {:continue, :restore_tables}}
  end

  @impl true
  def handle_continue(:restore_tables, state) do
    # Restore all tables from DETS (if files exist)
    # This happens after init returns, giving other modules time to create their ETS tables
    Enum.each(state.tables, &restore_table/1)

    # Schedule periodic snapshots
    schedule_snapshot(state.snapshot_interval_ms)

    {:noreply, state}
  end

  @impl true
  def handle_call(:snapshot_now, _from, state) do
    result = snapshot_all_tables(state.tables)
    {:reply, result, state}
  end

  @impl true
  def handle_info(:snapshot, state) do
    case snapshot_all_tables(state.tables) do
      :ok ->
        Logger.debug("Periodic snapshot completed successfully")

      {:error, reason} ->
        Logger.error("Periodic snapshot failed: #{inspect(reason)}")
    end

    # Schedule next snapshot
    schedule_snapshot(state.snapshot_interval_ms)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    Logger.info("Persistence shutting down, snapshotting all tables...")

    case snapshot_all_tables(state.tables) do
      :ok ->
        Logger.info("Graceful shutdown snapshot completed")

      {:error, reason} ->
        Logger.error("Shutdown snapshot failed: #{inspect(reason)}")
    end

    :ok
  end

  # Private Functions

  @spec restore_table(table_config()) :: :ok
  defp restore_table(%{ets_table: ets_table, dets_path: dets_path}) do
    if File.exists?(dets_path) do
      do_restore_from_dets(ets_table, dets_path)
    else
      Logger.debug("No DETS file found at #{dets_path}, starting fresh")
      :ok
    end
  end

  @spec do_restore_from_dets(atom(), String.t()) :: :ok
  defp do_restore_from_dets(ets_table, dets_path) do
    case :dets.open_file(ets_table, type: :set, file: String.to_charlist(dets_path)) do
      {:ok, dets_ref} ->
        restore_from_open_dets(dets_ref, ets_table, dets_path)

      {:error, reason} ->
        Logger.error("Failed to open DETS file #{dets_path}: #{inspect(reason)}")
        :ok
    end
  end

  @spec restore_from_open_dets(reference(), atom(), String.t()) :: :ok
  defp restore_from_open_dets(dets_ref, ets_table, dets_path) do
    # Wait for ETS table to be created by the owning GenServer
    wait_for_ets_table(ets_table)

    # Load DETS -> ETS
    case :dets.to_ets(dets_ref, ets_table) do
      ^ets_table ->
        count = :ets.info(ets_table, :size)
        Logger.info("Restored #{count} entries to #{ets_table} from #{dets_path}")

      {:error, reason} ->
        Logger.error("Failed to restore #{ets_table}: #{inspect(reason)}")
    end

    :dets.close(dets_ref)
    :ok
  end

  @spec wait_for_ets_table(atom(), non_neg_integer()) :: :ok
  defp wait_for_ets_table(table_name, max_retries \\ 50) do
    case :ets.whereis(table_name) do
      :undefined ->
        if max_retries > 0 do
          Process.sleep(100)
          wait_for_ets_table(table_name, max_retries - 1)
        else
          Logger.warning("ETS table #{table_name} not found after waiting, skipping restore")
          :ok
        end

      _ref ->
        :ok
    end
  end

  @spec snapshot_all_tables([table_config()]) :: :ok | {:error, term()}
  defp snapshot_all_tables(tables) do
    results =
      Enum.map(tables, fn table_config ->
        atomic_snapshot(table_config.ets_table, table_config.dets_path)
      end)

    case Enum.find(results, fn result -> match?({:error, _}, result) end) do
      nil -> :ok
      error -> error
    end
  end

  @spec atomic_snapshot(atom(), String.t()) :: :ok | {:error, term()}
  defp atomic_snapshot(ets_table, dets_path) do
    case :ets.whereis(ets_table) do
      :undefined ->
        Logger.debug("ETS table #{ets_table} not found, skipping snapshot")
        :ok

      _ref ->
        do_atomic_snapshot(ets_table, dets_path)
    end
  end

  @spec do_atomic_snapshot(atom(), String.t()) :: :ok | {:error, term()}
  defp do_atomic_snapshot(ets_table, dets_path) do
    temp_path = "#{dets_path}.tmp"

    # Delete any existing temp file from a previous failed snapshot
    File.rm(temp_path)

    # Use a unique DETS reference name based on the ETS table to avoid conflicts
    # when multiple snapshots run concurrently or from different processes
    dets_ref_name = :"temp_snapshot_#{ets_table}"

    case :dets.open_file(dets_ref_name, type: :set, file: String.to_charlist(temp_path)) do
      {:ok, dets_ref} ->
        copy_and_rename(ets_table, dets_ref, temp_path, dets_path)

      {:error, reason} ->
        Logger.error("Failed to open DETS file #{temp_path}: #{inspect(reason)}")
        {:error, {:open_failed, reason}}
    end
  end

  @spec copy_and_rename(atom(), reference(), String.t(), String.t()) :: :ok | {:error, term()}
  defp copy_and_rename(ets_table, dets_ref, temp_path, dets_path) do
    case :ets.to_dets(ets_table, dets_ref) do
      ^dets_ref ->
        finalize_snapshot(dets_ref, temp_path, dets_path)

      {:error, reason} ->
        :dets.close(dets_ref)
        File.rm(temp_path)
        Logger.error("Failed to copy #{ets_table} to DETS: #{inspect(reason)}")
        {:error, {:to_dets_failed, reason}}
    end
  end

  @spec finalize_snapshot(reference(), String.t(), String.t()) :: :ok | {:error, term()}
  defp finalize_snapshot(dets_ref, temp_path, dets_path) do
    :dets.sync(dets_ref)
    :dets.close(dets_ref)

    case File.rename(temp_path, dets_path) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("Failed to rename #{temp_path} to #{dets_path}: #{inspect(reason)}")
        # Clean up the temp file since rename failed
        File.rm(temp_path)
        {:error, {:rename_failed, reason}}
    end
  end

  @spec schedule_snapshot(non_neg_integer()) :: reference()
  defp schedule_snapshot(interval_ms) do
    Process.send_after(self(), :snapshot, interval_ms)
  end

  @spec cleanup_temp_files(String.t()) :: :ok
  defp cleanup_temp_files(meta_dir) do
    # Find and remove all .tmp files from previous failed snapshots
    case File.ls(meta_dir) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.ends_with?(&1, ".tmp"))
        |> Enum.each(fn tmp_file ->
          tmp_path = Path.join(meta_dir, tmp_file)
          File.rm(tmp_path)
          Logger.info("Cleaned up leftover temp file: #{tmp_path}")
        end)

      {:error, :enoent} ->
        # Directory doesn't exist yet, nothing to clean up
        :ok

      {:error, reason} ->
        Logger.warning("Failed to list directory #{meta_dir} for cleanup: #{inspect(reason)}")
        :ok
    end
  end
end
