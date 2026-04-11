defmodule NeonFS.Core.LockManager.Supervisor do
  @moduledoc """
  DynamicSupervisor for per-file lock manager processes.

  Each file with active locks gets its own `FileLock` GenServer, started on
  demand via `ensure_file_lock/1`. Processes exit automatically when all locks,
  opens, and leases are released.

  The supervisor is started as part of the core supervision tree.
  """

  use DynamicSupervisor

  alias NeonFS.Core.LockManager.FileLock

  @doc """
  Starts the lock manager supervisor.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Returns the pid of the `FileLock` for the given file, starting one if needed.
  """
  @spec ensure_file_lock(binary()) :: {:ok, pid()}
  def ensure_file_lock(file_id) do
    case Registry.lookup(NeonFS.Core.LockManager.Registry, file_id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        start_file_lock(file_id)
    end
  end

  defp start_file_lock(file_id) do
    spec = {FileLock, file_id: file_id}

    case DynamicSupervisor.start_child(__MODULE__, spec) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
