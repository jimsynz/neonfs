defmodule NeonFS.IO.WorkerSupervisor do
  @moduledoc """
  DynamicSupervisor for I/O drive workers.

  Each physical drive gets its own `NeonFS.IO.DriveWorker` process, started
  dynamically as drives are discovered. Workers are registered in
  `NeonFS.IO.WorkerRegistry` by drive ID for lookup.
  """

  use DynamicSupervisor

  alias NeonFS.IO.DriveWorker

  @registry NeonFS.IO.WorkerRegistry

  @doc """
  Starts the worker supervisor.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    DynamicSupervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Starts a drive worker under this supervisor.

  ## Options

    * `:drive_id` (required) — unique identifier for the drive
    * `:drive_type` — `:hdd` or `:ssd` (default: `:ssd`)
    * `:producer` — producer to subscribe to (default: `NeonFS.IO.Producer`)
    * `:strategy_config` — keyword options passed to the drive strategy
    * `:supervisor` — the supervisor to start under (default: `__MODULE__`)
  """
  @spec start_worker(keyword()) :: DynamicSupervisor.on_start_child()
  def start_worker(opts) do
    drive_id = Keyword.fetch!(opts, :drive_id)
    supervisor = Keyword.get(opts, :supervisor, __MODULE__)

    worker_opts =
      opts
      |> Keyword.delete(:supervisor)
      |> Keyword.put(:name, via_tuple(drive_id))

    child_spec = {DriveWorker, worker_opts}
    DynamicSupervisor.start_child(supervisor, child_spec)
  end

  @doc """
  Stops a drive worker by drive ID.

  Returns `:ok` if the worker was found and terminated, or
  `{:error, :not_found}` if no worker exists for the given drive.
  """
  @spec stop_worker(String.t(), keyword()) :: :ok | {:error, :not_found}
  def stop_worker(drive_id, opts \\ []) do
    supervisor = Keyword.get(opts, :supervisor, __MODULE__)

    case lookup_worker(drive_id) do
      {:ok, pid} ->
        DynamicSupervisor.terminate_child(supervisor, pid)

      :error ->
        {:error, :not_found}
    end
  end

  @doc """
  Looks up a drive worker PID by drive ID.
  """
  @spec lookup_worker(String.t()) :: {:ok, pid()} | :error
  def lookup_worker(drive_id) do
    case Registry.lookup(@registry, drive_id) do
      [{pid, _}] -> {:ok, pid}
      [] -> :error
    end
  end

  @doc """
  Returns the via tuple for registering a worker by drive ID.
  """
  @spec via_tuple(String.t()) :: {:via, module(), {module(), String.t()}}
  def via_tuple(drive_id) do
    {:via, Registry, {@registry, drive_id}}
  end
end
