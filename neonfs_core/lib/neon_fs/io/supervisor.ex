defmodule NeonFS.IO.Supervisor do
  @moduledoc """
  Top-level supervisor for the I/O scheduler subsystem.

  Manages the GenStage producer, priority adjuster, and a DynamicSupervisor
  for drive workers. On startup, queries `DriveRegistry` for all known local
  drives and starts a worker for each. Subscribes to drive lifecycle telemetry
  events to dynamically add/remove workers at runtime.

  ## Supervision Tree

      NeonFS.IO.Supervisor (:one_for_one)
      ├── NeonFS.IO.Producer (GenStage producer)
      ├── NeonFS.IO.PriorityAdjuster (GenServer)
      ├── NeonFS.IO.WorkerSupervisor (DynamicSupervisor)
      │   ├── DriveWorker (drive_a)
      │   ├── DriveWorker (drive_b)
      │   └── ...
      └── NeonFS.IO.Supervisor.DriveDiscovery (GenServer)

  ## Restart Behaviour

  Using `:one_for_one` means a worker crash only restarts that worker.
  If the producer crashes, workers lose their subscription but GenStage
  re-subscribes automatically via the `:subscribe_to` option in worker init.
  """

  use Supervisor
  require Logger

  alias NeonFS.IO.{PriorityAdjuster, Producer, WorkerSupervisor}

  @doc """
  Starts the I/O supervisor.

  ## Options

    * `:name` — supervisor name (default: `__MODULE__`)
    * `:drive_registry_mod` — module for querying drives (default: `NeonFS.Core.DriveRegistry`)
    * `:producer_opts` — options passed to the producer
    * `:adjuster_opts` — options passed to the priority adjuster
    * `:worker_supervisor_opts` — options passed to the worker supervisor
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    producer_opts = Keyword.get(opts, :producer_opts, [])
    adjuster_opts = Keyword.get(opts, :adjuster_opts, [])
    worker_supervisor_opts = Keyword.get(opts, :worker_supervisor_opts, [])
    drive_registry_mod = Keyword.get(opts, :drive_registry_mod, NeonFS.Core.DriveRegistry)

    producer_name = Keyword.get(producer_opts, :name, Producer)
    worker_sup_name = Keyword.get(worker_supervisor_opts, :name, WorkerSupervisor)

    # Attach telemetry handlers with resolved names so they route to the
    # correct WorkerSupervisor and Producer instances.
    handler_config = %{producer: producer_name, worker_supervisor: worker_sup_name}
    attach_drive_telemetry(handler_config)

    # DriveDiscovery is started last — all siblings (Producer, PriorityAdjuster,
    # WorkerSupervisor) are guaranteed to be running by the time it initialises,
    # so no sleep is needed. Discovery runs in handle_continue/2.
    discovery_opts = [
      drive_registry_mod: drive_registry_mod,
      handler_config: handler_config
    ]

    children = [
      {Producer, producer_opts},
      {PriorityAdjuster, adjuster_opts},
      {WorkerSupervisor, worker_supervisor_opts},
      {__MODULE__.DriveDiscovery, discovery_opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc false
  @spec start_worker_for_drive(String.t(), :ssd | :hdd, map()) :: :ok | :error
  def start_worker_for_drive(drive_id, drive_type, config) do
    case WorkerSupervisor.lookup_worker(drive_id) do
      {:ok, _pid} ->
        Logger.debug("I/O worker already running", drive_id: drive_id)
        :ok

      :error ->
        opts = [
          drive_id: drive_id,
          drive_type: drive_type,
          producer: config.producer,
          supervisor: config.worker_supervisor
        ]

        case WorkerSupervisor.start_worker(opts) do
          {:ok, _pid} ->
            :telemetry.execute(
              [:neonfs, :io, :supervisor, :worker_started],
              %{},
              %{drive_id: drive_id, drive_type: drive_type}
            )

            :ok

          {:error, reason} ->
            Logger.warning("Failed to start I/O worker",
              drive_id: drive_id,
              reason: inspect(reason)
            )

            :error
        end
    end
  end

  ## Private

  defp attach_drive_telemetry(config) do
    # Use a unique handler ID so multiple supervisor instances (in tests)
    # don't clobber each other's handlers.
    suffix = System.unique_integer([:positive])

    :telemetry.attach(
      "neonfs-io-drive-add-#{suffix}",
      [:neonfs, :drive_manager, :add],
      &handle_drive_add/4,
      config
    )

    :telemetry.attach(
      "neonfs-io-drive-remove-#{suffix}",
      [:neonfs, :drive_manager, :remove],
      &handle_drive_remove/4,
      config
    )
  end

  defp handle_drive_add(_event, _measurements, %{drive_id: drive_id, tier: tier}, config) do
    drive_type = tier_to_drive_type(tier)
    start_worker_for_drive(drive_id, drive_type, config)
  end

  defp handle_drive_remove(_event, _measurements, %{drive_id: drive_id}, config) do
    case WorkerSupervisor.stop_worker(drive_id, supervisor: config.worker_supervisor) do
      :ok ->
        :telemetry.execute(
          [:neonfs, :io, :supervisor, :worker_stopped],
          %{},
          %{drive_id: drive_id}
        )

      {:error, :not_found} ->
        :ok
    end
  end

  defp tier_to_drive_type(:cold), do: :hdd
  defp tier_to_drive_type(_tier), do: :ssd

  defmodule DriveDiscovery do
    @moduledoc false
    use GenServer, restart: :temporary
    require Logger

    @spec start_link(keyword()) :: GenServer.on_start()
    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      state = %{
        drive_registry_mod: Keyword.fetch!(opts, :drive_registry_mod),
        handler_config: Keyword.fetch!(opts, :handler_config)
      }

      {:ok, state, {:continue, :discover_drives}}
    end

    @impl true
    def handle_continue(:discover_drives, state) do
      drives =
        try do
          state.drive_registry_mod.list_drives()
        rescue
          _ -> []
        catch
          :exit, _ -> []
        end

      local_drives = Enum.filter(drives, &(&1.node == Node.self()))

      Enum.each(local_drives, fn drive ->
        drive_type = if drive.tier == :cold, do: :hdd, else: :ssd
        NeonFS.IO.Supervisor.start_worker_for_drive(drive.id, drive_type, state.handler_config)
      end)

      :telemetry.execute(
        [:neonfs, :io, :supervisor, :discovery_complete],
        %{count: length(local_drives)},
        %{}
      )

      Logger.info("I/O scheduler initial drive discovery complete", count: length(local_drives))

      {:stop, :normal, state}
    end
  end
end
