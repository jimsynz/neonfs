defmodule NeonFS.Core.DriveManager do
  @moduledoc """
  Orchestrates runtime drive lifecycle: add, remove, and list drives.

  DriveManager coordinates between BlobStore (NIF handles), DriveRegistry (cluster-wide
  drive tracking), DriveState (power management), and cluster.json (persistent config).

  Drives can be added and removed at runtime via the CLI without restarting the node.
  All changes are persisted to cluster.json for recovery on restart.

  ## Telemetry Events

    * `[:neonfs, :drive_manager, :add]` — emitted when a drive is added
    * `[:neonfs, :drive_manager, :remove]` — emitted when a drive is removed
  """

  use GenServer
  require Logger

  alias NeonFS.Cluster.State
  alias NeonFS.Core.{BlobStore, Drive, DriveConfig, DriveRegistry, DriveState}

  @valid_tiers [:hot, :warm, :cold]
  @drive_state_supervisor NeonFS.Core.DriveStateSupervisor

  ## Client API

  @doc """
  Starts the DriveManager GenServer.

  ## Options

    * `:name` - GenServer name (default: `__MODULE__`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Adds a new drive at runtime.

  Validates the config, opens a BlobStore handle, registers in DriveRegistry,
  starts a DriveState child, and persists to cluster.json.

  ## Parameters

    * `config` - Drive configuration map with keys:
      * `:path` (required) - Absolute path to the storage directory
      * `:tier` (required) - Storage tier: `:hot`, `:warm`, or `:cold`
      * `:capacity` (optional) - Capacity string (e.g. "1T", "500G") or integer bytes (default: 0)
      * `:id` (optional) - Unique drive ID (auto-generated from path if not provided)

  ## Returns

    * `{:ok, drive_map}` - The registered drive as a serialisable map
    * `{:error, reason}` - Validation or registration failure
  """
  @spec add_drive(map()) :: {:ok, map()} | {:error, term()}
  def add_drive(config) when is_map(config) do
    GenServer.call(__MODULE__, {:add_drive, config})
  end

  @doc """
  Removes a drive at runtime.

  Checks for data on the drive. If data exists and `:force` is not set,
  returns `{:error, :drive_has_data}`. Otherwise closes the BlobStore handle,
  deregisters from DriveRegistry, stops DriveState, and persists to cluster.json.

  ## Parameters

    * `drive_id` - Drive identifier to remove
    * `opts` - Optional keyword list:
      * `:force` - Skip data check (default: `false`)

  ## Returns

    * `:ok` - Drive removed successfully
    * `{:error, reason}` - Failure reason
  """
  @spec remove_drive(String.t(), keyword()) :: :ok | {:error, term()}
  def remove_drive(drive_id, opts \\ []) do
    GenServer.call(__MODULE__, {:remove_drive, drive_id, opts})
  end

  @doc """
  Lists all local drives as serialisable maps.
  """
  @spec list_drives() :: [map()]
  def list_drives do
    GenServer.call(__MODULE__, :list_drives)
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    drives = Application.get_env(:neonfs_core, :drives, [])

    command_module =
      Application.get_env(:neonfs_core, :drive_command_module, DriveCommand.Default)

    start_drive_state_children(drives, command_module)

    {:ok, %{command_module: command_module}}
  end

  @impl true
  def handle_call({:add_drive, config}, _from, state) do
    case do_add_drive(config, state.command_module) do
      {:ok, drive_map} ->
        :telemetry.execute(
          [:neonfs, :drive_manager, :add],
          %{},
          %{drive_id: drive_map.id, tier: drive_map.tier}
        )

        {:reply, {:ok, drive_map}, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:remove_drive, drive_id, opts}, _from, state) do
    case do_remove_drive(drive_id, opts) do
      :ok ->
        :telemetry.execute(
          [:neonfs, :drive_manager, :remove],
          %{},
          %{drive_id: drive_id}
        )

        {:reply, :ok, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(:list_drives, _from, state) do
    drives =
      DriveRegistry.drives_for_node(Node.self())
      |> Enum.map(&drive_to_map/1)

    {:reply, drives, state}
  end

  ## Private — Add drive

  defp do_add_drive(config, command_module) do
    with {:ok, parsed} <- validate_drive_config(config),
         :ok <- check_path_exists(parsed.path),
         :ok <- check_id_unique(parsed.id),
         {:ok, _drive_id} <- BlobStore.open_store(parsed),
         drive = Drive.from_config(parsed, Node.self()),
         :ok <- DriveRegistry.register_drive(drive),
         :ok <- start_single_drive_state(parsed, command_module),
         :ok <- validate_capacity(drive),
         :ok <- save_drives_to_cluster_state() do
      {:ok, drive_to_map(drive)}
    end
  end

  defp validate_drive_config(config) do
    path = to_string(config[:path] || config["path"] || "")
    tier_raw = config[:tier] || config["tier"] || "hot"
    capacity_raw = config[:capacity] || config["capacity"] || "0"
    id = to_string(config[:id] || config["id"] || generate_drive_id(path))

    with {:ok, tier} <- parse_tier(tier_raw),
         {:ok, capacity} <- parse_capacity(capacity_raw) do
      {:ok, %{id: id, path: path, tier: tier, capacity: capacity}}
    end
  end

  defp parse_tier(tier) when tier in @valid_tiers, do: {:ok, tier}

  defp parse_tier(tier) when is_binary(tier) and tier in ["hot", "warm", "cold"],
    do: {:ok, String.to_existing_atom(tier)}

  defp parse_tier(_), do: {:error, "Invalid tier. Must be hot, warm, or cold"}

  defp parse_capacity(capacity) when is_integer(capacity) and capacity >= 0, do: {:ok, capacity}

  defp parse_capacity(capacity) when is_binary(capacity) do
    DriveConfig.parse_capacity(capacity)
  end

  defp parse_capacity(_), do: {:ok, 0}

  defp check_path_exists(path) when byte_size(path) == 0, do: {:error, "Path is required"}

  defp check_path_exists(path) do
    case File.stat(path) do
      {:ok, %{type: :directory}} -> :ok
      {:ok, _} -> {:error, "Path exists but is not a directory: #{path}"}
      {:error, :enoent} -> {:error, "Path does not exist: #{path}"}
      {:error, reason} -> {:error, "Cannot access path #{path}: #{reason}"}
    end
  end

  defp check_id_unique(drive_id) do
    case DriveRegistry.get_drive(Node.self(), drive_id) do
      {:ok, _} -> {:error, {:duplicate_drive_id, drive_id}}
      {:error, :not_found} -> :ok
    end
  end

  defp validate_capacity(drive) do
    DriveConfig.validate_drives([drive])
    :ok
  end

  defp generate_drive_id(path) do
    path
    |> Path.basename()
    |> String.replace(~r/[^a-zA-Z0-9_-]/, "_")
  end

  ## Private — Remove drive

  defp do_remove_drive(drive_id, opts) do
    force = Keyword.get(opts, :force, false)

    with {:ok, _drive} <- get_local_drive(drive_id),
         :ok <- check_drive_data(drive_id, force),
         :ok <- BlobStore.close_store(drive_id),
         :ok <- DriveRegistry.deregister_drive(drive_id),
         :ok <- stop_drive_state(drive_id) do
      save_drives_to_cluster_state()
    end
  end

  defp get_local_drive(drive_id) do
    case DriveRegistry.get_drive(Node.self(), drive_id) do
      {:ok, drive} -> {:ok, drive}
      {:error, :not_found} -> {:error, {:unknown_drive, drive_id}}
    end
  end

  defp check_drive_data(_drive_id, true = _force), do: :ok

  defp check_drive_data(drive_id, _force) do
    case BlobStore.drive_has_data?(drive_id) do
      {:ok, false} -> :ok
      {:ok, true} -> {:error, :drive_has_data}
      {:error, reason} -> {:error, reason}
    end
  end

  defp stop_drive_state(drive_id) do
    via = DriveState.via_tuple(drive_id)

    case GenServer.whereis(via) do
      nil ->
        :ok

      pid ->
        DynamicSupervisor.terminate_child(@drive_state_supervisor, pid)
    end
  end

  ## Private — Cluster state persistence

  defp save_drives_to_cluster_state do
    drives =
      DriveRegistry.drives_for_node(Node.self())
      |> Enum.map(fn drive ->
        %{
          "id" => drive.id,
          "path" => drive.path,
          "tier" => Atom.to_string(drive.tier),
          "capacity" => to_string(drive.capacity_bytes)
        }
      end)

    case State.update_drives(drives) do
      :ok ->
        :ok

      {:error, :not_found} ->
        # cluster.json doesn't exist yet — this is fine for fresh installs
        Logger.debug("Skipping drive persistence: cluster.json not found")
        :ok

      {:error, reason} ->
        Logger.warning("Failed to persist drives to cluster.json: #{inspect(reason)}")
        :ok
    end
  end

  ## Private — DriveState management

  defp start_drive_state_children(drives, command_module) do
    Enum.each(drives, fn config ->
      start_single_drive_state(config, command_module)
    end)
  end

  defp start_single_drive_state(config, command_module) do
    drive_state_opts = drive_state_opts_from_config(config, command_module)
    drive_id = Keyword.fetch!(drive_state_opts, :drive_id)

    child_spec = %{
      id: {NeonFS.Core.DriveState, drive_id},
      start: {NeonFS.Core.DriveState, :start_link, [drive_state_opts]},
      restart: :permanent
    }

    case DynamicSupervisor.start_child(@drive_state_supervisor, child_spec) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> {:error, {:drive_state_failed, drive_id, reason}}
    end
  end

  defp drive_state_opts_from_config(config, command_module) do
    [
      drive_id: to_string(config[:id] || config["id"]),
      drive_path: to_string(config[:path] || config["path"]),
      power_management: config[:power_management] || config["power_management"] || false,
      idle_timeout: config[:idle_timeout] || config["idle_timeout"] || 1800,
      command_module: command_module
    ]
  end

  ## Private — Serialisation

  defp drive_to_map(%Drive{} = drive) do
    %{
      id: drive.id,
      node: Atom.to_string(drive.node),
      path: drive.path,
      tier: Atom.to_string(drive.tier),
      capacity_bytes: drive.capacity_bytes,
      used_bytes: drive.used_bytes,
      state: Atom.to_string(drive.state)
    }
  end
end
