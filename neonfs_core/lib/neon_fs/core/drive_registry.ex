defmodule NeonFS.Core.DriveRegistry do
  @moduledoc """
  Registry tracking all drives across the cluster.

  Each node reads its drive configuration from the application environment and
  registers drives with capacity, tier assignment, and state. The registry provides
  drive selection for writes (least-used drive within a tier) and cluster-wide
  tier discovery.

  The registry is local-first: each node knows its own drives authoritatively.
  Cluster-wide view is assembled by querying remote nodes' registries via periodic RPC.

  ## ETS Tables

  - `:drive_registry` — `{drive_id, Drive.t()}` for all known drives (local + remote)
  """

  use GenServer
  require Logger

  alias NeonFS.Core.Drive

  @ets_table :drive_registry
  @sync_interval_ms 30_000

  ## Client API

  @doc """
  Starts the DriveRegistry GenServer.

  ## Options

    * `:drives` - List of drive config maps (defaults to app env `:drives`)
    * `:sync_interval_ms` - Interval for cross-node sync (default: 30_000)
    * `:name` - GenServer name (default: `__MODULE__`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Registers a drive in the registry.
  """
  @spec register_drive(Drive.t()) :: :ok
  def register_drive(%Drive{} = drive) do
    GenServer.call(__MODULE__, {:register_drive, drive})
  end

  @doc """
  Looks up a specific drive by node and drive ID.
  """
  @spec get_drive(node(), String.t()) :: {:ok, Drive.t()} | {:error, :not_found}
  def get_drive(node, drive_id) do
    case :ets.lookup(@ets_table, {node, drive_id}) do
      [{_key, drive}] -> {:ok, drive}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Lists all drives across the cluster.
  """
  @spec list_drives() :: [Drive.t()]
  def list_drives do
    :ets.tab2list(@ets_table)
    |> Enum.map(fn {_key, drive} -> drive end)
    |> Enum.sort_by(& &1.id)
  end

  @doc """
  Returns drives assigned to a specific tier.
  """
  @spec drives_for_tier(atom()) :: [Drive.t()]
  def drives_for_tier(tier) do
    list_drives()
    |> Enum.filter(&(&1.tier == tier))
  end

  @doc """
  Returns drives on a specific node.
  """
  @spec drives_for_node(node()) :: [Drive.t()]
  def drives_for_node(node) do
    list_drives()
    |> Enum.filter(&(&1.node == node))
  end

  @doc """
  Selects the least-used active drive in a tier.

  Returns the drive with the lowest `used_bytes / capacity_bytes` ratio
  among active drives in the specified tier on the local node.

  ## Returns

    * `{:ok, drive}` - Selected drive
    * `{:error, :no_drives_in_tier}` - No active drives available in this tier
  """
  @spec select_drive(atom()) :: {:ok, Drive.t()} | {:error, :no_drives_in_tier}
  def select_drive(tier) do
    candidates =
      drives_for_tier(tier)
      |> Enum.filter(&(&1.node == Node.self() and &1.state == :active))

    :telemetry.execute(
      [:neonfs, :drive_registry, :select_drive],
      %{candidates: length(candidates)},
      %{tier: tier, node: Node.self()}
    )

    case candidates do
      [] ->
        {:error, :no_drives_in_tier}

      drives ->
        selected = Enum.min_by(drives, &Drive.usage_ratio/1)
        {:ok, selected}
    end
  end

  @doc """
  Triggers an immediate sync of remote drive metadata.

  Useful in tests or after cluster topology changes when you need
  remote drive info available without waiting for the periodic sync.
  """
  @spec sync_now() :: :ok
  def sync_now do
    GenServer.call(__MODULE__, :sync_now)
  end

  @doc """
  Updates the used_bytes for a drive.
  """
  @spec update_usage(String.t(), non_neg_integer()) :: :ok | {:error, :not_found}
  def update_usage(drive_id, used_bytes) do
    GenServer.call(__MODULE__, {:update_usage, drive_id, used_bytes})
  end

  @doc """
  Updates the state of a drive (:active or :standby).
  """
  @spec update_state(String.t(), Drive.state()) :: :ok | {:error, :not_found}
  def update_state(drive_id, state) when state in [:active, :standby] do
    GenServer.call(__MODULE__, {:update_state, drive_id, state})
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    table = :ets.new(@ets_table, [:named_table, :set, :public, read_concurrency: true])
    sync_interval = Keyword.get(opts, :sync_interval_ms, @sync_interval_ms)

    # Register local drives from config
    drives_config = Keyword.get(opts, :drives, Application.get_env(:neonfs_core, :drives, []))

    local_drives =
      Enum.map(drives_config, fn config ->
        Drive.from_config(config, Node.self())
      end)

    Enum.each(local_drives, fn drive ->
      key = drive_key(drive)
      :ets.insert(table, {key, drive})

      :telemetry.execute(
        [:neonfs, :drive_registry, :register],
        %{capacity_bytes: drive.capacity_bytes},
        %{drive_id: drive.id, node: drive.node, tier: drive.tier}
      )
    end)

    Logger.info("DriveRegistry started with #{length(local_drives)} local drives")

    state = %{
      table: table,
      sync_interval: sync_interval,
      local_drive_ids: Enum.map(local_drives, & &1.id)
    }

    # Sync remote drives immediately via handle_continue, then schedule periodic syncs
    {:ok, state, {:continue, :initial_sync}}
  end

  @impl true
  def handle_continue(:initial_sync, state) do
    sync_remote_drives(state.table, state.local_drive_ids)

    if state.sync_interval > 0 do
      Process.send_after(self(), :sync_remote_drives, state.sync_interval)
    end

    {:noreply, state}
  end

  @impl true
  def handle_call(:sync_now, _from, state) do
    sync_remote_drives(state.table, state.local_drive_ids)
    {:reply, :ok, state}
  end

  def handle_call({:register_drive, %Drive{} = drive}, _from, state) do
    key = drive_key(drive)
    :ets.insert(state.table, {key, drive})

    :telemetry.execute(
      [:neonfs, :drive_registry, :register],
      %{capacity_bytes: drive.capacity_bytes},
      %{drive_id: drive.id, node: drive.node, tier: drive.tier}
    )

    {:reply, :ok, state}
  end

  def handle_call({:update_usage, drive_id, used_bytes}, _from, state) do
    result = update_drive_field(state.table, drive_id, :used_bytes, used_bytes)
    {:reply, result, state}
  end

  def handle_call({:update_state, drive_id, new_state}, _from, state) do
    result = update_drive_field(state.table, drive_id, :state, new_state)
    {:reply, result, state}
  end

  @impl true
  def handle_info(:sync_remote_drives, state) do
    sync_remote_drives(state.table, state.local_drive_ids)

    if state.sync_interval > 0 do
      Process.send_after(self(), :sync_remote_drives, state.sync_interval)
    end

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  ## Private Functions

  defp drive_key(%Drive{node: node, id: id}), do: {node, id}

  defp update_drive_field(table, drive_id, field, value) do
    # Find the drive across all nodes
    case find_drive(table, drive_id) do
      {:ok, key, drive} ->
        updated = Map.put(drive, field, value)
        :ets.insert(table, {key, updated})
        :ok

      :not_found ->
        {:error, :not_found}
    end
  end

  defp find_drive(table, drive_id) do
    result =
      :ets.tab2list(table)
      |> Enum.find(fn {_key, drive} -> drive.id == drive_id end)

    case result do
      {key, drive} -> {:ok, key, drive}
      nil -> :not_found
    end
  end

  defp sync_remote_drives(table, local_drive_ids) do
    remote_nodes = Node.list()

    if Enum.empty?(remote_nodes) do
      :ok
    else
      # Remove stale remote entries for nodes no longer connected
      remove_stale_entries(table, remote_nodes, local_drive_ids)

      # Fetch drives from each remote node
      Enum.each(remote_nodes, fn node ->
        fetch_remote_drives(table, node)
      end)
    end
  end

  defp remove_stale_entries(table, connected_nodes, _local_drive_ids) do
    local_node = Node.self()

    :ets.tab2list(table)
    |> Enum.each(fn {{node, _id} = key, _drive} ->
      if node != local_node and node not in connected_nodes do
        :ets.delete(table, key)
      end
    end)
  end

  defp fetch_remote_drives(table, node) do
    case :rpc.call(node, __MODULE__, :list_drives, [], 5_000) do
      drives when is_list(drives) ->
        # Only insert drives belonging to that remote node
        drives
        |> Enum.filter(&(&1.node == node))
        |> Enum.each(fn drive ->
          key = drive_key(drive)
          :ets.insert(table, {key, drive})
        end)

      {:badrpc, reason} ->
        Logger.debug("Failed to sync drives from #{node}: #{inspect(reason)}")
    end
  end
end
