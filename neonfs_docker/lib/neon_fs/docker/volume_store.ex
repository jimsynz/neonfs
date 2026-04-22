defmodule NeonFS.Docker.VolumeStore do
  @moduledoc """
  Plugin-local record of Docker volumes the driver has been asked to
  manage.

  Docker calls `VolumeDriver.Create` the first time a container
  references a volume name, and `VolumeDriver.Remove` when the volume
  is explicitly removed from Docker's side. The plugin-local store
  tracks the set of known names plus the options Docker passed — the
  NeonFS volume itself is durable and persists independently of this
  record (so `Remove` only drops the local entry).

  The store is an in-memory map wrapped by a `GenServer` for
  serialisation. Losing the map on restart is acceptable — it is
  rebuilt lazily as Docker issues subsequent `Get` / `Create` calls.
  """

  use GenServer

  @type volume_name :: String.t()
  @type opts :: map()
  @type record :: %{name: volume_name(), opts: opts()}

  ## Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Record a volume with the given name and options.

  Idempotent — repeated creates for the same name overwrite the stored
  options, matching Docker's "create if missing" semantics.
  """
  @spec put(GenServer.server(), volume_name(), opts()) :: :ok
  def put(server \\ __MODULE__, name, opts) when is_binary(name) and is_map(opts) do
    GenServer.call(server, {:put, name, opts})
  end

  @doc """
  Remove a volume record. Returns `:ok` whether or not the record existed —
  matches the idempotence Docker expects.
  """
  @spec delete(GenServer.server(), volume_name()) :: :ok
  def delete(server \\ __MODULE__, name) when is_binary(name) do
    GenServer.call(server, {:delete, name})
  end

  @doc """
  Fetch a single record. Returns `{:error, :not_found}` if the name
  isn't known to the plugin.
  """
  @spec get(GenServer.server(), volume_name()) :: {:ok, record()} | {:error, :not_found}
  def get(server \\ __MODULE__, name) when is_binary(name) do
    GenServer.call(server, {:get, name})
  end

  @doc """
  Return all known volume records.
  """
  @spec list(GenServer.server()) :: [record()]
  def list(server \\ __MODULE__) do
    GenServer.call(server, :list)
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:put, name, opts}, _from, state) do
    {:reply, :ok, Map.put(state, name, %{name: name, opts: opts})}
  end

  def handle_call({:delete, name}, _from, state) do
    {:reply, :ok, Map.delete(state, name)}
  end

  def handle_call({:get, name}, _from, state) do
    case Map.fetch(state, name) do
      {:ok, record} -> {:reply, {:ok, record}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list, _from, state) do
    {:reply, Map.values(state), state}
  end
end
