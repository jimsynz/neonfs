defmodule NeonFS.Core.VolumeRegistry do
  @moduledoc """
  Registry for managing storage volumes.

  Uses ETS for concurrent read access with serialized writes through GenServer.
  Maintains two tables for fast lookups: by ID and by name.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.FileIndex
  alias NeonFS.Core.Volume

  @type volume_id :: binary()
  @type volume_name :: String.t()

  # Client API

  @doc """
  Starts the volume registry.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Creates a new volume with the given name and configuration.

  Returns `{:ok, volume}` if successful, or `{:error, reason}` if:
  - Name already exists
  - Configuration is invalid
  """
  @spec create(String.t(), keyword()) :: {:ok, Volume.t()} | {:error, String.t()}
  def create(name, opts \\ []) do
    GenServer.call(__MODULE__, {:create, name, opts})
  end

  @doc """
  Gets a volume by its ID.

  Returns `{:ok, volume}` if found, or `{:error, :not_found}` otherwise.
  """
  @spec get(volume_id()) :: {:ok, Volume.t()} | {:error, :not_found}
  def get(id) do
    case :ets.lookup(:volumes_by_id, id) do
      [{^id, volume}] -> {:ok, volume}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Gets a volume by its name.

  Returns `{:ok, volume}` if found, or `{:error, :not_found}` otherwise.
  """
  @spec get_by_name(volume_name()) :: {:ok, Volume.t()} | {:error, :not_found}
  def get_by_name(name) do
    case :ets.lookup(:volumes_by_name, name) do
      [{^name, volume_id}] -> get(volume_id)
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Lists all volumes.

  Returns a list of all volume structs.
  """
  @spec list() :: [Volume.t()]
  def list do
    :ets.tab2list(:volumes_by_id)
    |> Enum.map(fn {_id, volume} -> volume end)
    |> Enum.sort_by(& &1.name)
  end

  @doc """
  Updates a volume's configuration.

  Returns `{:ok, updated_volume}` if successful, or `{:error, reason}` if:
  - Volume not found
  - New configuration is invalid
  """
  @spec update(volume_id(), keyword()) :: {:ok, Volume.t()} | {:error, String.t()}
  def update(id, opts) do
    GenServer.call(__MODULE__, {:update, id, opts})
  end

  @doc """
  Updates a volume's statistics (size and chunk count).

  Returns `{:ok, updated_volume}` if successful, or `{:error, :not_found}` if not found.
  """
  @spec update_stats(volume_id(), keyword()) :: {:ok, Volume.t()} | {:error, :not_found}
  def update_stats(id, stats) do
    GenServer.call(__MODULE__, {:update_stats, id, stats})
  end

  @doc """
  Deletes a volume.

  Returns `:ok` if successful, or `{:error, reason}` if:
  - Volume not found
  - Volume contains files (must be empty)
  """
  @spec delete(volume_id()) :: :ok | {:error, String.t()}
  def delete(id) do
    GenServer.call(__MODULE__, {:delete, id})
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    # Trap exits so terminate/2 is called during shutdown
    Process.flag(:trap_exit, true)

    # Create ETS tables for fast lookups
    :ets.new(:volumes_by_id, [
      :named_table,
      :set,
      :public,
      read_concurrency: true
    ])

    :ets.new(:volumes_by_name, [
      :named_table,
      :set,
      :public,
      read_concurrency: true
    ])

    Logger.info("VolumeRegistry started")
    {:ok, %{}}
  end

  @impl true
  def terminate(_reason, _state) do
    # Save ETS tables to DETS before shutdown
    Logger.info("VolumeRegistry shutting down, saving tables...")
    meta_dir = NeonFS.Core.Persistence.meta_dir()

    NeonFS.Core.Persistence.snapshot_table(
      :volumes_by_id,
      Path.join(meta_dir, "volume_registry_by_id.dets")
    )

    NeonFS.Core.Persistence.snapshot_table(
      :volumes_by_name,
      Path.join(meta_dir, "volume_registry_by_name.dets")
    )

    Logger.info("VolumeRegistry tables saved")
    :ok
  end

  @impl true
  def handle_call({:create, name, opts}, _from, state) do
    reply =
      case get_by_name(name) do
        {:ok, _} ->
          {:error, "volume with name '#{name}' already exists"}

        {:error, :not_found} ->
          volume = Volume.new(name, opts)

          case Volume.validate(volume) do
            :ok ->
              insert_volume(volume)
              {:ok, volume}

            {:error, reason} ->
              {:error, reason}
          end
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:update, id, opts}, _from, state) do
    reply =
      case get(id) do
        {:ok, volume} ->
          updated = Volume.update(volume, opts)

          case Volume.validate(updated) do
            :ok ->
              insert_volume(updated)
              {:ok, updated}

            {:error, reason} ->
              {:error, reason}
          end

        {:error, :not_found} ->
          {:error, :not_found}
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:update_stats, id, stats}, _from, state) do
    reply =
      case get(id) do
        {:ok, volume} ->
          updated = Volume.update_stats(volume, stats)
          insert_volume(updated)
          {:ok, updated}

        {:error, :not_found} ->
          {:error, :not_found}
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    reply =
      case get(id) do
        {:ok, volume} ->
          check_and_delete_volume(volume)

        {:error, :not_found} ->
          {:error, :not_found}
      end

    {:reply, reply, state}
  end

  # Private helpers

  defp insert_volume(%Volume{} = volume) do
    :ets.insert(:volumes_by_id, {volume.id, volume})
    :ets.insert(:volumes_by_name, {volume.name, volume.id})
  end

  defp check_and_delete_volume(%Volume{} = volume) do
    # Check if volume has any files
    files = FileIndex.list_volume(volume.id)

    if Enum.empty?(files) do
      :ets.delete(:volumes_by_id, volume.id)
      :ets.delete(:volumes_by_name, volume.name)
      :ok
    else
      {:error, "volume contains #{length(files)} file(s), cannot delete"}
    end
  end
end
