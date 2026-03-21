defmodule NeonFS.NFS.MetadataCache do
  @moduledoc """
  ETS-backed metadata cache shared across all NFS-exported volumes.

  Reduces RPC round-trips to core nodes by caching file attributes,
  directory listings, and lookup results locally. The cache subscribes
  to volume events and invalidates affected entries when metadata changes
  on core nodes.

  ## Cache Tables

  A single ETS table with tagged keys:

    - `{:attrs, volume_name, path}` — file/dir attributes (`getattr` results)
    - `{:dir, volume_name, path}` — directory listings (`readdirplus` results)
    - `{:lookup, volume_name, parent_path, name}` — lookup results

  ## Handler Integration

  The ETS table is `:public` with `read_concurrency: true` so the Handler
  process can read directly without going through the GenServer. The public
  API functions (`get_attrs/3`, `put_attrs/4`, etc.) operate directly on the
  ETS table reference.
  """

  use GenServer
  require Logger

  alias NeonFS.Events.{
    DirCreated,
    DirDeleted,
    DirRenamed,
    Envelope,
    FileAclChanged,
    FileAttrsChanged,
    FileContentUpdated,
    FileCreated,
    FileDeleted,
    FileRenamed,
    FileTruncated,
    VolumeAclChanged,
    VolumeUpdated
  }

  # -- Public API --

  @doc """
  Start the MetadataCache GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Return the ETS table reference for direct reads from the Handler.
  """
  @spec table() :: :ets.table() | nil
  def table do
    GenServer.call(__MODULE__, :table)
  catch
    :exit, _ -> nil
  end

  @doc """
  Subscribe to events for a volume, enabling cache invalidation.

  Requires both `volume_name` (used for cache keys) and `volume_id`
  (the core UUID used for event topics). The mapping is stored internally
  so event dispatch can translate volume_id → volume_name for cache lookups.
  """
  @spec subscribe_volume(String.t(), binary()) :: :ok
  def subscribe_volume(volume_name, volume_id) do
    GenServer.cast(__MODULE__, {:subscribe_volume, volume_name, volume_id})
  end

  @doc """
  Unsubscribe from events for a volume and clear its cache entries.
  """
  @spec unsubscribe_volume(String.t(), binary()) :: :ok
  def unsubscribe_volume(volume_name, volume_id) do
    GenServer.cast(__MODULE__, {:unsubscribe_volume, volume_name, volume_id})
  end

  @doc """
  Get cached file/directory attributes.

  Returns `{:ok, attrs}` on cache hit, `:miss` on cache miss.
  """
  @spec get_attrs(:ets.table(), String.t(), String.t()) :: {:ok, term()} | :miss
  def get_attrs(table, volume_name, path) do
    case :ets.lookup(table, {:attrs, volume_name, path}) do
      [{_, attrs}] -> {:ok, attrs}
      [] -> :miss
    end
  rescue
    ArgumentError -> :miss
  end

  @doc """
  Cache file/directory attributes.
  """
  @spec put_attrs(:ets.table(), String.t(), String.t(), term()) :: :ok
  def put_attrs(table, volume_name, path, attrs) do
    :ets.insert(table, {{:attrs, volume_name, path}, attrs})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Get cached directory listing.

  Returns `{:ok, entries}` on cache hit, `:miss` on cache miss.
  """
  @spec get_dir_listing(:ets.table(), String.t(), String.t()) :: {:ok, term()} | :miss
  def get_dir_listing(table, volume_name, path) do
    case :ets.lookup(table, {:dir, volume_name, path}) do
      [{_, entries}] -> {:ok, entries}
      [] -> :miss
    end
  rescue
    ArgumentError -> :miss
  end

  @doc """
  Cache a directory listing.
  """
  @spec put_dir_listing(:ets.table(), String.t(), String.t(), term()) :: :ok
  def put_dir_listing(table, volume_name, path, entries) do
    :ets.insert(table, {{:dir, volume_name, path}, entries})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Get cached lookup result.

  Returns `{:ok, result}` on cache hit, `:miss` on cache miss.
  """
  @spec get_lookup(:ets.table(), String.t(), String.t(), String.t()) :: {:ok, term()} | :miss
  def get_lookup(table, volume_name, parent_path, name) do
    case :ets.lookup(table, {:lookup, volume_name, parent_path, name}) do
      [{_, result}] -> {:ok, result}
      [] -> :miss
    end
  rescue
    ArgumentError -> :miss
  end

  @doc """
  Cache a lookup result.
  """
  @spec put_lookup(:ets.table(), String.t(), String.t(), String.t(), term()) :: :ok
  def put_lookup(table, volume_name, parent_path, name, result) do
    :ets.insert(table, {{:lookup, volume_name, parent_path, name}, result})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Invalidate cached attributes for a specific path.
  """
  @spec invalidate_attrs(:ets.table(), String.t(), String.t()) :: :ok
  def invalidate_attrs(table, volume_name, path) do
    :ets.delete(table, {:attrs, volume_name, path})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Invalidate a cached directory listing for a specific path.
  """
  @spec invalidate_dir_listing(:ets.table(), String.t(), String.t()) :: :ok
  def invalidate_dir_listing(table, volume_name, path) do
    :ets.delete(table, {:dir, volume_name, path})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Invalidate a cached lookup result.
  """
  @spec invalidate_lookup(:ets.table(), String.t(), String.t(), String.t()) :: :ok
  def invalidate_lookup(table, volume_name, parent_path, name) do
    :ets.delete(table, {:lookup, volume_name, parent_path, name})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Invalidate all cache entries for a volume.
  """
  @spec invalidate_volume(:ets.table(), String.t()) :: :ok
  def invalidate_volume(table, volume_name) do
    :ets.match_delete(table, {{:attrs, volume_name, :_}, :_})
    :ets.match_delete(table, {{:dir, volume_name, :_}, :_})
    :ets.match_delete(table, {{:lookup, volume_name, :_, :_}, :_})
    :ok
  rescue
    ArgumentError -> :ok
  end

  # -- GenServer callbacks --

  @impl true
  def init(_opts) do
    table = :ets.new(:nfs_metadata_cache, [:set, :public, read_concurrency: true])

    {:ok,
     %{
       table: table,
       subscriptions: MapSet.new(),
       last_sequences: %{},
       volume_id_to_name: %{}
     }}
  end

  @impl true
  def handle_call(:table, _from, state) do
    {:reply, state.table, state}
  end

  @impl true
  def handle_cast({:subscribe_volume, volume_name, volume_id}, state) do
    NeonFS.Events.subscribe(volume_id)

    new_state = %{
      state
      | subscriptions: MapSet.put(state.subscriptions, volume_name),
        volume_id_to_name: Map.put(state.volume_id_to_name, volume_id, volume_name)
    }

    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:unsubscribe_volume, volume_name, volume_id}, state) do
    NeonFS.Events.unsubscribe(volume_id)
    invalidate_volume(state.table, volume_name)

    new_state = %{
      state
      | subscriptions: MapSet.delete(state.subscriptions, volume_name),
        volume_id_to_name: Map.delete(state.volume_id_to_name, volume_id)
    }

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:neonfs_event, %Envelope{} = envelope}, state) do
    state = check_sequence_gap(envelope, state)
    dispatch_event(envelope, state.table, state.volume_id_to_name)

    new_sequences =
      Map.put(state.last_sequences, envelope.source_node, envelope.sequence)

    {:noreply, %{state | last_sequences: new_sequences}}
  end

  def handle_info(:neonfs_invalidate_all, state) do
    :ets.delete_all_objects(state.table)
    Logger.debug("NFS MetadataCache: invalidated all entries (partition recovery)")
    {:noreply, %{state | last_sequences: %{}}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # -- Internal event dispatch --
  #
  # Events carry `volume_id` (UUID) but cache keys use `volume_name`.
  # The `id_to_name` map translates between them.

  defp dispatch_event(envelope, table, id_to_name) do
    case resolve_volume_name(envelope, id_to_name) do
      {:ok, vol_name} -> do_dispatch_event(envelope, table, vol_name)
      :skip -> :ok
    end
  end

  defp resolve_volume_name(%Envelope{event: event}, id_to_name) do
    vid = Map.get(event, :volume_id)

    case Map.get(id_to_name, vid) do
      nil -> :skip
      name -> {:ok, name}
    end
  end

  defp do_dispatch_event(%Envelope{event: %FileCreated{path: path}}, table, vol) do
    invalidate_dir_listing(table, vol, Path.dirname(path))
    invalidate_lookup(table, vol, Path.dirname(path), Path.basename(path))
  end

  defp do_dispatch_event(%Envelope{event: %FileContentUpdated{path: path}}, table, vol) do
    invalidate_attrs(table, vol, path)
  end

  defp do_dispatch_event(%Envelope{event: %FileTruncated{path: path}}, table, vol) do
    invalidate_attrs(table, vol, path)
  end

  defp do_dispatch_event(%Envelope{event: %FileDeleted{path: path}}, table, vol) do
    invalidate_attrs(table, vol, path)
    invalidate_dir_listing(table, vol, Path.dirname(path))
    invalidate_lookup(table, vol, Path.dirname(path), Path.basename(path))
  end

  defp do_dispatch_event(%Envelope{event: %FileAttrsChanged{path: path}}, table, vol) do
    invalidate_attrs(table, vol, path)
  end

  defp do_dispatch_event(
         %Envelope{event: %FileRenamed{old_path: old_path, new_path: new_path}},
         table,
         vol
       ) do
    invalidate_attrs(table, vol, old_path)
    invalidate_dir_listing(table, vol, Path.dirname(old_path))
    invalidate_dir_listing(table, vol, Path.dirname(new_path))
    invalidate_lookup(table, vol, Path.dirname(old_path), Path.basename(old_path))
    invalidate_lookup(table, vol, Path.dirname(new_path), Path.basename(new_path))
  end

  defp do_dispatch_event(%Envelope{event: %VolumeAclChanged{}}, table, vol) do
    invalidate_volume(table, vol)
  end

  defp do_dispatch_event(%Envelope{event: %FileAclChanged{path: path}}, table, vol) do
    invalidate_attrs(table, vol, path)
  end

  defp do_dispatch_event(%Envelope{event: %DirCreated{path: path}}, table, vol) do
    invalidate_dir_listing(table, vol, Path.dirname(path))
    invalidate_lookup(table, vol, Path.dirname(path), Path.basename(path))
  end

  defp do_dispatch_event(%Envelope{event: %DirDeleted{path: path}}, table, vol) do
    invalidate_dir_listing(table, vol, path)
    invalidate_dir_listing(table, vol, Path.dirname(path))
    invalidate_lookup(table, vol, Path.dirname(path), Path.basename(path))
  end

  defp do_dispatch_event(
         %Envelope{event: %DirRenamed{old_path: old_path, new_path: new_path}},
         table,
         vol
       ) do
    invalidate_dir_listing(table, vol, old_path)
    invalidate_dir_listing(table, vol, Path.dirname(old_path))
    invalidate_dir_listing(table, vol, Path.dirname(new_path))
    invalidate_lookup(table, vol, Path.dirname(old_path), Path.basename(old_path))
    invalidate_lookup(table, vol, Path.dirname(new_path), Path.basename(new_path))
  end

  defp do_dispatch_event(%Envelope{event: %VolumeUpdated{}}, table, vol) do
    invalidate_volume(table, vol)
  end

  defp do_dispatch_event(_envelope, _table, _vol), do: :ok

  # -- Gap detection --

  defp check_sequence_gap(%Envelope{source_node: source, sequence: seq}, state) do
    last = Map.get(state.last_sequences, source, 0)

    if seq > last + 1 do
      Logger.warning(
        "NFS MetadataCache: event gap from #{inspect(source)}: expected #{last + 1}, got #{seq}"
      )

      :ets.delete_all_objects(state.table)
      %{state | last_sequences: %{}}
    else
      state
    end
  end
end
