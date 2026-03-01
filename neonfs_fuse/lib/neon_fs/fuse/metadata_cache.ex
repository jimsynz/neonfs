defmodule NeonFS.FUSE.MetadataCache do
  @moduledoc """
  ETS-backed metadata cache for a single FUSE mount.

  Reduces RPC round-trips to core nodes by caching file attributes,
  directory listings, and lookup results locally. The cache subscribes
  to volume events and invalidates affected entries when metadata changes
  on core nodes.

  ## Cache Tables

  A single ETS table per cache instance with tagged keys:

    - `{:attrs, volume_id, path}` — file/dir attributes (`getattr` results)
    - `{:dir, volume_id, path}` — directory listings (`readdir` results)
    - `{:lookup, volume_id, parent_path, name}` — lookup results

  ## Event Handling

  Subscribes to volume events via `NeonFS.Events.subscribe/1`. On receiving
  an event, invalidates the relevant cache entries. Gap detection clears
  all entries if a sequence gap is detected (dropped message).

  ## Handler Integration

  The ETS table is `:public` with `read_concurrency: true` so the Handler
  process can read directly without going through the GenServer. The public
  API functions (`get_attrs/3`, `put_attrs/4`, etc.) operate directly on the
  ETS table reference.
  """

  use GenServer
  @behaviour NeonFS.Client.EventHandler

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
  Start the MetadataCache GenServer for a given volume.

  ## Options

    * `:volume_id` (required) — the volume to cache metadata for
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Return the ETS table reference for direct reads from the Handler.
  """
  @spec table(GenServer.server(), keyword()) :: :ets.table()
  def table(server, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    GenServer.call(server, :table, timeout)
  end

  @doc """
  Get cached file/directory attributes.

  Returns `{:ok, attrs}` on cache hit, `:miss` on cache miss.
  """
  @spec get_attrs(:ets.table(), binary(), binary()) :: {:ok, term()} | :miss
  def get_attrs(table, volume_id, path) do
    case :ets.lookup(table, {:attrs, volume_id, path}) do
      [{_, attrs}] -> {:ok, attrs}
      [] -> :miss
    end
  rescue
    ArgumentError -> :miss
  end

  @doc """
  Cache file/directory attributes.
  """
  @spec put_attrs(:ets.table(), binary(), binary(), term()) :: :ok
  def put_attrs(table, volume_id, path, attrs) do
    :ets.insert(table, {{:attrs, volume_id, path}, attrs})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Get cached directory listing.

  Returns `{:ok, entries}` on cache hit, `:miss` on cache miss.
  """
  @spec get_dir_listing(:ets.table(), binary(), binary()) :: {:ok, term()} | :miss
  def get_dir_listing(table, volume_id, path) do
    case :ets.lookup(table, {:dir, volume_id, path}) do
      [{_, entries}] -> {:ok, entries}
      [] -> :miss
    end
  rescue
    ArgumentError -> :miss
  end

  @doc """
  Cache a directory listing.
  """
  @spec put_dir_listing(:ets.table(), binary(), binary(), term()) :: :ok
  def put_dir_listing(table, volume_id, path, entries) do
    :ets.insert(table, {{:dir, volume_id, path}, entries})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Get cached lookup result.

  Returns `{:ok, result}` on cache hit, `:miss` on cache miss.
  """
  @spec get_lookup(:ets.table(), binary(), binary(), binary()) :: {:ok, term()} | :miss
  def get_lookup(table, volume_id, parent_path, name) do
    case :ets.lookup(table, {:lookup, volume_id, parent_path, name}) do
      [{_, result}] -> {:ok, result}
      [] -> :miss
    end
  rescue
    ArgumentError -> :miss
  end

  @doc """
  Cache a lookup result.
  """
  @spec put_lookup(:ets.table(), binary(), binary(), binary(), term()) :: :ok
  def put_lookup(table, volume_id, parent_path, name, result) do
    :ets.insert(table, {{:lookup, volume_id, parent_path, name}, result})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Invalidate all cache entries for a volume.
  """
  @spec invalidate_all(:ets.table(), binary()) :: :ok
  def invalidate_all(table, _volume_id) do
    :ets.delete_all_objects(table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  # -- EventHandler behaviour --

  @impl NeonFS.Client.EventHandler
  def handle_event(%Envelope{} = envelope) do
    # The behaviour callback dispatches via process dictionary state
    # set during init/1. Only callable from within the GenServer process.
    table = Process.get(:metadata_cache_table)
    volume_id = Process.get(:metadata_cache_volume_id)

    if table do
      dispatch_event(envelope, table, volume_id)
    end

    :ok
  end

  # -- GenServer callbacks --

  @impl true
  def init(opts) do
    volume_id = Keyword.fetch!(opts, :volume_id)
    table = :ets.new(:metadata_cache, [:set, :public, read_concurrency: true])

    # Store in process dictionary for the EventHandler behaviour callback
    Process.put(:metadata_cache_table, table)
    Process.put(:metadata_cache_volume_id, volume_id)

    NeonFS.Events.subscribe(volume_id)

    {:ok, %{volume_id: volume_id, table: table, last_sequences: %{}}}
  end

  @impl true
  def terminate(_reason, state) do
    NeonFS.Events.unsubscribe(state.volume_id)
    :ok
  end

  @impl true
  def handle_call(:table, _from, state) do
    {:reply, state.table, state}
  end

  @impl true
  def handle_info({:neonfs_event, %Envelope{} = envelope}, state) do
    state = check_sequence_gap(envelope, state)
    handle_event(envelope)

    {:noreply,
     %{
       state
       | last_sequences: Map.put(state.last_sequences, envelope.source_node, envelope.sequence)
     }}
  end

  def handle_info(:neonfs_invalidate_all, state) do
    :ets.delete_all_objects(state.table)
    Logger.debug("MetadataCache: invalidated all entries (partition recovery)")
    {:noreply, %{state | last_sequences: %{}}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # -- Internal event dispatch --

  defp dispatch_event(%Envelope{event: %FileCreated{path: path}}, table, vid) do
    invalidate_dir_listing(table, vid, Path.dirname(path))
    invalidate_lookup(table, vid, Path.dirname(path), Path.basename(path))
  end

  defp dispatch_event(%Envelope{event: %FileContentUpdated{path: path}}, table, vid) do
    invalidate_attrs(table, vid, path)
  end

  defp dispatch_event(%Envelope{event: %FileTruncated{path: path}}, table, vid) do
    invalidate_attrs(table, vid, path)
  end

  defp dispatch_event(%Envelope{event: %FileDeleted{path: path}}, table, vid) do
    invalidate_attrs(table, vid, path)
    invalidate_dir_listing(table, vid, Path.dirname(path))
    invalidate_lookup(table, vid, Path.dirname(path), Path.basename(path))
  end

  defp dispatch_event(%Envelope{event: %FileAttrsChanged{path: path}}, table, vid) do
    invalidate_attrs(table, vid, path)
  end

  defp dispatch_event(
         %Envelope{event: %FileRenamed{old_path: old_path, new_path: new_path}},
         table,
         vid
       ) do
    invalidate_attrs(table, vid, old_path)
    invalidate_dir_listing(table, vid, Path.dirname(old_path))
    invalidate_dir_listing(table, vid, Path.dirname(new_path))
    invalidate_lookup(table, vid, Path.dirname(old_path), Path.basename(old_path))
    invalidate_lookup(table, vid, Path.dirname(new_path), Path.basename(new_path))
  end

  defp dispatch_event(%Envelope{event: %VolumeAclChanged{}}, table, _vid) do
    :ets.delete_all_objects(table)
  end

  defp dispatch_event(%Envelope{event: %FileAclChanged{path: path}}, table, vid) do
    invalidate_attrs(table, vid, path)
  end

  defp dispatch_event(%Envelope{event: %DirCreated{path: path}}, table, vid) do
    invalidate_dir_listing(table, vid, Path.dirname(path))
    invalidate_lookup(table, vid, Path.dirname(path), Path.basename(path))
  end

  defp dispatch_event(%Envelope{event: %DirDeleted{path: path}}, table, vid) do
    invalidate_dir_listing(table, vid, path)
    invalidate_dir_listing(table, vid, Path.dirname(path))
    invalidate_lookup(table, vid, Path.dirname(path), Path.basename(path))
  end

  defp dispatch_event(
         %Envelope{event: %DirRenamed{old_path: old_path, new_path: new_path}},
         table,
         vid
       ) do
    invalidate_dir_listing(table, vid, old_path)
    invalidate_dir_listing(table, vid, Path.dirname(old_path))
    invalidate_dir_listing(table, vid, Path.dirname(new_path))
    invalidate_lookup(table, vid, Path.dirname(old_path), Path.basename(old_path))
    invalidate_lookup(table, vid, Path.dirname(new_path), Path.basename(new_path))
  end

  defp dispatch_event(%Envelope{event: %VolumeUpdated{}}, table, _vid) do
    :ets.delete_all_objects(table)
  end

  defp dispatch_event(_envelope, _table, _vid), do: :ok

  # -- Invalidation helpers --

  defp invalidate_attrs(table, volume_id, path) do
    :ets.delete(table, {:attrs, volume_id, path})
  end

  defp invalidate_dir_listing(table, volume_id, path) do
    :ets.delete(table, {:dir, volume_id, path})
  end

  defp invalidate_lookup(table, volume_id, parent_path, name) do
    :ets.delete(table, {:lookup, volume_id, parent_path, name})
  end

  # -- Gap detection --

  defp check_sequence_gap(%Envelope{source_node: source, sequence: seq}, state) do
    last = Map.get(state.last_sequences, source, 0)

    if seq > last + 1 do
      Logger.warning(
        "MetadataCache: event gap from #{inspect(source)}: expected #{last + 1}, got #{seq}"
      )

      :ets.delete_all_objects(state.table)
      %{state | last_sequences: %{}}
    else
      state
    end
  end
end
