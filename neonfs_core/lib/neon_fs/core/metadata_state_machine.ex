defmodule NeonFS.Core.MetadataStateMachine do
  @moduledoc """
  Ra state machine for cluster-wide metadata storage.

  This state machine manages:
  - Node membership and health
  - Volume definitions
  - User and group definitions
  - Segment assignments
  - Active write sessions

  For Phase 2, this provides the foundation for distributed consensus.
  Future phases will expand this to handle full cluster coordination.
  """

  @behaviour :ra_machine

  @type command ::
          {:put, key :: term(), value :: term()}
          | {:delete, key :: term()}
          | {:put_chunk, chunk_meta :: map()}
          | {:update_chunk_locations, hash :: binary(), locations :: [map()]}
          | {:delete_chunk, hash :: binary()}
          | {:commit_chunk, hash :: binary()}
          | {:add_write_ref, hash :: binary(), write_id :: String.t()}
          | {:remove_write_ref, hash :: binary(), write_id :: String.t()}
          | {:put_volume, volume_data :: map()}
          | {:delete_volume, volume_id :: binary()}
          | {:put_file, file_meta :: map()}
          | {:update_file, file_id :: binary(), updates :: map()}
          | {:delete_file, file_id :: binary()}

  @type state :: %{
          data: %{optional(term()) => term()},
          chunks: %{optional(binary()) => map()},
          files: %{optional(binary()) => map()},
          volumes: %{optional(binary()) => map()},
          version: non_neg_integer()
        }

  @doc """
  Initialize the state machine with an empty data map.
  """
  @impl :ra_machine
  def init(_config) do
    %{
      data: %{},
      chunks: %{},
      files: %{},
      volumes: %{},
      version: 0
    }
  end

  @doc """
  Apply a command to the state machine.

  Commands:
  - `{:put, key, value}` - Store a key-value pair
  - `{:delete, key}` - Remove a key
  """
  @impl :ra_machine
  def apply(_meta, {:put, key, value}, state) do
    new_data = Map.put(state.data, key, value)
    new_state = %{state | data: new_data, version: state.version + 1}

    # Emit telemetry
    :telemetry.execute(
      [:neonfs, :ra, :command, :put],
      %{version: new_state.version},
      %{key: key}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:delete, key}, state) do
    new_data = Map.delete(state.data, key)
    new_state = %{state | data: new_data, version: state.version + 1}

    # Emit telemetry
    :telemetry.execute(
      [:neonfs, :ra, :command, :delete],
      %{version: new_state.version},
      %{key: key}
    )

    {new_state, :ok, []}
  end

  # Handle Ra builtin command for machine version upgrades
  def apply(_meta, {:machine_version, from_version, to_version}, state) do
    # Ra sends this command when upgrading the state machine version
    # For now, we don't need to migrate state, just acknowledge the upgrade
    require Logger

    Logger.info("Ra machine version upgrade: #{from_version} -> #{to_version}")

    {state, :ok, []}
  end

  def apply(_meta, {:put_chunk, chunk_meta}, state) do
    hash = chunk_meta.hash
    new_chunks = Map.put(state.chunks, hash, chunk_meta)
    new_state = %{state | chunks: new_chunks, version: state.version + 1}

    # Emit telemetry
    :telemetry.execute(
      [:neonfs, :ra, :command, :put_chunk],
      %{version: new_state.version},
      %{hash: hash}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:update_chunk_locations, hash, locations}, state) do
    case Map.get(state.chunks, hash) do
      nil ->
        {state, {:error, :not_found}, []}

      chunk_meta ->
        updated_meta = Map.put(chunk_meta, :locations, locations)
        new_chunks = Map.put(state.chunks, hash, updated_meta)
        new_state = %{state | chunks: new_chunks, version: state.version + 1}

        # Emit telemetry
        :telemetry.execute(
          [:neonfs, :ra, :command, :update_chunk_locations],
          %{version: new_state.version},
          %{hash: hash, location_count: length(locations)}
        )

        {new_state, :ok, []}
    end
  end

  def apply(_meta, {:delete_chunk, hash}, state) do
    new_chunks = Map.delete(state.chunks, hash)
    new_state = %{state | chunks: new_chunks, version: state.version + 1}

    # Emit telemetry
    :telemetry.execute(
      [:neonfs, :ra, :command, :delete_chunk],
      %{version: new_state.version},
      %{hash: hash}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:commit_chunk, hash}, state) do
    case Map.get(state.chunks, hash) do
      nil ->
        {state, {:error, :not_found}, []}

      chunk_meta ->
        # Check if there are active write refs
        active_write_refs = Map.get(chunk_meta, :active_write_refs, MapSet.new())

        if MapSet.size(active_write_refs) == 0 do
          updated_meta = Map.put(chunk_meta, :commit_state, :committed)
          new_chunks = Map.put(state.chunks, hash, updated_meta)
          new_state = %{state | chunks: new_chunks, version: state.version + 1}

          # Emit telemetry
          :telemetry.execute(
            [:neonfs, :ra, :command, :commit_chunk],
            %{version: new_state.version},
            %{hash: hash}
          )

          {new_state, :ok, []}
        else
          {state, {:error, :has_active_writes}, []}
        end
    end
  end

  def apply(_meta, {:add_write_ref, hash, write_id}, state) do
    case Map.get(state.chunks, hash) do
      nil ->
        {state, {:error, :not_found}, []}

      chunk_meta ->
        active_write_refs = Map.get(chunk_meta, :active_write_refs, MapSet.new())
        updated_refs = MapSet.put(active_write_refs, write_id)
        updated_meta = Map.put(chunk_meta, :active_write_refs, updated_refs)
        new_chunks = Map.put(state.chunks, hash, updated_meta)
        new_state = %{state | chunks: new_chunks, version: state.version + 1}

        # Emit telemetry
        :telemetry.execute(
          [:neonfs, :ra, :command, :add_write_ref],
          %{version: new_state.version},
          %{hash: hash, write_id: write_id}
        )

        {new_state, :ok, []}
    end
  end

  def apply(_meta, {:remove_write_ref, hash, write_id}, state) do
    case Map.get(state.chunks, hash) do
      nil ->
        {state, {:error, :not_found}, []}

      chunk_meta ->
        active_write_refs = Map.get(chunk_meta, :active_write_refs, MapSet.new())
        updated_refs = MapSet.delete(active_write_refs, write_id)
        updated_meta = Map.put(chunk_meta, :active_write_refs, updated_refs)
        new_chunks = Map.put(state.chunks, hash, updated_meta)
        new_state = %{state | chunks: new_chunks, version: state.version + 1}

        # Emit telemetry
        :telemetry.execute(
          [:neonfs, :ra, :command, :remove_write_ref],
          %{version: new_state.version},
          %{hash: hash, write_id: write_id}
        )

        {new_state, :ok, []}
    end
  end

  def apply(_meta, {:put_volume, volume_data}, state) do
    # Ensure volumes map exists (for backwards compatibility with existing Ra state)
    volumes = Map.get(state, :volumes, %{})
    id = volume_data.id
    new_volumes = Map.put(volumes, id, volume_data)
    new_state = %{state | volumes: new_volumes, version: state.version + 1}

    # Emit telemetry
    :telemetry.execute(
      [:neonfs, :ra, :command, :put_volume],
      %{version: new_state.version},
      %{id: id, name: volume_data.name}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:delete_volume, volume_id}, state) do
    # Ensure volumes map exists (for backwards compatibility with existing Ra state)
    volumes = Map.get(state, :volumes, %{})
    new_volumes = Map.delete(volumes, volume_id)
    new_state = %{state | volumes: new_volumes, version: state.version + 1}

    # Emit telemetry
    :telemetry.execute(
      [:neonfs, :ra, :command, :delete_volume],
      %{version: new_state.version},
      %{id: volume_id}
    )

    {new_state, :ok, []}
  end

  # File metadata commands

  def apply(_meta, {:put_file, file_meta}, state) do
    # Ensure files map exists (for backwards compatibility with existing Ra state)
    files = Map.get(state, :files, %{})
    id = file_meta.id
    new_files = Map.put(files, id, file_meta)
    new_state = %{state | files: new_files, version: state.version + 1}

    # Emit telemetry
    :telemetry.execute(
      [:neonfs, :ra, :command, :put_file],
      %{version: new_state.version},
      %{id: id, path: file_meta[:path]}
    )

    {new_state, :ok, []}
  end

  def apply(_meta, {:update_file, file_id, updates}, state) do
    files = Map.get(state, :files, %{})

    case Map.get(files, file_id) do
      nil ->
        {state, {:error, :not_found}, []}

      file_meta ->
        updated_meta = Map.merge(file_meta, updates)
        new_files = Map.put(files, file_id, updated_meta)
        new_state = %{state | files: new_files, version: state.version + 1}

        # Emit telemetry
        :telemetry.execute(
          [:neonfs, :ra, :command, :update_file],
          %{version: new_state.version},
          %{id: file_id}
        )

        {new_state, :ok, []}
    end
  end

  def apply(_meta, {:delete_file, file_id}, state) do
    files = Map.get(state, :files, %{})
    new_files = Map.delete(files, file_id)
    new_state = %{state | files: new_files, version: state.version + 1}

    # Emit telemetry
    :telemetry.execute(
      [:neonfs, :ra, :command, :delete_file],
      %{version: new_state.version},
      %{id: file_id}
    )

    {new_state, :ok, []}
  end

  @doc """
  Handle state transitions. Called when the Ra server enters a new state.
  """
  @impl :ra_machine
  def state_enter(_state_name, _state) do
    # Called when the Ra server enters a new state (follower, candidate, leader)
    # No special handling needed for now
    []
  end

  @doc """
  Called when a snapshot is installed (e.g., during cluster catch-up).

  Returns a list of effects to execute after snapshot installation.
  """
  @impl :ra_machine
  def snapshot_installed(_meta, _state, _old_meta, _old_state) do
    # No effects needed after snapshot installation
    []
  end

  @doc """
  Return the state machine version for upgrade/migration support.
  """
  @impl :ra_machine
  def version, do: 1

  @doc """
  Return the module to handle a specific state machine version.

  All versions use this same module - we don't have multiple versions yet.
  """
  @impl :ra_machine
  def which_module(_version), do: __MODULE__
end
