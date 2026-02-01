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

  @type state :: %{
          data: %{optional(term()) => term()},
          chunks: %{optional(binary()) => map()},
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
  Take a snapshot of the current state for persistence.
  """
  @impl :ra_machine
  def snapshot_installed(_snapshot_meta, _snapshot_data, _snapshot_ref, state) do
    # Called when a snapshot is installed (e.g., during cluster catch-up)
    {state, :ok}
  end

  @doc """
  Return the state machine version for upgrade/migration support.
  """
  @impl :ra_machine
  def version, do: 1

  @doc """
  Return the module to handle a specific state machine version.
  """
  @impl :ra_machine
  def which_module(1), do: __MODULE__
end
