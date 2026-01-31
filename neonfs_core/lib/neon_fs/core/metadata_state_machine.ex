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

  @type state :: %{
          data: %{optional(term()) => term()},
          version: non_neg_integer()
        }

  @doc """
  Initialize the state machine with an empty data map.
  """
  @impl :ra_machine
  def init(_config) do
    %{
      data: %{},
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
