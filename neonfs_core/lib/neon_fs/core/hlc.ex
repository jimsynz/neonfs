defmodule NeonFS.Core.HLC do
  @moduledoc """
  Hybrid Logical Clock (HLC) for conflict resolution in the leaderless quorum
  metadata system.

  HLC combines wall clock time with a logical counter and node ID, ensuring
  monotonically increasing timestamps even under clock skew. Higher timestamps
  win during quorum reads (last-writer-wins).

  ## Timestamp Format

  A timestamp is a 3-tuple `{wall_ms, counter, node_id}`:
  - `wall_ms` — wall clock time in milliseconds
  - `counter` — logical counter for ordering events within the same millisecond
  - `node_id` — originating node for deterministic tiebreak

  ## State

  HLC state tracks the last issued timestamp to ensure monotonicity. All
  functions are pure — no side effects or GenServer. The caller is responsible
  for threading state through calls.

  ## Clock Skew Bounds

  The `max_clock_skew_ms` option (default 1000ms) bounds how far ahead a
  timestamp's wall component can be from the current wall clock. This prevents
  runaway timestamps from propagating through the cluster.
  """

  @type timestamp ::
          {wall_ms :: non_neg_integer(), counter :: non_neg_integer(), node_id :: node()}

  @type t :: %__MODULE__{
          node_id: node(),
          last_wall: non_neg_integer(),
          last_counter: non_neg_integer(),
          max_clock_skew_ms: non_neg_integer()
        }

  @default_max_clock_skew_ms 1_000

  defstruct node_id: nil,
            last_wall: 0,
            last_counter: 0,
            max_clock_skew_ms: @default_max_clock_skew_ms

  @doc """
  Creates initial HLC state for the given node.

  ## Options

    * `:max_clock_skew_ms` - Maximum allowed clock skew in milliseconds
      (default: #{@default_max_clock_skew_ms})
  """
  @spec new(node(), keyword()) :: t()
  def new(node_id, opts \\ []) do
    max_skew = Keyword.get(opts, :max_clock_skew_ms, @default_max_clock_skew_ms)

    %__MODULE__{
      node_id: node_id,
      max_clock_skew_ms: max_skew
    }
  end

  @doc """
  Generates a new HLC timestamp, advancing the state.

  Ensures monotonicity: if the wall clock hasn't advanced past the last
  timestamp, the logical counter is incremented. The wall time component is
  bounded to `wall + max_clock_skew_ms` to prevent runaway timestamps.

  Returns `{timestamp, new_state}`.
  """
  @spec now(t()) :: {timestamp(), t()}
  def now(%__MODULE__{} = state) do
    now(state, System.system_time(:millisecond))
  end

  @doc """
  Generates a new HLC timestamp using a specific wall clock value.

  This variant is useful for testing with deterministic clock values.
  """
  @spec now(t(), non_neg_integer()) :: {timestamp(), t()}
  def now(%__MODULE__{} = state, wall_ms) do
    max_wall = wall_ms + state.max_clock_skew_ms
    effective_wall = min(max(wall_ms, state.last_wall), max_wall)

    {counter, wall} =
      if effective_wall > state.last_wall do
        {0, effective_wall}
      else
        {state.last_counter + 1, state.last_wall}
      end

    timestamp = {wall, counter, state.node_id}
    new_state = %{state | last_wall: wall, last_counter: counter}

    {timestamp, new_state}
  end

  @doc """
  Incorporates a remote timestamp into the local HLC state.

  Returns `{:ok, timestamp, new_state}` if the remote timestamp is within the
  allowed skew bounds, or `{:error, :clock_skew_detected, skew_ms}` if the
  remote wall time exceeds local time by more than `max_clock_skew_ms`.
  """
  @spec receive_timestamp(t(), timestamp()) ::
          {:ok, timestamp(), t()} | {:error, :clock_skew_detected, non_neg_integer()}
  def receive_timestamp(%__MODULE__{} = state, remote_timestamp) do
    receive_timestamp(state, remote_timestamp, System.system_time(:millisecond))
  end

  @doc """
  Incorporates a remote timestamp using a specific local wall clock value.

  This variant is useful for testing with deterministic clock values.
  """
  @spec receive_timestamp(t(), timestamp(), non_neg_integer()) ::
          {:ok, timestamp(), t()} | {:error, :clock_skew_detected, non_neg_integer()}
  def receive_timestamp(
        %__MODULE__{} = state,
        {remote_wall, remote_counter, _remote_node},
        wall_ms
      ) do
    skew = remote_wall - wall_ms

    if skew > state.max_clock_skew_ms do
      {:error, :clock_skew_detected, skew}
    else
      max_wall = wall_ms + state.max_clock_skew_ms
      new_wall = min(max(wall_ms, max(state.last_wall, remote_wall)), max_wall)

      new_counter =
        cond do
          new_wall == state.last_wall and new_wall == remote_wall ->
            max(state.last_counter, remote_counter) + 1

          new_wall == state.last_wall ->
            state.last_counter + 1

          new_wall == remote_wall ->
            remote_counter + 1

          true ->
            0
        end

      timestamp = {new_wall, new_counter, state.node_id}
      new_state = %{state | last_wall: new_wall, last_counter: new_counter}

      {:ok, timestamp, new_state}
    end
  end

  @doc """
  Compares two HLC timestamps.

  Returns `:gt`, `:lt`, or `:eq`. Comparison order:
  1. Wall time (higher is greater)
  2. Logical counter (higher is greater)
  3. Node ID (lexicographic, for deterministic tiebreak)
  """
  @spec compare(timestamp(), timestamp()) :: :gt | :lt | :eq
  def compare({wall_a, counter_a, node_a}, {wall_b, counter_b, node_b}) do
    cond do
      wall_a > wall_b -> :gt
      wall_a < wall_b -> :lt
      counter_a > counter_b -> :gt
      counter_a < counter_b -> :lt
      node_a > node_b -> :gt
      node_a < node_b -> :lt
      true -> :eq
    end
  end

  @doc """
  Returns the higher of two HLC timestamps.

  Used during read repair and anti-entropy to select the most recent version.
  """
  @spec merge(timestamp(), timestamp()) :: timestamp()
  def merge(timestamp_a, timestamp_b) do
    case compare(timestamp_a, timestamp_b) do
      :gt -> timestamp_a
      :lt -> timestamp_b
      :eq -> timestamp_a
    end
  end

  @doc """
  Serialises an HLC timestamp to a compact binary.

  Format: 8 bytes wall_ms (big-endian) + 4 bytes counter (big-endian) +
  variable-length node_id (Erlang term_to_binary).
  """
  @spec to_binary(timestamp()) :: binary()
  def to_binary({wall_ms, counter, node_id}) do
    node_bin = :erlang.term_to_binary(node_id)
    <<wall_ms::unsigned-big-64, counter::unsigned-big-32, node_bin::binary>>
  end

  @doc """
  Deserialises an HLC timestamp from a binary produced by `to_binary/1`.
  """
  @spec from_binary(binary()) :: timestamp()
  def from_binary(<<wall_ms::unsigned-big-64, counter::unsigned-big-32, node_bin::binary>>) do
    node_id = :erlang.binary_to_term(node_bin)
    {wall_ms, counter, node_id}
  end
end
