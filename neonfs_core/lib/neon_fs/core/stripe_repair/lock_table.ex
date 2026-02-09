defmodule NeonFS.Core.StripeRepair.LockTable do
  @moduledoc """
  ETS-based lock table to prevent concurrent repairs of the same stripe.

  Node-local: each node manages its own repair locks.
  """

  @table :stripe_repair_locks

  @doc """
  Initialises the lock table. Called once during application startup.
  """
  @spec init() :: :ok
  def init do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [:named_table, :set, :public])
    end

    :ok
  end

  @doc """
  Acquires a repair lock for the given stripe ID.

  Returns `:ok` if the lock was acquired, or `{:error, :repair_in_progress}`
  if another repair is already running for this stripe.
  """
  @spec acquire_lock(binary()) :: :ok | {:error, :repair_in_progress}
  def acquire_lock(stripe_id) when is_binary(stripe_id) do
    init()

    case :ets.insert_new(@table, {stripe_id, self(), System.monotonic_time(:millisecond)}) do
      true -> :ok
      false -> {:error, :repair_in_progress}
    end
  end

  @doc """
  Releases a repair lock for the given stripe ID.
  """
  @spec release_lock(binary()) :: :ok
  def release_lock(stripe_id) when is_binary(stripe_id) do
    :ets.delete(@table, stripe_id)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Checks if a stripe is currently being repaired.
  """
  @spec locked?(binary()) :: boolean()
  def locked?(stripe_id) when is_binary(stripe_id) do
    case :ets.lookup(@table, stripe_id) do
      [{^stripe_id, _pid, _time}] -> true
      [] -> false
    end
  rescue
    ArgumentError -> false
  end
end
