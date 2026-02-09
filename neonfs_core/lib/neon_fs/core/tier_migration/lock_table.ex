defmodule NeonFS.Core.TierMigration.LockTable do
  @moduledoc """
  ETS-based lock table to prevent concurrent migrations of the same chunk.

  Node-local: each node manages its own migration locks. Cross-node lock
  coordination is not needed because each migration has a single "owning"
  node that runs the migration.
  """

  @table :tier_migration_locks

  @doc """
  Initializes the lock table. Called once during application startup.
  """
  @spec init() :: :ok
  def init do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [:named_table, :set, :public])
    end

    :ok
  end

  @doc """
  Acquires a migration lock for the given chunk hash.

  Returns `:ok` if the lock was acquired, or `{:error, :migration_in_progress}`
  if another migration is already running for this chunk.
  """
  @spec acquire_lock(binary()) :: :ok | {:error, :migration_in_progress}
  def acquire_lock(chunk_hash) when is_binary(chunk_hash) do
    init()

    case :ets.insert_new(@table, {chunk_hash, self(), System.monotonic_time(:millisecond)}) do
      true -> :ok
      false -> {:error, :migration_in_progress}
    end
  end

  @doc """
  Releases a migration lock for the given chunk hash.
  """
  @spec release_lock(binary()) :: :ok
  def release_lock(chunk_hash) when is_binary(chunk_hash) do
    :ets.delete(@table, chunk_hash)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Checks if a chunk is currently being migrated.
  """
  @spec locked?(binary()) :: boolean()
  def locked?(chunk_hash) when is_binary(chunk_hash) do
    case :ets.lookup(@table, chunk_hash) do
      [{^chunk_hash, _pid, _time}] -> true
      [] -> false
    end
  rescue
    ArgumentError -> false
  end

  @doc """
  Returns all currently held locks.
  """
  @spec list_locks() :: [{binary(), pid(), integer()}]
  def list_locks do
    :ets.tab2list(@table)
  rescue
    ArgumentError -> []
  end
end
