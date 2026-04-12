defmodule WebdavServer.LockStore.ETS do
  @moduledoc """
  ETS-based in-memory lock store for single-node deployments.

  Locks are stored in a named ETS table with TTL-based expiry.
  Expired locks are cleaned up lazily on access.
  """

  @behaviour WebdavServer.LockStore

  @table __MODULE__

  @doc """
  Initialise the ETS table. Call this once during application startup
  or in your test setup.
  """
  @spec init() :: :ok
  def init do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])
    end

    :ok
  end

  @doc """
  Remove all locks. Useful in tests.
  """
  @spec reset() :: :ok
  def reset do
    init()
    :ets.delete_all_objects(@table)
    :ok
  end

  @impl true
  def lock(path, scope, type, owner, timeout) do
    init()
    now = System.system_time(:second)
    existing = get_active_locks(path, now)

    if conflict?(existing, scope) do
      {:error, :conflict}
    else
      token = generate_token()

      lock_info = %{
        token: token,
        path: path,
        scope: scope,
        type: type,
        owner: owner,
        timeout: timeout,
        expires_at: now + timeout
      }

      :ets.insert(@table, {token, lock_info})
      {:ok, token}
    end
  end

  @impl true
  def unlock(token) do
    init()

    case :ets.lookup(@table, token) do
      [{^token, _}] ->
        :ets.delete(@table, token)
        :ok

      [] ->
        {:error, :not_found}
    end
  end

  @impl true
  def refresh(token, timeout) do
    init()
    now = System.system_time(:second)

    case :ets.lookup(@table, token) do
      [{^token, lock_info}] ->
        if lock_info.expires_at > now do
          updated = %{lock_info | timeout: timeout, expires_at: now + timeout}
          :ets.insert(@table, {token, updated})
          {:ok, updated}
        else
          :ets.delete(@table, token)
          {:error, :not_found}
        end

      [] ->
        {:error, :not_found}
    end
  end

  @impl true
  def get_locks(path) do
    init()
    now = System.system_time(:second)
    get_active_locks(path, now)
  end

  @impl true
  def check_token(path, token) do
    init()
    now = System.system_time(:second)

    case :ets.lookup(@table, token) do
      [{^token, %{path: ^path, expires_at: expires_at}}] when expires_at > now ->
        :ok

      [{^token, %{expires_at: expires_at}}] when expires_at <= now ->
        :ets.delete(@table, token)
        {:error, :invalid_token}

      _ ->
        {:error, :invalid_token}
    end
  end

  defp get_active_locks(path, now) do
    :ets.tab2list(@table)
    |> Enum.filter(fn {_token, info} -> info.path == path and info.expires_at > now end)
    |> Enum.map(fn {_token, info} -> info end)
  end

  defp conflict?(existing_locks, :exclusive), do: existing_locks != []

  defp conflict?(existing_locks, :shared),
    do: Enum.any?(existing_locks, &(&1.scope == :exclusive))

  defp generate_token, do: Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)
end
