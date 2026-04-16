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
  def lock(path, scope, type, depth, owner, timeout) do
    init()
    now = System.system_time(:second)

    if conflict_with_existing?(path, depth, scope, now) do
      {:error, :conflict}
    else
      token = generate_token()

      lock_info = %{
        token: token,
        path: path,
        scope: scope,
        type: type,
        depth: depth,
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
  def get_locks_covering(path) do
    init()
    now = System.system_time(:second)

    :ets.tab2list(@table)
    |> Enum.filter(fn {_token, info} ->
      info.expires_at > now and covers?(info, path)
    end)
    |> Enum.map(fn {_token, info} -> info end)
  end

  @impl true
  def get_descendant_locks(path) do
    init()
    now = System.system_time(:second)

    :ets.tab2list(@table)
    |> Enum.filter(fn {_token, info} ->
      info.expires_at > now and descendant?(info.path, path)
    end)
    |> Enum.map(fn {_token, info} -> info end)
  end

  @impl true
  def check_token(path, token) do
    init()
    now = System.system_time(:second)

    case :ets.lookup(@table, token) do
      [{^token, %{expires_at: expires_at} = info}] when expires_at > now ->
        if covers?(info, path), do: :ok, else: {:error, :invalid_token}

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

  defp covers?(%{path: lock_path}, lock_path), do: true

  defp covers?(%{path: lock_path, depth: :infinity}, target_path) do
    List.starts_with?(target_path, lock_path) and length(target_path) > length(lock_path)
  end

  defp covers?(_, _), do: false

  defp descendant?(lock_path, ancestor_path) do
    List.starts_with?(lock_path, ancestor_path) and length(lock_path) > length(ancestor_path)
  end

  defp conflict_with_existing?(path, depth, scope, now) do
    :ets.tab2list(@table)
    |> Enum.filter(fn {_t, info} -> info.expires_at > now end)
    |> Enum.any?(fn {_t, info} ->
      overlaps?(info, path, depth) and scope_conflicts?(info.scope, scope)
    end)
  end

  defp overlaps?(%{path: existing_path, depth: existing_depth}, path, depth) do
    cond do
      existing_path == path -> true
      depth == :infinity and List.starts_with?(existing_path, path) -> true
      existing_depth == :infinity and List.starts_with?(path, existing_path) -> true
      true -> false
    end
  end

  defp scope_conflicts?(:exclusive, _), do: true
  defp scope_conflicts?(_, :exclusive), do: true
  defp scope_conflicts?(_, _), do: false

  defp generate_token, do: Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)
end
