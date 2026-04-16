defmodule NeonFS.WebDAV.LockStore do
  @moduledoc """
  WebDAV lock store backed by the distributed lock manager.

  Translates WebDAV LOCK/UNLOCK operations into DLM byte-range locks
  so that file locks are visible across all access protocols (FUSE,
  NFS, S3, WebDAV).

  WebDAV exclusive write locks become DLM exclusive full-file locks.
  WebDAV shared locks become DLM shared full-file locks.

  For non-existent files (lock-null resources per RFC 4918 §7.3), a
  deterministic path-based ID is generated so the DLM can coordinate
  across WebDAV nodes. Lock-null resources are visible in PROPFIND but
  return 404 on GET. When a PUT creates the real file, the lock is
  promoted to use the actual file ID.

  A local ETS table indexes lock tokens to their DLM state. If the
  WebDAV node restarts, the ETS table is lost but DLM locks expire
  via TTL — clients simply re-lock.
  """

  @behaviour WebdavServer.LockStore

  alias NeonFS.Client.Router

  require Logger

  @table __MODULE__

  # Full-file lock range — large enough to cover any file.
  @full_file_range {0, 0xFFFFFFFFFFFFFFFF}

  # Prefix for deterministic lock-null IDs to avoid collision with real file IDs.
  @lock_null_prefix "lock-null:"

  # --- Lifecycle ---

  @doc """
  Initialise the ETS table. Call once during application startup.
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

  # --- LockStore callbacks ---

  @impl true
  def lock(path, scope, type, owner, timeout) do
    init()

    case resolve_file_id(path) do
      {:ok, file_id, :existing} ->
        lock_via_dlm(file_id, path, scope, type, owner, timeout, false)

      {:ok, path_id, :lock_null} ->
        lock_via_dlm(path_id, path, scope, type, owner, timeout, true)

      :local_only ->
        lock_local(path, scope, type, owner, timeout)
    end
  end

  @impl true
  def unlock(token) do
    init()

    case :ets.lookup(@table, token) do
      [{^token, lock_info}] ->
        maybe_unlock_dlm(lock_info)
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
          maybe_renew_dlm(lock_info, timeout)
          updated = %{lock_info | timeout: timeout, expires_at: now + timeout}
          :ets.insert(@table, {token, updated})
          {:ok, updated |> Map.delete(:file_id) |> Map.delete(:lock_null)}
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

  # --- Lock-null resource queries ---

  @doc """
  Check whether the given path has an active lock-null lock.
  """
  @spec lock_null?(WebdavServer.LockStore.path()) :: boolean()
  def lock_null?(path) do
    init()
    now = System.system_time(:second)

    :ets.tab2list(@table)
    |> Enum.any?(fn {_token, info} ->
      info.path == path and info.lock_null == true and info.expires_at > now
    end)
  end

  @doc """
  Return all active lock-null paths that are direct children of the given
  parent directory. Used by the backend to include lock-null resources in
  PROPFIND directory listings.

  `parent_path` is a list of path segments for the parent (e.g. `["vol"]`
  for the volume root, or `["vol", "dir"]` for a subdirectory).
  """
  @spec get_lock_null_paths([String.t()]) :: [[String.t()]]
  def get_lock_null_paths(parent_path) do
    init()
    now = System.system_time(:second)
    parent_len = length(parent_path)

    :ets.tab2list(@table)
    |> Enum.filter(fn {_token, info} ->
      info.lock_null == true and info.expires_at > now and
        length(info.path) == parent_len + 1 and
        List.starts_with?(info.path, parent_path)
    end)
    |> Enum.map(fn {_token, info} -> info.path end)
    |> Enum.uniq()
  end

  @doc """
  Promote a lock-null resource to a regular locked resource after the file
  has been created. Acquires a DLM lock on the real file ID, releases the
  path-based lock-null DLM lock, and updates the ETS entry.
  """
  @spec promote_lock_null(WebdavServer.LockStore.path(), String.t()) :: :ok
  def promote_lock_null(path, real_file_id) do
    init()
    now = System.system_time(:second)

    :ets.tab2list(@table)
    |> Enum.filter(fn {_token, info} ->
      info.path == path and info.lock_null == true and info.expires_at > now
    end)
    |> Enum.each(fn {token, info} ->
      remaining_ttl_ms = max((info.expires_at - now) * 1000, 1000)
      lock_type = scope_to_lock_type(info.scope)

      case call_lock_manager(:lock, [
             real_file_id,
             token,
             @full_file_range,
             lock_type,
             [ttl: remaining_ttl_ms]
           ]) do
        :ok ->
          call_lock_manager(:unlock, [info.file_id, token, @full_file_range])
          updated = %{info | file_id: real_file_id, lock_null: false}
          :ets.insert(@table, {token, updated})

        {:error, reason} ->
          Logger.warning(
            "Failed to promote lock-null to file #{real_file_id}: #{inspect(reason)}"
          )
      end
    end)

    :ok
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

  # --- DLM integration ---

  defp lock_via_dlm(file_id, path, scope, type, owner, timeout, lock_null?) do
    token = generate_token()
    lock_type = scope_to_lock_type(scope)
    ttl_ms = timeout * 1000

    case call_lock_manager(:lock, [file_id, token, @full_file_range, lock_type, [ttl: ttl_ms]]) do
      :ok ->
        now = System.system_time(:second)

        lock_info = %{
          token: token,
          path: path,
          scope: scope,
          type: type,
          owner: owner,
          timeout: timeout,
          expires_at: now + timeout,
          file_id: file_id,
          lock_null: lock_null?
        }

        :ets.insert(@table, {token, lock_info})
        {:ok, token}

      {:error, :timeout} ->
        {:error, :conflict}

      {:error, _reason} ->
        {:error, :conflict}
    end
  end

  defp lock_local(path, scope, type, owner, timeout) do
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
        expires_at: now + timeout,
        file_id: nil,
        lock_null: false
      }

      :ets.insert(@table, {token, lock_info})
      {:ok, token}
    end
  end

  defp maybe_unlock_dlm(%{file_id: nil}), do: :ok

  defp maybe_unlock_dlm(%{file_id: file_id, token: token}) do
    case call_lock_manager(:unlock, [file_id, token, @full_file_range]) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to release DLM lock for file #{file_id}: #{inspect(reason)}")

        :ok
    end
  end

  defp maybe_renew_dlm(%{file_id: nil}, _timeout), do: :ok

  defp maybe_renew_dlm(%{file_id: file_id, token: token}, timeout) do
    ttl_ms = timeout * 1000

    case call_lock_manager(:renew, [file_id, token, [ttl: ttl_ms]]) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to renew DLM lock for file #{file_id}: #{inspect(reason)}")

        :ok
    end
  end

  # --- Path resolution ---

  defp resolve_file_id([volume_name | rest]) when rest != [] do
    file_path = "/" <> Enum.join(rest, "/")

    case call_core(:get_file_meta, [volume_name, file_path]) do
      {:ok, meta} ->
        {:ok, meta.id, :existing}

      {:error, :not_found} ->
        {:ok, generate_path_id(volume_name, file_path), :lock_null}

      {:error, _} ->
        :local_only
    end
  rescue
    _ -> :local_only
  end

  defp resolve_file_id(_path), do: :local_only

  # --- Helpers ---

  defp get_active_locks(path, now) do
    :ets.tab2list(@table)
    |> Enum.filter(fn {_token, info} -> info.path == path and info.expires_at > now end)
    |> Enum.map(fn {_token, info} -> info |> Map.delete(:file_id) |> Map.delete(:lock_null) end)
  end

  defp conflict?(existing_locks, :exclusive), do: existing_locks != []

  defp conflict?(existing_locks, :shared),
    do: Enum.any?(existing_locks, &(&1.scope == :exclusive))

  defp scope_to_lock_type(:exclusive), do: :exclusive
  defp scope_to_lock_type(:shared), do: :shared

  defp call_core(function, args) do
    case Application.get_env(:neonfs_webdav, :core_call_fn) do
      nil -> Router.call(NeonFS.Core, function, args)
      fun when is_function(fun, 2) -> fun.(function, args)
    end
  end

  defp call_lock_manager(function, args) do
    case Application.get_env(:neonfs_webdav, :lock_manager_call_fn) do
      nil -> Router.call(NeonFS.Core.LockManager, function, args)
      fun when is_function(fun, 2) -> fun.(function, args)
    end
  end

  defp generate_token, do: Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)

  defp generate_path_id(volume_name, file_path) do
    @lock_null_prefix <>
      (:crypto.hash(:sha256, "#{volume_name}:#{file_path}")
       |> Base.encode16(case: :lower))
  end
end
