defmodule NeonFS.WebDAV.LockStore do
  @moduledoc """
  WebDAV lock store backed by the distributed lock manager and the
  namespace coordinator.

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

  ## Collection locks (`Depth: infinity`)

  Hierarchical conflict detection cannot be expressed against the DLM
  alone — the DLM coordinates content-level access by file ID, while a
  collection lock claims a region of the **namespace**. WebDAV LOCK
  with `Depth: infinity` therefore acquires a
  `NeonFS.Core.NamespaceCoordinator` subtree claim alongside the DLM
  byte-range lock; this is replicated across every core node via Ra,
  so a `Depth: infinity` lock on `/docs` taken from WebDAV node A is
  visible to node B's next claim attempt. Issue #226 / sub-issue #301.

  The claim's holder is `NeonFS.WebDAV.LockStore.NamespaceHolder` —
  one stable pid per WebDAV node. Coordinator-side cleanup runs when
  the holder dies (WebDAV node crash / disconnect), so stale subtree
  claims don't outlive the node that took them.

  When the coordinator is unreachable (e.g. test setups without a Ra
  cluster or transient outages), the lock store falls back to its
  local ETS hierarchical scan — single-node correctness is preserved
  even if cross-node coordination is temporarily unavailable.

  A local ETS table indexes lock tokens to their DLM state. If the
  WebDAV node restarts, the ETS table is lost but DLM locks expire
  via TTL — clients simply re-lock.
  """

  @behaviour Davy.LockStore

  alias NeonFS.Client.Router
  alias NeonFS.WebDAV.LockStore.NamespaceHolder

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
  def lock(path, scope, type, depth, owner, timeout) do
    init()

    case acquire_namespace_claim(path, depth, scope) do
      {:error, :conflict} ->
        {:error, :conflict}

      result ->
        # `result` is either {:ok, claim_id} or :coordinator_unavailable;
        # either way we still need the local pre-check (it covers
        # depth=0 LOCKs that don't take a namespace claim, and serves
        # as the single-node fallback when the coordinator is down).
        if local_conflict?(path, depth, scope) do
          maybe_release_claim(result)
          {:error, :conflict}
        else
          claim_id = claim_id_from(result)

          do_lock(path, scope, type, depth, owner, timeout, claim_id)
        end
    end
  end

  defp do_lock(path, scope, type, depth, owner, timeout, claim_id) do
    attrs = %{
      path: path,
      scope: scope,
      type: type,
      depth: depth,
      owner: owner,
      timeout: timeout,
      namespace_claim_id: claim_id
    }

    case resolve_file_id(path) do
      {:ok, file_id, :existing} -> lock_via_dlm(file_id, attrs, false)
      {:ok, path_id, :lock_null} -> lock_via_dlm(path_id, attrs, true)
      :local_only -> lock_local(attrs)
    end
  end

  defp claim_id_from({:ok, claim_id}), do: claim_id
  defp claim_id_from(_), do: nil

  defp maybe_release_claim({:ok, claim_id}) when is_binary(claim_id) do
    _ = call_namespace_coordinator(:release, [claim_id])
    :ok
  end

  defp maybe_release_claim(_), do: :ok

  @impl true
  def unlock(token) do
    init()

    case :ets.lookup(@table, token) do
      [{^token, lock_info}] ->
        maybe_unlock_dlm(lock_info)
        maybe_release_claim_id(Map.get(lock_info, :namespace_claim_id))
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
          {:ok, public_lock_info(updated)}
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
    |> Enum.map(fn {_token, info} -> public_lock_info(info) end)
  end

  @impl true
  def get_descendant_locks(path) do
    init()
    now = System.system_time(:second)

    :ets.tab2list(@table)
    |> Enum.filter(fn {_token, info} ->
      info.expires_at > now and descendant?(info.path, path)
    end)
    |> Enum.map(fn {_token, info} -> public_lock_info(info) end)
  end

  # --- Lock-null resource queries ---

  @doc """
  Check whether the given path has an active lock-null lock.
  """
  @spec lock_null?(Davy.LockStore.path()) :: boolean()
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
  @spec promote_lock_null(Davy.LockStore.path(), String.t()) :: :ok
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
      [{^token, %{expires_at: expires_at} = info}] when expires_at > now ->
        if covers?(info, path), do: :ok, else: {:error, :invalid_token}

      [{^token, %{expires_at: expires_at}}] when expires_at <= now ->
        :ets.delete(@table, token)
        {:error, :invalid_token}

      _ ->
        {:error, :invalid_token}
    end
  end

  # --- DLM integration ---

  defp lock_via_dlm(file_id, attrs, lock_null?) do
    token = generate_token()
    lock_type = scope_to_lock_type(attrs.scope)
    ttl_ms = attrs.timeout * 1000

    case call_lock_manager(:lock, [file_id, token, @full_file_range, lock_type, [ttl: ttl_ms]]) do
      :ok ->
        :ets.insert(@table, {token, build_lock_info(attrs, token, file_id, lock_null?)})
        {:ok, token}

      {:error, _reason} ->
        # `:timeout` and any other DLM rejection collapse into a WebDAV
        # `:conflict`. Release the namespace claim we may already hold.
        maybe_release_claim_id(attrs.namespace_claim_id)
        {:error, :conflict}
    end
  end

  defp lock_local(attrs) do
    token = generate_token()
    :ets.insert(@table, {token, build_lock_info(attrs, token, nil, false)})
    {:ok, token}
  end

  defp build_lock_info(attrs, token, file_id, lock_null?) do
    now = System.system_time(:second)

    %{
      token: token,
      path: attrs.path,
      scope: attrs.scope,
      type: attrs.type,
      depth: attrs.depth,
      owner: attrs.owner,
      timeout: attrs.timeout,
      expires_at: now + attrs.timeout,
      file_id: file_id,
      lock_null: lock_null?,
      namespace_claim_id: attrs.namespace_claim_id
    }
  end

  defp maybe_release_claim_id(nil), do: :ok

  defp maybe_release_claim_id(claim_id) when is_binary(claim_id) do
    maybe_release_claim({:ok, claim_id})
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

  # Strip fields that are bookkeeping for the lock store and not part
  # of the `Davy.LockStore` reply contract.
  defp public_lock_info(info) do
    Map.drop(info, [:file_id, :lock_null, :namespace_claim_id])
  end

  defp get_active_locks(path, now) do
    :ets.tab2list(@table)
    |> Enum.filter(fn {_token, info} -> info.path == path and info.expires_at > now end)
    |> Enum.map(fn {_token, info} -> public_lock_info(info) end)
  end

  defp covers?(%{path: lock_path}, lock_path), do: true

  defp covers?(%{path: lock_path, depth: :infinity}, target_path) do
    List.starts_with?(target_path, lock_path) and length(target_path) > length(lock_path)
  end

  defp covers?(_, _), do: false

  defp descendant?(lock_path, ancestor_path) do
    List.starts_with?(lock_path, ancestor_path) and length(lock_path) > length(ancestor_path)
  end

  defp local_conflict?(path, depth, scope) do
    now = System.system_time(:second)

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

  defp call_namespace_coordinator(function, args) do
    case Application.get_env(:neonfs_webdav, :namespace_coordinator_call_fn) do
      nil ->
        Router.call(NeonFS.Core.NamespaceCoordinator, function, args)

      fun when is_function(fun, 2) ->
        fun.(function, args)
    end
  catch
    _, _ -> {:error, :unavailable}
  end

  # Acquires a `claim_subtree_for/4` claim when the LOCK is `Depth: infinity`.
  # Returns:
  #   * `{:ok, claim_id}`           — claim acquired, store the id and release on UNLOCK
  #   * `{:error, :conflict}`       — coordinator reports a conflicting claim
  #   * `:coordinator_unavailable`  — coordinator unreachable; caller should
  #                                   fall back to single-node ETS conflict
  #                                   detection (preserves existing behaviour
  #                                   in tests / outages)
  #   * `:not_applicable`           — depth=0 or other case not migrated; no
  #                                   namespace claim is taken
  defp acquire_namespace_claim(_path, depth, _scope) when depth != :infinity do
    :not_applicable
  end

  defp acquire_namespace_claim(path, :infinity, scope) do
    case namespace_holder_pid() do
      pid when is_pid(pid) ->
        path_str = path_to_string(path)

        case call_namespace_coordinator(:claim_subtree_for, [path_str, scope, pid]) do
          {:ok, claim_id} when is_binary(claim_id) ->
            {:ok, claim_id}

          {:error, :conflict, _conflicting_id} ->
            {:error, :conflict}

          {:error, _reason} ->
            :coordinator_unavailable

          _ ->
            :coordinator_unavailable
        end

      _ ->
        :coordinator_unavailable
    end
  end

  defp namespace_holder_pid do
    case Application.get_env(:neonfs_webdav, :namespace_holder_pid_fn) do
      nil ->
        case GenServer.whereis(NamespaceHolder) do
          pid when is_pid(pid) -> pid
          _ -> nil
        end

      fun when is_function(fun, 0) ->
        fun.()
    end
  end

  defp path_to_string(path) when is_list(path) do
    "/" <> Enum.join(path, "/")
  end

  defp generate_token, do: Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)

  defp generate_path_id(volume_name, file_path) do
    @lock_null_prefix <>
      (:crypto.hash(:sha256, "#{volume_name}:#{file_path}")
       |> Base.encode16(case: :lower))
  end
end
