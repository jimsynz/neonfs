defmodule NeonFS.WebDAV.LockStore do
  @moduledoc """
  WebDAV lock store backed by the distributed lock manager, the
  namespace coordinator, and the cluster KV store.

  Translates WebDAV LOCK/UNLOCK operations into DLM byte-range locks
  so that file locks are visible across all access protocols (FUSE,
  NFS, S3, WebDAV).

  WebDAV exclusive write locks become DLM exclusive full-file locks.
  WebDAV shared locks become DLM shared full-file locks.

  For non-existent files (lock-null resources per RFC 4918 §7.3), the
  store takes a `NeonFS.Core.NamespaceCoordinator` `claim_path` instead
  of a DLM lock — content-addressed coordination is the wrong primitive
  for a name that has no content yet. The claim is replicated across
  every core node via Ra, so a lock-null on `/docs/draft.txt` taken
  from WebDAV node A is visible to node B's next claim attempt.
  Lock-null resources are visible in PROPFIND but return 404 on GET.
  When a PUT creates the real file, the namespace claim is released
  and a DLM lock is taken on the actual file ID. Issue #226 / sub-issue
  #302.

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

  ## Cluster-shared token index (#1178)

  The token → lock-state index lives in the Ra-backed cluster KV store
  under `webdav_lock:<token>` keys, so a load balancer can route a
  client's LOCK, writes carrying `If: <token>`, refresh, and UNLOCK to
  different WebDAV nodes. The KV index is bookkeeping — conflict
  arbitration stays with the DLM (existing files) and the namespace
  coordinator (lock-null and `Depth: infinity` claims), both of which
  are cluster-level already. The KV-scan conflict pre-check covers
  depth-0 locks on existing files between DLM acquisitions and serves
  as the conservative first gate.

  If a WebDAV node dies, namespace claims it held are released by the
  coordinator's holder-pid `:DOWN` cleanup, and DLM locks expire via
  TTL; the KV entries are swept by `LockStore.Cleaner` when they
  expire. Clients simply re-lock.
  """

  @behaviour Davy.LockStore

  alias NeonFS.Client.KV
  alias NeonFS.Client.Router
  alias NeonFS.WebDAV.LockStore.NamespaceHolder

  require Logger

  @key_prefix "webdav_lock:"

  # Full-file lock range — large enough to cover any file.
  @full_file_range {0, 0xFFFFFFFFFFFFFFFF}

  # --- Lifecycle ---

  @doc """
  Remove all locks. Useful in tests.
  """
  @spec reset() :: :ok
  def reset do
    case kv(:list_prefix, [@key_prefix]) do
      entries when is_list(entries) ->
        Enum.each(entries, fn {key, _info} -> kv(:delete, [key]) end)

      _ ->
        :ok
    end

    :ok
  end

  # --- LockStore callbacks ---

  @impl true
  def lock(path, scope, type, depth, owner, timeout) do
    resolution = resolve_file_id(path)

    case acquire_namespace_claim(path, depth, scope, resolution) do
      {:error, :conflict} ->
        {:error, :conflict}

      result ->
        # `result` is either {:ok, claim_id}, :coordinator_unavailable,
        # or :not_applicable; either way we still need the KV-scan
        # pre-check (it covers depth=0 LOCKs on existing files that
        # don't take a namespace claim).
        if conflict?(path, depth, scope) do
          maybe_release_claim(result)
          {:error, :conflict}
        else
          claim_id = claim_id_from(result)

          do_lock(resolution, path, scope, type, depth, owner, timeout, claim_id)
        end
    end
  end

  defp do_lock(resolution, path, scope, type, depth, owner, timeout, claim_id) do
    attrs = %{
      path: path,
      scope: scope,
      type: type,
      depth: depth,
      owner: owner,
      timeout: timeout,
      namespace_claim_id: claim_id
    }

    case resolution do
      {:existing, file_id} -> lock_via_dlm(file_id, attrs)
      :lock_null -> lock_lock_null(attrs)
      :local_only -> lock_unresolved(attrs)
    end
  end

  defp claim_id_from({:ok, claim_id}), do: claim_id
  defp claim_id_from(_), do: nil

  defp maybe_release_claim({:ok, claim_id}) when is_binary(claim_id) do
    _ = call_namespace_coordinator(:release, [claim_id])
    :ok
  end

  defp maybe_release_claim(_), do: :ok

  @doc """
  Best-effort release of a namespace coordinator claim by id. Used by
  the periodic `Cleaner` to release claims for entries it expires from
  the KV index — namespace claims have no TTL of their own, so
  forgetting them would leak the claim until the holder pid dies.

  Errors are swallowed: the cleaner has no way to surface a transient
  coordinator outage, and the holder-pid `:DOWN` cleanup on the
  coordinator side is the durable backstop.
  """
  @spec release_namespace_claim(String.t()) :: :ok
  def release_namespace_claim(claim_id) when is_binary(claim_id) do
    maybe_release_claim({:ok, claim_id})
  end

  @impl true
  def unlock(token) do
    case fetch_lock(token) do
      {:ok, lock_info} ->
        maybe_unlock_dlm(lock_info)
        maybe_release_claim_id(Map.get(lock_info, :namespace_claim_id))
        kv(:delete, [lock_key(token)])
        :ok

      :error ->
        {:error, :not_found}
    end
  end

  @impl true
  def refresh(token, timeout) do
    now = System.system_time(:second)

    case fetch_lock(token) do
      {:ok, lock_info} when lock_info.expires_at > now ->
        maybe_renew_dlm(lock_info, timeout)
        updated = %{lock_info | timeout: timeout, expires_at: now + timeout}

        case store_lock(updated) do
          :ok -> {:ok, public_lock_info(updated)}
          {:error, _reason} -> {:error, :not_found}
        end

      {:ok, _expired} ->
        kv(:delete, [lock_key(token)])
        {:error, :not_found}

      :error ->
        {:error, :not_found}
    end
  end

  @impl true
  def get_locks(path) do
    active_locks()
    |> Enum.filter(&(&1.path == path))
    |> Enum.map(&public_lock_info/1)
  end

  @impl true
  def get_locks_covering(path) do
    active_locks()
    |> Enum.filter(&covers?(&1, path))
    |> Enum.map(&public_lock_info/1)
  end

  @impl true
  def get_descendant_locks(path) do
    active_locks()
    |> Enum.filter(&descendant?(&1.path, path))
    |> Enum.map(&public_lock_info/1)
  end

  # --- Lock-null resource queries ---

  @doc """
  Check whether the given path has an active lock-null lock.
  """
  @spec lock_null?(Davy.LockStore.path()) :: boolean()
  def lock_null?(path) do
    active_locks()
    |> Enum.any?(&(&1.path == path and &1.lock_null == true))
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
    parent_len = length(parent_path)

    active_locks()
    |> Enum.filter(fn info ->
      info.lock_null == true and
        length(info.path) == parent_len + 1 and
        List.starts_with?(info.path, parent_path)
    end)
    |> Enum.map(& &1.path)
    |> Enum.uniq()
  end

  @doc """
  Promote a lock-null resource to a regular locked resource after the file
  has been created. Acquires a DLM lock on the real file ID, releases the
  namespace coordinator claim that was held while the path was lock-null,
  and updates the KV entry.
  """
  @spec promote_lock_null(Davy.LockStore.path(), String.t()) :: :ok
  def promote_lock_null(path, real_file_id) do
    now = System.system_time(:second)

    active_locks()
    |> Enum.filter(&(&1.path == path and &1.lock_null == true))
    |> Enum.each(fn info ->
      remaining_ttl_ms = max((info.expires_at - now) * 1000, 1000)
      lock_type = scope_to_lock_type(info.scope)

      case call_lock_manager(:lock, [
             real_file_id,
             info.token,
             @full_file_range,
             lock_type,
             [ttl: remaining_ttl_ms]
           ]) do
        :ok ->
          maybe_release_claim_id(info.namespace_claim_id)
          updated = %{info | file_id: real_file_id, lock_null: false, namespace_claim_id: nil}
          _ = store_lock(updated)
          :ok

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
    now = System.system_time(:second)

    case fetch_lock(token) do
      {:ok, %{expires_at: expires_at} = info} when expires_at > now ->
        if covers?(info, path), do: :ok, else: {:error, :invalid_token}

      {:ok, _expired} ->
        kv(:delete, [lock_key(token)])
        {:error, :invalid_token}

      :error ->
        {:error, :invalid_token}
    end
  end

  # --- KV index ---

  @doc """
  All non-expired lock entries from the cluster KV index. Also used by
  the `Cleaner` to find expired entries (with `include_expired: true`).
  """
  @spec active_locks(keyword()) :: [map()]
  def active_locks(opts \\ []) do
    now = System.system_time(:second)
    include_expired? = Keyword.get(opts, :include_expired, false)

    case kv(:list_prefix, [@key_prefix]) do
      entries when is_list(entries) ->
        infos = Enum.map(entries, fn {_key, info} -> info end)

        if include_expired? do
          infos
        else
          Enum.filter(infos, &(&1.expires_at > now))
        end

      {:error, reason} ->
        # A failed scan must not grant conflicting locks by reporting
        # "no locks" — but every conflict that matters is arbitrated by
        # the DLM or the namespace coordinator, which are queried on
        # the lock path regardless. Degrade to an empty view.
        Logger.warning("WebDAV lock index scan failed: #{inspect(reason)}")
        []
    end
  end

  @doc """
  Delete a lock entry from the KV index by token. Used by the `Cleaner`.
  """
  @spec delete_entry(String.t()) :: :ok
  def delete_entry(token) when is_binary(token) do
    kv(:delete, [lock_key(token)])
    :ok
  end

  defp fetch_lock(token) when is_binary(token) do
    case kv(:get, [lock_key(token)]) do
      {:ok, info} -> {:ok, info}
      {:error, _} -> :error
    end
  end

  defp fetch_lock(_token), do: :error

  defp store_lock(%{token: token} = info) do
    case kv(:put, [lock_key(token), info]) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp lock_key(token), do: @key_prefix <> token

  # --- DLM integration ---

  defp lock_via_dlm(file_id, attrs) do
    token = generate_token()
    lock_type = scope_to_lock_type(attrs.scope)
    ttl_ms = attrs.timeout * 1000

    case call_lock_manager(:lock, [file_id, token, @full_file_range, lock_type, [ttl: ttl_ms]]) do
      :ok ->
        persist_or_rollback(build_lock_info(attrs, token, file_id, false), fn ->
          maybe_unlock_dlm(%{file_id: file_id, token: token})
        end)

      {:error, _reason} ->
        # `:timeout` and any other DLM rejection collapse into a WebDAV
        # `:conflict`. Release the namespace claim we may already hold.
        maybe_release_claim_id(attrs.namespace_claim_id)
        {:error, :conflict}
    end
  end

  defp lock_lock_null(attrs) do
    token = generate_token()
    persist_or_rollback(build_lock_info(attrs, token, nil, true), fn -> :ok end)
  end

  # Path resolution failed (core transiently unreachable while looking
  # up the file). The KV write is the arbiter of whether the lock can
  # be granted at all — if the cluster is unreachable, the LOCK fails.
  defp lock_unresolved(attrs) do
    token = generate_token()
    persist_or_rollback(build_lock_info(attrs, token, nil, false), fn -> :ok end)
  end

  defp persist_or_rollback(info, rollback) do
    case store_lock(info) do
      :ok ->
        {:ok, info.token}

      {:error, reason} ->
        Logger.warning("Failed to persist WebDAV lock in KV index: #{inspect(reason)}")
        rollback.()
        maybe_release_claim_id(info.namespace_claim_id)
        {:error, :conflict}
    end
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
      {:ok, meta} -> {:existing, meta.id}
      {:error, :not_found} -> :lock_null
      {:error, _} -> :local_only
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

  defp covers?(%{path: lock_path}, lock_path), do: true

  defp covers?(%{path: lock_path, depth: :infinity}, target_path) do
    List.starts_with?(target_path, lock_path) and length(target_path) > length(lock_path)
  end

  defp covers?(_, _), do: false

  defp descendant?(lock_path, ancestor_path) do
    List.starts_with?(lock_path, ancestor_path) and length(lock_path) > length(ancestor_path)
  end

  defp conflict?(path, depth, scope) do
    active_locks()
    |> Enum.any?(fn info ->
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

  defp kv(function, args) do
    case Application.get_env(:neonfs_webdav, :kv_call_fn) do
      nil -> apply(KV, function, args)
      fun when is_function(fun, 2) -> fun.(function, args)
    end
  catch
    _, reason -> {:error, reason}
  end

  # Acquires a namespace coordinator claim for the LOCK request.
  #
  #   * `Depth: infinity`           → `claim_subtree_for/4`
  #   * lock-null (path not found)  → `claim_path_for/4`
  #   * everything else             → `:not_applicable`
  #
  # Return shape:
  #   * `{:ok, claim_id}`           — claim acquired, store the id and release on UNLOCK
  #   * `{:error, :conflict}`       — coordinator reports a conflicting claim
  #   * `:coordinator_unavailable`  — coordinator unreachable; the KV-scan
  #                                   pre-check is the remaining gate
  #   * `:not_applicable`           — no namespace claim is needed
  defp acquire_namespace_claim(path, :infinity, scope, _resolution) do
    do_acquire_claim(:claim_subtree_for, path, scope)
  end

  defp acquire_namespace_claim(path, _depth, scope, :lock_null) do
    do_acquire_claim(:claim_path_for, path, scope)
  end

  defp acquire_namespace_claim(_path, _depth, _scope, _resolution), do: :not_applicable

  defp do_acquire_claim(function, path, scope) do
    case namespace_holder_pid() do
      pid when is_pid(pid) ->
        path_str = path_to_string(path)

        case call_namespace_coordinator(function, [path_str, scope, pid]) do
          {:ok, claim_id} when is_binary(claim_id) ->
            {:ok, claim_id}

          {:error, %NeonFS.Error.Conflict{}} ->
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
end
