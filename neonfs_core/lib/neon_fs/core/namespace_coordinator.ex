defmodule NeonFS.Core.NamespaceCoordinator do
  @moduledoc """
  Distributed namespace-aware lock coordinator (sub-issue #300 of #226).

  Coordinates **claims over regions of the namespace** — separate from
  the DLM's content-level coordination. Where the DLM answers "who can
  touch this file's bytes", the namespace coordinator answers "who owns
  this name (or this directory subtree)".

  Operations that need this primitive:

    * WebDAV `Depth: infinity` collection locks (subtree claims).
    * Atomic `O_EXCL | O_CREAT` / `If-None-Match: *` (path claims with
      `:exclusive` scope on a name that doesn't yet exist).
    * Atomic cross-directory rename (paired claims on src + dst — see
      sub-issue #304).
    * `mkdir` / `rmdir` race resolution (path claims — see #305).
    * Lock-null resources (RFC 4918 §7.3 — replaces the synthetic-id
      DLM workaround per #302).

  ## Storage

  Backed by Ra: claims live in `MetadataStateMachine.namespace_claims`,
  replicated across every core node and queried locally. The
  GenServer below is a thin BEAM-side wrapper that adds two things
  Ra alone doesn't provide:

    * **Process-tied lifetime** — every `claim_path/2` /
      `claim_subtree/2` call records the holder pid, monitors it, and
      releases its claims on `:DOWN`. A dead interface node doesn't
      leak locks.
    * **Caller convenience** — claim ids are returned as opaque
      strings so callers don't depend on the internal sequencing.

  ## Conflict semantics (RFC 4918 §10.4 collection locks)

      exclusive vs *           = conflict
      shared    vs shared      = ok (compatible)
      shared    vs exclusive   = conflict

  Plus multi-granularity:

      subtree(/a)   conflicts with any *-claim on /a/x.
      path(/a/x)    conflicts with subtree(/a) (or any ancestor subtree).
      subtree(/a)   conflicts with subtree(/a/b)  (overlapping subtrees).

  See `NeonFS.Core.MetadataStateMachine` for the wire-level command
  shape.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{MetadataStateMachine, RaSupervisor}

  @typedoc "Opaque claim id returned by the coordinator on success."
  @type claim_id :: String.t()

  @typedoc "Lock scope — RFC 4918 §10.4."
  @type scope :: :exclusive | :shared

  ## Client API

  @doc """
  Starts the coordinator. Registered under the module name. Tests
  that need an isolated instance can pass `:name` to override.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Claim a single path. The current process is registered as the
  holder; if it dies before `release/2` is called, the coordinator
  releases the claim automatically. Returns `{:ok, claim_id}` or
  `{:error, :conflict, conflicting_claim_id}` when an existing claim
  collides.
  """
  @spec claim_path(GenServer.server(), String.t(), scope()) ::
          {:ok, claim_id()} | {:error, :conflict, claim_id()} | {:error, term()}
  def claim_path(server \\ __MODULE__, path, scope)
      when is_binary(path) and scope in [:exclusive, :shared] do
    claim_path_for(server, path, scope, self())
  end

  @doc """
  Same as `claim_path/3` but lets the caller specify an explicit holder
  pid. Useful when the call originates on a remote node via
  `NeonFS.Client.Router.call/4` — the RPC handler `self()` would
  otherwise be a short-lived process that dies the moment the call
  returns, dropping the claim. Pass a long-lived pid on the calling
  node (e.g. a per-node holder GenServer) so the coordinator monitors
  something stable.
  """
  @spec claim_path_for(GenServer.server(), String.t(), scope(), pid()) ::
          {:ok, claim_id()} | {:error, :conflict, claim_id()} | {:error, term()}
  def claim_path_for(server \\ __MODULE__, path, scope, holder)
      when is_binary(path) and scope in [:exclusive, :shared] and is_pid(holder) do
    GenServer.call(server, {:claim, :path, path, scope, holder})
  end

  @doc """
  Claim a path and every descendant. Same lifetime / return shape as
  `claim_path/3`.
  """
  @spec claim_subtree(GenServer.server(), String.t(), scope()) ::
          {:ok, claim_id()} | {:error, :conflict, claim_id()} | {:error, term()}
  def claim_subtree(server \\ __MODULE__, path, scope)
      when is_binary(path) and scope in [:exclusive, :shared] do
    claim_subtree_for(server, path, scope, self())
  end

  @doc """
  Same as `claim_subtree/3` but lets the caller specify an explicit
  holder pid. See `claim_path_for/4` for the cross-node motivation.
  """
  @spec claim_subtree_for(GenServer.server(), String.t(), scope(), pid()) ::
          {:ok, claim_id()} | {:error, :conflict, claim_id()} | {:error, term()}
  def claim_subtree_for(server \\ __MODULE__, path, scope, holder)
      when is_binary(path) and scope in [:exclusive, :shared] and is_pid(holder) do
    GenServer.call(server, {:claim, :subtree, path, scope, holder})
  end

  @typedoc """
  A rename claim is a *pair* of `:exclusive` `:path` claims allocated
  atomically and held by the same holder pid — one for the source path
  and one for the destination. The opaque return type carries both
  ids; callers pass it back to `release_rename/2` to release the pair
  atomically.
  """
  @type rename_claim_id :: {claim_id(), claim_id()}

  @doc """
  Claim a path as the placeholder for a new file (atomic
  create-if-not-exist). Returns `{:ok, claim_id}` on success,
  `{:error, :exists}` when another in-flight `claim_create` already
  covers the path, and `{:error, :conflict, conflict_id}` when the
  path is covered by an unrelated claim (e.g. a `Depth: infinity`
  collection lock).

  The check against an *already-existing* file at `path` lives in the
  caller (e.g. `WriteOperation` per #592) — the coordinator treats path
  strings as opaque coordination tokens and only ensures no two
  `claim_create` calls win for the same path. Callers should perform a
  `FileIndex.get_by_path/2` precheck and only invoke this primitive
  when no entry is found.

  Sub-issue #591 of #303.
  """
  @spec claim_create(GenServer.server(), String.t()) ::
          {:ok, claim_id()}
          | {:error, :exists}
          | {:error, :conflict, claim_id()}
          | {:error, term()}
  def claim_create(server \\ __MODULE__, path) when is_binary(path) do
    claim_create_for(server, path, self())
  end

  @doc """
  Same as `claim_create/2` but lets the caller specify an explicit
  holder pid. See `claim_path_for/4` for the cross-node motivation.
  """
  @spec claim_create_for(GenServer.server(), String.t(), pid()) ::
          {:ok, claim_id()}
          | {:error, :exists}
          | {:error, :conflict, claim_id()}
          | {:error, term()}
  def claim_create_for(server \\ __MODULE__, path, holder)
      when is_binary(path) and is_pid(holder) do
    GenServer.call(server, {:claim_create, path, holder})
  end

  @doc """
  Pin a path as held by an open file handle (sub-issue #637 of #306).
  Multiple pins on the same path coexist — each open `fd` is a separate
  pin. Returns `{:ok, claim_id}` on success or `{:error, :conflict,
  conflicting_id}` when the path is covered by an `:exclusive` claim
  (e.g. an in-flight rename or a `Depth: infinity` collection lock).

  Pin lifetime is tied to the calling process: when the holder pid
  dies the coordinator's `:DOWN` handler releases the pin via the
  standard bulk-release path. That cleanup is what the unlink-while-
  open story (#306) relies on — a crashed FUSE peer doesn't leak pins.
  """
  @spec claim_pinned(GenServer.server(), String.t()) ::
          {:ok, claim_id()}
          | {:error, :conflict, claim_id()}
          | {:error, term()}
  def claim_pinned(server \\ __MODULE__, path) when is_binary(path) do
    claim_pinned_for(server, path, self())
  end

  @doc """
  Same as `claim_pinned/2` but lets the caller specify an explicit
  holder pid. See `claim_path_for/4` for the cross-node motivation.
  """
  @spec claim_pinned_for(GenServer.server(), String.t(), pid()) ::
          {:ok, claim_id()}
          | {:error, :conflict, claim_id()}
          | {:error, term()}
  def claim_pinned_for(server \\ __MODULE__, path, holder)
      when is_binary(path) and is_pid(holder) do
    GenServer.call(server, {:claim_pinned, path, holder})
  end

  @doc """
  Return every `:pinned` claim at exactly `path`. Used by the unlink-
  while-open path (#306) to ask "is this file held open anywhere in
  the cluster?". Reads are served locally from the Ra follower's
  state, so this is cheap.
  """
  @spec claims_for_path(GenServer.server(), String.t()) ::
          {:ok, [{claim_id(), MetadataStateMachine.namespace_claim()}]} | {:error, term()}
  def claims_for_path(server \\ __MODULE__, path) when is_binary(path) do
    GenServer.call(server, {:claims_for_path, path, :pinned})
  end

  @typedoc """
  POSIX byte-range: `{offset, length}`. `length == 0` is the POSIX
  convention for "to end of file". Used by `claim_byte_range/4` and
  `query_byte_range/4` (#673).
  """
  @type byte_range :: {non_neg_integer(), non_neg_integer()}

  @doc """
  Claim a POSIX byte-range advisory lock (#673). `range` is
  `{offset, length}` (length 0 = to-EOF). `scope` is `:exclusive`
  (write lock) or `:shared` (read lock).

  Two byte-range claims on the same path conflict only when their
  ranges overlap and at least one is `:exclusive`. Different paths
  never conflict regardless of range. Byte-range claims do not
  interact with `:path` / `:subtree` / `:create` / `:pinned` claims —
  they live in a separate logical namespace.

  Returns `{:ok, claim_id}` on success or `{:error, :conflict,
  conflicting_claim_id}` when an overlapping incompatible claim
  already exists. The blocking `SETLKW` variant is tracked in #679.
  """
  @spec claim_byte_range(GenServer.server(), String.t(), byte_range(), scope()) ::
          {:ok, claim_id()} | {:error, :conflict, claim_id()} | {:error, term()}
  def claim_byte_range(server \\ __MODULE__, path, range, scope)
      when is_binary(path) and scope in [:exclusive, :shared] do
    claim_byte_range_for(server, path, range, scope, self())
  end

  @doc """
  Same as `claim_byte_range/4` but lets the caller specify an
  explicit holder pid. See `claim_path_for/4` for the cross-node
  motivation.
  """
  @spec claim_byte_range_for(GenServer.server(), String.t(), byte_range(), scope(), pid()) ::
          {:ok, claim_id()} | {:error, :conflict, claim_id()} | {:error, term()}
  def claim_byte_range_for(server \\ __MODULE__, path, range, scope, holder)
      when is_binary(path) and scope in [:exclusive, :shared] and
             is_pid(holder) and is_tuple(range) and tuple_size(range) == 2 do
    GenServer.call(server, {:claim_byte_range, path, range, scope, holder})
  end

  @doc """
  Non-blocking conflict probe for `GETLK` (#673). Returns
  `{:ok, :unlocked}` when no conflicting byte-range claim covers
  `range` at `scope`, or `{:ok, {:locked, holder, conflicting_range,
  conflicting_scope}}` describing the first conflict found.

  Reads are served locally from the Ra follower's state, so this is
  cheap. Useful for `GETLK` which doesn't actually acquire a lock —
  it just asks whether one *would* acquire if attempted.
  """
  @spec query_byte_range(GenServer.server(), String.t(), byte_range(), scope()) ::
          {:ok, :unlocked}
          | {:ok, {:locked, term(), byte_range(), scope()}}
          | {:error, term()}
  def query_byte_range(server \\ __MODULE__, path, range, scope)
      when is_binary(path) and scope in [:exclusive, :shared] and
             is_tuple(range) and tuple_size(range) == 2 do
    GenServer.call(server, {:query_byte_range, path, range, scope})
  end

  @doc """
  Atomically pin both paths of a cross-directory rename. The two paths
  are claimed `:exclusive :path` in a single Ra command, so a competing
  rename or claim on either side either both succeeds or fails up
  front — there is no window where one path is reserved without the
  other.

  Renames whose destination sits inside the source's subtree (e.g.
  `mv /a /a/b/c`) return `{:error, :einval}` — a cycle the filesystem
  cannot represent.

  See sub-issue #304.
  """
  @spec claim_rename(GenServer.server(), String.t(), String.t()) ::
          {:ok, rename_claim_id()}
          | {:error, :conflict, claim_id()}
          | {:error, :einval}
          | {:error, term()}
  def claim_rename(server \\ __MODULE__, src, dst) when is_binary(src) and is_binary(dst) do
    claim_rename_for(server, src, dst, self())
  end

  @doc """
  Same as `claim_rename/3` but lets the caller specify an explicit
  holder pid. See `claim_path_for/4` for the cross-node motivation.
  """
  @spec claim_rename_for(GenServer.server(), String.t(), String.t(), pid()) ::
          {:ok, rename_claim_id()}
          | {:error, :conflict, claim_id()}
          | {:error, :einval}
          | {:error, term()}
  def claim_rename_for(server \\ __MODULE__, src, dst, holder)
      when is_binary(src) and is_binary(dst) and is_pid(holder) do
    GenServer.call(server, {:claim_rename, src, dst, holder})
  end

  @doc """
  Release a rename claim allocated by `claim_rename/3`. Releases both
  paths atomically. Idempotent: releasing a pair where one or both ids
  have already been released returns `:ok`.
  """
  @spec release_rename(GenServer.server(), rename_claim_id()) :: :ok | {:error, term()}
  def release_rename(server \\ __MODULE__, {src_id, dst_id})
      when is_binary(src_id) and is_binary(dst_id) do
    with :ok <- release(server, src_id) do
      release(server, dst_id)
    end
  end

  @doc """
  Release a claim by id. Idempotent — releasing a non-existent or
  already-released claim returns `:ok`.
  """
  @spec release(GenServer.server(), claim_id()) :: :ok | {:error, term()}
  def release(server \\ __MODULE__, claim_id) when is_binary(claim_id) do
    GenServer.call(server, {:release, claim_id})
  end

  @doc """
  List every claim whose path starts with `prefix`. Pass `""` to list
  all. Reads are served locally from the Ra follower's state, so this
  is cheap.
  """
  @spec list_claims(GenServer.server(), String.t()) ::
          {:ok, [{claim_id(), MetadataStateMachine.namespace_claim()}]} | {:error, term()}
  def list_claims(server \\ __MODULE__, prefix \\ "") when is_binary(prefix) do
    GenServer.call(server, {:list_claims, prefix})
  end

  ## Server callbacks

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)
    # `holders` maps `pid -> %{ref: monitor_ref, claim_ids: MapSet.t()}`.
    # The MapSet is purely optimistic; the Ra side is the authority,
    # but tracking ids locally lets us short-circuit the bulk-release
    # command when a holder never claimed anything.
    {:ok, %{holders: %{}}}
  end

  @impl true
  def handle_call({:claim, type, path, scope, holder}, _from, state) do
    cmd =
      case type do
        :path -> {:claim_namespace_path, path, scope, holder}
        :subtree -> {:claim_namespace_subtree, path, scope, holder}
      end

    case ra_command(cmd) do
      {:ok, claim_id} ->
        {:reply, {:ok, claim_id}, track_claim(state, holder, claim_id)}

      {:error, :conflict, conflicting_id} ->
        {:reply, {:error, :conflict, conflicting_id}, state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  def handle_call({:claim_create, path, holder}, _from, state) do
    case ra_command({:claim_namespace_create, path, holder}) do
      {:ok, claim_id} ->
        {:reply, {:ok, claim_id}, track_claim(state, holder, claim_id)}

      {:error, :exists} ->
        {:reply, {:error, :exists}, state}

      {:error, :conflict, conflicting_id} ->
        {:reply, {:error, :conflict, conflicting_id}, state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  def handle_call({:claim_pinned, path, holder}, _from, state) do
    case ra_command({:claim_namespace_pinned, path, holder}) do
      {:ok, claim_id} ->
        {:reply, {:ok, claim_id}, track_claim(state, holder, claim_id)}

      {:error, :conflict, conflicting_id} ->
        {:reply, {:error, :conflict, conflicting_id}, state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  def handle_call({:claims_for_path, path, type}, _from, state) do
    result =
      try do
        case RaSupervisor.local_query(
               &MetadataStateMachine.list_namespace_claims_at(&1, path, type)
             ) do
          {:ok, claims} -> {:ok, claims}
          {:error, _} = err -> err
        end
      catch
        :exit, _ -> {:error, :ra_not_available}
      end

    {:reply, result, state}
  end

  def handle_call({:claim_byte_range, path, range, scope, holder}, _from, state) do
    case ra_command({:claim_namespace_byte_range, path, range, scope, holder}) do
      {:ok, claim_id} ->
        {:reply, {:ok, claim_id}, track_claim(state, holder, claim_id)}

      {:error, :conflict, conflicting_id} ->
        {:reply, {:error, :conflict, conflicting_id}, state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  def handle_call({:query_byte_range, path, range, scope}, _from, state) do
    result =
      try do
        case RaSupervisor.local_query(
               &MetadataStateMachine.list_namespace_claims_at(&1, path, :byte_range)
             ) do
          {:ok, claims} ->
            {:ok, find_byte_range_conflict(claims, range, scope)}

          {:error, _} = err ->
            err
        end
      catch
        :exit, _ -> {:error, :ra_not_available}
      end

    {:reply, result, state}
  end

  def handle_call({:claim_rename, src, dst, holder}, _from, state) do
    case ra_command({:claim_namespace_rename, src, dst, holder}) do
      {:ok, {src_id, dst_id}} ->
        state = track_claim(state, holder, src_id)
        state = track_claim(state, holder, dst_id)
        {:reply, {:ok, {src_id, dst_id}}, state}

      {:error, :einval} ->
        {:reply, {:error, :einval}, state}

      {:error, :conflict, conflicting_id} ->
        {:reply, {:error, :conflict, conflicting_id}, state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  def handle_call({:release, claim_id}, _from, state) do
    case ra_command({:release_namespace_claim, claim_id}) do
      :ok -> {:reply, :ok, untrack_claim(state, claim_id)}
      {:error, _} = err -> {:reply, err, state}
    end
  end

  def handle_call({:list_claims, prefix}, _from, state) do
    result =
      try do
        case RaSupervisor.local_query(&MetadataStateMachine.list_namespace_claims(&1, prefix)) do
          {:ok, claims} -> {:ok, claims}
          {:error, _} = err -> err
        end
      catch
        :exit, _ -> {:error, :ra_not_available}
      end

    {:reply, result, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    case Map.pop(state.holders, pid) do
      {nil, _} ->
        {:noreply, state}

      {%{claim_ids: ids}, holders} when ids == %MapSet{} ->
        {:noreply, %{state | holders: holders}}

      {_holder_state, holders} ->
        case ra_command({:release_namespace_claims_for_holder, pid}) do
          {:ok, count} ->
            Logger.debug("Released claims for dead holder",
              holder: inspect(pid),
              count: count
            )

          {:error, reason} ->
            Logger.warning("Failed to release claims for dead holder",
              holder: inspect(pid),
              reason: inspect(reason)
            )
        end

        {:noreply, %{state | holders: holders}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Internal

  # Walk the byte-range claims at a path and find the first one that
  # would conflict with `range` at `scope`. Returns `:unlocked` when
  # nothing collides, or a `{:locked, holder, range, scope}` tuple
  # describing the conflict — useful for `GETLK` replies that name
  # the blocking lock.
  defp find_byte_range_conflict(claims, range, scope) do
    Enum.find_value(claims, :unlocked, fn
      {_id,
       %{
         type: :byte_range,
         range: existing_range,
         scope: existing_scope,
         holder: holder
       }} ->
        if scopes_conflict?(scope, existing_scope) and
             ranges_overlap?(range, existing_range) do
          {:locked, holder, existing_range, existing_scope}
        end

      _ ->
        nil
    end)
  end

  defp scopes_conflict?(:exclusive, _), do: true
  defp scopes_conflict?(_, :exclusive), do: true
  defp scopes_conflict?(:shared, :shared), do: false

  # `range_a` and `range_b` are `{offset, length}` pairs. POSIX
  # convention: `length == 0` means "to EOF" — represented here by
  # `:infinity`. Two ranges overlap when each one's start is strictly
  # before the other's end. Adjacent but non-overlapping ranges (a..b,
  # b..c) do not conflict.
  defp ranges_overlap?({start_a, len_a}, {start_b, len_b}) do
    end_a = if len_a == 0, do: :infinity, else: start_a + len_a
    end_b = if len_b == 0, do: :infinity, else: start_b + len_b
    range_lt?(start_a, end_b) and range_lt?(start_b, end_a)
  end

  defp range_lt?(_a, :infinity), do: true
  defp range_lt?(:infinity, _b), do: false
  defp range_lt?(a, b), do: a < b

  defp track_claim(state, holder, claim_id) do
    {ref, claim_ids} =
      case Map.get(state.holders, holder) do
        nil ->
          {Process.monitor(holder), MapSet.new()}

        %{ref: ref, claim_ids: ids} ->
          {ref, ids}
      end

    new_holder = %{ref: ref, claim_ids: MapSet.put(claim_ids, claim_id)}
    %{state | holders: Map.put(state.holders, holder, new_holder)}
  end

  defp untrack_claim(state, claim_id) do
    holders =
      Enum.reduce(state.holders, %{}, fn {holder, %{ref: ref, claim_ids: ids}}, acc ->
        new_ids = MapSet.delete(ids, claim_id)

        if MapSet.size(new_ids) == 0 do
          Process.demonitor(ref, [:flush])
          acc
        else
          Map.put(acc, holder, %{ref: ref, claim_ids: new_ids})
        end
      end)

    %{state | holders: holders}
  end

  defp ra_command(cmd) do
    case RaSupervisor.command(cmd) do
      {:ok, :ok, _leader} -> :ok
      {:ok, {:ok, value}, _leader} -> {:ok, value}
      {:ok, {:error, :conflict, conflicting_id}, _leader} -> {:error, :conflict, conflicting_id}
      {:ok, {:error, reason}, _leader} -> {:error, reason}
      {:ok, other, _leader} -> {:error, {:unexpected_reply, other}}
      {:error, :noproc} -> {:error, :ra_not_available}
      {:error, reason} -> {:error, reason}
      {:timeout, _node} -> {:error, :timeout}
    end
  catch
    :exit, _ -> {:error, :ra_not_available}
  end
end
