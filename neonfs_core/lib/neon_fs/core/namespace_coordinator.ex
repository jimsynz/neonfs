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

  @typedoc """
  Opaque token returned by `claim_byte_range_wait/5` when the call
  blocks. Pass it to `cancel_wait/2` to remove the queued waiter.
  """
  @type wait_token :: reference()

  @doc """
  Blocking variant of `claim_byte_range/4` (#679). Same arguments;
  three possible return shapes:

    * `{:ok, claim_id}` — no conflict; claim acquired immediately.
    * `{:wait, wait_token}` — conflict detected; the calling
      process is registered as a waiter against the conflicting
      claim. When the conflict clears (the conflicting claim is
      released, or its holder dies and the bulk-release runs), the
      coordinator retries the claim on the waiter's behalf and
      sends `{:byte_range_acquired, wait_token, claim_id}` to the
      `holder` pid. If a fresh conflict is encountered against a
      different claim during the retry the waiter is re-queued; the
      same `wait_token` remains valid.

  The waiter pid (the `holder` argument, since the holder is what
  the coordinator monitors) is registered locally on this node's
  GenServer — wait queues do not cross nodes. Cross-node signalling
  works because release events fire telemetry on every Ra replica,
  and each node's coordinator processes its own queue when its local
  apply hits.
  """
  @spec claim_byte_range_wait(GenServer.server(), String.t(), byte_range(), scope()) ::
          {:ok, claim_id()}
          | {:wait, wait_token()}
          | {:error, term()}
  def claim_byte_range_wait(server \\ __MODULE__, path, range, scope)
      when is_binary(path) and scope in [:exclusive, :shared] and
             is_tuple(range) and tuple_size(range) == 2 do
    claim_byte_range_wait_for(server, path, range, scope, self())
  end

  @doc """
  Same as `claim_byte_range_wait/4` but lets the caller specify the
  holder pid — see `claim_path_for/4` for the cross-node motivation.
  The holder is also the pid the coordinator notifies when the wait
  fires.
  """
  @spec claim_byte_range_wait_for(
          GenServer.server(),
          String.t(),
          byte_range(),
          scope(),
          pid()
        ) :: {:ok, claim_id()} | {:wait, wait_token()} | {:error, term()}
  def claim_byte_range_wait_for(server \\ __MODULE__, path, range, scope, holder)
      when is_binary(path) and scope in [:exclusive, :shared] and is_pid(holder) and
             is_tuple(range) and tuple_size(range) == 2 do
    GenServer.call(server, {:claim_byte_range_wait, path, range, scope, holder})
  end

  @doc """
  Cancel a pending wait. Idempotent — cancelling a token that was
  never registered, already fired, or already cancelled returns
  `:ok`.

  Used by FUSE INTERRUPT (#675) to unblock a queued SETLKW when
  userspace interrupts the syscall.
  """
  @spec cancel_wait(GenServer.server(), wait_token()) :: :ok
  def cancel_wait(server \\ __MODULE__, wait_token) when is_reference(wait_token) do
    GenServer.call(server, {:cancel_wait, wait_token})
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
    # Subscribe to release-event telemetry so cross-node releases
    # (replicated via Ra apply on every replica) wake any local
    # waiters whose conflict has cleared. The handler dispatches
    # back to the coordinator GenServer via cast so the queue work
    # happens in this process's context (#679).
    attach_release_telemetry(self())

    # `holders` maps `pid -> %{ref: monitor_ref, claim_ids: MapSet.t()}`.
    # The MapSet is purely optimistic; the Ra side is the authority,
    # but tracking ids locally lets us short-circuit the bulk-release
    # command when a holder never claimed anything.
    #
    # `wait_queue` maps `conflicting_claim_id -> [waiter_entry]` in
    # registration order (FIFO when signalled). Each waiter entry
    # carries everything needed to retry the claim and identify the
    # caller for cancellation / cleanup. `wait_index` maps the
    # public `wait_token -> conflicting_claim_id` for O(1) cancel
    # lookup. `waiter_monitors` maps `pid -> [wait_token]` so a
    # waiter-DOWN can find every wait the dying pid owns.
    {:ok,
     %{
       holders: %{},
       wait_queue: %{},
       wait_index: %{},
       waiter_monitors: %{}
     }}
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

  def handle_call({:claim_byte_range_wait, path, range, scope, holder}, _from, state) do
    do_claim_byte_range_wait(path, range, scope, holder, state)
  end

  def handle_call({:cancel_wait, wait_token}, _from, state) do
    {:reply, :ok, do_cancel_wait(wait_token, state)}
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
    # A dying pid can be both a holder (had at least one claim) and
    # a waiter (was queued on another claim) — drop both, in either
    # order. The waiter cleanup is local; the holder cleanup hits Ra.
    state = drop_waits_for_pid(pid, state)

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

  # Wake-up signal from the release telemetry handler attached in
  # `init/1`. Carries the `claim_id` that just released — every
  # waiter queued against it gets a retry attempt. Same shape for
  # bulk releases (the telemetry handler sends one message per
  # released id).
  def handle_info({:released_claim_id, claim_id}, state) do
    {:noreply, wake_waiters_for_claim(claim_id, state)}
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

  ## Wait-queue helpers (#679)

  # Try to claim; on no-conflict, behave exactly like
  # `claim_byte_range_for/5`. On conflict, register the calling
  # holder as a waiter against the conflicting claim and return
  # `{:wait, wait_token}`. The same token survives re-queues against
  # different claims during the retry path — the caller's wait is
  # only "complete" when they receive `{:byte_range_acquired, ...}`
  # or call `cancel_wait/2`.
  defp do_claim_byte_range_wait(path, range, scope, holder, state) do
    case ra_command({:claim_namespace_byte_range, path, range, scope, holder}) do
      {:ok, claim_id} ->
        {:reply, {:ok, claim_id}, track_claim(state, holder, claim_id)}

      {:error, :conflict, conflicting_id} ->
        {token, state} = enqueue_waiter(state, holder, conflicting_id, path, range, scope)
        {:reply, {:wait, token}, state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  # Register a waiter against `conflicting_id`. Returns the public
  # `wait_token` plus the updated state. Monitors the holder pid if
  # we don't already have a monitor for it (sharing the existing
  # monitor with `track_claim`'s book-keeping when the same pid both
  # holds and waits — see `drop_waits_for_pid/2` for the symmetric
  # cleanup).
  defp enqueue_waiter(state, holder, conflicting_id, path, range, scope) do
    token = make_ref()
    state = ensure_waiter_monitor(state, holder)

    entry = %{
      token: token,
      holder: holder,
      path: path,
      range: range,
      scope: scope
    }

    queue = Map.get(state.wait_queue, conflicting_id, [])

    state = %{
      state
      | wait_queue: Map.put(state.wait_queue, conflicting_id, queue ++ [entry]),
        wait_index: Map.put(state.wait_index, token, conflicting_id),
        waiter_monitors:
          Map.update(
            state.waiter_monitors,
            holder,
            [token],
            fn tokens -> [token | tokens] end
          )
    }

    {token, state}
  end

  # Make sure we monitor `holder`. If it's already in `holders`
  # (because the same pid holds claims) we're already monitoring; in
  # that case the `track_claim` path's `:ref` is reused and we don't
  # need to create a second monitor. The `holders` book-keeping owns
  # the lifecycle of that ref.
  defp ensure_waiter_monitor(state, holder) do
    cond do
      Map.has_key?(state.waiter_monitors, holder) -> state
      Map.has_key?(state.holders, holder) -> state
      true -> %{state | holders: state.holders, waiter_monitors: state.waiter_monitors}
    end
    |> ensure_holder_monitor(holder)
  end

  # Add a holder entry with no claims (just so we monitor the pid).
  # If the pid already has an entry, no-op. The MapSet stays empty
  # until `track_claim` adds the first claim_id.
  defp ensure_holder_monitor(state, holder) do
    case Map.get(state.holders, holder) do
      nil ->
        ref = Process.monitor(holder)
        new_holder = %{ref: ref, claim_ids: MapSet.new()}
        %{state | holders: Map.put(state.holders, holder, new_holder)}

      _ ->
        state
    end
  end

  # Remove a wait entry by token. Idempotent — unknown / already-
  # consumed tokens are no-ops.
  defp do_cancel_wait(token, state) do
    case Map.pop(state.wait_index, token) do
      {nil, _} ->
        state

      {conflicting_id, wait_index} ->
        queue =
          state.wait_queue
          |> Map.get(conflicting_id, [])
          |> Enum.reject(fn entry -> entry.token == token end)

        wait_queue =
          if queue == [] do
            Map.delete(state.wait_queue, conflicting_id)
          else
            Map.put(state.wait_queue, conflicting_id, queue)
          end

        %{state | wait_queue: wait_queue, wait_index: wait_index}
        |> drop_waiter_monitor_for_token(token)
    end
  end

  # Pop the token from `waiter_monitors[pid]`. If the pid has no
  # remaining waits AND no remaining claims, demonitor and drop.
  defp drop_waiter_monitor_for_token(state, token) do
    case find_waiter_pid(state.waiter_monitors, token) do
      nil ->
        state

      pid ->
        remaining = state.waiter_monitors |> Map.get(pid, []) |> List.delete(token)

        waiter_monitors =
          if remaining == [] do
            Map.delete(state.waiter_monitors, pid)
          else
            Map.put(state.waiter_monitors, pid, remaining)
          end

        state = %{state | waiter_monitors: waiter_monitors}
        maybe_demonitor_pure_waiter(state, pid, remaining)
    end
  end

  defp find_waiter_pid(monitors, token) do
    Enum.find_value(monitors, fn {pid, tokens} ->
      if token in tokens, do: pid
    end)
  end

  # If the pid has no remaining waits AND its `holders` entry is
  # empty (no claims), demonitor and drop. Pids that hold claims keep
  # their monitor alive for the holder-DOWN bulk-release path.
  defp maybe_demonitor_pure_waiter(state, pid, []) do
    case Map.get(state.holders, pid) do
      %{claim_ids: ids, ref: ref} ->
        if MapSet.size(ids) == 0 do
          Process.demonitor(ref, [:flush])
          %{state | holders: Map.delete(state.holders, pid)}
        else
          state
        end

      _ ->
        state
    end
  end

  defp maybe_demonitor_pure_waiter(state, _pid, _remaining), do: state

  # Drop every wait entry owned by `pid`. Called from the DOWN
  # handler — the waiter is gone, no point retrying their claims.
  defp drop_waits_for_pid(pid, state) do
    case Map.pop(state.waiter_monitors, pid) do
      {nil, _} ->
        state

      {tokens, waiter_monitors} ->
        {wait_queue, wait_index} =
          Enum.reduce(tokens, {state.wait_queue, state.wait_index}, &drop_token/2)

        %{
          state
          | wait_queue: wait_queue,
            wait_index: wait_index,
            waiter_monitors: waiter_monitors
        }
    end
  end

  defp drop_token(token, {wait_queue, wait_index}) do
    case Map.pop(wait_index, token) do
      {nil, wait_index} ->
        {wait_queue, wait_index}

      {conflicting_id, wait_index} ->
        {drop_token_from_queue(wait_queue, conflicting_id, token), wait_index}
    end
  end

  defp drop_token_from_queue(wait_queue, conflicting_id, token) do
    queue =
      wait_queue
      |> Map.get(conflicting_id, [])
      |> Enum.reject(fn entry -> entry.token == token end)

    if queue == [] do
      Map.delete(wait_queue, conflicting_id)
    else
      Map.put(wait_queue, conflicting_id, queue)
    end
  end

  # Release fired for `claim_id` — retry every waiter that was queued
  # against it, in registration order. Successful retries notify the
  # waiter and stop; conflicts re-queue against the new conflicting
  # claim under the same token.
  defp wake_waiters_for_claim(claim_id, state) do
    case Map.pop(state.wait_queue, claim_id) do
      {nil, _} ->
        state

      {entries, wait_queue} ->
        state = %{state | wait_queue: wait_queue}
        Enum.reduce(entries, state, &retry_waiter/2)
    end
  end

  defp retry_waiter(entry, state) do
    case ra_command(
           {:claim_namespace_byte_range, entry.path, entry.range, entry.scope, entry.holder}
         ) do
      {:ok, new_claim_id} ->
        send(entry.holder, {:byte_range_acquired, entry.token, new_claim_id})
        state = consume_wait_index(state, entry.token, entry.holder)
        track_claim(state, entry.holder, new_claim_id)

      {:error, :conflict, new_conflicting_id} ->
        # Re-queue under the same token against the fresh conflict.
        new_queue = Map.get(state.wait_queue, new_conflicting_id, [])

        %{
          state
          | wait_queue: Map.put(state.wait_queue, new_conflicting_id, new_queue ++ [entry]),
            wait_index: Map.put(state.wait_index, entry.token, new_conflicting_id)
        }

      {:error, reason} ->
        Logger.warning(
          "Wait-queue retry failed token=#{inspect(entry.token)} reason=#{inspect(reason)}"
        )

        send(entry.holder, {:byte_range_wait_error, entry.token, reason})
        consume_wait_index(state, entry.token, entry.holder)
    end
  end

  # Drop the entry from `wait_index` + `waiter_monitors` after a
  # successful acquire or a permanent error. Mirror of
  # `drop_waiter_monitor_for_token/2` but takes the holder pid
  # directly to skip the lookup.
  defp consume_wait_index(state, token, pid) do
    state = %{state | wait_index: Map.delete(state.wait_index, token)}
    remaining = state.waiter_monitors |> Map.get(pid, []) |> List.delete(token)

    waiter_monitors =
      if remaining == [] do
        Map.delete(state.waiter_monitors, pid)
      else
        Map.put(state.waiter_monitors, pid, remaining)
      end

    state = %{state | waiter_monitors: waiter_monitors}
    maybe_demonitor_pure_waiter(state, pid, remaining)
  end

  # Telemetry attachment fires on every Ra apply of a release event,
  # which means every replica's coordinator gets the wake signal —
  # cross-node release propagation comes for free. The handler casts
  # back to the coordinator pid so queue work happens in this
  # process's context (no concurrent state mutation from a telemetry
  # callback). Bulk-release (`release_namespace_claims_for_holder`)
  # broadcasts one message per released id.
  defp attach_release_telemetry(coordinator_pid) do
    handler_id = "namespace-coordinator-wait-queue-#{:erlang.pid_to_list(coordinator_pid)}"

    :telemetry.attach_many(
      handler_id,
      [
        [:neonfs, :ra, :command, :release_namespace_claim],
        [:neonfs, :ra, :command, :release_namespace_claims_for_holder]
      ],
      &__MODULE__.__release_telemetry_handler__/4,
      coordinator_pid
    )
  end

  @doc false
  def __release_telemetry_handler__(
        [:neonfs, :ra, :command, :release_namespace_claim],
        _measurements,
        %{claim_id: claim_id},
        coordinator_pid
      ) do
    send(coordinator_pid, {:released_claim_id, claim_id})
  end

  def __release_telemetry_handler__(
        [:neonfs, :ra, :command, :release_namespace_claims_for_holder],
        _measurements,
        %{released_claim_ids: ids},
        coordinator_pid
      ) do
    for id <- ids, do: send(coordinator_pid, {:released_claim_id, id})
  end

  def __release_telemetry_handler__(_, _, _, _), do: :ok

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
