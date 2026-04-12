defmodule NeonFS.Core.LockManager.FileLock do
  @moduledoc """
  Per-file lock manager GenServer.

  Manages byte-range locks, share modes, and leases for a single file.
  Spawned on demand when the first lock is acquired, exits when all locks
  are released and no opens or leases remain.

  Lock state is in-memory only — it does not survive process restart.
  Clients re-establish locks during a grace period after recovery.

  ## Lock Types

  - **Byte-range locks**: Lock a range `[offset, offset+length)` as shared
    (read) or exclusive (write). Used by all protocols.
  - **Share modes**: Control concurrent file opens (SMB). A client declares
    permitted concurrent access on open.
  - **Leases/oplocks**: Performance grants allowing aggressive caching.
    Broken when a conflicting access occurs.

  ## TTL

  Every lock, open, and lease has a TTL. Protocol adapters renew on behalf
  of their clients. Expired entries are cleaned up periodically.
  """

  use GenServer

  require Logger

  @default_ttl_ms 90_000
  @ttl_check_interval_ms 10_000
  @idle_timeout_ms 60_000

  @type lock_type :: :shared | :exclusive
  @type lock_mode :: :advisory | :mandatory
  @type share_access :: :read | :write | :read_write | :none
  @type share_deny :: :none | :read | :write | :read_write
  @type lease_type :: :read | :read_write | :write | :handle
  @type client_ref :: term()

  @type range :: {offset :: non_neg_integer(), length :: non_neg_integer()}

  @type lock_entry :: %{
          client_ref: client_ref(),
          range: range(),
          type: lock_type(),
          mode: lock_mode(),
          expires_at: integer()
        }

  @type open_entry :: %{
          client_ref: client_ref(),
          access: share_access(),
          deny: share_deny(),
          expires_at: integer()
        }

  @type lease_entry :: %{
          client_ref: client_ref(),
          type: lease_type(),
          break_callback: (-> :ok) | nil,
          expires_at: integer()
        }

  @type state :: %{
          file_id: binary(),
          locks: [lock_entry()],
          opens: [open_entry()],
          leases: [lease_entry()],
          wait_queue: :queue.queue(),
          write_wait_queue: :queue.queue(),
          ttl_timer: reference() | nil
        }

  ## Client API

  @doc """
  Starts a FileLock process for the given file ID.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    file_id = Keyword.fetch!(opts, :file_id)
    name = via_tuple(file_id)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Tests whether a byte-range lock would conflict without acquiring it.

  Returns `:ok` if the lock would be granted, or
  `{:error, :conflict, holder_info}` with details of the conflicting lock.
  """
  @spec test_lock(pid() | GenServer.name(), client_ref(), range(), lock_type()) ::
          :ok | {:error, :conflict, map()}
  def test_lock(server, client_ref, range, type) do
    GenServer.call(server, {:test_lock, client_ref, range, type})
  end

  @doc """
  Acquires a byte-range lock. Blocks if a conflicting lock is held.
  """
  @spec lock(pid() | GenServer.name(), client_ref(), range(), lock_type(), keyword()) ::
          :ok | {:error, :timeout}
  def lock(server, client_ref, range, type, opts \\ []) do
    ttl = Keyword.get(opts, :ttl, @default_ttl_ms)
    mode = Keyword.get(opts, :mode, :advisory)
    timeout = Keyword.get(opts, :timeout, 5_000)

    GenServer.call(server, {:lock, client_ref, range, type, mode, ttl}, timeout)
  end

  @doc """
  Releases a specific byte-range lock.
  """
  @spec unlock(pid() | GenServer.name(), client_ref(), range()) :: :ok
  def unlock(server, client_ref, range) do
    GenServer.call(server, {:unlock, client_ref, range})
  end

  @doc """
  Releases all locks, opens, and leases held by a client.
  """
  @spec unlock_all(pid() | GenServer.name(), client_ref()) :: :ok
  def unlock_all(server, client_ref) do
    GenServer.call(server, {:unlock_all, client_ref})
  end

  @doc """
  Opens a file with share mode semantics (SMB).
  """
  @spec open(pid() | GenServer.name(), client_ref(), share_access(), share_deny(), keyword()) ::
          :ok | {:error, :share_violation}
  def open(server, client_ref, access, deny, opts \\ []) do
    ttl = Keyword.get(opts, :ttl, @default_ttl_ms)
    GenServer.call(server, {:open, client_ref, access, deny, ttl})
  end

  @doc """
  Closes a file (releases share mode).
  """
  @spec close(pid() | GenServer.name(), client_ref()) :: :ok
  def close(server, client_ref) do
    GenServer.call(server, {:close, client_ref})
  end

  @doc """
  Grants a lease (oplock/delegation) on the file.
  """
  @spec grant_lease(pid() | GenServer.name(), client_ref(), lease_type(), keyword()) ::
          :ok | {:error, :conflict}
  def grant_lease(server, client_ref, lease_type, opts \\ []) do
    ttl = Keyword.get(opts, :ttl, @default_ttl_ms)
    break_callback = Keyword.get(opts, :break_callback)
    GenServer.call(server, {:grant_lease, client_ref, lease_type, break_callback, ttl})
  end

  @doc """
  Breaks a lease held by a client, invoking the break callback.
  """
  @spec break_lease(pid() | GenServer.name(), client_ref()) ::
          :ok | {:error, :not_found}
  def break_lease(server, client_ref) do
    GenServer.call(server, {:break_lease, client_ref})
  end

  @doc """
  Renews the TTL for all entries held by a client.
  """
  @spec renew(pid() | GenServer.name(), client_ref(), keyword()) :: :ok | {:error, :not_found}
  def renew(server, client_ref, opts \\ []) do
    ttl = Keyword.get(opts, :ttl, @default_ttl_ms)
    GenServer.call(server, {:renew, client_ref, ttl})
  end

  @doc """
  Checks whether a write to the given byte range is permitted.

  Returns `:ok` if the write is allowed, or an error if blocked:

  - `{:error, :lock_conflict}` — a mandatory byte-range lock held by
    another client overlaps the write range. Advisory locks are ignored.
  - `{:error, :share_denied}` — another client has the file open with
    `deny: :write` or `deny: :read_write`, blocking writes from other
    clients regardless of byte range.
  """
  @spec check_write(pid() | GenServer.name(), client_ref(), range()) ::
          :ok | {:error, :lock_conflict | :share_denied}
  def check_write(server, client_ref, range) do
    GenServer.call(server, {:check_write, client_ref, range})
  end

  @doc """
  Checks whether a write is permitted, blocking until the conflict clears.

  Like `check_write/3` but instead of returning an error immediately when
  a mandatory lock or deny-write share mode blocks the write, the caller
  is queued and unblocked when the conflict is released.

  The `timeout` on the GenServer call controls how long the caller waits.
  """
  @spec check_write_blocking(pid() | GenServer.name(), client_ref(), range(), keyword()) ::
          :ok | {:error, :lock_conflict | :share_denied}
  def check_write_blocking(server, client_ref, range, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    GenServer.call(server, {:check_write_blocking, client_ref, range}, timeout)
  end

  @doc """
  Returns the current lock/open/lease status for this file.
  """
  @spec status(pid() | GenServer.name()) :: map()
  def status(server) do
    GenServer.call(server, :status)
  end

  @doc false
  @spec via_tuple(binary()) :: {:via, Registry, {NeonFS.Core.LockManager.Registry, binary()}}
  def via_tuple(file_id) do
    {:via, Registry, {NeonFS.Core.LockManager.Registry, file_id}}
  end

  ## Server callbacks

  @impl true
  def init(opts) do
    file_id = Keyword.fetch!(opts, :file_id)
    timer = schedule_ttl_check()

    :telemetry.execute(
      [:neonfs, :lock_manager, :file_lock, :started],
      %{},
      %{file_id: file_id}
    )

    {:ok,
     %{
       file_id: file_id,
       locks: [],
       opens: [],
       leases: [],
       wait_queue: :queue.new(),
       write_wait_queue: :queue.new(),
       ttl_timer: timer
     }, @idle_timeout_ms}
  end

  @impl true
  def handle_call({:test_lock, client_ref, range, type}, _from, state) do
    now = System.monotonic_time(:millisecond)
    state = purge_expired(state, now)

    case check_lock_conflict(state.locks, client_ref, range, type) do
      :ok ->
        {:reply, :ok, state, @idle_timeout_ms}

      {:wait, _holder_ref} ->
        holder = find_conflicting_lock(state.locks, client_ref, range, type)

        holder_info = %{
          type: holder.type,
          range: holder.range,
          svid: extract_svid(holder.client_ref),
          oh: extract_oh(holder.client_ref)
        }

        {:reply, {:error, :conflict, holder_info}, state, @idle_timeout_ms}
    end
  end

  def handle_call({:lock, client_ref, range, type, mode, ttl}, from, state) do
    now = System.monotonic_time(:millisecond)
    state = purge_expired(state, now)

    case check_lock_conflict(state.locks, client_ref, range, type) do
      :ok ->
        entry = %{
          client_ref: client_ref,
          range: range,
          type: type,
          mode: mode,
          expires_at: now + ttl
        }

        new_state = %{state | locks: [entry | state.locks]}

        :telemetry.execute(
          [:neonfs, :lock_manager, :file_lock, :locked],
          %{},
          %{file_id: state.file_id, client_ref: client_ref, type: type}
        )

        {:reply, :ok, new_state, @idle_timeout_ms}

      {:wait, _holder} ->
        new_queue = :queue.in({from, client_ref, range, type, mode, ttl}, state.wait_queue)
        {:noreply, %{state | wait_queue: new_queue}, @idle_timeout_ms}
    end
  end

  def handle_call({:unlock, client_ref, range}, _from, state) do
    new_locks =
      Enum.reject(state.locks, fn entry ->
        entry.client_ref == client_ref and entry.range == range
      end)

    new_state = %{state | locks: new_locks}
    new_state = process_wait_queue(new_state)
    new_state = process_write_wait_queue(new_state)

    :telemetry.execute(
      [:neonfs, :lock_manager, :file_lock, :unlocked],
      %{},
      %{file_id: state.file_id, client_ref: client_ref}
    )

    maybe_stop(new_state)
  end

  def handle_call({:unlock_all, client_ref}, _from, state) do
    new_locks = Enum.reject(state.locks, &(&1.client_ref == client_ref))
    new_opens = Enum.reject(state.opens, &(&1.client_ref == client_ref))
    new_leases = Enum.reject(state.leases, &(&1.client_ref == client_ref))

    new_state = %{state | locks: new_locks, opens: new_opens, leases: new_leases}
    new_state = process_wait_queue(new_state)
    new_state = process_write_wait_queue(new_state)

    maybe_stop(new_state)
  end

  def handle_call({:open, client_ref, access, deny, ttl}, _from, state) do
    now = System.monotonic_time(:millisecond)
    state = purge_expired(state, now)

    case check_share_conflict(state.opens, client_ref, access, deny) do
      :ok ->
        entry = %{
          client_ref: client_ref,
          access: access,
          deny: deny,
          expires_at: now + ttl
        }

        new_state = %{state | opens: [entry | state.opens]}
        {:reply, :ok, new_state, @idle_timeout_ms}

      :conflict ->
        {:reply, {:error, :share_violation}, state, @idle_timeout_ms}
    end
  end

  def handle_call({:close, client_ref}, _from, state) do
    new_opens = Enum.reject(state.opens, &(&1.client_ref == client_ref))
    new_state = %{state | opens: new_opens}
    new_state = process_write_wait_queue(new_state)
    maybe_stop(new_state)
  end

  def handle_call({:grant_lease, client_ref, lease_type, break_callback, ttl}, _from, state) do
    now = System.monotonic_time(:millisecond)
    state = purge_expired(state, now)

    case check_lease_conflict(state.leases, client_ref, lease_type) do
      :ok ->
        entry = %{
          client_ref: client_ref,
          type: lease_type,
          break_callback: break_callback,
          expires_at: now + ttl
        }

        new_state = %{state | leases: [entry | state.leases]}
        {:reply, :ok, new_state, @idle_timeout_ms}

      :conflict ->
        {:reply, {:error, :conflict}, state, @idle_timeout_ms}
    end
  end

  def handle_call({:break_lease, client_ref}, _from, state) do
    case Enum.find(state.leases, &(&1.client_ref == client_ref)) do
      nil ->
        {:reply, {:error, :not_found}, state, @idle_timeout_ms}

      lease ->
        if lease.break_callback, do: safe_break_callback(lease.break_callback)
        new_leases = Enum.reject(state.leases, &(&1.client_ref == client_ref))
        new_state = %{state | leases: new_leases}
        maybe_stop(new_state)
    end
  end

  def handle_call({:renew, client_ref, ttl}, _from, state) do
    now = System.monotonic_time(:millisecond)
    new_expires = now + ttl

    {found, new_state} = renew_entries(state, client_ref, new_expires)

    if found do
      {:reply, :ok, new_state, @idle_timeout_ms}
    else
      {:reply, {:error, :not_found}, state, @idle_timeout_ms}
    end
  end

  def handle_call({:check_write, client_ref, range}, _from, state) do
    now = System.monotonic_time(:millisecond)
    state = purge_expired(state, now)

    with :ok <- check_mandatory_write_conflict(state.locks, client_ref, range),
         :ok <- check_write_share_mode(state.opens, client_ref) do
      {:reply, :ok, state, @idle_timeout_ms}
    else
      :conflict -> {:reply, {:error, :lock_conflict}, state, @idle_timeout_ms}
      :share_denied -> {:reply, {:error, :share_denied}, state, @idle_timeout_ms}
    end
  end

  def handle_call({:check_write_blocking, client_ref, range}, from, state) do
    now = System.monotonic_time(:millisecond)
    state = purge_expired(state, now)

    with :ok <- check_mandatory_write_conflict(state.locks, client_ref, range),
         :ok <- check_write_share_mode(state.opens, client_ref) do
      {:reply, :ok, state, @idle_timeout_ms}
    else
      _reason ->
        new_queue = :queue.in({from, client_ref, range}, state.write_wait_queue)

        :telemetry.execute(
          [:neonfs, :lock_manager, :file_lock, :write_blocked],
          %{},
          %{file_id: state.file_id, client_ref: client_ref}
        )

        {:noreply, %{state | write_wait_queue: new_queue}, @idle_timeout_ms}
    end
  end

  def handle_call(:status, _from, state) do
    now = System.monotonic_time(:millisecond)

    status = %{
      file_id: state.file_id,
      lock_count: length(state.locks),
      open_count: length(state.opens),
      lease_count: length(state.leases),
      wait_queue_length: :queue.len(state.wait_queue),
      write_wait_queue_length: :queue.len(state.write_wait_queue),
      locks: Enum.map(state.locks, &sanitise_entry(&1, now)),
      opens: Enum.map(state.opens, &sanitise_entry(&1, now)),
      leases: Enum.map(state.leases, &sanitise_entry(&1, now))
    }

    {:reply, status, state, @idle_timeout_ms}
  end

  @impl true
  def handle_info(:ttl_check, state) do
    now = System.monotonic_time(:millisecond)
    new_state = purge_expired(state, now)
    new_state = process_wait_queue(new_state)
    new_state = process_write_wait_queue(new_state)
    timer = schedule_ttl_check()
    new_state = %{new_state | ttl_timer: timer}

    if empty?(new_state) do
      {:stop, :normal, new_state}
    else
      {:noreply, new_state, @idle_timeout_ms}
    end
  end

  def handle_info(:timeout, state) do
    if empty?(state) do
      :telemetry.execute(
        [:neonfs, :lock_manager, :file_lock, :stopped],
        %{},
        %{file_id: state.file_id, reason: :idle}
      )

      {:stop, :normal, state}
    else
      {:noreply, state, @idle_timeout_ms}
    end
  end

  @impl true
  def terminate(_reason, state) do
    if state.ttl_timer, do: Process.cancel_timer(state.ttl_timer)

    :telemetry.execute(
      [:neonfs, :lock_manager, :file_lock, :stopped],
      %{},
      %{file_id: state.file_id, reason: :terminate}
    )

    :ok
  end

  ## Private functions

  defp check_write_share_mode(opens, client_ref) do
    denied =
      Enum.any?(opens, fn entry ->
        entry.client_ref != client_ref and
          entry.deny in [:write, :read_write]
      end)

    if denied, do: :share_denied, else: :ok
  end

  defp check_mandatory_write_conflict(locks, client_ref, {offset, length}) do
    conflict =
      Enum.any?(locks, fn entry ->
        entry.client_ref != client_ref and
          entry.mode == :mandatory and
          ranges_overlap?(entry.range, {offset, length})
      end)

    if conflict, do: :conflict, else: :ok
  end

  defp check_lock_conflict(locks, client_ref, {offset, length}, type) do
    conflicting =
      Enum.find(locks, fn entry ->
        entry.client_ref != client_ref and
          ranges_overlap?(entry.range, {offset, length}) and
          lock_types_conflict?(entry.type, type)
      end)

    case conflicting do
      nil -> :ok
      entry -> {:wait, entry.client_ref}
    end
  end

  defp ranges_overlap?({o1, l1}, {o2, l2}) do
    o1 < o2 + l2 and o2 < o1 + l1
  end

  defp lock_types_conflict?(:exclusive, _), do: true
  defp lock_types_conflict?(_, :exclusive), do: true
  defp lock_types_conflict?(:shared, :shared), do: false

  defp check_share_conflict(opens, _client_ref, access, deny) do
    conflict =
      Enum.any?(opens, fn existing ->
        denied_by_existing?(existing.deny, access) or
          denied_by_new?(deny, existing.access)
      end)

    if conflict, do: :conflict, else: :ok
  end

  defp denied_by_existing?(deny, access) do
    case {deny, access} do
      {:none, _} -> false
      {:read, a} when a in [:read, :read_write] -> true
      {:write, a} when a in [:write, :read_write] -> true
      {:read_write, a} when a != :none -> true
      _ -> false
    end
  end

  defp denied_by_new?(deny, existing_access) do
    denied_by_existing?(deny, existing_access)
  end

  defp check_lease_conflict(leases, client_ref, _lease_type) do
    conflict =
      Enum.any?(leases, fn entry ->
        entry.client_ref != client_ref
      end)

    if conflict, do: :conflict, else: :ok
  end

  defp purge_expired(state, now) do
    new_locks = Enum.filter(state.locks, &(&1.expires_at > now))
    new_opens = Enum.filter(state.opens, &(&1.expires_at > now))
    new_leases = Enum.filter(state.leases, &(&1.expires_at > now))

    expired_lock_count = length(state.locks) - length(new_locks)
    expired_open_count = length(state.opens) - length(new_opens)
    expired_lease_count = length(state.leases) - length(new_leases)
    total_expired = expired_lock_count + expired_open_count + expired_lease_count

    if total_expired > 0 do
      :telemetry.execute(
        [:neonfs, :lock_manager, :file_lock, :expired],
        %{count: total_expired},
        %{
          file_id: state.file_id,
          locks: expired_lock_count,
          opens: expired_open_count,
          leases: expired_lease_count
        }
      )
    end

    %{state | locks: new_locks, opens: new_opens, leases: new_leases}
  end

  defp process_wait_queue(state) do
    {new_queue, new_state} = drain_wait_queue(:queue.to_list(state.wait_queue), state, [])
    %{new_state | wait_queue: :queue.from_list(new_queue)}
  end

  defp process_write_wait_queue(state) do
    new_queue = drain_write_wait_queue(:queue.to_list(state.write_wait_queue), state, [])
    %{state | write_wait_queue: :queue.from_list(new_queue)}
  end

  defp drain_wait_queue([], state, still_waiting) do
    {Enum.reverse(still_waiting), state}
  end

  defp drain_wait_queue(
         [{from, client_ref, range, type, mode, ttl} | rest],
         state,
         still_waiting
       ) do
    case check_lock_conflict(state.locks, client_ref, range, type) do
      :ok ->
        now = System.monotonic_time(:millisecond)

        entry = %{
          client_ref: client_ref,
          range: range,
          type: type,
          mode: mode,
          expires_at: now + ttl
        }

        GenServer.reply(from, :ok)
        new_state = %{state | locks: [entry | state.locks]}
        drain_wait_queue(rest, new_state, still_waiting)

      _ ->
        drain_wait_queue(rest, state, [
          {from, client_ref, range, type, mode, ttl} | still_waiting
        ])
    end
  end

  defp drain_write_wait_queue([], _state, still_waiting) do
    Enum.reverse(still_waiting)
  end

  defp drain_write_wait_queue(
         [{from, client_ref, range} | rest],
         state,
         still_waiting
       ) do
    with :ok <- check_mandatory_write_conflict(state.locks, client_ref, range),
         :ok <- check_write_share_mode(state.opens, client_ref) do
      :telemetry.execute(
        [:neonfs, :lock_manager, :file_lock, :write_unblocked],
        %{},
        %{file_id: state.file_id, client_ref: client_ref}
      )

      GenServer.reply(from, :ok)
      drain_write_wait_queue(rest, state, still_waiting)
    else
      _ ->
        drain_write_wait_queue(rest, state, [{from, client_ref, range} | still_waiting])
    end
  end

  defp renew_entries(state, client_ref, new_expires) do
    {found_lock, new_locks} = renew_list(state.locks, client_ref, new_expires)
    {found_open, new_opens} = renew_list(state.opens, client_ref, new_expires)
    {found_lease, new_leases} = renew_list(state.leases, client_ref, new_expires)

    found = found_lock or found_open or found_lease
    {found, %{state | locks: new_locks, opens: new_opens, leases: new_leases}}
  end

  defp renew_list(entries, client_ref, new_expires) do
    {renewed, found} =
      Enum.map_reduce(entries, false, fn entry, found ->
        if entry.client_ref == client_ref do
          {%{entry | expires_at: new_expires}, true}
        else
          {entry, found}
        end
      end)

    {found, renewed}
  end

  defp empty?(state) do
    state.locks == [] and state.opens == [] and state.leases == [] and
      :queue.is_empty(state.wait_queue) and :queue.is_empty(state.write_wait_queue)
  end

  defp maybe_stop(new_state) do
    if empty?(new_state) do
      :telemetry.execute(
        [:neonfs, :lock_manager, :file_lock, :stopped],
        %{},
        %{file_id: new_state.file_id, reason: :empty}
      )

      {:stop, :normal, :ok, new_state}
    else
      {:reply, :ok, new_state, @idle_timeout_ms}
    end
  end

  defp schedule_ttl_check do
    Process.send_after(self(), :ttl_check, @ttl_check_interval_ms)
  end

  defp sanitise_entry(entry, now) do
    Map.put(entry, :ttl_remaining_ms, max(0, entry.expires_at - now))
    |> Map.delete(:expires_at)
    |> Map.delete(:break_callback)
  end

  defp find_conflicting_lock(locks, client_ref, {offset, length}, type) do
    Enum.find(locks, fn entry ->
      entry.client_ref != client_ref and
        ranges_overlap?(entry.range, {offset, length}) and
        lock_types_conflict?(entry.type, type)
    end)
  end

  defp extract_svid({_caller_name, svid}) when is_integer(svid), do: svid
  defp extract_svid(_), do: 0

  defp extract_oh({_caller_name, _svid}), do: <<>>
  defp extract_oh(_), do: <<>>

  defp safe_break_callback(callback) do
    Task.start(fn ->
      try do
        callback.()
      rescue
        error -> Logger.warning("Lease break callback failed", error: Exception.message(error))
      end
    end)
  end
end
