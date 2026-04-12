defmodule NeonFS.Core.LockManager do
  @moduledoc """
  Distributed lock manager for cross-protocol file locking.

  Provides byte-range locks, share modes, and leases (oplocks/delegations)
  across all NeonFS access protocols (FUSE, NFS, S3, SMB, WebDAV).

  ## Routing

  Each file ID is deterministically hashed to a single core node (the lock
  master). Lock operations are GenServer calls to the master — in-memory,
  no Ra consensus. Lock load distributes across core nodes proportional to
  file count.

  ## Lock types

  - **Byte-range locks**: `[offset, offset+length)`, shared or exclusive
  - **Share modes**: SMB open semantics — access + deny flags
  - **Leases/oplocks**: Performance grants with break callbacks

  ## Protocol translation

  Each protocol adapter translates its semantics into the internal
  representation. The lock manager is protocol-agnostic.

  | Protocol | Internal operation |
  |----------|-------------------|
  | SMB byte-range lock | `lock/5` with `:mandatory` mode |
  | NFSv4 lock | `lock/5` with `:mandatory` mode |
  | NLM (NFSv3) | `lock/5` with `:advisory` mode |
  | POSIX flock/fcntl | `lock/5` with `:advisory` mode |
  | SMB share mode | `open/5` |
  | SMB oplock/lease | `grant_lease/4` |
  | NFSv4 delegation | `grant_lease/4` |
  """

  alias NeonFS.Core.LockManager.FileLock
  alias NeonFS.Core.LockManager.Supervisor, as: LockSupervisor
  alias NeonFS.Core.{MetadataRing, ServiceRegistry}

  @type file_id :: binary()

  @doc """
  Acquires a byte-range lock on a file.

  Returns `:ok` if the lock was granted, `{:error, :conflict}` if an
  incompatible lock is held and no waiting is possible, or blocks until
  the conflicting lock is released.

  ## Options

    * `:ttl` — lock TTL in milliseconds (default: 90_000)
    * `:mode` — `:advisory` or `:mandatory` (default: `:advisory`)
    * `:timeout` — call timeout in milliseconds (default: 5_000)
  """
  @spec lock(file_id(), FileLock.client_ref(), FileLock.range(), FileLock.lock_type(), keyword()) ::
          :ok | {:error, :timeout | :unavailable}
  def lock(file_id, client_ref, range, type, opts \\ []) do
    with_file_lock(file_id, fn pid ->
      FileLock.lock(pid, client_ref, range, type, opts)
    end)
  end

  @doc """
  Releases a specific byte-range lock.
  """
  @spec unlock(file_id(), FileLock.client_ref(), FileLock.range()) ::
          :ok | {:error, :unavailable}
  def unlock(file_id, client_ref, range) do
    with_existing_file_lock(file_id, fn pid ->
      FileLock.unlock(pid, client_ref, range)
    end)
  end

  @doc """
  Releases all locks, opens, and leases held by a client on a file.
  """
  @spec unlock_all(file_id(), FileLock.client_ref()) :: :ok | {:error, :unavailable}
  def unlock_all(file_id, client_ref) do
    with_existing_file_lock(file_id, fn pid ->
      FileLock.unlock_all(pid, client_ref)
    end)
  end

  @doc """
  Opens a file with share mode semantics (SMB).

  ## Options

    * `:ttl` — open TTL in milliseconds (default: 90_000)
  """
  @spec open(
          file_id(),
          FileLock.client_ref(),
          FileLock.share_access(),
          FileLock.share_deny(),
          keyword()
        ) :: :ok | {:error, :share_violation | :unavailable}
  def open(file_id, client_ref, access, deny, opts \\ []) do
    with_file_lock(file_id, fn pid ->
      FileLock.open(pid, client_ref, access, deny, opts)
    end)
  end

  @doc """
  Closes a file (releases share mode).
  """
  @spec close(file_id(), FileLock.client_ref()) :: :ok | {:error, :unavailable}
  def close(file_id, client_ref) do
    with_existing_file_lock(file_id, fn pid ->
      FileLock.close(pid, client_ref)
    end)
  end

  @doc """
  Grants a lease (oplock/delegation) on a file.

  ## Options

    * `:ttl` — lease TTL in milliseconds (default: 90_000)
    * `:break_callback` — zero-arity function called when the lease is broken
  """
  @spec grant_lease(file_id(), FileLock.client_ref(), FileLock.lease_type(), keyword()) ::
          :ok | {:error, :conflict | :unavailable}
  def grant_lease(file_id, client_ref, lease_type, opts \\ []) do
    with_file_lock(file_id, fn pid ->
      FileLock.grant_lease(pid, client_ref, lease_type, opts)
    end)
  end

  @doc """
  Breaks a lease held by a client, invoking the break callback.
  """
  @spec break_lease(file_id(), FileLock.client_ref()) ::
          :ok | {:error, :not_found | :unavailable}
  def break_lease(file_id, client_ref) do
    with_existing_file_lock(file_id, fn pid ->
      FileLock.break_lease(pid, client_ref)
    end)
  end

  @doc """
  Renews all locks, opens, and leases held by a client on a file.

  ## Options

    * `:ttl` — new TTL in milliseconds (default: 90_000)
  """
  @spec renew(file_id(), FileLock.client_ref(), keyword()) ::
          :ok | {:error, :not_found | :unavailable}
  def renew(file_id, client_ref, opts \\ []) do
    with_existing_file_lock(file_id, fn pid ->
      FileLock.renew(pid, client_ref, opts)
    end)
  end

  @doc """
  Checks whether a write to the given byte range of a file is permitted.

  Returns `:ok` if the write is allowed, or an error if blocked:

  - `{:error, :lock_conflict}` — a mandatory byte-range lock held by
    another client overlaps the write range. Advisory locks are ignored.
  - `{:error, :share_denied}` — another client has the file open with
    `deny: :write` or `deny: :read_write`.
  - `{:error, :unavailable}` — the lock master node is unreachable.

  If no lock state exists for the file (no FileLock process), the write
  is always permitted.
  """
  @spec check_write(file_id(), FileLock.client_ref(), FileLock.range()) ::
          :ok | {:error, :lock_conflict | :share_denied | :unavailable}
  def check_write(file_id, client_ref, range) do
    with_existing_file_lock(file_id, fn pid ->
      FileLock.check_write(pid, client_ref, range)
    end)
  end

  @doc """
  Like `check_write/3` but blocks until the conflict clears.

  When a mandatory byte-range lock or deny-write share mode would block
  the write, the caller is queued and unblocked when the conflicting
  entry is released or expires.

  ## Options

    * `:timeout` — how long to wait in milliseconds (default: 5_000)
  """
  @spec check_write_blocking(file_id(), FileLock.client_ref(), FileLock.range(), keyword()) ::
          :ok | {:error, :unavailable}
  def check_write_blocking(file_id, client_ref, range, opts \\ []) do
    with_existing_file_lock(file_id, fn pid ->
      FileLock.check_write_blocking(pid, client_ref, range, opts)
    end)
  end

  @doc """
  Returns the status of locks on a file.
  """
  @spec status(file_id()) :: {:ok, map()} | {:error, :not_found | :unavailable}
  def status(file_id) do
    case lookup_file_lock(file_id) do
      {:ok, pid} -> {:ok, FileLock.status(pid)}
      :not_found -> {:error, :not_found}
    end
  end

  @doc """
  Returns the lock master node for a given file ID.

  Uses consistent hashing to deterministically route to a core node.
  """
  @spec master_for(file_id()) :: node()
  def master_for(file_id) do
    ring = lock_ring()
    {_segment, [master | _]} = MetadataRing.locate(ring, file_id)
    master
  end

  ## Private functions

  defp with_file_lock(file_id, fun) do
    master = master_for(file_id)

    if master == Node.self() do
      {:ok, pid} = LockSupervisor.ensure_file_lock(file_id)
      fun.(pid)
    else
      :erpc.call(master, __MODULE__, :local_with_file_lock, [file_id, fun])
    end
  catch
    :exit, {:erpc, :noconnection} -> {:error, :unavailable}
    :exit, _ -> {:error, :unavailable}
  end

  defp with_existing_file_lock(file_id, fun) do
    master = master_for(file_id)

    if master == Node.self() do
      case lookup_file_lock(file_id) do
        {:ok, pid} -> fun.(pid)
        :not_found -> :ok
      end
    else
      :erpc.call(master, __MODULE__, :local_with_existing_file_lock, [file_id, fun])
    end
  catch
    :exit, {:erpc, :noconnection} -> {:error, :unavailable}
    :exit, _ -> {:error, :unavailable}
  end

  defp lookup_file_lock(file_id) do
    case Registry.lookup(NeonFS.Core.LockManager.Registry, file_id) do
      [{pid, _}] -> {:ok, pid}
      [] -> :not_found
    end
  end

  @doc false
  @spec local_with_file_lock(file_id(), (pid() -> term())) :: term()
  def local_with_file_lock(file_id, fun) do
    {:ok, pid} = LockSupervisor.ensure_file_lock(file_id)
    fun.(pid)
  end

  @doc false
  @spec local_with_existing_file_lock(file_id(), (pid() -> term())) :: term()
  def local_with_existing_file_lock(file_id, fun) do
    case lookup_file_lock(file_id) do
      {:ok, pid} -> fun.(pid)
      :not_found -> :ok
    end
  end

  defp lock_ring do
    core_nodes = [Node.self() | ServiceRegistry.connected_nodes_by_type(:core)]

    MetadataRing.new(Enum.uniq(core_nodes),
      virtual_nodes_per_physical: 64,
      replicas: 1
    )
  rescue
    ArgumentError ->
      MetadataRing.new([Node.self()],
        virtual_nodes_per_physical: 64,
        replicas: 1
      )
  end
end
