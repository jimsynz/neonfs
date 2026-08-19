defmodule NeonFS.Client.HostLock do
  @moduledoc """
  Serialises an operation across every process on one host.

  Provisioning cluster credentials is per *host*, but the thing that performs it
  is a Kubernetes init container, and several pods can be scheduled onto a host
  at once. Without serialisation each of them redeems, spending a redemption
  budget per pod instead of per host.

  ## Why a file, and why a deadline

  The lock has to hold between unrelated OS processes in separate containers, so
  it cannot live in a BEAM: the only thing they share is the host state
  directory. `File.open/2` with `:exclusive` is `O_CREAT | O_EXCL`, which the
  kernel makes atomic, so exactly one caller creates the file and the rest see
  `:eexist`.

  What that does not give us is release on death. A holder killed mid-operation
  — an init container that hits its deadline, a node reboot — leaves the file
  behind, and a lock nothing can clear would wedge every pod that later lands
  on that host. Since the holder may be in another PID namespace, its liveness
  is not observable, so staleness is decided by age: a lock older than
  `:stale_after_ms` is broken and taken.

  That means the timeout is a real bound on the operation, not a guess to be
  tuned upward. A holder still working when its lock is broken will race the
  breaker, which is why `with_lock/2` is for operations that are safe to run
  twice — redeeming credentials is, costing one extra unit of an invite's
  budget.
  """

  alias NeonFS.Cluster.State

  require Logger

  @default_stale_after_ms 120_000
  @default_wait_ms 60_000
  @poll_ms 100

  @doc """
  Run `fun` while holding the host lock named `name`.

  Returns `fun`'s value, `{:error, {:lock_timeout, name}}` if the lock could not
  be taken before the wait deadline, or `{:error, reason}` if the lock file
  could not be created at all.

  ## Options

    * `:dir` — directory holding the lock file. Defaults to the cluster meta
      directory, which is the state a host provisioned this way already shares.
    * `:stale_after_ms` — age at which an existing lock is broken (default
      #{@default_stale_after_ms}).
    * `:wait_ms` — how long to wait for a holder to finish (default
      #{@default_wait_ms}).
    * `:on_wait` — zero-arity function called once when the lock is found held.
      Lets a caller report that it is waiting rather than appearing hung.
  """
  @spec with_lock(String.t(), (-> result), keyword()) :: result | {:error, term()}
        when result: term()
  def with_lock(name, fun, opts \\ []) when is_binary(name) and is_function(fun, 0) do
    path = lock_path(name, opts)
    deadline = System.monotonic_time(:millisecond) + Keyword.get(opts, :wait_ms, @default_wait_ms)

    case acquire(path, deadline, opts, _notified? = false) do
      :ok ->
        try do
          fun.()
        after
          release(path)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp acquire(path, deadline, opts, notified?) do
    case File.open(path, [:write, :exclusive]) do
      {:ok, device} ->
        IO.binwrite(device, held_by())
        File.close(device)
        :ok

      {:error, :eexist} ->
        contend(path, deadline, opts, notified?)

      {:error, :enoent} ->
        case File.mkdir_p(Path.dirname(path)) do
          :ok -> acquire(path, deadline, opts, notified?)
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp contend(path, deadline, opts, notified?) do
    notified? || run_on_wait(opts)

    cond do
      stale?(path, opts) ->
        Logger.warning("Breaking a stale host lock", path: path)
        File.rm(path)
        acquire(path, deadline, opts, true)

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, {:lock_timeout, Path.basename(path)}}

      true ->
        Process.sleep(@poll_ms)
        acquire(path, deadline, opts, true)
    end
  end

  # Age is taken from the filesystem rather than from the lock's contents so a
  # truncated or half-written lock is still judged, and mtime rather than ctime
  # because only mtime survives the file being read.
  defp stale?(path, opts) do
    stale_after = Keyword.get(opts, :stale_after_ms, @default_stale_after_ms)

    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{mtime: mtime}} ->
        System.os_time(:second) - mtime > div(stale_after, 1000)

      {:error, :enoent} ->
        false

      {:error, _reason} ->
        false
    end
  end

  defp release(path) do
    case File.rm(path) do
      :ok ->
        :ok

      {:error, :enoent} ->
        # Someone judged this lock stale and broke it while it was held, so the
        # operation ran longer than `:stale_after_ms`. Worth saying out loud:
        # it means another caller is running the same operation concurrently.
        Logger.warning("Host lock was already gone on release", path: path)
        :ok

      {:error, reason} ->
        Logger.warning("Could not release host lock", path: path, reason: inspect(reason))
        :ok
    end
  end

  defp run_on_wait(opts) do
    case Keyword.get(opts, :on_wait) do
      fun when is_function(fun, 0) -> fun.()
      _ -> :ok
    end

    false
  end

  # Diagnostic only — nothing reads this back to decide anything, because the
  # holder is usually in another PID namespace where neither the pid nor the
  # node name means anything locally.
  defp held_by do
    "#{Node.self()} os_pid=#{System.pid()} at=#{DateTime.utc_now() |> DateTime.to_iso8601()}\n"
  end

  defp lock_path(name, opts) do
    dir = Keyword.get_lazy(opts, :dir, fn -> State.meta_dir() end)
    Path.join(dir, ".#{name}.lock")
  end
end
