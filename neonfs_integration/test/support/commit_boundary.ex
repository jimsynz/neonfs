defmodule NeonFS.Integration.CommitBoundary do
  @moduledoc """
  Crashes a core node's `FileIndex` in the window between a metadata batch
  reaching consensus and its post-commit effects running.

  Called on the peer itself via `PeerCluster.rpc/5` — every process it
  touches is node-local, and tracing a remote pid is neither cheap nor
  reliable.

  The window is invisible from outside the node. `FileIndex` publishes a
  whole flush as one `VolumeCommitter` call, and then — in the same
  process, with the write already durable — releases the intent lease,
  materialises its ETS cache, broadcasts the change event and replies to
  the caller. Suspending the volume's commit worker parks the batch at
  exactly that point: the `$gen_call` sits in the worker's mailbox,
  nothing has been published, and `FileIndex` is blocked waiting. Killing
  `FileIndex` there and then resuming the worker publishes the root set
  with none of its effects and no reply to anyone.

  What survives is the durable outcome and nothing else, which is the
  property the atomic root set exists to provide.
  """

  alias NeonFS.Core.{FileIndex, VolumeCommitter}
  alias NeonFS.TestSupport.ClusterCase

  @dispatch_timeout 30_000
  @publication_timeout 60_000
  @death_timeout 10_000
  @restart_timeout 30_000

  @doc """
  Runs `module.function(args)` against the local `FileIndex`, killing it
  once its batch has been handed to the commit worker and before any
  post-commit effect can run.

  Returns the commit worker's reply — `{:ok, roots}` when the publication
  reached consensus — after `FileIndex` has been restarted by its
  supervisor. The operation's own return value dies with the caller,
  which is the situation being reproduced.
  """
  @spec crash_before_effects(binary(), module(), atom(), [term()]) :: term()
  def crash_before_effects(volume_id, module, function, args) do
    worker = commit_worker(volume_id)
    file_index = Process.whereis(FileIndex)

    :sys.suspend(worker)

    reply =
      try do
        {tag, caller} = park_batch(file_index, worker, volume_id, {module, function, args})
        kill_and_await(file_index)
        published = publish_parked_batch(worker, tag)
        refute_effects_ran(caller, volume_id)
        published
      catch
        kind, reason ->
          :sys.resume(worker)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end

    await_restart(file_index)
    reply
  end

  defp park_batch(file_index, worker, volume_id, {module, function, args}) do
    parent = self()

    :erlang.trace(file_index, true, [:send])
    caller = spawn(fn -> send(parent, {:operation_returned, apply(module, function, args)}) end)
    caller_ref = Process.monitor(caller)
    tag = await_dispatch(file_index, worker, volume_id)
    :erlang.trace(file_index, false, [:send])
    {tag, caller_ref}
  end

  # Without this the whole harness could quietly become a no-op: a test
  # that asserts a complete operation passes just as happily when the
  # crash lands too late and every effect ran. Replying is the last of
  # those effects, so a caller that got an answer is proof the window was
  # missed. A caller that died with its call is proof it wasn't.
  defp refute_effects_ran(caller_ref, volume_id) do
    receive do
      {:DOWN, ^caller_ref, :process, _pid, _reason} -> :ok
    after
      @death_timeout -> raise "the operation on #{volume_id} neither returned nor died"
    end

    receive do
      {:operation_returned, result} ->
        raise "the operation returned #{inspect(result)}, so its post-commit effects all ran"
    after
      0 ->
        :ok
    end
  end

  # The batch is in the suspended worker's mailbox the moment `FileIndex`
  # sends the call, which is what makes the kill deterministic rather than
  # a race against a polling loop.
  #
  # Returns the call's reply tag. `gen` addresses a reply to an alias of
  # the caller rather than to the caller itself, so the tag — not the
  # `FileIndex` pid — is what identifies this batch's reply once
  # `FileIndex` is gone.
  defp await_dispatch(file_index, worker, volume_id) do
    receive do
      {:trace, ^file_index, :send, {:"$gen_call", {_pid, tag}, {:commit, ^volume_id, _m, _o}},
       ^worker} ->
        tag
    after
      @dispatch_timeout -> raise no_dispatch_message(volume_id)
    end
  end

  # An operation that never reaches the commit worker usually failed its
  # planning stage instead — an unacquirable intent lease, a missing
  # parent, a rejected path. Saying so beats a bare timeout.
  defp no_dispatch_message(volume_id) do
    receive do
      {:operation_returned, result} ->
        "the operation returned #{inspect(result)} without committing anything on #{volume_id}"
    after
      0 ->
        "FileIndex never dispatched a #{volume_id} batch to its commit worker"
    end
  end

  defp kill_and_await(file_index) do
    ref = Process.monitor(file_index)
    Process.exit(file_index, :kill)

    receive do
      {:DOWN, ^ref, :process, ^file_index, _reason} -> :ok
    after
      @death_timeout -> raise "FileIndex survived an untrappable kill"
    end
  end

  defp publish_parked_batch(worker, tag) do
    :erlang.trace(worker, true, [:send])
    :sys.resume(worker)
    reply = await_publication(worker, tag)
    :erlang.trace(worker, false, [:send])
    reply
  end

  # The reply is addressed to an alias of a process that no longer exists,
  # which the VM reports as `:send_to_non_existing_process` rather than
  # `:send`. Both carry the payload; accepting either keeps this from
  # depending on when the kill lands in the process table.
  defp await_publication(worker, tag) do
    receive do
      {:trace, ^worker, kind, {^tag, reply}, _to}
      when kind in [:send, :send_to_non_existing_process] ->
        reply
    after
      @publication_timeout -> raise "the commit worker never published the parked batch"
    end
  end

  defp await_restart(old_pid) do
    restarted? = fn ->
      case Process.whereis(FileIndex) do
        nil -> false
        ^old_pid -> false
        pid -> Process.alive?(pid)
      end
    end

    case ClusterCase.wait_until(restarted?, timeout: @restart_timeout) do
      :ok -> :ok
      {:error, :timeout} -> raise "FileIndex was not restarted after being killed"
    end
  end

  defp commit_worker(volume_id) do
    GenServer.whereis({:via, PartitionSupervisor, {VolumeCommitter.Supervisor, volume_id}}) ||
      raise "no commit worker is running for volume #{volume_id}"
  end
end
