defmodule NeonFS.Block.Ublk.Target do
  @moduledoc """
  A ublk-attached device: the helper process, its per-queue sockets, and the
  IO they carry.

  The peer of `NeonFS.Block.ConnectionHandler`. That one takes IO off a TCP
  socket an `nbd-client` opened; this one takes it off a Unix socket a helper
  process opened, having got it from the kernel over io_uring. Both hold their
  device through `NeonFS.Block.DeviceRegistry` and answer IO through the
  `NeonFS.Block.Frontend` callbacks — there is one implementation of the
  device, one attachment claim protecting it, and two ways for a guest to
  reach it.

  ## One helper process per device

  Its lifetime is the attachment's, so a fault in the ublk binding or in
  io_uring takes one guest's device rather than the node's whole data path.
  The accepted cost is a process spawn on the attach path and N sockets on a
  node with many attachments.

  ## One socket and one process per queue

  Concurrency is then real: a slow IO blocks its own queue and nothing else.
  The accepted cost is that the queue count becomes an attach-time parameter
  an operator can get wrong, and that tearing a device down has to reap every
  socket and process rather than one.

  ## A dead helper is recovered, not fatal

  The device is created with `UBLK_F_USER_RECOVERY`, so a helper that dies
  leaves the kernel holding the device *quiesced* rather than taking it away:
  IO is held, `/dev/ublkbN` stays where it is, and a new helper can resume it
  in place. That is what this process does — kill the orphaned queue servers,
  spawn a helper in recovery mode against the same device id, accept its
  sockets, wait for it to say the device is back.

  It matters that the path does not change. Restarting a helper without
  `USER_RECOVERY` would create a *fresh* device at a fresh `/dev/ublkbN`,
  which is no use to a guest holding the old one — so before the flag, taking
  the device down was the honest answer.

  ### Bounded, and not for every failure

  Recovery is bounded by `@recovery_attempts` within `@recovery_window_ms`. A
  helper that dies once is a crash; one that dies five times in a minute is a
  helper that cannot serve this device, and retrying it forever would hold the
  attachment claim against a device nothing can use. On exhaustion the device
  goes, which is the behaviour that preceded this.

  A **fence** is never recovered. Being fenced means another node owns the
  device now, and every IO in flight belongs to an epoch that is no longer
  current — resuming would be the one outcome fencing exists to prevent.
  """

  use GenServer

  alias NeonFS.Block.{DeviceRegistry, Frontend, Ublk}
  alias NeonFS.Block.Ublk.Capability

  require Logger

  @default_queues 1
  @default_queue_depth 64
  @max_devices 16
  @ready_timeout_ms 30_000
  @recovery_attempts 5
  @recovery_window_ms 60_000

  @type opts :: [
          export: String.t(),
          queues: pos_integer(),
          queue_depth: pos_integer(),
          name: GenServer.name()
        ]

  @doc """
  Attaches `export` as a ublk device, taking the cluster-wide claim on it.
  """
  @spec start_link(opts()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, if(name, do: [name: name], else: []))
  end

  @doc "The device this target published, once the helper has it."
  @spec device_path(GenServer.server()) :: {:ok, Path.t()} | {:error, term()}
  def device_path(target), do: GenServer.call(target, :device_path)

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    export = Keyword.fetch!(opts, :export)

    with :ok <- Capability.check(),
         {:ok, device_id} <- free_device_id(),
         {:ok, device} <- DeviceRegistry.attach(export, self()),
         {:ok, listeners} <- listen(Keyword.get(opts, :queues, @default_queues)) do
      info = Frontend.impl().export_info(device)
      helper = spawn_helper(info, listeners, device_id, opts)

      # Accepting comes first: the helper connects its queues before the
      # kernel publishes the device, so a target waiting for readiness with
      # nothing accepting would deadlock against its own helper.
      servers = accept_queues(device, info, listeners)

      case await_ready(helper, device_id) do
        :ok ->
          {:ok,
           %{
             device_id: device_id,
             device_path: device_path_for(device_id),
             export: export,
             helper: helper,
             info: info,
             device: device,
             opts: opts,
             listeners: listeners,
             recoveries: [],
             servers: servers
           }}

        {:error, reason} ->
          {:stop, reason}
      end
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call(:device_path, _from, state), do: {:reply, {:ok, state.device_path}, state}

  # The device this process holds has been taken from it. Everything in
  # flight belongs to an epoch that is no longer current, so the device goes
  # rather than completing any of it.
  @impl GenServer
  def handle_info({:fenced, export, current_epoch}, %{export: export} = state) do
    Logger.warning("ublk device fenced; dropping it",
      export: export,
      current_epoch: current_epoch
    )

    {:stop, {:shutdown, {:fenced, current_epoch}}, state}
  end

  def handle_info({:EXIT, port, reason}, %{helper: port} = state) do
    recover(state, {:helper_exited, reason})
  end

  # A queue server that stops has lost its socket, which is what a dead
  # helper looks like from this side — and whichever of the two is noticed
  # first, the answer is the same. A device serving some queues and stalling
  # the rest is the state neither of them may be left in.
  def handle_info({:EXIT, pid, reason}, state) do
    if pid in state.servers do
      recover(state, {:queue_stopped, reason})
    else
      {:noreply, state}
    end
  end

  def handle_info({port, {:exit_status, status}}, %{helper: port} = state) do
    recover(state, {:helper_exited, {:exit_status, status}})
  end

  # The helper's stdout and stderr, which is where its own diagnostics go.
  def handle_info({port, {:data, output}}, %{helper: port} = state) do
    Logger.warning("ublk helper: #{String.trim(output)}", export: state.export)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    DeviceRegistry.detach(state.export, self())

    Enum.each(state.listeners, fn {_queue, %{socket: socket, path: path}} ->
      :gen_tcp.close(socket)
      File.rm(path)
    end)

    :ok
  end

  # Whichever symptom arrived, the helper is gone: reap what is left of it,
  # start another against the quiesced device, and resume. A budget stops a
  # helper that cannot serve this device from holding its attachment claim
  # forever.
  defp recover(state, cause) do
    recoveries = recent(state.recoveries)

    if length(recoveries) >= @recovery_attempts do
      Logger.error("ublk helper failed too often; the device goes with it",
        export: state.export,
        cause: inspect(cause),
        attempts: length(recoveries)
      )

      emit(:recovery_exhausted, state, %{attempts: length(recoveries)})
      {:stop, {:ublk_recovery_exhausted, cause}, state}
    else
      Logger.warning("ublk helper gone; recovering the device in place",
        export: state.export,
        cause: inspect(cause),
        device_path: state.device_path
      )

      emit(:recovery_started, state, %{attempt: length(recoveries) + 1})
      restart_helper(%{state | recoveries: [now() | recoveries]}, cause)
    end
  end

  # The same geometry and the same attach options as the first helper, from
  # state rather than re-derived: a recovering helper is reopening a device
  # the driver already describes, so the two must not disagree about it.
  defp restart_helper(state, cause) do
    reap(state)

    helper = spawn_helper(state.info, state.listeners, state.device_id, state.opts, recover: true)
    servers = accept_queues(state.device, state.info, state.listeners)

    case await_ready(helper, state.device_id) do
      :ok ->
        Logger.info("ublk device recovered", export: state.export, device_path: state.device_path)
        emit(:recovery_completed, state, %{})
        {:noreply, %{state | helper: helper, servers: servers}}

      {:error, reason} ->
        # The replacement could not take the device either. That is another
        # failure against the budget rather than a distinct outcome, so it
        # goes back through the same door.
        recover(%{state | helper: helper, servers: servers}, {:recovery_failed, reason, cause})
    end
  end

  # The old helper's sockets are dead and its queue servers are blocked on
  # them. Nothing here waits for them to notice.
  defp reap(state) do
    Enum.each(state.servers, &Process.exit(&1, :kill))
    if is_port(state.helper), do: safely_close(state.helper)
  end

  defp safely_close(port) do
    Port.close(port)
  rescue
    ArgumentError -> :ok
  end

  defp recent(recoveries) do
    cutoff = now() - @recovery_window_ms
    Enum.filter(recoveries, &(&1 > cutoff))
  end

  defp now, do: System.monotonic_time(:millisecond)

  defp emit(event, state, measurements) do
    :telemetry.execute(
      [:neonfs, :block, :ublk, event],
      Map.put(measurements, :count, 1),
      %{export: state.export, device_path: state.device_path}
    )
  end

  # The node picks the device number rather than reading back whichever the
  # driver allocated, so it knows the path before the helper has started —
  # the same shape CSI's NBD attach uses to pick its own `/dev/nbdX`. Racing
  # another attach for the same number is possible and is what the helper's
  # own failure to create the device then reports.
  defp free_device_id do
    case Enum.find(0..(@max_devices - 1), &(not File.exists?(device_path_for(&1)))) do
      nil -> {:error, {:no_free_ublk_device, @max_devices}}
      id -> {:ok, id}
    end
  end

  defp device_path_for(device_id), do: "/dev/ublkb#{device_id}"

  # The helper announces itself only after `start_dev`, which is the one
  # moment the device is known to exist. Waiting for the socket instead would
  # hand out a path to nothing, because the queues connect before that.
  defp await_ready(helper, device_id) do
    expected = "ready #{device_id}"

    receive do
      {^helper, {:data, output}} ->
        if String.contains?(output, expected),
          do: :ok,
          else: await_ready(helper, device_id)

      {^helper, {:exit_status, status}} ->
        {:error, {:ublk_helper_exited, status}}

      {:EXIT, ^helper, reason} ->
        {:error, {:ublk_helper_exited, reason}}
    after
      @ready_timeout_ms -> {:error, {:ublk_device_never_appeared, device_path_for(device_id)}}
    end
  end

  # A listening socket per queue, named by the queue so the helper can find
  # each one from the prefix it was given rather than being told N paths.
  #
  # The OS pid is in the name as well as a unique integer, because the
  # integer restarts with the VM: without it a node that crashed would find
  # its own dead sockets at the paths it wants and fail every attach with
  # `:eaddrinuse` until someone cleaned `/tmp` by hand. With it, a file at
  # one of these paths belongs either to this VM — which has not used that
  # integer before — or to a process that no longer exists, so removing it
  # cannot take a live listener from anyone.
  defp listen(queues) do
    prefix =
      Path.join(
        System.tmp_dir!(),
        "neonfs-ublk-#{System.pid()}-#{System.unique_integer([:positive])}"
      )

    Enum.reduce_while(0..(queues - 1), {:ok, %{}}, fn queue, {:ok, acc} ->
      path = "#{prefix}.#{queue}"
      File.rm(path)

      case :gen_tcp.listen(0, [
             {:ifaddr, {:local, path}},
             :binary,
             packet: :raw,
             active: false,
             backlog: @max_devices,
             reuseaddr: true
           ]) do
        {:ok, socket} ->
          {:cont, {:ok, Map.put(acc, queue, %{socket: socket, path: path, prefix: prefix})}}

        {:error, reason} ->
          {:halt, {:error, {:ublk_socket_failed, path, reason}}}
      end
    end)
  end

  # The helper learns the geometry from its environment rather than over the
  # socket: it has to size the device before it accepts a single IO, so a
  # handshake would only be a second way to say what is already known here.
  defp spawn_helper(info, listeners, device_id, opts, mode \\ []) do
    %{prefix: prefix} = listeners |> Map.values() |> hd()

    Port.open({:spawn_executable, Capability.helper_path()}, [
      :binary,
      :exit_status,
      :stderr_to_stdout,
      env:
        recovery_env(mode) ++
          [
            {~c"NEONFS_UBLK_SOCKET", charlist(prefix)},
            {~c"NEONFS_UBLK_ID", charlist(device_id)},
            {~c"NEONFS_UBLK_SIZE_BYTES", charlist(info.size)},
            {~c"NEONFS_UBLK_BLOCK_BYTES", charlist(info.logical_block_size)},
            {~c"NEONFS_UBLK_QUEUES", charlist(Keyword.get(opts, :queues, @default_queues))},
            {~c"NEONFS_UBLK_QUEUE_DEPTH",
             charlist(Keyword.get(opts, :queue_depth, @default_queue_depth))}
          ]
    ])
  end

  # Present or absent, never a value: the helper reads it with `is_ok`, so a
  # `false` would enable recovery on a fresh device and fail at the driver.
  defp recovery_env(mode) do
    if Keyword.get(mode, :recover, false),
      do: [{~c"NEONFS_UBLK_RECOVER", ~c"1"}],
      else: []
  end

  defp charlist(value), do: value |> to_string() |> String.to_charlist()

  # One process per queue, each owning its accepted socket. Linked, so a
  # queue that cannot serve takes the device down rather than leaving it
  # partly answering.
  defp accept_queues(device, info, listeners) do
    Enum.map(listeners, fn {queue, %{socket: listener}} ->
      spawn_link(fn -> Ublk.Queue.serve(device, info, queue, listener) end)
    end)
  end
end
