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

  ## Losing anything is losing the device

  A dead helper, a dead queue server or a fence all stop this process, which
  drops the ublk device. The kernel then sees a device that went away rather
  than one that silently stalls, and a guest filesystem will very likely
  remount read-only — which is harsh but honest, and preferable to the wedge
  it replaces. Restarting the helper in place was rejected: without
  `UBLK_F_USER_RECOVERY` a fresh device gets a fresh `/dev/ublkbN`, so
  anything holding the old path errors regardless.
  """

  use GenServer

  alias NeonFS.Block.{DeviceRegistry, Frontend, Ublk}
  alias NeonFS.Block.Ublk.Capability

  require Logger

  @default_queues 1
  @default_queue_depth 64
  @max_devices 16
  @ready_timeout_ms 30_000

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
             listeners: listeners,
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
    Logger.error("ublk helper exited; the device goes with it",
      export: state.export,
      reason: inspect(reason)
    )

    {:stop, {:helper_exited, reason}, state}
  end

  # A queue server that stops has lost its socket, and a device missing one
  # queue answers some IO and stalls the rest.
  def handle_info({:EXIT, pid, reason}, state) do
    if pid in state.servers do
      Logger.error("ublk queue server stopped; the device goes with it",
        export: state.export,
        reason: inspect(reason)
      )

      {:stop, {:queue_stopped, reason}, state}
    else
      {:noreply, state}
    end
  end

  def handle_info({port, {:exit_status, status}}, %{helper: port} = state) do
    {:stop, {:helper_exited, {:exit_status, status}}, state}
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
  defp listen(queues) do
    prefix = Path.join(System.tmp_dir!(), "neonfs-ublk-#{System.unique_integer([:positive])}")

    Enum.reduce_while(0..(queues - 1), {:ok, %{}}, fn queue, {:ok, acc} ->
      path = "#{prefix}.#{queue}"

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
  defp spawn_helper(info, listeners, device_id, opts) do
    %{prefix: prefix} = listeners |> Map.values() |> hd()

    Port.open({:spawn_executable, Capability.helper_path()}, [
      :binary,
      :exit_status,
      :stderr_to_stdout,
      env: [
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
