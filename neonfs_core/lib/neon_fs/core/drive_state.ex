defmodule NeonFS.Core.DriveState do
  @moduledoc """
  Per-drive GenServer managing power state transitions.

  Implements a state machine with states:

    * `:active` — drive is spinning and ready for I/O
    * `:idle` — no recent I/O, transitioning to spin-down
    * `:spinning_down` — spin-down command in progress
    * `:standby` — drive is spun down, needs spin-up for access
    * `:spinning_up` — spin-up command in progress

  Concurrent readers waiting for a spin-up share the same spin-up operation
  rather than issuing duplicate commands.

  Drives with `power_management: false` remain permanently `:active`.

  ## Telemetry Events

    * `[:neonfs, :drive_state, :transition]` — emitted on every state change
      with metadata `%{drive_id: id, from: old_state, to: new_state}`
  """

  use GenServer
  require Logger

  alias NeonFS.Core.DriveRegistry

  @type power_state :: :active | :idle | :spinning_down | :standby | :spinning_up

  @registry NeonFS.Core.DriveStateRegistry

  ## Client API

  @doc """
  Starts a DriveState GenServer for a specific drive.

  ## Options

    * `:drive_id` (required) — unique drive identifier
    * `:drive_path` (required) — filesystem path to the drive
    * `:power_management` — enable power management (default: `false`)
    * `:idle_timeout` — seconds of inactivity before spin-down (default: 1800)
    * `:command_module` — module implementing `DriveCommand` (default: `DriveCommand.Default`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    drive_id = Keyword.fetch!(opts, :drive_id)
    GenServer.start_link(__MODULE__, opts, name: via_tuple(drive_id))
  end

  @doc """
  Ensures the drive is active and ready for I/O.

  Returns `:ok` immediately if the drive is active or idle.
  Blocks during spin-up if the drive is in standby.
  Multiple callers waiting for spin-up share the same operation.
  """
  @spec ensure_active(String.t()) :: :ok | {:error, :spin_up_failed}
  def ensure_active(drive_id) do
    GenServer.call(via_tuple(drive_id), :ensure_active, 60_000)
  rescue
    ArgumentError -> :ok
  catch
    :exit, {:noproc, _} -> :ok
  end

  @doc """
  Records an I/O operation, resetting the idle timer.

  Called by BlobStore on every read/write. This is a cast (fire-and-forget)
  to avoid adding latency to the I/O path.
  """
  @spec record_io(String.t()) :: :ok
  def record_io(drive_id) do
    GenServer.cast(via_tuple(drive_id), :record_io)
  rescue
    ArgumentError -> :ok
  catch
    :exit, {:noproc, _} -> :ok
  end

  @doc """
  Returns the current power state of the drive.
  """
  @spec get_state(String.t()) :: power_state()
  def get_state(drive_id) do
    GenServer.call(via_tuple(drive_id), :get_state)
  rescue
    ArgumentError -> :active
  catch
    :exit, {:noproc, _} -> :active
  end

  @doc false
  @spec via_tuple(String.t()) :: {:via, module(), {module(), String.t()}}
  def via_tuple(drive_id) do
    {:via, Registry, {@registry, drive_id}}
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    drive_id = Keyword.fetch!(opts, :drive_id)
    drive_path = Keyword.fetch!(opts, :drive_path)
    power_management = Keyword.get(opts, :power_management, false)
    idle_timeout_sec = Keyword.get(opts, :idle_timeout, 1800)
    command_module = Keyword.get(opts, :command_module, NeonFS.Core.DriveCommand.Default)

    state = %{
      drive_id: drive_id,
      drive_path: drive_path,
      power_state: :active,
      command_module: command_module,
      idle_timeout_ms: idle_timeout_sec * 1000,
      idle_timer: nil,
      spin_up_waiters: [],
      power_management: power_management
    }

    state =
      if power_management do
        schedule_idle_timeout(state)
      else
        state
      end

    {:ok, state}
  end

  ## handle_call — ensure_active

  @impl true
  def handle_call(:ensure_active, _from, %{power_management: false} = state) do
    {:reply, :ok, state}
  end

  def handle_call(:ensure_active, _from, %{power_state: :active} = state) do
    {:reply, :ok, state}
  end

  def handle_call(:ensure_active, _from, %{power_state: :idle} = state) do
    state = cancel_idle_timer(state)
    state = transition_to(:active, state)
    state = schedule_idle_timeout(state)
    {:reply, :ok, state}
  end

  def handle_call(:ensure_active, from, %{power_state: :standby} = state) do
    state = %{state | spin_up_waiters: [from]}
    start_spin_up(state)
    state = transition_to(:spinning_up, state)
    {:noreply, state}
  end

  def handle_call(:ensure_active, from, %{power_state: :spinning_up} = state) do
    state = %{state | spin_up_waiters: [from | state.spin_up_waiters]}
    {:noreply, state}
  end

  def handle_call(:ensure_active, from, %{power_state: :spinning_down} = state) do
    state = %{state | spin_up_waiters: [from | state.spin_up_waiters]}
    {:noreply, state}
  end

  ## handle_call — get_state

  def handle_call(:get_state, _from, state) do
    {:reply, state.power_state, state}
  end

  ## handle_cast — record_io

  @impl true
  def handle_cast(:record_io, %{power_management: false} = state) do
    {:noreply, state}
  end

  def handle_cast(:record_io, %{power_state: ps} = state) when ps in [:active, :idle] do
    state = cancel_idle_timer(state)

    state =
      if state.power_state == :idle do
        transition_to(:active, state)
      else
        state
      end

    state = schedule_idle_timeout(state)
    {:noreply, state}
  end

  def handle_cast(:record_io, state) do
    {:noreply, state}
  end

  ## handle_info — idle_timeout

  @impl true
  def handle_info(:idle_timeout, %{power_state: :active} = state) do
    state = transition_to(:idle, state)
    start_spin_down(state)
    state = transition_to(:spinning_down, state)
    {:noreply, state}
  end

  def handle_info(:idle_timeout, state) do
    {:noreply, state}
  end

  ## handle_info — spin_down_complete

  def handle_info({:spin_down_complete, :ok}, %{power_state: :spinning_down} = state) do
    state = transition_to(:standby, state)
    handle_post_spindown(state)
  end

  def handle_info({:spin_down_complete, {:error, reason}}, %{power_state: :spinning_down} = state) do
    Logger.warning("Drive #{state.drive_id} spin-down failed: #{inspect(reason)}")
    state = transition_to(:active, state)
    state = schedule_idle_timeout(state)
    notify_waiters(state, :ok)
    {:noreply, %{state | spin_up_waiters: []}}
  end

  ## handle_info — spin_up_complete

  def handle_info({:spin_up_complete, :ok}, %{power_state: :spinning_up} = state) do
    state = transition_to(:active, state)
    state = schedule_idle_timeout(state)
    notify_waiters(state, :ok)
    {:noreply, %{state | spin_up_waiters: []}}
  end

  def handle_info({:spin_up_complete, {:error, reason}}, %{power_state: :spinning_up} = state) do
    Logger.warning("Drive #{state.drive_id} spin-up failed: #{inspect(reason)}")
    state = transition_to(:standby, state)
    notify_waiters(state, {:error, :spin_up_failed})
    {:noreply, %{state | spin_up_waiters: []}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  ## Private — State transitions

  defp transition_to(new_state, state) do
    old_state = state.power_state

    if old_state != new_state do
      :telemetry.execute(
        [:neonfs, :drive_state, :transition],
        %{},
        %{drive_id: state.drive_id, from: old_state, to: new_state}
      )

      update_registry_state(state.drive_id, new_state)
    end

    %{state | power_state: new_state}
  end

  defp update_registry_state(drive_id, power_state) do
    registry_state = if power_state in [:active, :idle], do: :active, else: :standby
    DriveRegistry.update_state(drive_id, registry_state)
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  ## Private — Timers

  defp schedule_idle_timeout(state) do
    state = cancel_idle_timer(state)
    ref = Process.send_after(self(), :idle_timeout, state.idle_timeout_ms)
    %{state | idle_timer: ref}
  end

  defp cancel_idle_timer(%{idle_timer: nil} = state), do: state

  defp cancel_idle_timer(%{idle_timer: ref} = state) do
    Process.cancel_timer(ref)
    %{state | idle_timer: nil}
  end

  ## Private — Spin operations

  defp start_spin_down(state) do
    parent = self()
    command_module = state.command_module
    path = state.drive_path

    Task.start(fn ->
      result = command_module.spin_down(path)
      send(parent, {:spin_down_complete, result})
    end)
  end

  defp start_spin_up(state) do
    parent = self()
    command_module = state.command_module
    path = state.drive_path

    Task.start(fn ->
      result = command_module.spin_up(path)
      send(parent, {:spin_up_complete, result})
    end)
  end

  ## Private — Waiters

  defp notify_waiters(state, reply) do
    Enum.each(state.spin_up_waiters, fn from ->
      GenServer.reply(from, reply)
    end)
  end

  defp handle_post_spindown(state) do
    if state.spin_up_waiters != [] do
      start_spin_up(state)
      state = transition_to(:spinning_up, state)
      {:noreply, state}
    else
      {:noreply, state}
    end
  end
end
