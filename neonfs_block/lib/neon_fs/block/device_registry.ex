defmodule NeonFS.Block.DeviceRegistry do
  @moduledoc """
  Node-wide record of which devices are attached, and by whom.

  A device is attached by *connections*, not by one connection: a client
  opening several sockets to the same export is how blk-mq gets its
  parallelism, and each of those has to see the same device. Resolving the
  export once and sharing the handle also keeps an attach from costing a
  metadata round trip per socket.

  Each attaching connection is monitored, so a client that dies without
  detaching still releases its hold. The device is released when its last
  connection goes — not before, or a surviving connection would be left
  holding a handle to a detached device.

  ## Telemetry

    * `[:neonfs, :block, :attached]` — Measurements: `holders`. Metadata:
      `export`.
    * `[:neonfs, :block, :detached]` — Measurements: `holders`, which is `0`
      when the device was released. Metadata: `export`.
  """

  use GenServer

  alias NeonFS.Block.Device

  @type export :: String.t()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  Attaches `holder` to `export`, resolving the backing file on first use.

  Returns the shared device handle. A second attach of an already-attached
  export does not re-resolve it.
  """
  @spec attach(export(), pid(), GenServer.server()) :: {:ok, Device.t()} | {:error, term()}
  def attach(export, holder, server \\ __MODULE__) do
    GenServer.call(server, {:attach, export, holder}, 30_000)
  end

  @doc """
  Releases `holder`'s hold on `export`.

  Idempotent: detaching something this holder does not hold is `:ok`, so a
  connection closing after an error it already handled is not an error.
  """
  @spec detach(export(), pid(), GenServer.server()) :: :ok
  def detach(export, holder, server \\ __MODULE__) do
    GenServer.call(server, {:detach, export, holder}, 30_000)
  end

  @doc """
  The exports currently attached, and how many connections hold each.
  """
  @spec attached(GenServer.server()) :: %{export() => pos_integer()}
  def attached(server \\ __MODULE__) do
    GenServer.call(server, :attached)
  end

  @impl GenServer
  def init(_opts) do
    {:ok, %{devices: %{}, holders: %{}}}
  end

  @impl GenServer
  def handle_call({:attach, export, holder}, _from, state) do
    case Map.fetch(state.devices, export) do
      {:ok, %{device: device}} ->
        {:reply, {:ok, device}, note_attach(state, export, holder)}

      :error ->
        case Device.open(export) do
          {:ok, device} ->
            state = put_in(state.devices[export], %{device: device, holders: MapSet.new()})
            {:reply, {:ok, device}, note_attach(state, export, holder)}

          {:error, _reason} = error ->
            {:reply, error, state}
        end
    end
  end

  def handle_call({:detach, export, holder}, _from, state) do
    {:reply, :ok, note_detach(state, export, holder)}
  end

  def handle_call(:attached, _from, state) do
    counts =
      Map.new(state.devices, fn {export, %{holders: holders}} ->
        {export, MapSet.size(holders)}
      end)

    {:reply, counts, state}
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    exports = Map.get(state.holders, pid, MapSet.new())
    state = %{state | holders: Map.delete(state.holders, pid)}

    {:noreply, Enum.reduce(exports, state, &note_detach(&2, &1, pid, monitored: false))}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp note_attach(state, export, holder) do
    state = add_holder(state, export, holder)
    emit(:attached, export, holder_count(state, export))
    state
  end

  defp note_detach(state, export, holder, opts \\ []) do
    state = drop_holder(state, export, holder, opts)
    emit(:detached, export, holder_count(state, export))
    state
  end

  defp holder_count(state, export) do
    case Map.fetch(state.devices, export) do
      {:ok, %{holders: holders}} -> MapSet.size(holders)
      :error -> 0
    end
  end

  defp emit(event, export, holders) do
    :telemetry.execute([:neonfs, :block, event], %{holders: holders}, %{export: export})
  end

  defp add_holder(state, export, holder) do
    unless Map.has_key?(state.holders, holder), do: Process.monitor(holder)

    state
    |> update_in([:devices, export, :holders], &MapSet.put(&1, holder))
    |> update_in([:holders], fn holders ->
      Map.update(holders, holder, MapSet.new([export]), &MapSet.put(&1, export))
    end)
  end

  defp drop_holder(state, export, holder, opts) do
    case Map.fetch(state.devices, export) do
      {:ok, %{holders: holders}} ->
        remaining = MapSet.delete(holders, holder)

        state
        |> release_if_last(export, remaining)
        |> forget_holder(holder, export, Keyword.get(opts, :monitored, true))

      :error ->
        state
    end
  end

  defp release_if_last(state, export, remaining) do
    if MapSet.size(remaining) == 0 do
      %{state | devices: Map.delete(state.devices, export)}
    else
      put_in(state.devices[export].holders, remaining)
    end
  end

  defp forget_holder(state, _holder, _export, false), do: state

  defp forget_holder(state, holder, export, true) do
    update_in(state.holders, &drop_export(&1, holder, export))
  end

  defp drop_export(holders, holder, export) do
    holders
    |> Map.get(holder, MapSet.new())
    |> MapSet.delete(export)
    |> case do
      remaining -> retain_holder(holders, holder, remaining)
    end
  end

  # A holder with no exports left is forgotten rather than kept as an empty
  # set, so the monitor bookkeeping does not grow with every closed connection.
  defp retain_holder(holders, holder, remaining) do
    if MapSet.size(remaining) == 0 do
      Map.delete(holders, holder)
    else
      Map.put(holders, holder, remaining)
    end
  end
end
