defmodule NeonFS.Client.Registrar do
  @moduledoc """
  Periodically registers a non-core service with the cluster.
  """

  use GenServer
  require Logger

  alias NeonFS.Client
  alias NeonFS.Client.ServiceType

  @default_interval_ms 5_000

  @type state :: %{
          interval_ms: pos_integer(),
          metadata: map(),
          registered?: boolean(),
          type: ServiceType.t()
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    type = Keyword.fetch!(opts, :type)
    metadata = Keyword.get(opts, :metadata, %{})

    interval_ms =
      Keyword.get_lazy(opts, :interval_ms, fn ->
        Application.get_env(:neonfs_client, :registration_interval, @default_interval_ms)
      end)

    state = %{interval_ms: interval_ms, metadata: metadata, registered?: false, type: type}

    {:ok, state, {:continue, :register}}
  end

  @impl true
  def terminate(_reason, state) do
    if state.registered? do
      case Client.deregister(state.type) do
        :ok -> :ok
        {:error, reason} -> Logger.debug("Failed to deregister service", reason: inspect(reason))
      end
    end

    :ok
  end

  @impl true
  def handle_continue(:register, state) do
    state = register_service(state)
    schedule_registration(state.interval_ms)
    {:noreply, state}
  end

  @impl true
  def handle_info(:register, state) do
    state = register_service(state)
    schedule_registration(state.interval_ms)
    {:noreply, state}
  end

  defp register_service(state) do
    case Client.register(state.type, state.metadata) do
      :ok ->
        log_registration_success(state)
        %{state | registered?: true}

      {:error, reason} ->
        log_registration_failure(state, reason)
        %{state | registered?: false}
    end
  end

  defp log_registration_success(%{registered?: true}), do: :ok

  defp log_registration_success(state) do
    Logger.info("Registered service with cluster", service_type: state.type)
  end

  defp log_registration_failure(%{registered?: false}, _reason), do: :ok

  defp log_registration_failure(state, reason) do
    Logger.warning("Service registration lost", service_type: state.type, reason: inspect(reason))
  end

  defp schedule_registration(interval_ms) do
    Process.send_after(self(), :register, interval_ms)
  end
end
