defmodule NeonFS.Core.MetricsSupervisor do
  @moduledoc """
  Supervises metrics and health observability components.
  """

  use Supervisor

  alias NeonFS.Core.Telemetry

  @doc "Starts the metrics supervision tree."
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Returns child specs for metrics components."
  @spec child_specs(keyword()) :: [Supervisor.child_spec()]
  def child_specs(opts \\ []) do
    [
      %{
        id: TelemetryMetricsPrometheus.Core,
        start: {TelemetryMetricsPrometheus.Core, :start_link, [prometheus_opts(opts)]}
      },
      %{
        id: :telemetry_poller,
        start: {NeonFS.Core.TelemetryPoller, :start_link, [telemetry_poller_opts(opts)]}
      },
      %{
        id: Bandit,
        start: {NeonFS.Core.BanditLauncher, :start_link, [bandit_opts(opts)]},
        shutdown: 15_000
      }
    ]
  end

  @impl true
  def init(opts) do
    Supervisor.init(child_specs(opts), strategy: :one_for_one)
  end

  @doc "Returns whether metrics stack is enabled."
  @spec enabled?() :: boolean()
  def enabled? do
    Application.get_env(:neonfs_core, :metrics_enabled, true)
  end

  defp bandit_opts(opts) do
    bind = Keyword.get(opts, :bind, Application.get_env(:neonfs_core, :metrics_bind, "0.0.0.0"))
    port = Keyword.get(opts, :port, Application.get_env(:neonfs_core, :metrics_port, 9568))

    [
      plug: NeonFS.Core.MetricsPlug,
      scheme: :http,
      ip: parse_ip(bind),
      port: port
    ]
  end

  defp parse_ip(bind) when is_binary(bind) do
    case :inet.parse_strict_address(String.to_charlist(bind)) do
      {:ok, ip} -> ip
      {:error, _reason} -> {0, 0, 0, 0}
    end
  end

  defp parse_ip(_), do: {0, 0, 0, 0}

  defp prometheus_opts(opts) do
    Keyword.get(opts, :prometheus_opts, metrics: Telemetry.metrics())
  end

  defp telemetry_poller_opts(opts) do
    Keyword.get(opts, :telemetry_poller_opts, [])
  end
end
