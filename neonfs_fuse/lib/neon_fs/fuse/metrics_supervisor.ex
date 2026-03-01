defmodule NeonFS.FUSE.MetricsSupervisor do
  @moduledoc """
  Supervises FUSE metrics components: Prometheus aggregation and the
  HTTP endpoint serving `/metrics` on port 9569.

  Disabled by default. Enable via application config:

      config :neonfs_fuse, :metrics_enabled, true
  """

  use Supervisor

  alias NeonFS.FUSE.Telemetry

  @default_port 9569
  @default_bind "0.0.0.0"

  @doc "Starts the metrics supervision tree."
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      %{
        id: TelemetryMetricsPrometheus.Core,
        start:
          {TelemetryMetricsPrometheus.Core, :start_link,
           [Keyword.get(opts, :prometheus_opts, metrics: Telemetry.metrics())]}
      },
      %{
        id: Bandit,
        start: {Bandit, :start_link, [bandit_opts(opts)]},
        shutdown: 15_000
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc "Returns whether the FUSE metrics endpoint is enabled."
  @spec enabled?() :: boolean()
  def enabled? do
    Application.get_env(:neonfs_fuse, :metrics_enabled, false)
  end

  defp bandit_opts(opts) do
    bind =
      Keyword.get(opts, :bind, Application.get_env(:neonfs_fuse, :metrics_bind, @default_bind))

    port =
      Keyword.get(opts, :port, Application.get_env(:neonfs_fuse, :metrics_port, @default_port))

    [
      plug: NeonFS.FUSE.MetricsPlug,
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
end
