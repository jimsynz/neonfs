defmodule NeonFS.FUSE.MetricsPlug do
  @moduledoc """
  HTTP endpoint for FUSE node Prometheus metrics.

  Serves `GET /metrics` in Prometheus text exposition format on a
  configurable port (default 9569, one above the core metrics port).
  """
  @behaviour Plug

  import Plug.Conn

  @impl Plug
  def init(opts) do
    [
      scrape_fun: Keyword.get(opts, :scrape_fun, &TelemetryMetricsPrometheus.Core.scrape/0)
    ]
  end

  @impl Plug
  def call(conn, opts) do
    case {conn.method, conn.request_path} do
      {"GET", "/metrics"} ->
        send_metrics(conn, opts)

      {_method, "/metrics"} ->
        send_resp(conn, 405, "Method Not Allowed")

      {_method, _path} ->
        send_resp(conn, 404, "Not Found")
    end
  end

  defp send_metrics(conn, opts) do
    body = opts[:scrape_fun].()

    conn
    |> put_resp_header("content-type", "text/plain; version=0.0.4; charset=utf-8")
    |> send_resp(200, body)
  end
end
