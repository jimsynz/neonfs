defmodule NeonFS.Core.MetricsPlug do
  @moduledoc """
  HTTP endpoints for Prometheus metrics and health checks.
  """
  @behaviour Plug

  import Plug.Conn

  alias NeonFS.Core.HealthCheck

  @impl Plug
  def init(opts) do
    [
      health_check_fun: Keyword.get(opts, :health_check_fun, &HealthCheck.check/0),
      scrape_fun: Keyword.get(opts, :scrape_fun, &TelemetryMetricsPrometheus.Core.scrape/0)
    ]
  end

  @impl Plug
  def call(conn, opts) do
    case {conn.method, conn.request_path} do
      {"GET", "/health"} ->
        send_health(conn, opts)

      {"GET", "/metrics"} ->
        send_metrics(conn, opts)

      {_method, path} when path in ["/health", "/metrics"] ->
        send_resp(conn, 405, "Method Not Allowed")

      {_method, _path} ->
        send_resp(conn, 404, "Not Found")
    end
  end

  defp send_health(conn, opts) do
    health_report = opts[:health_check_fun].()
    status = health_http_status(health_report)

    conn
    |> put_resp_header("content-type", "application/json")
    |> send_resp(status, json_body(health_report))
  end

  defp send_metrics(conn, opts) do
    body = opts[:scrape_fun].()

    conn
    |> put_resp_header("content-type", "text/plain; version=0.0.4; charset=utf-8")
    |> send_resp(200, body)
  end

  defp health_http_status(%{status: :healthy}), do: 200
  defp health_http_status(%{status: :degraded}), do: 200
  defp health_http_status(%{status: :unhealthy}), do: 503
  defp health_http_status(_), do: 503

  defp json_body(value) do
    value
    |> normalise_for_json()
    |> :json.encode()
    |> IO.iodata_to_binary()
  end

  defp normalise_for_json(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp normalise_for_json(%_{} = struct) do
    struct
    |> Map.from_struct()
    |> normalise_for_json()
  end

  defp normalise_for_json(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {normalise_for_json(key), normalise_for_json(value)} end)
  end

  defp normalise_for_json(list) when is_list(list), do: Enum.map(list, &normalise_for_json/1)
  defp normalise_for_json(tuple) when is_tuple(tuple), do: inspect(tuple)
  defp normalise_for_json(value), do: value
end
