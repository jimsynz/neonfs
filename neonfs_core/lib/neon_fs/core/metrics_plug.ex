defmodule NeonFS.Core.MetricsPlug do
  @moduledoc """
  HTTP endpoints for Prometheus metrics and health checks.
  """
  @behaviour Plug

  import Plug.Conn

  alias NeonFS.Client.HealthCheck
  alias NeonFS.Cluster.InviteRedemption

  require Logger

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

      {"POST", "/api/cluster/redeem-invite"} ->
        handle_redeem_invite(conn)

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

  defp handle_redeem_invite(conn) do
    with {:ok, body, conn} <- read_body(conn),
         {:ok, params} <- decode_json(body),
         {:ok, encrypted_response} <- InviteRedemption.redeem(params) do
      conn
      |> put_resp_header("content-type", "application/octet-stream")
      |> send_resp(200, encrypted_response)
    else
      {:error, :invalid_json} ->
        send_json_error(conn, 400, "invalid_json", "Request body must be valid JSON")

      {:error, :invalid_params} ->
        send_json_error(conn, 400, "invalid_params", "Missing required fields")

      {:error, :expired} ->
        send_json_error(conn, 401, "expired", "Invite token has expired")

      {:error, :invalid_proof} ->
        send_json_error(conn, 401, "invalid_proof", "Invalid token proof")

      {:error, :invalid_csr} ->
        send_json_error(conn, 400, "invalid_csr", "Invalid certificate signing request")

      {:error, :cluster_not_initialized} ->
        send_json_error(conn, 503, "cluster_not_initialized", "Cluster not initialised")

      {:error, reason} ->
        Logger.warning("Invite redemption failed", reason: inspect(reason))
        send_json_error(conn, 500, "internal_error", "Invite redemption failed")
    end
  end

  defp decode_json(body) do
    {:ok, :json.decode(body)}
  rescue
    _ -> {:error, :invalid_json}
  end

  defp send_json_error(conn, status, error, message) do
    body =
      %{"error" => error, "message" => message}
      |> :json.encode()
      |> IO.iodata_to_binary()

    conn
    |> put_resp_header("content-type", "application/json")
    |> send_resp(status, body)
  end

  defp health_http_status(%{status: :healthy}), do: 200
  defp health_http_status(%{status: :degraded}), do: 200
  defp health_http_status(%{status: :unhealthy}), do: 503
  defp health_http_status(_), do: 503

  defp json_body(value) do
    value
    |> HealthCheck.normalise_for_json()
    |> :json.encode()
    |> IO.iodata_to_binary()
  end
end
