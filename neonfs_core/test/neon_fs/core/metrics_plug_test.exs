defmodule NeonFS.Core.MetricsPlugTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias NeonFS.Core.MetricsPlug

  test "GET /metrics returns prometheus response" do
    opts =
      MetricsPlug.init(
        health_check_fun: fn -> %{status: :healthy} end,
        scrape_fun: fn -> "# HELP neonfs_metric test metric\nneonfs_metric 1\n" end
      )

    conn =
      :get
      |> conn("/metrics")
      |> MetricsPlug.call(opts)

    assert conn.status == 200

    assert Plug.Conn.get_resp_header(conn, "content-type") == [
             "text/plain; version=0.0.4; charset=utf-8"
           ]

    assert conn.resp_body =~ "# HELP neonfs_metric"
  end

  test "GET /health returns 200 with json body when healthy" do
    opts =
      MetricsPlug.init(
        health_check_fun: fn -> %{checked_at: DateTime.utc_now(), status: :healthy} end,
        scrape_fun: fn -> "" end
      )

    conn =
      :get
      |> conn("/health")
      |> MetricsPlug.call(opts)

    assert conn.status == 200
    assert Plug.Conn.get_resp_header(conn, "content-type") == ["application/json"]

    decoded = :json.decode(conn.resp_body)
    assert decoded["status"] == "healthy"
    assert is_binary(decoded["checked_at"])
  end

  test "GET /health returns 503 when unhealthy" do
    opts =
      MetricsPlug.init(
        health_check_fun: fn -> %{status: :unhealthy} end,
        scrape_fun: fn -> "" end
      )

    conn =
      :get
      |> conn("/health")
      |> MetricsPlug.call(opts)

    assert conn.status == 503
  end

  test "GET unknown path returns 404" do
    opts =
      MetricsPlug.init(
        health_check_fun: fn -> %{status: :healthy} end,
        scrape_fun: fn -> "" end
      )

    conn =
      :get
      |> conn("/unknown")
      |> MetricsPlug.call(opts)

    assert conn.status == 404
  end

  test "POST /metrics returns 405" do
    opts =
      MetricsPlug.init(
        health_check_fun: fn -> %{status: :healthy} end,
        scrape_fun: fn -> "" end
      )

    conn =
      :post
      |> conn("/metrics")
      |> MetricsPlug.call(opts)

    assert conn.status == 405
  end
end
