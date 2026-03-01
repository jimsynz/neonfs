defmodule NeonFS.FUSE.MetricsPlugTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias NeonFS.FUSE.MetricsPlug

  test "GET /metrics returns prometheus response" do
    opts =
      MetricsPlug.init(
        scrape_fun: fn -> "# HELP neonfs_fuse_metric test\nneonfs_fuse_metric 1\n" end
      )

    conn =
      :get
      |> conn("/metrics")
      |> MetricsPlug.call(opts)

    assert conn.status == 200

    assert Plug.Conn.get_resp_header(conn, "content-type") == [
             "text/plain; version=0.0.4; charset=utf-8"
           ]

    assert conn.resp_body =~ "# HELP neonfs_fuse_metric"
  end

  test "POST /metrics returns 405" do
    opts = MetricsPlug.init(scrape_fun: fn -> "" end)

    conn =
      :post
      |> conn("/metrics")
      |> MetricsPlug.call(opts)

    assert conn.status == 405
  end

  test "GET /unknown returns 404" do
    opts = MetricsPlug.init(scrape_fun: fn -> "" end)

    conn =
      :get
      |> conn("/unknown")
      |> MetricsPlug.call(opts)

    assert conn.status == 404
  end
end
