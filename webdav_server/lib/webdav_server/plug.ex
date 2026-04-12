defmodule WebdavServer.Plug do
  @moduledoc """
  Plug implementation for a WebDAV server.

  Mount this plug in your router and provide a backend module implementing
  `WebdavServer.Backend`:

      forward "/dav", WebdavServer.Plug, backend: MyApp.DavBackend

  ## Options

  - `:backend` (required) — module implementing `WebdavServer.Backend`
  - `:lock_store` — module implementing `WebdavServer.LockStore`
    (default: `WebdavServer.LockStore.ETS`)
  - `:lock_timeout` — default lock timeout in seconds (default: 1800)
  - `:allow_infinity_depth` — allow `Depth: infinity` on PROPFIND (default: false)
  """

  @behaviour Plug

  alias WebdavServer.Handler

  @impl true
  def init(opts) do
    %{
      backend: Keyword.fetch!(opts, :backend),
      lock_store: Keyword.get(opts, :lock_store, WebdavServer.LockStore.ETS),
      lock_timeout: Keyword.get(opts, :lock_timeout, 1800),
      allow_infinity_depth: Keyword.get(opts, :allow_infinity_depth, false)
    }
  end

  @impl true
  def call(conn, opts) do
    conn = Plug.Conn.fetch_query_params(conn)

    case opts.backend.authenticate(conn) do
      {:ok, auth} ->
        dispatch(conn, Map.put(opts, :auth, auth))

      {:error, :unauthorized} ->
        conn
        |> Plug.Conn.put_resp_header("www-authenticate", "Basic realm=\"WebDAV\"")
        |> Plug.Conn.send_resp(401, "Unauthorized")
    end
  end

  defp dispatch(%Plug.Conn{method: "OPTIONS"} = conn, opts),
    do: Handler.Options.handle(conn, opts)

  defp dispatch(%Plug.Conn{method: "GET"} = conn, opts), do: Handler.Get.handle(conn, opts)
  defp dispatch(%Plug.Conn{method: "HEAD"} = conn, opts), do: Handler.Get.handle(conn, opts)
  defp dispatch(%Plug.Conn{method: "PUT"} = conn, opts), do: Handler.Put.handle(conn, opts)
  defp dispatch(%Plug.Conn{method: "DELETE"} = conn, opts), do: Handler.Delete.handle(conn, opts)
  defp dispatch(%Plug.Conn{method: "MKCOL"} = conn, opts), do: Handler.Mkcol.handle(conn, opts)
  defp dispatch(%Plug.Conn{method: "COPY"} = conn, opts), do: Handler.CopyMove.handle(conn, opts)
  defp dispatch(%Plug.Conn{method: "MOVE"} = conn, opts), do: Handler.CopyMove.handle(conn, opts)

  defp dispatch(%Plug.Conn{method: "PROPFIND"} = conn, opts),
    do: Handler.Propfind.handle(conn, opts)

  defp dispatch(%Plug.Conn{method: "PROPPATCH"} = conn, opts),
    do: Handler.Proppatch.handle(conn, opts)

  defp dispatch(%Plug.Conn{method: "LOCK"} = conn, opts), do: Handler.Lock.handle(conn, opts)
  defp dispatch(%Plug.Conn{method: "UNLOCK"} = conn, opts), do: Handler.Lock.handle(conn, opts)

  defp dispatch(conn, _opts) do
    Plug.Conn.send_resp(conn, 405, "Method Not Allowed")
  end
end
