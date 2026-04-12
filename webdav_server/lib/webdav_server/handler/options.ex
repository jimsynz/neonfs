defmodule WebdavServer.Handler.Options do
  @moduledoc false

  import Plug.Conn

  @allowed_methods "OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, COPY, MOVE, PROPFIND, PROPPATCH, LOCK, UNLOCK"

  @doc false
  @spec handle(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def handle(conn, _opts) do
    conn
    |> put_resp_header("allow", @allowed_methods)
    |> put_resp_header("dav", "1, 2")
    |> put_resp_header("ms-author-via", "DAV")
    |> send_resp(200, "")
  end
end
