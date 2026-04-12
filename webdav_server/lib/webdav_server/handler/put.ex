defmodule WebdavServer.Handler.Put do
  @moduledoc false

  import Plug.Conn
  alias WebdavServer.Handler.Helpers

  @doc false
  @spec handle(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def handle(conn, opts) do
    path = Helpers.resource_path(conn)

    case Helpers.check_lock(conn, path, opts.lock_store) do
      :ok -> do_put(conn, opts, path)
      {:error, :locked} -> send_resp(conn, 423, "Locked")
    end
  end

  defp do_put(conn, opts, path) do
    {:ok, body, conn} = read_body(conn)

    content_type =
      case get_req_header(conn, "content-type") do
        [ct] -> ct
        _ -> "application/octet-stream"
      end

    existing? = match?({:ok, _}, opts.backend.resolve(opts.auth, path))

    case opts.backend.put_content(opts.auth, path, body, %{content_type: content_type}) do
      {:ok, resource} ->
        conn = maybe_put_etag(conn, resource)
        send_resp(conn, if(existing?, do: 204, else: 201), "")

      {:error, error} ->
        Helpers.send_error(conn, error)
    end
  end

  defp maybe_put_etag(conn, %{etag: etag}) when is_binary(etag),
    do: put_resp_header(conn, "etag", etag)

  defp maybe_put_etag(conn, _), do: conn
end
