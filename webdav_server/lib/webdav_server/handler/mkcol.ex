defmodule WebdavServer.Handler.Mkcol do
  @moduledoc false

  import Plug.Conn
  alias WebdavServer.Handler.Helpers

  @doc false
  @spec handle(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def handle(conn, opts) do
    path = Helpers.resource_path(conn)

    with :ok <- check_no_body(conn),
         :ok <- Helpers.check_lock(conn, path, opts.lock_store) do
      case opts.backend.create_collection(opts.auth, path) do
        :ok -> send_resp(conn, 201, "")
        {:error, error} -> Helpers.send_error(conn, error)
      end
    else
      {:error, :unsupported_media_type} ->
        send_resp(conn, 415, "Unsupported Media Type")

      {:error, :locked} ->
        send_resp(conn, 423, "Locked")
    end
  end

  defp check_no_body(conn) do
    case get_req_header(conn, "content-length") do
      ["0"] -> :ok
      [] -> :ok
      _ -> {:error, :unsupported_media_type}
    end
  end
end
