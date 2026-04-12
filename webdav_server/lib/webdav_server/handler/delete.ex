defmodule WebdavServer.Handler.Delete do
  @moduledoc false

  import Plug.Conn
  alias Plug.Conn.Status
  alias WebdavServer.Handler.Helpers
  alias WebdavServer.XML

  @doc false
  @spec handle(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def handle(conn, opts) do
    path = Helpers.resource_path(conn)

    with :ok <- Helpers.check_lock(conn, path, opts.lock_store),
         {:ok, resource} <- opts.backend.resolve(opts.auth, path) do
      case opts.backend.delete(opts.auth, resource) do
        :ok ->
          send_resp(conn, 204, "")

        {:partial, failures} ->
          send_multistatus_failures(conn, failures)

        {:error, error} ->
          Helpers.send_error(conn, error)
      end
    else
      {:error, :locked} ->
        send_resp(conn, 423, "Locked")

      {:error, error} ->
        Helpers.send_error(conn, error)
    end
  end

  defp send_multistatus_failures(conn, failures) do
    response_elements =
      Enum.map(failures, fn {path, error} ->
        status = WebdavServer.Error.status_code(error.code)
        href = Helpers.href(%{conn | path_info: path}, path)

        Saxy.XML.element("D:response", [], [
          Saxy.XML.element("D:href", [], [href]),
          Saxy.XML.element("D:status", [], [
            "HTTP/1.1 #{status} #{Status.reason_phrase(status)}"
          ])
        ])
      end)

    body = XML.multistatus(response_elements)

    conn
    |> put_resp_content_type("application/xml", nil)
    |> send_resp(207, body)
  end
end
