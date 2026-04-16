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
         :ok <- check_descendant_locks(conn, path, opts.lock_store),
         {:ok, resource} <- opts.backend.resolve(opts.auth, path) do
      case opts.backend.delete(opts.auth, resource) do
        :ok ->
          send_resp(conn, 204, "")

        {:partial, failures} ->
          send_multistatus_failures(conn, failures, :error)

        {:error, error} ->
          Helpers.send_error(conn, error)
      end
    else
      {:error, :locked} ->
        send_resp(conn, 423, "Locked")

      {:locked_descendants, locked_paths} ->
        send_multistatus_locked_descendants(conn, locked_paths)

      {:error, error} ->
        Helpers.send_error(conn, error)
    end
  end

  defp check_descendant_locks(conn, path, lock_store) do
    case lock_store.get_descendant_locks(path) do
      [] ->
        :ok

      descendant_locks ->
        tokens = Helpers.extract_lock_tokens(conn)

        locked =
          descendant_locks
          |> Enum.filter(fn info -> not Enum.member?(tokens, info.token) end)
          |> Enum.map(& &1.path)
          |> Enum.uniq()

        case locked do
          [] -> :ok
          paths -> {:locked_descendants, paths}
        end
    end
  end

  defp send_multistatus_failures(conn, failures, :error) do
    response_elements =
      Enum.map(failures, fn {path, error} ->
        status = WebdavServer.Error.status_code(error.code)
        response_element_for(conn, path, status)
      end)

    send_multistatus(conn, response_elements)
  end

  defp send_multistatus_locked_descendants(conn, locked_paths) do
    response_elements =
      Enum.map(locked_paths, fn path -> response_element_for(conn, path, 423) end)

    send_multistatus(conn, response_elements)
  end

  defp response_element_for(conn, path, status) do
    href = Helpers.href(%{conn | path_info: path}, path)

    Saxy.XML.element("D:response", [], [
      Saxy.XML.element("D:href", [], [href]),
      Saxy.XML.element("D:status", [], [
        "HTTP/1.1 #{status} #{Status.reason_phrase(status)}"
      ])
    ])
  end

  defp send_multistatus(conn, response_elements) do
    body = XML.multistatus(response_elements)

    conn
    |> put_resp_content_type("application/xml", nil)
    |> send_resp(207, body)
  end
end
