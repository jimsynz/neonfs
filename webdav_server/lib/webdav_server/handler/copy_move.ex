defmodule WebdavServer.Handler.CopyMove do
  @moduledoc false

  import Plug.Conn
  alias WebdavServer.Handler.Helpers

  @doc false
  @spec handle(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def handle(conn, opts) do
    path = Helpers.resource_path(conn)

    with {:ok, dest_path} <- Helpers.parse_destination(conn),
         :ok <- check_not_self(path, dest_path),
         :ok <- check_not_descendant(conn.method, path, dest_path),
         :ok <- maybe_check_lock(conn, path, dest_path, opts) do
      overwrite? = Helpers.parse_overwrite(conn)

      {:ok, resource} = opts.backend.resolve(opts.auth, path)

      result =
        if conn.method == "COPY" do
          opts.backend.copy(opts.auth, resource, dest_path, overwrite?)
        else
          opts.backend.move(opts.auth, resource, dest_path, overwrite?)
        end

      case result do
        {:ok, :created} -> send_resp(conn, 201, "")
        {:ok, :no_content} -> send_resp(conn, 204, "")
        {:error, error} -> Helpers.send_error(conn, error)
      end
    else
      {:error, :bad_request} ->
        send_resp(conn, 400, "Missing Destination header")

      {:error, :forbidden} ->
        send_resp(conn, 403, "Cannot copy/move to descendant of self")

      {:error, :locked} ->
        send_resp(conn, 423, "Locked")
    end
  end

  defp check_not_self(path, path), do: {:error, :forbidden}
  defp check_not_self(_, _), do: :ok

  defp check_not_descendant("COPY", source, dest), do: check_descendant(source, dest)
  defp check_not_descendant("MOVE", source, dest), do: check_descendant(source, dest)
  defp check_not_descendant(_, _, _), do: :ok

  defp check_descendant(source, dest) do
    if List.starts_with?(dest, source) and length(dest) > length(source) do
      {:error, :forbidden}
    else
      :ok
    end
  end

  defp maybe_check_lock(conn, _source, dest_path, opts) do
    # MOVE requires lock check on source; both require check on destination
    if conn.method == "MOVE" do
      source_path = Helpers.resource_path(conn)

      with :ok <- Helpers.check_lock(conn, source_path, opts.lock_store) do
        Helpers.check_lock(conn, dest_path, opts.lock_store)
      end
    else
      Helpers.check_lock(conn, dest_path, opts.lock_store)
    end
  end
end
