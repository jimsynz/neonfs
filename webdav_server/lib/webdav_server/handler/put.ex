defmodule WebdavServer.Handler.Put do
  @moduledoc false

  import Plug.Conn
  alias WebdavServer.Handler.Helpers

  # Slice size used when streaming the request body to the backend.
  # 64 KiB matches the order of FUSE max_read; large enough to amortise
  # syscalls, small enough to keep peak memory bounded.
  @body_chunk_size 64 * 1024

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
    content_type =
      case get_req_header(conn, "content-type") do
        [ct] -> ct
        _ -> "application/octet-stream"
      end

    existing? = match?({:ok, _}, opts.backend.resolve(opts.auth, path))
    backend_opts = %{content_type: content_type}

    {result, conn} = dispatch_put(conn, opts, path, backend_opts)

    case result do
      {:ok, resource} ->
        conn = maybe_put_etag(conn, resource)
        send_resp(conn, if(existing?, do: 204, else: 201), "")

      {:error, error} ->
        Helpers.send_error(conn, error)
    end
  end

  defp dispatch_put(conn, opts, path, backend_opts) do
    if function_exported?(opts.backend, :put_content_stream, 4) do
      stream_put(conn, opts, path, backend_opts)
    else
      buffered_put(conn, opts, path, backend_opts)
    end
  end

  defp stream_put(conn, opts, path, backend_opts) do
    # `Plug.Conn.read_body/2` returns an updated `conn` each call, but
    # `Stream.resource` discards its accumulator once iteration ends.
    # Stash the latest conn in the process dictionary so we can return
    # the up-to-date one to the caller after the backend consumes the
    # stream. The handler runs in a single process, so this is safe.
    Process.put(:webdav_put_conn, conn)

    body_stream =
      Stream.resource(
        fn -> :continue end,
        &next_body_chunk/1,
        fn _ -> :ok end
      )

    result = opts.backend.put_content_stream(opts.auth, path, body_stream, backend_opts)
    conn = Process.get(:webdav_put_conn, conn)
    Process.delete(:webdav_put_conn)
    {result, conn}
  end

  defp next_body_chunk(:done), do: {:halt, :done}

  defp next_body_chunk(:continue) do
    conn = Process.get(:webdav_put_conn)

    case read_body(conn, length: @body_chunk_size) do
      {:ok, "", new_conn} ->
        Process.put(:webdav_put_conn, new_conn)
        {:halt, :done}

      {:ok, chunk, new_conn} ->
        Process.put(:webdav_put_conn, new_conn)
        {[chunk], :done}

      {:more, chunk, new_conn} ->
        Process.put(:webdav_put_conn, new_conn)
        {[chunk], :continue}
    end
  end

  defp buffered_put(conn, opts, path, backend_opts) do
    {:ok, body, conn} = read_body(conn)
    result = opts.backend.put_content(opts.auth, path, body, backend_opts)
    {result, conn}
  end

  defp maybe_put_etag(conn, %{etag: etag}) when is_binary(etag),
    do: put_resp_header(conn, "etag", etag)

  defp maybe_put_etag(conn, _), do: conn
end
