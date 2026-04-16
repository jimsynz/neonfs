defmodule WebdavServer.Handler.Get do
  @moduledoc false

  import Plug.Conn
  alias WebdavServer.Handler.Helpers

  @doc false
  @spec handle(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def handle(conn, opts) do
    path = Helpers.resource_path(conn)

    case opts.backend.resolve(opts.auth, path) do
      {:ok, %{type: :collection}} ->
        send_resp(conn, 405, "Method Not Allowed")

      {:ok, resource} ->
        serve_content(conn, opts, resource)

      {:error, error} ->
        Helpers.send_error(conn, error)
    end
  end

  defp serve_content(conn, opts, resource) do
    range_opts = parse_range(conn)

    case opts.backend.get_content(opts.auth, resource, range_opts) do
      {:ok, content} -> send_content(conn, resource, content, range_opts)
      {:error, error} -> Helpers.send_error(conn, error)
    end
  end

  defp send_content(conn, resource, content, range_opts) do
    conn =
      conn
      |> put_content_type(resource)
      |> put_etag(resource)
      |> put_last_modified(resource)
      |> put_resp_header("accept-ranges", "bytes")

    if streamable?(content) do
      send_streamed_content(conn, resource, content, range_opts)
    else
      send_binary_content(conn, resource, content, range_opts)
    end
  end

  defp send_binary_content(conn, resource, content, range_opts) do
    body = if is_list(content), do: IO.iodata_to_binary(content), else: content

    {status, conn} =
      case range_opts do
        %{range: {start_byte, _}} ->
          actual_end = start_byte + byte_size(body) - 1
          total = if is_integer(resource.content_length), do: resource.content_length, else: "*"
          range_header = "bytes #{start_byte}-#{actual_end}/#{total}"

          {206,
           conn
           |> put_resp_header("content-range", range_header)
           |> put_resp_header("content-length", Integer.to_string(byte_size(body)))}

        _ ->
          {200, put_content_length(conn, resource)}
      end

    if conn.method == "HEAD" do
      send_resp(conn, status, "")
    else
      send_resp(conn, status, body)
    end
  end

  defp send_streamed_content(conn, resource, stream, range_opts) do
    {status, conn} =
      case range_opts do
        %{range: {start_byte, end_byte}} ->
          content_length = compute_range_length(start_byte, end_byte, resource.content_length)
          actual_end = start_byte + content_length - 1
          total = if is_integer(resource.content_length), do: resource.content_length, else: "*"
          range_header = "bytes #{start_byte}-#{actual_end}/#{total}"

          {206,
           conn
           |> put_resp_header("content-range", range_header)
           |> put_resp_header("content-length", Integer.to_string(content_length))}

        _ ->
          {200, put_content_length(conn, resource)}
      end

    if conn.method == "HEAD" do
      send_resp(conn, status, "")
    else
      stream_chunks(conn, status, stream)
    end
  end

  defp stream_chunks(conn, status, stream) do
    conn = send_chunked(conn, status)

    Enum.reduce_while(stream, conn, fn data, conn ->
      case chunk(conn, data) do
        {:ok, conn} -> {:cont, conn}
        {:error, :closed} -> {:halt, conn}
      end
    end)
  end

  defp streamable?(content) when is_binary(content), do: false
  defp streamable?(content) when is_list(content), do: false
  defp streamable?(_content), do: true

  defp compute_range_length(start_byte, nil, total) when is_integer(total),
    do: total - start_byte

  defp compute_range_length(start_byte, end_byte, _total), do: end_byte - start_byte + 1

  defp put_content_type(conn, %{content_type: ct}) when is_binary(ct),
    do: put_resp_header(conn, "content-type", ct)

  defp put_content_type(conn, _),
    do: put_resp_header(conn, "content-type", "application/octet-stream")

  defp put_etag(conn, %{etag: etag}) when is_binary(etag),
    do: put_resp_header(conn, "etag", etag)

  defp put_etag(conn, _), do: conn

  defp put_last_modified(conn, %{last_modified: %DateTime{} = dt}),
    do: put_resp_header(conn, "last-modified", format_http_date(dt))

  defp put_last_modified(conn, _), do: conn

  defp put_content_length(conn, %{content_length: len}) when is_integer(len),
    do: put_resp_header(conn, "content-length", Integer.to_string(len))

  defp put_content_length(conn, _), do: conn

  defp parse_range(conn) do
    case get_req_header(conn, "range") do
      ["bytes=" <> range_spec] -> parse_range_spec(range_spec)
      _ -> %{}
    end
  end

  defp parse_range_spec(range_spec) do
    case String.split(range_spec, "-", parts: 2) do
      [start_str, end_str] ->
        case parse_int(start_str) do
          nil -> %{}
          start_byte -> %{range: {start_byte, parse_int(end_str)}}
        end

      _ ->
        %{}
    end
  end

  defp parse_int(""), do: nil

  defp parse_int(str) do
    case Integer.parse(str) do
      {n, ""} -> n
      _ -> nil
    end
  end

  defp format_http_date(datetime) do
    Calendar.strftime(datetime, "%a, %d %b %Y %H:%M:%S GMT")
  end
end
