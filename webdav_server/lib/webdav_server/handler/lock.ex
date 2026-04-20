defmodule WebdavServer.Handler.Lock do
  @moduledoc false

  import Plug.Conn
  alias WebdavServer.{Handler.Helpers, XML}

  @dav_ns "DAV:"

  @doc false
  @spec handle(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def handle(%{method: "LOCK"} = conn, opts), do: handle_lock(conn, opts)
  def handle(%{method: "UNLOCK"} = conn, opts), do: handle_unlock(conn, opts)

  defp handle_lock(conn, opts) do
    path = Helpers.resource_path(conn)
    timeout = Helpers.parse_timeout(conn, opts.lock_timeout)
    depth = parse_lock_depth(conn)

    case {Helpers.extract_lock_tokens(conn), peek_body(conn)} do
      {[token | _], :empty} -> refresh_lock(conn, opts, token, timeout)
      _ -> create_lock(conn, opts, path, depth, timeout)
    end
  end

  defp create_lock(conn, opts, path, depth, timeout) do
    # audit:bounded WebDAV LOCK body is a small lockinfo XML (RFC 4918 §9.10)
    case read_body(conn) do
      {:ok, "", _conn} ->
        send_resp(conn, 400, "Lock request requires a body")

      {:ok, body, _conn} ->
        case parse_lock_body(body) do
          {:ok, scope, owner} -> acquire_lock(conn, opts, path, scope, depth, owner, timeout)
          {:error, :bad_request} -> send_resp(conn, 400, "Invalid LOCK body")
        end
    end
  end

  defp acquire_lock(conn, opts, path, scope, depth, owner, timeout) do
    case opts.lock_store.lock(path, scope, :write, depth, owner, timeout) do
      {:ok, token} ->
        send_lock_response(conn, opts, path, scope, depth, owner, token, timeout)

      {:error, :conflict} ->
        send_resp(conn, 423, "Locked")
    end
  end

  defp send_lock_response(conn, opts, path, scope, depth, owner, token, timeout) do
    lock_info = %{
      token: token,
      path: path,
      scope: scope,
      type: :write,
      depth: depth,
      owner: owner,
      timeout: timeout,
      expires_at: System.system_time(:second) + timeout
    }

    status =
      case opts.backend.resolve(opts.auth, path) do
        {:ok, _} -> 200
        {:error, _} -> maybe_create_empty(opts, path)
      end

    body = XML.lock_response(lock_info)

    conn
    |> put_resp_content_type("application/xml", nil)
    |> put_resp_header("lock-token", "<opaquelocktoken:#{token}>")
    |> send_resp(status, body)
  end

  defp refresh_lock(conn, opts, token, timeout) do
    case opts.lock_store.refresh(token, timeout) do
      {:ok, lock_info} ->
        body = XML.lock_response(lock_info)

        conn
        |> put_resp_content_type("application/xml", nil)
        |> send_resp(200, body)

      {:error, :not_found} ->
        send_resp(conn, 412, "Precondition Failed")
    end
  end

  defp handle_unlock(conn, opts) do
    case extract_lock_token_header(conn) do
      nil ->
        send_resp(conn, 400, "Missing Lock-Token header")

      token ->
        case opts.lock_store.unlock(token) do
          :ok -> send_resp(conn, 204, "")
          {:error, :not_found} -> send_resp(conn, 409, "Conflict")
        end
    end
  end

  defp extract_lock_token_header(conn) do
    case get_req_header(conn, "lock-token") do
      [value] ->
        value
        |> String.trim_leading("<")
        |> String.trim_trailing(">")
        |> String.trim_leading("opaquelocktoken:")

      [] ->
        nil
    end
  end

  defp parse_lock_body(body) do
    case XML.parse(body) do
      {:ok, {_, "lockinfo", _, children}} ->
        scope = parse_lock_scope(children)
        owner = parse_lock_owner(children)
        {:ok, scope, owner}

      _ ->
        {:error, :bad_request}
    end
  end

  defp parse_lock_scope(children) do
    case XML.find_dav_child(children, "lockscope") do
      {_, _, _, scope_children} ->
        if XML.find_dav_child(scope_children, "shared"), do: :shared, else: :exclusive

      nil ->
        :exclusive
    end
  end

  defp parse_lock_owner(children) do
    case XML.find_dav_child(children, "owner") do
      {_, _, _, owner_children} -> extract_owner(owner_children)
      nil -> nil
    end
  end

  defp extract_owner(owner_children) do
    case XML.find_dav_child(owner_children, "href") do
      {@dav_ns, "href", _, [text]} when is_binary(text) -> text
      _ -> extract_owner_text(owner_children)
    end
  end

  defp extract_owner_text(children) do
    text =
      children
      |> Enum.filter(&is_binary/1)
      |> Enum.join()
      |> String.trim()

    if text == "", do: nil, else: text
  end

  defp peek_body(conn) do
    case get_req_header(conn, "content-length") do
      ["0"] -> :empty
      [] -> :empty
      _ -> :has_body
    end
  end

  defp parse_lock_depth(conn) do
    case Helpers.parse_depth(conn, :infinity) do
      0 -> 0
      _ -> :infinity
    end
  end

  defp maybe_create_empty(opts, path) do
    case opts.backend.put_content(opts.auth, path, "", %{
           content_type: "application/octet-stream"
         }) do
      {:ok, _} -> 201
      {:error, _} -> 200
    end
  end
end
