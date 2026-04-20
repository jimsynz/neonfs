defmodule WebdavServer.Handler.Proppatch do
  @moduledoc false

  import Plug.Conn
  alias WebdavServer.{Handler.Helpers, XML}

  @dav_ns "DAV:"

  @doc false
  @spec handle(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def handle(conn, opts) do
    path = Helpers.resource_path(conn)

    with :ok <- Helpers.check_lock(conn, path, opts.lock_store),
         {:ok, resource} <- opts.backend.resolve(opts.auth, path),
         # audit:bounded PROPPATCH body is a small XML property list (RFC 4918 §9.2)
         {:ok, body, _conn} <- read_body(conn),
         {:ok, operations} <- parse_proppatch_body(body) do
      case opts.backend.set_properties(opts.auth, resource, operations) do
        :ok ->
          send_proppatch_success(conn, path, operations)

        {:error, error} ->
          Helpers.send_error(conn, error)
      end
    else
      {:error, :locked} ->
        send_resp(conn, 423, "Locked")

      {:error, :bad_request} ->
        send_resp(conn, 400, "Invalid PROPPATCH body")

      {:error, error} when is_struct(error, WebdavServer.Error) ->
        Helpers.send_error(conn, error)
    end
  end

  defp parse_proppatch_body(body) do
    case XML.parse(body) do
      {:ok, {_, "propertyupdate", _, children}} ->
        operations = Enum.flat_map(children, &parse_update_instruction/1)
        {:ok, operations}

      _ ->
        {:error, :bad_request}
    end
  end

  defp parse_update_instruction({@dav_ns, "set", _, children}) do
    case XML.find_dav_child(children, "prop") do
      {_, _, _, prop_children} ->
        Enum.flat_map(prop_children, fn
          {ns, name, _, value_children} ->
            value = extract_text(value_children)
            [{:set, {ns, name}, value}]

          _ ->
            []
        end)

      nil ->
        []
    end
  end

  defp parse_update_instruction({@dav_ns, "remove", _, children}) do
    case XML.find_dav_child(children, "prop") do
      {_, _, _, prop_children} ->
        Enum.flat_map(prop_children, fn
          {ns, name, _, _} -> [{:remove, {ns, name}}]
          _ -> []
        end)

      nil ->
        []
    end
  end

  defp parse_update_instruction(_), do: []

  defp extract_text(children) do
    children
    |> Enum.filter(&is_binary/1)
    |> Enum.join()
  end

  defp send_proppatch_success(conn, path, operations) do
    href = Helpers.href(conn, path)

    prop_elements =
      Enum.map(operations, fn
        {:set, {_ns, name}, _} -> {"D:#{name}", Saxy.XML.empty_element("D:#{name}", [])}
        {:remove, {_ns, name}} -> {"D:#{name}", Saxy.XML.empty_element("D:#{name}", [])}
      end)

    response = XML.response_element(href, [{200, prop_elements}])
    body = XML.multistatus([response])

    conn
    |> put_resp_content_type("application/xml", nil)
    |> send_resp(207, body)
  end
end
