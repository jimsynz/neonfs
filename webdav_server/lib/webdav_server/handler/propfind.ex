defmodule WebdavServer.Handler.Propfind do
  @moduledoc false

  import Plug.Conn
  alias WebdavServer.{Handler.Helpers, Resource, XML}

  @dav_ns "DAV:"

  @all_dav_properties [
    "resourcetype",
    "getcontentlength",
    "getcontenttype",
    "getetag",
    "getlastmodified",
    "creationdate",
    "displayname",
    "supportedlock",
    "lockdiscovery"
  ]

  @doc false
  @spec handle(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def handle(conn, opts) do
    path = Helpers.resource_path(conn)
    depth = Helpers.parse_depth(conn, 0)

    if depth == :infinity and not opts.allow_infinity_depth do
      conn
      |> put_resp_header("dav", "1, 2")
      |> send_resp(403, "Depth: infinity not allowed")
    else
      case opts.backend.resolve(opts.auth, path) do
        {:ok, resource} ->
          request_type = parse_propfind_body(conn)
          resources = collect_resources(resource, depth, opts)
          send_propfind_response(conn, opts, resources, request_type)

        {:error, error} ->
          Helpers.send_error(conn, error)
      end
    end
  end

  defp parse_propfind_body(conn) do
    case read_body(conn) do
      {:ok, "", _conn} ->
        :allprop

      {:ok, body, _conn} ->
        parse_propfind_xml(body)
    end
  end

  defp parse_propfind_xml(body) do
    case XML.parse(body) do
      {:ok, {_, "propfind", _, children}} ->
        cond do
          XML.find_dav_child(children, "allprop") -> :allprop
          XML.find_dav_child(children, "propname") -> :propname
          prop = XML.find_dav_child(children, "prop") -> {:prop, extract_props(prop)}
          true -> :allprop
        end

      _ ->
        :allprop
    end
  end

  defp extract_props({_, _, _, children}) do
    XML.extract_property_names(children)
  end

  defp collect_resources(resource, 0, _opts), do: [resource]

  defp collect_resources(%Resource{type: :collection} = resource, depth, opts) when depth > 0 do
    case opts.backend.get_members(opts.auth, resource) do
      {:ok, members} ->
        if depth == 1 do
          [resource | members]
        else
          [resource | Enum.flat_map(members, &collect_resources(&1, depth - 1, opts))]
        end

      {:error, _} ->
        [resource]
    end
  end

  defp collect_resources(resource, _, _opts), do: [resource]

  defp send_propfind_response(conn, opts, resources, request_type) do
    response_elements = Enum.map(resources, &build_response(conn, opts, &1, request_type))
    body = XML.multistatus(response_elements)

    conn
    |> put_resp_content_type("application/xml", nil)
    |> send_resp(207, body)
  end

  defp build_response(conn, opts, resource, :allprop) do
    href = Helpers.href(conn, resource.path)
    locks = opts.lock_store.get_locks(resource.path)

    found_props =
      @all_dav_properties
      |> Enum.map(fn name -> {name, get_standard_property(resource, name, locks)} end)
      |> Enum.filter(fn {_, val} -> val != nil end)
      |> Enum.map(fn {name, val} -> {"D:#{name}", val} end)

    XML.response_element(href, [{200, found_props}])
  end

  defp build_response(conn, _opts, resource, :propname) do
    href = Helpers.href(conn, resource.path)

    prop_elements =
      @all_dav_properties
      |> Enum.map(fn name -> {"D:#{name}", Saxy.XML.empty_element("D:#{name}", [])} end)

    XML.response_element(href, [{200, prop_elements}])
  end

  defp build_response(conn, opts, resource, {:prop, requested_props}) do
    href = Helpers.href(conn, resource.path)
    locks = opts.lock_store.get_locks(resource.path)

    {dav_props, custom_props} =
      Enum.split_with(requested_props, fn {ns, _} -> ns == @dav_ns end)

    found = []
    not_found = []

    {found, not_found} =
      Enum.reduce(dav_props, {found, not_found}, fn {_, name}, {f, nf} ->
        case get_standard_property(resource, name, locks) do
          nil -> {f, [{"D:#{name}", Saxy.XML.empty_element("D:#{name}", [])} | nf]}
          val -> {[{"D:#{name}", val} | f], nf}
        end
      end)

    {found, not_found} =
      if custom_props != [] do
        results = opts.backend.get_properties(opts.auth, resource, custom_props)

        Enum.reduce(results, {found, not_found}, fn
          {{ns, name}, {:ok, value}}, {f, nf} ->
            el = build_custom_prop_element(ns, name, value)
            {[{qualified_name(ns, name), el} | f], nf}

          {{ns, name}, {:error, _}}, {f, nf} ->
            qname = qualified_name(ns, name)
            {f, [{qname, Saxy.XML.empty_element(qname, [])} | nf]}
        end)
      else
        {found, not_found}
      end

    propstats =
      []
      |> maybe_add_propstat(200, Enum.reverse(found))
      |> maybe_add_propstat(404, Enum.reverse(not_found))

    XML.response_element(href, propstats)
  end

  defp maybe_add_propstat(propstats, _status, []), do: propstats
  defp maybe_add_propstat(propstats, status, props), do: propstats ++ [{status, props}]

  defp get_standard_property(resource, "resourcetype", _locks),
    do: XML.resourcetype_element(resource.type)

  defp get_standard_property(%{content_length: len}, "getcontentlength", _locks)
       when is_integer(len),
       do: Saxy.XML.element("D:getcontentlength", [], [Integer.to_string(len)])

  defp get_standard_property(%{content_type: ct}, "getcontenttype", _locks) when is_binary(ct),
    do: Saxy.XML.element("D:getcontenttype", [], [ct])

  defp get_standard_property(%{etag: etag}, "getetag", _locks) when is_binary(etag),
    do: Saxy.XML.element("D:getetag", [], [etag])

  defp get_standard_property(%{last_modified: %DateTime{} = dt}, "getlastmodified", _locks),
    do: Saxy.XML.element("D:getlastmodified", [], [format_http_date(dt)])

  defp get_standard_property(%{creation_date: %DateTime{} = dt}, "creationdate", _locks),
    do: Saxy.XML.element("D:creationdate", [], [DateTime.to_iso8601(dt)])

  defp get_standard_property(%{display_name: name}, "displayname", _locks) when is_binary(name),
    do: Saxy.XML.element("D:displayname", [], [name])

  defp get_standard_property(_resource, "supportedlock", _locks),
    do: XML.supported_lock_element()

  defp get_standard_property(_resource, "lockdiscovery", locks),
    do: XML.lock_discovery_element(locks)

  defp get_standard_property(_, _, _), do: nil

  defp build_custom_prop_element(_ns, _name, value) when is_binary(value) do
    # For custom props, the full element is built by the caller
    Saxy.XML.element("custom", [], [value])
  end

  defp build_custom_prop_element(_ns, _name, {_, _, _} = el), do: el

  defp qualified_name(@dav_ns, name), do: "D:#{name}"
  defp qualified_name(_, name), do: name

  defp format_http_date(datetime) do
    Calendar.strftime(datetime, "%a, %d %b %Y %H:%M:%S GMT")
  end
end
