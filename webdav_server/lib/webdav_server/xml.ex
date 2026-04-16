defmodule WebdavServer.XML do
  @moduledoc """
  XML parsing and building utilities for WebDAV.

  Handles namespace-aware parsing of request bodies (PROPFIND, PROPPATCH, LOCK)
  and generation of multistatus XML responses using Saxy.
  """

  import Saxy.XML

  @dav_ns "DAV:"

  # --- Parsing ---

  @doc """
  Parse an XML string into a namespace-resolved tree.

  Returns `{:ok, resolved_tree}` or `{:error, reason}`.
  Each element becomes `{namespace, local_name, attributes, children}`.
  """
  @spec parse(String.t()) ::
          {:ok, resolved_element()} | {:error, term()}
  @type resolved_element ::
          {String.t(), String.t(), [{String.t(), String.t()}], [resolved_element() | String.t()]}
  def parse(xml_string) do
    case Saxy.SimpleForm.parse_string(xml_string) do
      {:ok, tree} -> {:ok, resolve_namespaces(tree, %{})}
      {:error, reason} -> {:error, reason}
    end
  end

  defp resolve_namespaces({tag_name, attrs, children}, parent_ns_map) do
    ns_map = extract_namespace_declarations(attrs, parent_ns_map)
    {namespace, local_name} = resolve_tag(tag_name, ns_map)
    non_ns_attrs = reject_xmlns_attrs(attrs)

    resolved_children =
      children
      |> Enum.reject(&whitespace_text?/1)
      |> Enum.map(fn
        {_, _, _} = child -> resolve_namespaces(child, ns_map)
        text when is_binary(text) -> text
      end)

    {namespace, local_name, non_ns_attrs, resolved_children}
  end

  defp extract_namespace_declarations(attrs, parent_map) do
    Enum.reduce(attrs, parent_map, fn
      {"xmlns:" <> prefix, uri}, acc -> Map.put(acc, prefix, uri)
      {"xmlns", uri}, acc -> Map.put(acc, "", uri)
      _, acc -> acc
    end)
  end

  defp resolve_tag(tag_name, ns_map) do
    case String.split(tag_name, ":", parts: 2) do
      [prefix, local] -> {Map.get(ns_map, prefix, ""), local}
      [local] -> {Map.get(ns_map, "", ""), local}
    end
  end

  defp reject_xmlns_attrs(attrs) do
    Enum.reject(attrs, fn
      {"xmlns:" <> _, _} -> true
      {"xmlns", _} -> true
      _ -> false
    end)
  end

  defp whitespace_text?(text) when is_binary(text), do: String.trim(text) == ""
  defp whitespace_text?(_), do: false

  # --- Convenience matchers ---

  @doc """
  Check whether a resolved element matches the DAV: namespace with the given local name.
  """
  @spec dav_element?({String.t(), String.t(), term(), term()}, String.t()) :: boolean()
  def dav_element?({@dav_ns, name, _, _}, name), do: true
  def dav_element?(_, _), do: false

  @doc """
  Find the first child element matching `{DAV:, local_name}`.
  """
  @spec find_dav_child([resolved_element() | String.t()], String.t()) ::
          resolved_element() | nil
  def find_dav_child(children, local_name) do
    Enum.find(children, fn
      {@dav_ns, ^local_name, _, _} -> true
      _ -> false
    end)
  end

  @doc """
  Extract `{namespace, local_name}` tuples from child elements of a `<prop>` element.
  """
  @spec extract_property_names([resolved_element() | String.t()]) :: [{String.t(), String.t()}]
  def extract_property_names(children) do
    Enum.flat_map(children, fn
      {ns, name, _, _} -> [{ns, name}]
      _ -> []
    end)
  end

  # --- Building ---

  @doc """
  Build a complete 207 Multi-Status XML response body.
  """
  @spec multistatus([Saxy.XML.element()]) :: iodata()
  def multistatus(response_elements) do
    root = element("D:multistatus", [{"xmlns:D", @dav_ns}], response_elements)
    Saxy.encode_to_iodata!(root, version: "1.0")
  end

  @doc """
  Build a `<D:response>` element for a single resource.

  `propstats` is a list of `{status_code, [{property_name, value_element}]}`.
  """
  @spec response_element(String.t(), [{pos_integer(), [{String.t(), Saxy.XML.element()}]}]) ::
          Saxy.XML.element()
  def response_element(href, propstats) do
    propstat_elements =
      Enum.map(propstats, fn {status, props} ->
        prop_children = Enum.map(props, fn {_name, el} -> el end)

        element("D:propstat", [], [
          element("D:prop", [], prop_children),
          element("D:status", [], ["HTTP/1.1 #{status} #{status_text(status)}"])
        ])
      end)

    element("D:response", [], [
      element("D:href", [], [href]) | propstat_elements
    ])
  end

  @doc """
  Build a property element for a standard DAV: property.
  """
  @spec dav_property(String.t(), String.t() | tuple() | nil) :: Saxy.XML.element()
  def dav_property(name, nil), do: empty_element("D:#{name}", [])
  def dav_property(name, value) when is_binary(value), do: element("D:#{name}", [], [value])

  def dav_property(name, {_, _, _} = child),
    do: element("D:#{name}", [], [child])

  @doc """
  Build a `<D:resourcetype>` element — empty for files, contains
  `<D:collection/>` for collections.
  """
  @spec resourcetype_element(:file | :collection) :: Saxy.XML.element()
  def resourcetype_element(:file), do: empty_element("D:resourcetype", [])

  def resourcetype_element(:collection),
    do: element("D:resourcetype", [], [empty_element("D:collection", [])])

  @doc """
  Build the `<D:supportedlock>` element advertising exclusive and shared write locks.
  """
  @spec supported_lock_element() :: Saxy.XML.element()
  def supported_lock_element do
    element("D:supportedlock", [], [
      lockentry_element(:exclusive),
      lockentry_element(:shared)
    ])
  end

  @doc """
  Build a `<D:lockdiscovery>` element from a list of active locks.
  """
  @spec lock_discovery_element([WebdavServer.LockStore.lock_info()]) :: Saxy.XML.element()
  def lock_discovery_element([]), do: empty_element("D:lockdiscovery", [])

  def lock_discovery_element(locks) do
    element("D:lockdiscovery", [], Enum.map(locks, &activelock_element/1))
  end

  @doc """
  Build a full lock response body (for LOCK method responses).
  """
  @spec lock_response(WebdavServer.LockStore.lock_info()) :: iodata()
  def lock_response(lock_info) do
    root =
      element("D:prop", [{"xmlns:D", @dav_ns}], [
        element("D:lockdiscovery", [], [activelock_element(lock_info)])
      ])

    Saxy.encode_to_iodata!(root, version: "1.0")
  end

  @doc """
  Build an XML error response body.
  """
  @spec error_body(String.t()) :: iodata()
  def error_body(message) do
    root =
      element("D:error", [{"xmlns:D", @dav_ns}], [
        element("D:message", [], [message])
      ])

    Saxy.encode_to_iodata!(root, version: "1.0")
  end

  # --- Private helpers ---

  defp lockentry_element(scope) do
    element("D:lockentry", [], [
      element("D:lockscope", [], [empty_element("D:#{scope}", [])]),
      element("D:locktype", [], [empty_element("D:write", [])])
    ])
  end

  defp activelock_element(lock_info) do
    timeout_str =
      if lock_info.timeout == :infinity, do: "Infinite", else: "Second-#{lock_info.timeout}"

    depth_str =
      case Map.get(lock_info, :depth, :infinity) do
        0 -> "0"
        _ -> "infinity"
      end

    children = [
      element("D:locktype", [], [empty_element("D:write", [])]),
      element("D:lockscope", [], [empty_element("D:#{lock_info.scope}", [])]),
      element("D:depth", [], [depth_str]),
      element("D:timeout", [], [timeout_str]),
      element("D:locktoken", [], [
        element("D:href", [], ["opaquelocktoken:#{lock_info.token}"])
      ])
    ]

    children =
      if lock_info.owner do
        children ++ [element("D:owner", [], [element("D:href", [], [lock_info.owner])])]
      else
        children
      end

    element("D:activelock", [], children)
  end

  defp status_text(200), do: "OK"
  defp status_text(201), do: "Created"
  defp status_text(204), do: "No Content"
  defp status_text(207), do: "Multi-Status"
  defp status_text(403), do: "Forbidden"
  defp status_text(404), do: "Not Found"
  defp status_text(409), do: "Conflict"
  defp status_text(412), do: "Precondition Failed"
  defp status_text(423), do: "Locked"
  defp status_text(424), do: "Failed Dependency"
  defp status_text(507), do: "Insufficient Storage"
  defp status_text(_), do: "Unknown"
end
