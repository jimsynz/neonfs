defmodule WebdavServer.XMLTest do
  use ExUnit.Case, async: true

  alias WebdavServer.XML

  describe "parse/1" do
    test "resolves DAV: namespace with D: prefix" do
      xml = """
      <?xml version="1.0" encoding="utf-8"?>
      <D:propfind xmlns:D="DAV:">
        <D:prop>
          <D:getcontentlength/>
        </D:prop>
      </D:propfind>
      """

      {:ok, {ns, "propfind", _, children}} = XML.parse(xml)
      assert ns == "DAV:"

      prop = XML.find_dav_child(children, "prop")
      assert prop != nil

      {_, "prop", _, prop_children} = prop
      names = XML.extract_property_names(prop_children)
      assert {"DAV:", "getcontentlength"} in names
    end

    test "resolves default namespace (no prefix)" do
      xml = """
      <?xml version="1.0" encoding="utf-8"?>
      <propfind xmlns="DAV:">
        <prop>
          <getcontentlength/>
        </prop>
      </propfind>
      """

      {:ok, {"DAV:", "propfind", _, children}} = XML.parse(xml)

      prop = XML.find_dav_child(children, "prop")
      assert prop != nil
    end

    test "resolves custom prefix" do
      xml = """
      <?xml version="1.0" encoding="utf-8"?>
      <dav:propfind xmlns:dav="DAV:">
        <dav:prop>
          <dav:resourcetype/>
        </dav:prop>
      </dav:propfind>
      """

      {:ok, {"DAV:", "propfind", _, _}} = XML.parse(xml)
    end

    test "strips whitespace-only text nodes" do
      xml = """
      <?xml version="1.0" encoding="utf-8"?>
      <D:propfind xmlns:D="DAV:">
        <D:allprop/>
      </D:propfind>
      """

      {:ok, {_, _, _, children}} = XML.parse(xml)
      # Should only contain the allprop element, no whitespace strings
      assert length(children) == 1
      assert XML.find_dav_child(children, "allprop") != nil
    end

    test "returns error for invalid XML" do
      assert {:error, _} = XML.parse("<unclosed")
    end
  end

  describe "multistatus/1" do
    test "generates valid XML with DAV: namespace" do
      response = XML.response_element("/test.txt", [{200, []}])
      body = XML.multistatus([response])
      xml = IO.iodata_to_binary(body)

      assert xml =~ "<?xml"
      assert xml =~ "D:multistatus"
      assert xml =~ "xmlns:D=\"DAV:\""
      assert xml =~ "D:response"
      assert xml =~ "/test.txt"
    end
  end

  describe "response_element/2" do
    test "groups properties by status code" do
      import Saxy.XML

      propstats = [
        {200, [{"D:getcontentlength", element("D:getcontentlength", [], ["1024"])}]},
        {404, [{"D:custom", empty_element("D:custom", [])}]}
      ]

      el = XML.response_element("/file.txt", propstats)
      xml = IO.iodata_to_binary(Saxy.encode_to_iodata!(el, []))

      assert xml =~ "200 OK"
      assert xml =~ "404 Not Found"
      assert xml =~ "getcontentlength"
    end
  end

  describe "resourcetype_element/1" do
    test "returns empty element for files" do
      el = XML.resourcetype_element(:file)
      xml = IO.iodata_to_binary(Saxy.encode_to_iodata!(el, []))

      assert xml =~ "resourcetype"
      refute xml =~ "collection"
    end

    test "returns collection child for collections" do
      el = XML.resourcetype_element(:collection)
      xml = IO.iodata_to_binary(Saxy.encode_to_iodata!(el, []))

      assert xml =~ "collection"
    end
  end

  describe "supported_lock_element/0" do
    test "includes exclusive and shared write locks" do
      el = XML.supported_lock_element()
      xml = IO.iodata_to_binary(Saxy.encode_to_iodata!(el, []))

      assert xml =~ "exclusive"
      assert xml =~ "shared"
      assert xml =~ "write"
    end
  end

  describe "lock_response/1" do
    test "generates lock discovery XML" do
      lock_info = %{
        token: "abc123",
        path: ["test.txt"],
        scope: :exclusive,
        type: :write,
        owner: "user@example.com",
        timeout: 600,
        expires_at: System.system_time(:second) + 600
      }

      body = XML.lock_response(lock_info)
      xml = IO.iodata_to_binary(body)

      assert xml =~ "lockdiscovery"
      assert xml =~ "exclusive"
      assert xml =~ "write"
      assert xml =~ "opaquelocktoken:abc123"
      assert xml =~ "Second-600"
      assert xml =~ "user@example.com"
    end
  end
end
