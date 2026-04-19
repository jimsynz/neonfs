defmodule NeonFS.Integration.WebDAVTest do
  @moduledoc """
  Integration tests for the WebDAV API against a real multi-node NeonFS cluster.

  Starts a 3-node core cluster via PeerCluster, then launches a Bandit HTTP
  server running the WebDAV plug on the test runner. Uses Req as an HTTP client
  to verify end-to-end WebDAV protocol compatibility.
  """
  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Client.{Connection, CostFunction, Discovery}
  alias NeonFS.Integration.WebDAVCoreBridge

  @moduletag timeout: 180_000
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    init_multi_node_cluster(cluster, name: "webdav-test")

    node1 = PeerCluster.get_node!(cluster, :node1)
    WebDAVCoreBridge.store_core_node(node1.node)

    Application.put_env(:neonfs_webdav, :core_call_fn, &WebDAVCoreBridge.call/2)

    # Start client infrastructure on the test runner so NeonFS.Client.ChunkReader
    # can resolve a core node via Router → CostFunction for WebDAV GETs.
    start_supervised!({Connection, bootstrap_nodes: [node1.node]})
    start_supervised!(Discovery)
    start_supervised!(CostFunction)

    :ok =
      wait_until(fn ->
        match?({:ok, _}, Connection.connected_core_node())
      end)

    :ok =
      wait_until(
        fn ->
          case Discovery.get_core_nodes() do
            [_ | _] -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )

    {:ok, server} =
      Bandit.start_link(
        plug:
          {NeonFS.WebDAV.HealthPlug,
           backend: NeonFS.WebDAV.Backend, core_nodes_fn: fn -> [node1.node] end},
        port: 0,
        ip: :loopback,
        startup_log: false
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

    on_exit(fn ->
      Supervisor.stop(server)
      Application.delete_env(:neonfs_webdav, :core_call_fn)
      WebDAVCoreBridge.cleanup()
    end)

    %{port: port}
  end

  defp base_url(port), do: "http://localhost:#{port}"

  @webdav_methods %{
    propfind: "PROPFIND",
    mkcol: "MKCOL",
    copy: "COPY",
    move: "MOVE"
  }

  defp webdav_request(method, path, port, opts \\ []) do
    body = Keyword.get(opts, :body)
    headers = Keyword.get(opts, :headers, [])
    http_method = Map.get(@webdav_methods, method, method)

    req_opts = [method: http_method, url: "#{base_url(port)}#{path}", headers: headers]
    req_opts = if body, do: Keyword.put(req_opts, :body, body), else: req_opts

    Req.request!(req_opts)
  end

  defp get_header(%{headers: headers}, name) when is_map(headers) do
    case Map.get(headers, name) do
      [value | _] -> value
      nil -> nil
    end
  end

  describe "health endpoint" do
    test "GET /health returns 200 with ok status", %{port: port} do
      resp = Req.get!("#{base_url(port)}/health")

      assert resp.status == 200
      assert resp.body["status"] == "ok"
      assert resp.body["writable"] == true
      assert resp.body["readable"] == true
      assert resp.body["quorum_reachable"] == true
    end
  end

  describe "PROPFIND on root" do
    test "lists volumes at root", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-propfind-root"])

      resp =
        webdav_request(:propfind, "/", port, headers: [{"depth", "1"}])

      assert resp.status == 207
      body = resp.body
      assert is_binary(body)
      assert body =~ "wdv-propfind-root"
    end
  end

  describe "PROPFIND on volume" do
    test "lists files in a volume", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-propfind-vol"])

      WebDAVCoreBridge.call(:write_file, [
        "wdv-propfind-vol",
        "/listed.txt",
        "content",
        []
      ])

      resp =
        webdav_request(:propfind, "/wdv-propfind-vol/", port, headers: [{"depth", "1"}])

      assert resp.status == 207
      assert resp.body =~ "listed.txt"
    end
  end

  describe "PUT + GET round-trip" do
    test "writes and reads back file content", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-putget"])
      content = "Hello from WebDAV integration test"

      put_resp =
        webdav_request(:put, "/wdv-putget/greeting.txt", port,
          body: content,
          headers: [{"content-type", "text/plain"}]
        )

      assert put_resp.status in [200, 201, 204]

      get_resp = webdav_request(:get, "/wdv-putget/greeting.txt", port)

      assert get_resp.status == 200
      assert get_resp.body == content
    end

    test "binary data round-trips correctly", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-binary"])
      binary_data = :crypto.strong_rand_bytes(64 * 1024)

      webdav_request(:put, "/wdv-binary/data.bin", port,
        body: binary_data,
        headers: [{"content-type", "application/octet-stream"}]
      )

      get_resp = webdav_request(:get, "/wdv-binary/data.bin", port)

      assert get_resp.status == 200
      assert get_resp.body == binary_data
    end

    test "nested paths work correctly", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-nested"])

      webdav_request(:put, "/wdv-nested/path/to/deep/file.txt", port, body: "nested content")

      get_resp = webdav_request(:get, "/wdv-nested/path/to/deep/file.txt", port)

      assert get_resp.status == 200
      assert get_resp.body == "nested content"
    end

    test "preserves content type", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-ctype"])

      webdav_request(:put, "/wdv-ctype/page.html", port,
        body: "<html></html>",
        headers: [{"content-type", "text/html"}]
      )

      get_resp = webdav_request(:get, "/wdv-ctype/page.html", port)

      assert get_resp.status == 200
      assert get_header(get_resp, "content-type") =~ "text/html"
    end
  end

  describe "MKCOL" do
    test "creates a directory that accepts file uploads", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-mkcol"])

      resp = webdav_request(:mkcol, "/wdv-mkcol/new-dir/", port)

      assert resp.status == 201

      webdav_request(:put, "/wdv-mkcol/new-dir/child.txt", port, body: "inside dir")

      get_resp = webdav_request(:get, "/wdv-mkcol/new-dir/child.txt", port)
      assert get_resp.status == 200
      assert get_resp.body == "inside dir"
    end

    test "nested MKCOL creates intermediate path", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-mkcol-nest"])

      resp = webdav_request(:mkcol, "/wdv-mkcol-nest/a/b/", port)

      assert resp.status == 201
    end

    test "PROPFIND depth=0 on MKCOL'd directory returns 207", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-mkcol-propfind"])

      mkcol_resp = webdav_request(:mkcol, "/wdv-mkcol-propfind/subdir/", port)
      assert mkcol_resp.status == 201

      resp =
        webdav_request(:propfind, "/wdv-mkcol-propfind/subdir/", port, headers: [{"depth", "0"}])

      assert resp.status == 207
      assert resp.body =~ "subdir"
    end

    test "PROPFIND depth=1 on volume includes MKCOL'd directories", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-mkcol-list"])

      webdav_request(:mkcol, "/wdv-mkcol-list/alpha/", port)

      webdav_request(:put, "/wdv-mkcol-list/file.txt", port, body: "content")

      resp =
        webdav_request(:propfind, "/wdv-mkcol-list/", port, headers: [{"depth", "1"}])

      assert resp.status == 207
      assert resp.body =~ "alpha"
      assert resp.body =~ "file.txt"
    end
  end

  describe "DELETE" do
    test "removes a file", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-delete"])

      webdav_request(:put, "/wdv-delete/doomed.txt", port, body: "bye")

      delete_resp = webdav_request(:delete, "/wdv-delete/doomed.txt", port)

      assert delete_resp.status == 204

      get_resp = webdav_request(:get, "/wdv-delete/doomed.txt", port)

      assert get_resp.status == 404
    end

    test "cannot delete volumes", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-delete-vol"])

      resp = webdav_request(:delete, "/wdv-delete-vol/", port)

      assert resp.status == 403
    end
  end

  describe "COPY" do
    test "copies a file within the same volume", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-copy"])

      webdav_request(:put, "/wdv-copy/original.txt", port, body: "original data")

      copy_resp =
        webdav_request(:copy, "/wdv-copy/original.txt", port,
          headers: [{"destination", "#{base_url(port)}/wdv-copy/copied.txt"}]
        )

      assert copy_resp.status == 201

      get_resp = webdav_request(:get, "/wdv-copy/copied.txt", port)
      assert get_resp.status == 200
      assert get_resp.body == "original data"

      source_resp = webdav_request(:get, "/wdv-copy/original.txt", port)
      assert source_resp.status == 200
      assert source_resp.body == "original data"
    end
  end

  describe "MOVE" do
    test "moves a file within the same volume", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-move"])

      webdav_request(:put, "/wdv-move/old-name.txt", port, body: "moving data")

      move_resp =
        webdav_request(:move, "/wdv-move/old-name.txt", port,
          headers: [{"destination", "#{base_url(port)}/wdv-move/new-name.txt"}]
        )

      assert move_resp.status == 201

      get_resp = webdav_request(:get, "/wdv-move/new-name.txt", port)
      assert get_resp.status == 200
      assert get_resp.body == "moving data"

      old_resp = webdav_request(:get, "/wdv-move/old-name.txt", port)
      assert old_resp.status == 404
    end

    test "moves a file between volumes", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-move-src"])
      WebDAVCoreBridge.call(:create_volume, ["wdv-move-dst"])

      webdav_request(:put, "/wdv-move-src/cross.txt", port, body: "cross-volume")

      move_resp =
        webdav_request(:move, "/wdv-move-src/cross.txt", port,
          headers: [{"destination", "#{base_url(port)}/wdv-move-dst/cross.txt"}]
        )

      assert move_resp.status == 201

      get_resp = webdav_request(:get, "/wdv-move-dst/cross.txt", port)
      assert get_resp.status == 200
      assert get_resp.body == "cross-volume"

      old_resp = webdav_request(:get, "/wdv-move-src/cross.txt", port)
      assert old_resp.status == 404
    end
  end

  describe "cross-node reads" do
    test "data written via WebDAV is readable from another core node", %{
      cluster: cluster,
      port: port
    } do
      WebDAVCoreBridge.call(:create_volume, ["wdv-cross"])
      test_data = :crypto.strong_rand_bytes(32 * 1024)

      webdav_request(:put, "/wdv-cross/cross-node.bin", port, body: test_data)

      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
          "wdv-cross",
          "/cross-node.bin"
        ])

      assert read_data == test_data
    end

    test "data written directly to core is readable via WebDAV", %{
      cluster: cluster,
      port: port
    } do
      WebDAVCoreBridge.call(:create_volume, ["wdv-cross-read"])
      test_data = :crypto.strong_rand_bytes(16 * 1024)

      {:ok, _file} =
        PeerCluster.rpc(cluster, :node3, NeonFS.TestHelpers, :write_file, [
          "wdv-cross-read",
          "/direct-write.bin",
          test_data
        ])

      get_resp = webdav_request(:get, "/wdv-cross-read/direct-write.bin", port)
      assert get_resp.status == 200
      assert get_resp.body == test_data
    end
  end

  describe "overwrite semantics" do
    test "PUT overwrites existing file", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-overwrite"])

      webdav_request(:put, "/wdv-overwrite/file.txt", port, body: "v1")
      webdav_request(:put, "/wdv-overwrite/file.txt", port, body: "v2")

      get_resp = webdav_request(:get, "/wdv-overwrite/file.txt", port)
      assert get_resp.status == 200
      assert get_resp.body == "v2"
    end
  end

  describe "error cases" do
    test "GET on non-existent file returns 404", %{port: port} do
      WebDAVCoreBridge.call(:create_volume, ["wdv-errors"])

      resp = webdav_request(:get, "/wdv-errors/no-such-file.txt", port)

      assert resp.status == 404
    end

    test "GET on non-existent volume returns 404", %{port: port} do
      resp = webdav_request(:get, "/wdv-no-such-volume/file.txt", port)

      assert resp.status == 404
    end

    test "MKCOL on non-existent volume returns 409", %{port: port} do
      resp = webdav_request(:mkcol, "/wdv-no-vol/dir/", port)

      assert resp.status == 409
    end
  end
end
