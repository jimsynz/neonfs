defmodule WebdavServer.IntegrationTest do
  @moduledoc """
  Integration tests exercising the full HTTP stack via Bandit + Req.

  These tests complement the Plug.Test-based tests by verifying that
  requests work through a real TCP connection.
  """
  use ExUnit.Case

  alias WebdavServer.LockStore
  alias WebdavServer.Test.{MemoryBackend, TestServer, WebdavClient}

  setup do
    MemoryBackend.start()
    LockStore.ETS.reset()
    {:ok, port} = TestServer.start()
    {:ok, port: port}
  end

  describe "OPTIONS" do
    test "returns DAV compliance headers", %{port: port} do
      resp = WebdavClient.options(port, "/")

      assert resp.status == 200
      assert header(resp, "dav") == "1, 2"
      assert header(resp, "ms-author-via") == "DAV"
      assert header(resp, "allow") =~ "PROPFIND"
    end
  end

  describe "file lifecycle" do
    test "PUT → GET → DELETE round-trip", %{port: port} do
      put_resp = WebdavClient.put(port, "/hello.txt", "hello world", content_type: "text/plain")
      assert put_resp.status == 201

      get_resp = WebdavClient.get(port, "/hello.txt")
      assert get_resp.status == 200
      assert get_resp.body == "hello world"
      assert header(get_resp, "content-type") == "text/plain"

      del_resp = WebdavClient.delete(port, "/hello.txt")
      assert del_resp.status == 204

      get_resp2 = WebdavClient.get(port, "/hello.txt")
      assert get_resp2.status == 404
    end

    test "PUT overwrites existing file", %{port: port} do
      WebdavClient.put(port, "/file.txt", "v1")
      resp = WebdavClient.put(port, "/file.txt", "v2")
      assert resp.status == 204

      get_resp = WebdavClient.get(port, "/file.txt")
      assert get_resp.body == "v2"
    end

    test "HEAD returns headers without body", %{port: port} do
      WebdavClient.put(port, "/file.txt", "content", content_type: "text/plain")

      resp = WebdavClient.head(port, "/file.txt")
      assert resp.status == 200
      assert resp.body == ""
      assert header(resp, "content-type") == "text/plain"
    end

    test "GET with Range header returns partial content", %{port: port} do
      WebdavClient.put(port, "/range.txt", "0123456789ABCDEF")

      resp =
        WebdavClient.get(port, "/range.txt", headers: [{"range", "bytes=5-9"}])

      assert resp.status == 206
      assert resp.body == "56789"
      assert header(resp, "content-range") =~ "bytes 5-9/"
      assert header(resp, "accept-ranges") == "bytes"
    end

    test "GET without Range header returns full content", %{port: port} do
      WebdavClient.put(port, "/full.txt", "complete")

      resp = WebdavClient.get(port, "/full.txt")
      assert resp.status == 200
      assert resp.body == "complete"
      assert header(resp, "accept-ranges") == "bytes"
    end
  end

  describe "collections" do
    test "MKCOL creates a collection", %{port: port} do
      resp = WebdavClient.mkcol(port, "/mydir")
      assert resp.status == 201
    end

    test "MKCOL fails when parent missing", %{port: port} do
      resp = WebdavClient.mkcol(port, "/a/b")
      assert resp.status == 409
    end

    test "files inside collections", %{port: port} do
      WebdavClient.mkcol(port, "/docs")
      WebdavClient.put(port, "/docs/readme.txt", "read me")

      resp = WebdavClient.get(port, "/docs/readme.txt")
      assert resp.status == 200
      assert resp.body == "read me"
    end

    test "DELETE removes collection recursively", %{port: port} do
      WebdavClient.mkcol(port, "/dir")
      WebdavClient.put(port, "/dir/a.txt", "a")
      WebdavClient.put(port, "/dir/b.txt", "b")

      resp = WebdavClient.delete(port, "/dir")
      assert resp.status == 204

      assert WebdavClient.get(port, "/dir/a.txt").status == 404
    end
  end

  describe "COPY and MOVE" do
    test "COPY duplicates a file", %{port: port} do
      WebdavClient.put(port, "/src.txt", "data")

      resp = WebdavClient.copy(port, "/src.txt", "/dst.txt")
      assert resp.status == 201

      assert WebdavClient.get(port, "/dst.txt").body == "data"
      assert WebdavClient.get(port, "/src.txt").status == 200
    end

    test "MOVE relocates a file", %{port: port} do
      WebdavClient.put(port, "/old.txt", "data")

      resp = WebdavClient.move(port, "/old.txt", "/new.txt")
      assert resp.status == 201

      assert WebdavClient.get(port, "/new.txt").body == "data"
      assert WebdavClient.get(port, "/old.txt").status == 404
    end

    test "COPY with overwrite=F returns 412", %{port: port} do
      WebdavClient.put(port, "/a.txt", "a")
      WebdavClient.put(port, "/b.txt", "b")

      resp = WebdavClient.copy(port, "/a.txt", "/b.txt", overwrite: false)
      assert resp.status == 412
    end

    test "COPY collection recursively", %{port: port} do
      WebdavClient.mkcol(port, "/src")
      WebdavClient.put(port, "/src/file.txt", "content")

      resp = WebdavClient.copy(port, "/src", "/dst")
      assert resp.status == 201

      assert WebdavClient.get(port, "/dst/file.txt").body == "content"
    end
  end

  describe "PROPFIND" do
    test "allprop returns standard properties", %{port: port} do
      WebdavClient.put(port, "/test.txt", "hello", content_type: "text/plain")

      resp = WebdavClient.propfind(port, "/test.txt", depth: "0")
      assert resp.status == 207

      body = resp.body
      assert body =~ "multistatus"
      assert body =~ "getcontentlength"
      assert body =~ "getetag"
      assert body =~ "resourcetype"
    end

    test "depth 1 lists collection members", %{port: port} do
      WebdavClient.mkcol(port, "/dir")
      WebdavClient.put(port, "/dir/a.txt", "a")
      WebdavClient.put(port, "/dir/b.txt", "b")

      resp = WebdavClient.propfind(port, "/dir", depth: "1")
      assert resp.status == 207

      body = resp.body
      assert body =~ "/dir"
      assert body =~ "a.txt"
      assert body =~ "b.txt"
    end

    test "collection shows resourcetype collection", %{port: port} do
      WebdavClient.mkcol(port, "/col")

      resp = WebdavClient.propfind(port, "/col", depth: "0")
      assert resp.body =~ "collection"
    end

    test "specific property request", %{port: port} do
      WebdavClient.put(port, "/test.txt", "data")

      body = """
      <?xml version="1.0" encoding="utf-8"?>
      <D:propfind xmlns:D="DAV:">
        <D:prop>
          <D:getcontentlength/>
          <D:nonexistent/>
        </D:prop>
      </D:propfind>
      """

      resp = WebdavClient.propfind(port, "/test.txt", depth: "0", body: body)
      assert resp.status == 207
      assert resp.body =~ "getcontentlength"
      assert resp.body =~ "404 Not Found"
    end

    test "depth infinity is rejected by default", %{port: port} do
      resp = WebdavClient.propfind(port, "/", depth: "infinity")
      assert resp.status == 403
    end
  end

  describe "PROPPATCH" do
    test "sets a property", %{port: port} do
      WebdavClient.put(port, "/test.txt", "data")

      body = """
      <?xml version="1.0" encoding="utf-8"?>
      <D:propertyupdate xmlns:D="DAV:">
        <D:set>
          <D:prop>
            <D:displayname>My File</D:displayname>
          </D:prop>
        </D:set>
      </D:propertyupdate>
      """

      resp = WebdavClient.proppatch(port, "/test.txt", body)
      assert resp.status == 207
      assert resp.body =~ "200 OK"
    end
  end

  describe "LOCK and UNLOCK" do
    test "lock and unlock lifecycle", %{port: port} do
      WebdavClient.put(port, "/test.txt", "data")

      lock_resp = WebdavClient.lock(port, "/test.txt")
      assert lock_resp.status == 200
      assert lock_resp.body =~ "lockdiscovery"
      assert lock_resp.body =~ "exclusive"

      lock_token = header(lock_resp, "lock-token")
      assert lock_token =~ "opaquelocktoken:"

      unlock_resp = WebdavClient.unlock(port, "/test.txt", lock_token)
      assert unlock_resp.status == 204
    end

    test "locked resource rejects PUT without token", %{port: port} do
      WebdavClient.put(port, "/test.txt", "data")

      lock_resp = WebdavClient.lock(port, "/test.txt")
      assert lock_resp.status == 200

      put_resp = WebdavClient.put(port, "/test.txt", "new data")
      assert put_resp.status == 423
    end

    test "exclusive lock prevents second lock", %{port: port} do
      WebdavClient.put(port, "/test.txt", "data")

      first = WebdavClient.lock(port, "/test.txt")
      assert first.status == 200

      second = WebdavClient.lock(port, "/test.txt")
      assert second.status == 423
    end

    test "lock creates empty file if resource missing", %{port: port} do
      resp = WebdavClient.lock(port, "/new-file.txt")
      assert resp.status == 201
    end
  end

  describe "error cases" do
    test "unsupported method returns 405", %{port: port} do
      resp =
        Req.request!(base_url: "http://localhost:#{port}", url: "/", method: :post, body: "x")

      assert resp.status == 405
    end

    test "GET on collection returns 405", %{port: port} do
      resp = WebdavClient.get(port, "/")
      assert resp.status == 405
    end
  end

  defp header(resp, name) do
    case resp.headers[name] do
      [value] -> value
      values when is_list(values) -> Enum.join(values, ", ")
      nil -> nil
    end
  end
end
