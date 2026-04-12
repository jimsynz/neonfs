defmodule NeonFS.WebDAV.HealthPlugTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias NeonFS.WebDAV.HealthPlug

  defp healthy_fn, do: fn -> [:core@node1] end
  defp unhealthy_fn, do: fn -> [] end

  defp call_health(method, path, core_nodes_fn) do
    opts =
      HealthPlug.init(
        backend: NeonFS.WebDAV.HealthPlugTest.StubBackend,
        core_nodes_fn: core_nodes_fn
      )

    conn(method, path)
    |> HealthPlug.call(opts)
  end

  describe "GET /health" do
    test "returns 200 with ok status when cluster is healthy" do
      conn = call_health(:get, "/health", healthy_fn())

      assert conn.status == 200
      assert get_resp_header(conn, "content-type") =~ "application/json"

      body = Jason.decode!(conn.resp_body)
      assert body["status"] == "ok"
      assert body["writable"] == true
      assert body["readable"] == true
      assert body["reason"] == nil
      assert body["quorum_reachable"] == true
    end

    test "returns 503 with unavailable status when no core nodes" do
      conn = call_health(:get, "/health", unhealthy_fn())

      assert conn.status == 503

      body = Jason.decode!(conn.resp_body)
      assert body["status"] == "unavailable"
      assert body["writable"] == false
      assert body["readable"] == false
      assert body["reason"] == "no-core-nodes"
      assert body["quorum_reachable"] == false
    end
  end

  describe "degraded mode — write operations" do
    test "PUT returns 503 with retry-after and status header" do
      conn = call_health(:put, "/some/file.txt", unhealthy_fn())

      assert conn.status == 503
      assert get_resp_header(conn, "retry-after") == "30"
      assert get_resp_header(conn, "x-neonfs-status") == "no-core-nodes"
    end

    test "DELETE returns 503 when degraded" do
      conn = call_health(:delete, "/some/file.txt", unhealthy_fn())

      assert conn.status == 503
      assert get_resp_header(conn, "x-neonfs-status") == "no-core-nodes"
    end

    test "MKCOL returns 503 when degraded" do
      conn = call_health("MKCOL", "/some/dir", unhealthy_fn())

      assert conn.status == 503
      assert get_resp_header(conn, "x-neonfs-status") == "no-core-nodes"
    end

    test "COPY returns 503 when degraded" do
      conn = call_health("COPY", "/some/file.txt", unhealthy_fn())

      assert conn.status == 503
      assert get_resp_header(conn, "x-neonfs-status") == "no-core-nodes"
    end

    test "MOVE returns 503 when degraded" do
      conn = call_health("MOVE", "/some/file.txt", unhealthy_fn())

      assert conn.status == 503
      assert get_resp_header(conn, "x-neonfs-status") == "no-core-nodes"
    end
  end

  describe "degraded mode — read operations" do
    test "GET delegates to inner plug with x-neonfs-status header" do
      conn = call_health(:get, "/some/file.txt", unhealthy_fn())

      assert get_resp_header(conn, "x-neonfs-status") == "no-core-nodes"
      assert conn.status != 503
    end

    test "PROPFIND delegates to inner plug with x-neonfs-status header" do
      conn = call_health("PROPFIND", "/some/dir", unhealthy_fn())

      assert get_resp_header(conn, "x-neonfs-status") == "no-core-nodes"
      assert conn.status != 503
    end
  end

  describe "healthy mode" do
    test "requests delegate without x-neonfs-status header" do
      conn = call_health(:get, "/some/file.txt", healthy_fn())

      assert get_resp_header(conn, "x-neonfs-status") == nil
    end

    test "write operations delegate when healthy" do
      conn = call_health(:put, "/some/file.txt", healthy_fn())

      assert conn.status != 503
      assert get_resp_header(conn, "retry-after") == nil
    end
  end

  defp get_resp_header(conn, key) do
    case Enum.find(conn.resp_headers, fn {k, _} -> k == key end) do
      {_, value} -> value
      nil -> nil
    end
  end

  defmodule StubBackend do
    @moduledoc false
    @behaviour WebdavServer.Backend

    @impl true
    def authenticate(_conn), do: {:ok, %{user: "stub"}}

    @impl true
    def resolve(_auth, _path) do
      {:ok,
       %WebdavServer.Resource{
         path: ["stub"],
         type: :file,
         etag: "stub-etag",
         content_type: "text/plain",
         content_length: 4,
         last_modified: DateTime.utc_now()
       }}
    end

    @impl true
    def get_properties(_auth, _resource, props) do
      Enum.map(props, fn prop -> {prop, {:error, :not_found}} end)
    end

    @impl true
    def set_properties(_auth, _resource, _ops), do: :ok

    @impl true
    def get_content(_auth, _resource, _opts), do: {:ok, "stub"}

    @impl true
    def put_content(_auth, path, _body, _opts) do
      {:ok, %WebdavServer.Resource{path: path, type: :file, etag: "new-etag", content_length: 4}}
    end

    @impl true
    def delete(_auth, _resource), do: :ok

    @impl true
    def copy(_auth, _resource, _dest, _overwrite?), do: {:ok, :created}

    @impl true
    def move(_auth, _resource, _dest, _overwrite?), do: {:ok, :created}

    @impl true
    def create_collection(_auth, _path), do: :ok

    @impl true
    def get_members(_auth, _resource), do: {:ok, []}
  end
end
