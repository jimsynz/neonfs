defmodule NeonFS.S3.HealthPlugTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias NeonFS.S3.HealthPlug

  defp healthy_fn, do: fn -> [:core@node1] end
  defp unhealthy_fn, do: fn -> [] end

  defp call_health(method, path, core_nodes_fn) do
    opts =
      HealthPlug.init(
        backend: NeonFS.S3.HealthPlugTest.StubBackend,
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
      conn = call_health(:put, "/my-bucket/my-key", unhealthy_fn())

      assert conn.status == 503
      assert get_resp_header(conn, "retry-after") == "30"
      assert get_resp_header(conn, "x-neonfs-status") == "no-core-nodes"
    end

    test "POST returns 503 when degraded" do
      conn = call_health(:post, "/my-bucket/my-key?uploads", unhealthy_fn())

      assert conn.status == 503
      assert get_resp_header(conn, "x-neonfs-status") == "no-core-nodes"
    end

    test "DELETE returns 503 when degraded" do
      conn = call_health(:delete, "/my-bucket/my-key", unhealthy_fn())

      assert conn.status == 503
      assert get_resp_header(conn, "x-neonfs-status") == "no-core-nodes"
    end
  end

  describe "degraded mode — read operations" do
    test "GET delegates to inner plug with x-neonfs-status header" do
      conn = call_health(:get, "/my-bucket/my-key", unhealthy_fn())

      assert get_resp_header(conn, "x-neonfs-status") == "no-core-nodes"
      # Request reaches Firkin.Plug (403 from SigV4 auth proves delegation)
      assert conn.status != 503
    end

    test "HEAD delegates to inner plug with x-neonfs-status header" do
      conn = call_health(:head, "/my-bucket/my-key", unhealthy_fn())

      assert get_resp_header(conn, "x-neonfs-status") == "no-core-nodes"
      assert conn.status != 503
    end
  end

  describe "healthy mode" do
    test "requests delegate without x-neonfs-status header" do
      conn = call_health(:get, "/my-bucket/my-key", healthy_fn())

      # Reaches Firkin.Plug without degraded headers
      assert get_resp_header(conn, "x-neonfs-status") == nil
    end

    test "write operations delegate when healthy" do
      conn = call_health(:put, "/my-bucket/my-key", healthy_fn())

      # Not blocked — reaches Firkin.Plug (any non-503 status)
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
    @behaviour Firkin.Backend

    @impl true
    def lookup_credential(_access_key_id) do
      {:ok,
       %Firkin.Credential{
         access_key_id: "stub",
         secret_access_key: "stub-secret",
         identity: %{}
       }}
    end

    @impl true
    def list_buckets(_ctx), do: {:ok, []}

    @impl true
    def create_bucket(_ctx, _bucket), do: :ok

    @impl true
    def delete_bucket(_ctx, _bucket), do: :ok

    @impl true
    def head_bucket(_ctx, _bucket), do: :ok

    @impl true
    def get_bucket_location(_ctx, _bucket), do: {:ok, "neonfs"}

    @impl true
    def get_object(_ctx, _bucket, _key, _opts) do
      {:ok,
       %Firkin.Object{
         body: "data",
         content_type: "application/octet-stream",
         content_length: 4,
         etag: "etag",
         last_modified: DateTime.utc_now(),
         metadata: %{}
       }}
    end

    @impl true
    def put_object(_ctx, _bucket, _key, _body, _opts), do: {:ok, "etag"}

    @impl true
    def delete_object(_ctx, _bucket, _key), do: :ok

    @impl true
    def delete_objects(_ctx, _bucket, _keys),
      do: {:ok, %Firkin.DeleteResult{deleted: [], errors: []}}

    @impl true
    def head_object(_ctx, _bucket, _key) do
      {:ok,
       %Firkin.ObjectMeta{
         key: "key",
         etag: "etag",
         size: 4,
         last_modified: DateTime.utc_now(),
         content_type: "application/octet-stream"
       }}
    end

    @impl true
    def list_objects_v2(_ctx, _bucket, _opts) do
      {:ok,
       %Firkin.ListResult{
         name: "bucket",
         prefix: nil,
         delimiter: nil,
         contents: [],
         common_prefixes: [],
         key_count: 0,
         max_keys: 1000,
         is_truncated: false
       }}
    end

    @impl true
    def copy_object(_ctx, _dest_bucket, _dest_key, _src_bucket, _src_key) do
      {:ok, %Firkin.CopyResult{etag: "etag", last_modified: DateTime.utc_now()}}
    end
  end
end
