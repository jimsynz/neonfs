defmodule NeonFS.S3.IntegrationTest do
  @moduledoc """
  Integration tests for the S3 API against a real multi-node NeonFS cluster.

  Starts a 3-node core cluster via PeerCluster, then launches a Bandit HTTP
  server running the S3 plug on the test runner. Uses ExAws.S3 as an external
  client to verify end-to-end S3 protocol compatibility.

  Credential lookups go through real NeonFS.Core.S3CredentialManager on the
  core nodes via RPC.
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Client.{Connection, CostFunction, Discovery}
  alias NeonFS.S3.IntegrationTest.CoreBridge, as: S3CoreBridge
  alias NeonFS.S3.MultipartStore

  @moduletag timeout: 180_000
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    init_multi_node_cluster(cluster, name: "s3-test")

    node1 = PeerCluster.get_node!(cluster, :node1)
    S3CoreBridge.store_core_node(node1.node)
    {_access_key, _secret_key} = S3CoreBridge.create_test_credential(node1.node)

    Application.put_env(:neonfs_s3, :core_call_fn, &S3CoreBridge.call/2)

    # The test runner has no TLS data-plane pool to the peer core
    # nodes, so the real `ChunkWriter.write_file_stream/4` can't
    # ship chunks. Stash the bytes in an ETS table keyed by sha256
    # and concatenate on commit — same semantics as production's
    # ship → commit split, executed locally.
    Application.put_env(:neonfs_s3, :ship_chunks_fn, &S3CoreBridge.ship_chunks/3)
    Application.put_env(:neonfs_s3, :commit_refs_fn, &S3CoreBridge.commit_refs/4)

    # Start client infrastructure on the test runner so NeonFS.Client.ChunkReader
    # can resolve a core node via Router → CostFunction for object GETs.
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

    {server, port} =
      start_bandit_with_retry(
        plug:
          {NeonFS.S3.HealthPlug,
           backend: NeonFS.S3.Backend, core_nodes_fn: fn -> [node1.node] end},
        port: 0,
        ip: :loopback,
        startup_log: false
      )

    {:ok, multipart_store} = MultipartStore.start_link([])

    config =
      [
        access_key_id: S3CoreBridge.test_access_key(),
        secret_access_key: S3CoreBridge.test_secret_key(),
        scheme: "http://",
        host: "localhost",
        port: port,
        region: "neonfs",
        s3: [
          scheme: "http://",
          host: "localhost",
          port: port,
          region: "neonfs"
        ]
      ]

    on_exit(fn ->
      if Process.alive?(multipart_store), do: GenServer.stop(multipart_store)
      Supervisor.stop(server)
      Application.delete_env(:neonfs_s3, :core_call_fn)
      Application.delete_env(:neonfs_s3, :ship_chunks_fn)
      Application.delete_env(:neonfs_s3, :commit_refs_fn)
      S3CoreBridge.cleanup()
    end)

    %{config: config}
  end

  defp request!(op, config) do
    case ExAws.request(op, config) do
      {:ok, result} -> result
      {:error, reason} -> raise "ExAws request failed: #{inspect(reason)}"
    end
  end

  defp get_header(%{headers: headers}, name) when is_list(headers) do
    Enum.find_value(headers, fn
      {^name, value} -> value
      _ -> nil
    end)
  end

  defp get_header(%{headers: headers}, name) when is_map(headers) do
    Map.get(headers, name)
  end

  defp ensure_bucket(name, config) do
    case ExAws.S3.put_bucket(name, "neonfs") |> ExAws.request(config) do
      {:ok, _} -> :ok
      {:error, {:http_error, 409, _}} -> :ok
    end
  end

  # Each test uses unique bucket names to avoid collisions in shared cluster.

  describe "bucket operations" do
    test "create, head, list, and delete a bucket", %{config: config} do
      ExAws.S3.put_bucket("s3t-bucket-crud", "neonfs") |> request!(config)

      assert %{status_code: 200} =
               ExAws.S3.head_bucket("s3t-bucket-crud") |> request!(config)

      result = ExAws.S3.list_buckets() |> request!(config)
      names = Enum.map(result.body.buckets, & &1.name)
      assert "s3t-bucket-crud" in names

      assert %{status_code: 204} =
               ExAws.S3.delete_bucket("s3t-bucket-crud") |> request!(config)

      assert {:error, {:http_error, 404, _}} =
               ExAws.S3.head_bucket("s3t-bucket-crud") |> ExAws.request(config)
    end

    test "creating duplicate bucket returns 409", %{config: config} do
      ExAws.S3.put_bucket("s3t-bucket-dup", "neonfs") |> request!(config)

      assert {:error, {:http_error, 409, _}} =
               ExAws.S3.put_bucket("s3t-bucket-dup", "neonfs") |> ExAws.request(config)
    end

    test "head non-existent bucket returns 404", %{config: config} do
      assert {:error, {:http_error, 404, _}} =
               ExAws.S3.head_bucket("s3t-no-such-bucket") |> ExAws.request(config)
    end
  end

  describe "object operations" do
    setup %{config: config} do
      ensure_bucket("s3t-obj", config)
      :ok
    end

    test "put and get object round-trips content", %{config: config} do
      body = "hello from integration test"

      put_result =
        ExAws.S3.put_object("s3t-obj", "greeting.txt", body)
        |> request!(config)

      etag = get_header(put_result, "etag")
      assert etag =~ ~r/^"[a-f0-9]+"$/

      get_result = ExAws.S3.get_object("s3t-obj", "greeting.txt") |> request!(config)
      assert get_result.body == body
      assert get_header(get_result, "etag") == etag
    end

    test "head object returns content-length and etag", %{config: config} do
      ExAws.S3.put_object("s3t-obj", "head-test.bin", "12345") |> request!(config)

      result = ExAws.S3.head_object("s3t-obj", "head-test.bin") |> request!(config)
      assert get_header(result, "content-length") == "5"
      assert get_header(result, "etag") =~ ~r/^"[a-f0-9]+"$/
    end

    test "delete object removes it", %{config: config} do
      ExAws.S3.put_object("s3t-obj", "to-delete.txt", "bye") |> request!(config)

      assert %{status_code: 204} =
               ExAws.S3.delete_object("s3t-obj", "to-delete.txt") |> request!(config)

      assert {:error, {:http_error, 404, _}} =
               ExAws.S3.get_object("s3t-obj", "to-delete.txt") |> ExAws.request(config)
    end

    test "get non-existent object returns 404", %{config: config} do
      assert {:error, {:http_error, 404, _}} =
               ExAws.S3.get_object("s3t-obj", "nope.txt") |> ExAws.request(config)
    end

    test "overwrite existing object updates content", %{config: config, cluster: cluster} do
      ExAws.S3.put_object("s3t-obj", "overwrite.txt", "v1") |> request!(config)
      ExAws.S3.put_object("s3t-obj", "overwrite.txt", "v2") |> request!(config)
      :ok = wait_for_ra_apply_consensus(cluster)

      result = ExAws.S3.get_object("s3t-obj", "overwrite.txt") |> request!(config)
      assert result.body == "v2"
    end

    test "nested key paths work correctly", %{config: config, cluster: cluster} do
      ExAws.S3.put_object("s3t-obj", "path/to/nested.txt", "nested content")
      |> request!(config)

      :ok = wait_for_ra_apply_consensus(cluster)

      result = ExAws.S3.get_object("s3t-obj", "path/to/nested.txt") |> request!(config)
      assert result.body == "nested content"
    end

    test "binary data round-trips correctly", %{config: config, cluster: cluster} do
      binary_data = :crypto.strong_rand_bytes(64 * 1024)

      ExAws.S3.put_object("s3t-obj", "binary.bin", binary_data) |> request!(config)
      :ok = wait_for_ra_apply_consensus(cluster)

      result = ExAws.S3.get_object("s3t-obj", "binary.bin") |> request!(config)
      assert result.body == binary_data
    end
  end

  describe "copy object" do
    setup %{config: config} do
      ensure_bucket("s3t-copy-src", config)
      ensure_bucket("s3t-copy-dst", config)
      ExAws.S3.put_object("s3t-copy-src", "original.txt", "original data") |> request!(config)
      :ok
    end

    test "rejects cross-bucket copy with NotImplemented", %{config: config} do
      assert {:error, {:http_error, 501, _body}} =
               ExAws.S3.put_object_copy(
                 "s3t-copy-dst",
                 "copied.txt",
                 "s3t-copy-src",
                 "original.txt"
               )
               |> ExAws.request(config)

      # Destination is not created
      assert {:error, {:http_error, 404, _}} =
               ExAws.S3.get_object("s3t-copy-dst", "copied.txt") |> ExAws.request(config)

      # Source is untouched
      source = ExAws.S3.get_object("s3t-copy-src", "original.txt") |> request!(config)
      assert source.body == "original data"
    end

    test "copies object within same bucket", %{config: config} do
      ExAws.S3.put_object_copy("s3t-copy-src", "copy.txt", "s3t-copy-src", "original.txt")
      |> request!(config)

      result = ExAws.S3.get_object("s3t-copy-src", "copy.txt") |> request!(config)
      assert result.body == "original data"
    end
  end

  describe "batch delete" do
    setup %{config: config} do
      ensure_bucket("s3t-batch", config)
      ExAws.S3.put_object("s3t-batch", "x.txt", "x") |> request!(config)
      ExAws.S3.put_object("s3t-batch", "y.txt", "y") |> request!(config)
      ExAws.S3.put_object("s3t-batch", "z.txt", "z") |> request!(config)
      :ok
    end

    test "deletes multiple objects, leaves others intact", %{config: config} do
      result =
        ExAws.S3.delete_multiple_objects("s3t-batch", ["x.txt", "y.txt"])
        |> request!(config)

      assert result.status_code == 200

      z_result = ExAws.S3.get_object("s3t-batch", "z.txt") |> request!(config)
      assert z_result.body == "z"

      assert {:error, {:http_error, 404, _}} =
               ExAws.S3.get_object("s3t-batch", "x.txt") |> ExAws.request(config)

      assert {:error, {:http_error, 404, _}} =
               ExAws.S3.get_object("s3t-batch", "y.txt") |> ExAws.request(config)
    end
  end

  describe "multipart upload" do
    setup %{config: config} do
      ensure_bucket("s3t-mp", config)
      :ok
    end

    test "full multipart upload lifecycle", %{config: config} do
      init_result =
        ExAws.S3.initiate_multipart_upload("s3t-mp", "large.bin") |> request!(config)

      assert init_result.body.bucket == "s3t-mp"
      assert init_result.body.key == "large.bin"
      upload_id = init_result.body.upload_id
      assert is_binary(upload_id) and upload_id != ""

      part1 =
        ExAws.S3.upload_part("s3t-mp", "large.bin", upload_id, 1, "part-one-")
        |> request!(config)

      etag1 = get_header(part1, "etag")

      part2 =
        ExAws.S3.upload_part("s3t-mp", "large.bin", upload_id, 2, "part-two")
        |> request!(config)

      etag2 = get_header(part2, "etag")

      complete_result =
        ExAws.S3.complete_multipart_upload(
          "s3t-mp",
          "large.bin",
          upload_id,
          %{1 => etag1, 2 => etag2}
        )
        |> request!(config)

      assert complete_result.body.bucket == "s3t-mp"
      assert complete_result.body.key == "large.bin"

      get_result = ExAws.S3.get_object("s3t-mp", "large.bin") |> request!(config)
      assert get_result.body == "part-one-part-two"
    end

    test "abort multipart upload", %{config: config} do
      init_result =
        ExAws.S3.initiate_multipart_upload("s3t-mp", "abort.bin") |> request!(config)

      upload_id = init_result.body.upload_id

      assert %{status_code: 204} =
               ExAws.S3.abort_multipart_upload("s3t-mp", "abort.bin", upload_id)
               |> request!(config)
    end
  end

  describe "cross-node reads" do
    setup %{config: config} do
      ensure_bucket("s3t-cross", config)
      :ok
    end

    # `Volume.MetadataWriter` only fans out the volume's *root segment*
    # via `ChunkReplicator`; index-tree chunks are written through a
    # single local `store_handle`. Cross-node metadata reads land on a
    # peer that has the root pointer but not the tree leaves. Tracked
    # in #903.
    @tag :skip
    test "data written via S3 is readable from another core node", %{
      cluster: cluster,
      config: config
    } do
      test_data = :crypto.strong_rand_bytes(32 * 1024)

      ExAws.S3.put_object("s3t-cross", "cross-node.bin", test_data) |> request!(config)

      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
          "s3t-cross",
          "/cross-node.bin"
        ])

      assert read_data == test_data
    end

    @tag :skip
    test "data written directly to core is readable via S3", %{
      cluster: cluster,
      config: config
    } do
      test_data = :crypto.strong_rand_bytes(16 * 1024)

      {:ok, _file} =
        PeerCluster.rpc(cluster, :node3, NeonFS.TestHelpers, :write_file_from_binary, [
          "s3t-cross",
          "/direct-write.bin",
          test_data
        ])

      get_result = ExAws.S3.get_object("s3t-cross", "direct-write.bin") |> request!(config)
      assert get_result.body == test_data
    end
  end

  describe "authentication" do
    test "bad credentials return 403", %{config: config} do
      bad_config = Keyword.merge(config, access_key_id: "BAD", secret_access_key: "WRONG")

      assert {:error, {:http_error, 403, _}} =
               ExAws.S3.list_buckets() |> ExAws.request(bad_config)
    end
  end

  describe "health endpoint" do
    test "GET /health returns 200 with ok status", %{config: config} do
      port = Keyword.get(config, :port)

      {:ok, {_status, _headers, body}} =
        :httpc.request(:get, {~c"http://localhost:#{port}/health", []}, [], [])

      decoded = Jason.decode!(body)
      assert decoded["status"] == "ok"
      assert decoded["writable"] == true
      assert decoded["readable"] == true
    end
  end
end
