defmodule NeonFS.S3.MultipartCrossNodeTest do
  @moduledoc """
  Cluster-shared multipart upload state (#1177): bookkeeping lives in
  the Ra-backed cluster KV store, so a load balancer can route each
  request of a multipart upload to a different S3 node.

  ## Cluster shape

    * `node1`: `:neonfs_core` (Ra member, KV store, chunk holder).
    * `node2`, `node3`: `:neonfs_s3` (real S3 apps on per-peer ports).

  ## Coverage

  Drives a real SigV4 HTTP multipart upload round-robin across the two
  S3 peers: `CreateMultipartUpload` on node2, `UploadPart`s alternating
  between node3 and node2, `ListParts` observed from node2 for parts
  uploaded via node3, `CompleteMultipartUpload` on node3, and a final
  `GetObject` through node2 verifying the assembled bytes.
  """

  use ExUnit.Case, async: false

  alias NeonFS.TestSupport.{ClusterCase, PeerCluster}

  @moduletag timeout: 300_000
  @moduletag :integration

  @bucket "mp-xnode"
  @key "round-robin.bin"

  test "multipart upload round-robined across two S3 nodes succeeds" do
    cluster =
      PeerCluster.start_cluster!(3,
        roles: %{
          node1: [:neonfs_core],
          node2: [:neonfs_s3],
          node3: [:neonfs_s3]
        }
      )

    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    PeerCluster.connect_nodes(cluster)
    :ok = ClusterCase.init_mixed_role_cluster(cluster, name: "s3-mp-xnode")

    {:ok, credential} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.CredentialManager, :create, [
        %{user: "mp-xnode-test"}
      ])

    config_a = exaws_config(cluster, :node2, credential)
    config_b = exaws_config(cluster, :node3, credential)

    # The S3 peers' credential lookups and volume creation go through
    # their own client Router to core; retry the first request until
    # that path is warm on a loaded runner.
    ensure_bucket(@bucket, config_a)

    # Initiate on node2.
    init_result = ExAws.S3.initiate_multipart_upload(@bucket, @key) |> request!(config_a)
    upload_id = init_result.body.upload_id
    assert is_binary(upload_id) and upload_id != ""

    # Upload parts alternating across the two S3 nodes.
    parts = [
      {1, String.duplicate("alpha-", 1024), config_b},
      {2, String.duplicate("bravo-", 1024), config_a},
      {3, String.duplicate("charlie-", 1024), config_b}
    ]

    etags =
      for {part_number, body, config} <- parts do
        result =
          ExAws.S3.upload_part(@bucket, @key, upload_id, part_number, body) |> request!(config)

        {part_number, get_header(result, "etag")}
      end

    # node2 must see all three parts, including the two uploaded via
    # node3 — the bookkeeping is cluster state, not node state.
    list_result = ExAws.S3.list_parts(@bucket, @key, upload_id) |> request!(config_a)
    listed_numbers = list_result.body.parts |> Enum.map(&String.to_integer(&1.part_number))
    assert Enum.sort(listed_numbers) == [1, 2, 3]

    # Complete on node3 — a node that never saw the initiate.
    complete_result =
      ExAws.S3.complete_multipart_upload(@bucket, @key, upload_id, Map.new(etags))
      |> request!(config_b)

    assert complete_result.body.bucket == @bucket
    assert complete_result.body.key == @key

    expected = parts |> Enum.map(fn {_n, body, _c} -> body end) |> IO.iodata_to_binary()

    get_result = ExAws.S3.get_object(@bucket, @key) |> request!(config_a)
    assert get_result.body == expected

    # The upload is gone from cluster state once completed — observed
    # from the node that initiated it.
    assert {:error, {:http_error, 404, _}} =
             ExAws.S3.list_parts(@bucket, @key, upload_id) |> ExAws.request(config_a)
  end

  defp exaws_config(cluster, peer, credential) do
    port = PeerCluster.get_node!(cluster, peer).interface_ports.s3

    [
      access_key_id: credential.access_key_id,
      secret_access_key: credential.secret_access_key,
      scheme: "http://",
      host: "127.0.0.1",
      port: port,
      region: "neonfs",
      s3: [scheme: "http://", host: "127.0.0.1", port: port, region: "neonfs"]
    ]
  end

  defp ensure_bucket(name, config) do
    deadline = System.monotonic_time(:millisecond) + 60_000
    do_ensure_bucket(name, config, deadline)
  end

  defp do_ensure_bucket(name, config, deadline) do
    case ExAws.S3.put_bucket(name, "neonfs") |> ExAws.request(config) do
      {:ok, _} ->
        :ok

      {:error, {:http_error, 409, _}} ->
        :ok

      {:error, reason} ->
        if System.monotonic_time(:millisecond) > deadline do
          raise "bucket creation never succeeded: #{inspect(reason)}"
        else
          Process.sleep(500)
          do_ensure_bucket(name, config, deadline)
        end
    end
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
end
