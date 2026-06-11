defmodule NeonFS.WebDAV.CrossNodeStateTest do
  @moduledoc """
  Cluster-shared WebDAV gateway state (#1178): the lock-token index
  lives in the Ra-backed cluster KV store and dead properties live in
  core file metadata, so a load balancer can route a WebDAV session's
  requests to any gateway node.

  ## Cluster shape

    * `node1`: `:neonfs_core` (Ra member, KV store, DLM, coordinator).
    * `node2`, `node3`: `:neonfs_webdav` (real gateways on per-peer ports).

  ## Coverage

  Drives one WebDAV session alternating between the two gateways:
  LOCK on node2; an unauthorised PUT via node3 rejected with `423
  Locked`; the same PUT accepted via node3 with the lock token in the
  `If` header; PROPPATCH of a dead property via node2 visible in a
  PROPFIND via node3; UNLOCK via node3 with the token issued by node2;
  and a final tokenless PUT via node2 confirming the lock is gone.
  """

  use ExUnit.Case, async: false

  alias NeonFS.TestSupport.{ClusterCase, PeerCluster}

  @moduletag timeout: 300_000
  @moduletag :integration

  @volume "wd-xnode"

  @lockinfo_xml """
  <?xml version="1.0" encoding="utf-8"?>
  <D:lockinfo xmlns:D="DAV:">
    <D:lockscope><D:exclusive/></D:lockscope>
    <D:locktype><D:write/></D:locktype>
    <D:owner>cross-node-test</D:owner>
  </D:lockinfo>
  """

  @proppatch_xml """
  <?xml version="1.0" encoding="utf-8"?>
  <D:propertyupdate xmlns:D="DAV:" xmlns:Z="urn:x-test:">
    <D:set><D:prop><Z:author>cross-node-author-value</Z:author></D:prop></D:set>
  </D:propertyupdate>
  """

  @propfind_xml """
  <?xml version="1.0" encoding="utf-8"?>
  <D:propfind xmlns:D="DAV:" xmlns:Z="urn:x-test:">
    <D:prop><Z:author/></D:prop>
  </D:propfind>
  """

  test "locks and dead properties follow the session across two WebDAV nodes" do
    cluster =
      PeerCluster.start_cluster!(3,
        roles: %{
          node1: [:neonfs_core],
          node2: [:neonfs_webdav],
          node3: [:neonfs_webdav]
        }
      )

    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    PeerCluster.connect_nodes(cluster)

    :ok =
      ClusterCase.init_mixed_role_cluster(cluster,
        name: "webdav-xnode",
        volumes: [{@volume, %{}}]
      )

    url_a = gateway_url(cluster, :node2)
    url_b = gateway_url(cluster, :node3)

    doc_a = "#{url_a}/#{@volume}/doc.txt"
    doc_b = "#{url_b}/#{@volume}/doc.txt"

    # First write retries until the gateway's client infrastructure is
    # warm on a loaded runner.
    put_until_success(doc_a, "v1")

    # LOCK via node2 — the token must be honoured everywhere.
    lock_resp =
      Req.request!(method: "LOCK", url: doc_a, body: @lockinfo_xml, retry: false)

    assert lock_resp.status == 200
    token = extract_lock_token(lock_resp)

    # Tokenless write via node3 is refused: the lock taken on node2 is
    # cluster state, not node state.
    assert %{status: 423} =
             Req.request!(method: "PUT", url: doc_b, body: "intruder", retry: false)

    # The same write with the node2-issued token via node3 succeeds.
    assert %{status: status} =
             Req.request!(
               method: "PUT",
               url: doc_b,
               body: "v2",
               headers: [{"if", "(<opaquelocktoken:#{token}>)"}],
               retry: false
             )

    assert status in [200, 201, 204]

    # Dead properties: PROPPATCH via node2, PROPFIND via node3.
    assert %{status: 207} =
             Req.request!(
               method: "PROPPATCH",
               url: doc_a,
               body: @proppatch_xml,
               headers: [{"if", "(<opaquelocktoken:#{token}>)"}],
               retry: false
             )

    propfind_resp =
      Req.request!(
        method: "PROPFIND",
        url: doc_b,
        body: @propfind_xml,
        headers: [{"depth", "0"}],
        retry: false
      )

    assert propfind_resp.status == 207
    assert to_string(propfind_resp.body) =~ "cross-node-author-value"

    # UNLOCK via node3 with the token issued by node2.
    assert %{status: 204} =
             Req.request!(
               method: "UNLOCK",
               url: doc_b,
               headers: [{"lock-token", "<opaquelocktoken:#{token}>"}],
               retry: false
             )

    # The lock is gone cluster-wide: a tokenless write via node2 now
    # succeeds.
    assert %{status: status} =
             Req.request!(method: "PUT", url: doc_a, body: "v3", retry: false)

    assert status in [200, 201, 204]
  end

  defp gateway_url(cluster, peer) do
    port = PeerCluster.get_node!(cluster, peer).interface_ports.webdav
    "http://127.0.0.1:#{port}"
  end

  defp extract_lock_token(resp) do
    [header | _] = resp.headers["lock-token"]
    [_, token] = Regex.run(~r/<opaquelocktoken:([^>]+)>/, header)
    token
  end

  defp put_until_success(url, body) do
    deadline = System.monotonic_time(:millisecond) + 60_000
    do_put_until_success(url, body, deadline)
  end

  defp do_put_until_success(url, body, deadline) do
    case Req.request(method: "PUT", url: url, body: body, retry: false) do
      {:ok, %{status: status}} when status in [200, 201, 204] ->
        :ok

      other ->
        if System.monotonic_time(:millisecond) > deadline do
          raise "initial PUT never succeeded: #{inspect(other)}"
        else
          Process.sleep(500)
          do_put_until_success(url, body, deadline)
        end
    end
  end
end
