defmodule NeonFS.FUSE.IntegrationTest.MetadataCacheInvalidationTest do
  @moduledoc """
  Integration test for `NeonFS.FUSE.MetadataCache` cross-node
  invalidation — splits out of the original `EventNotificationTest`
  in `neonfs_integration` so the FUSE-specific cache assertions live
  with the FUSE package (#600 / #582). The non-FUSE event-delivery
  scenarios stay in `neonfs_integration` where they belong.
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  @moduletag timeout: 300_000
  @moduletag :integration
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "fuse-cache-test")
    %{}
  end

  setup %{cluster: cluster} do
    suffix = System.unique_integer([:positive])
    volume_name = "fuse-cache-vol-#{suffix}"

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        volume_name,
        %{}
      ])

    %{volume_id: volume.id, volume_name: volume_name}
  end

  describe "FUSE metadata cache invalidation" do
    test "MetadataCache is invalidated by cross-node events",
         %{cluster: cluster, volume_id: vid, volume_name: vname} do
      {:ok, cache_pid} =
        PeerCluster.rpc(
          cluster,
          :node2,
          GenServer,
          :start,
          [NeonFS.FUSE.MetadataCache, [volume_id: vid]]
        )

      cache_table =
        PeerCluster.rpc(cluster, :node2, NeonFS.FUSE.MetadataCache, :table, [cache_pid])

      PeerCluster.rpc(
        cluster,
        :node2,
        NeonFS.FUSE.MetadataCache,
        :put_attrs,
        [cache_table, vid, "/cached-file.txt", %{size: 42}]
      )

      {:ok, %{size: 42}} =
        PeerCluster.rpc(
          cluster,
          :node2,
          NeonFS.FUSE.MetadataCache,
          :get_attrs,
          [cache_table, vid, "/cached-file.txt"]
        )

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          vname,
          "/cached-file.txt",
          "real data"
        ])

      # Populate a directory listing cache entry
      PeerCluster.rpc(
        cluster,
        :node2,
        NeonFS.FUSE.MetadataCache,
        :put_dir_listing,
        [cache_table, vid, "/", [{"old.txt", "/old.txt", 0o100644}]]
      )

      # Write another file — triggers FileCreated which invalidates "/" dir listing
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          vname,
          "/new-file.txt",
          "data"
        ])

      assert_eventually timeout: 10_000 do
        result =
          PeerCluster.rpc(
            cluster,
            :node2,
            NeonFS.FUSE.MetadataCache,
            :get_dir_listing,
            [cache_table, vid, "/"]
          )

        result == :miss
      end
    end
  end
end
