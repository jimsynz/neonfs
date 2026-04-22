defmodule NeonFS.Integration.ReplicationTest do
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 300_000
  # Phase 5 quorum-replicated metadata enables multi-node read/write
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster)
    %{}
  end

  setup %{cluster: cluster} do
    suffix = System.unique_integer([:positive])
    volume_name = "repl-vol-#{suffix}"

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [volume_name, %{}])

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [volume_name])

    %{volume_id: volume.id, volume_name: volume_name}
  end

  describe "write replication" do
    test "data is replicated to all nodes", %{
      cluster: cluster,
      volume_id: vid,
      volume_name: vname
    } do
      # Subscribe on node2, write on node1, wait for the event
      subscribe_then_act(
        cluster,
        :node2,
        vid,
        fn ->
          {:ok, _} =
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              vname,
              "/test.txt",
              "hello world"
            ])
        end,
        timeout: 15_000
      )

      # Event arrived — metadata is replicated. Read should succeed.
      {:ok, data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          vname,
          "/test.txt"
        ])

      assert data == "hello world"

      {:ok, data} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
          vname,
          "/test.txt"
        ])

      assert data == "hello world"
    end

    test "large file chunking and replication", %{
      cluster: cluster,
      volume_id: vid,
      volume_name: vname
    } do
      # Write 1MB of data
      data = :crypto.strong_rand_bytes(1_024 * 1_024)

      subscribe_then_act(
        cluster,
        :node2,
        vid,
        fn ->
          {:ok, _} =
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              vname,
              "/large.bin",
              data
            ])
        end,
        timeout: 15_000
      )

      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
          vname,
          "/large.bin"
        ])

      assert read_data == data
    end

    test "multiple files can be written and read", %{
      cluster: cluster,
      volume_id: vid,
      volume_name: vname
    } do
      # Write several files
      files = [
        {"/file1.txt", "content one"},
        {"/file2.txt", "content two"},
        {"/file3.txt", "content three"}
      ]

      for {path, content} <- files do
        subscribe_then_act(
          cluster,
          :node2,
          vid,
          fn ->
            {:ok, _} =
              PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
                vname,
                path,
                content
              ])
          end,
          timeout: 15_000
        )
      end

      # Verify all files are readable from node2
      for {path, expected_content} <- files do
        {:ok, data} =
          PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
            vname,
            path
          ])

        assert data == expected_content
      end
    end
  end
end
