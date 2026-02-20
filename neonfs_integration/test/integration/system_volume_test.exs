defmodule NeonFS.Integration.SystemVolumeTest do
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 300_000
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster)
    %{}
  end

  describe "system volume lifecycle" do
    test "cluster init creates system volume visible on all nodes", %{cluster: cluster} do
      for node_name <- [:node1, :node2, :node3] do
        assert_eventually timeout: 30_000 do
          case PeerCluster.rpc(
                 cluster,
                 node_name,
                 NeonFS.Core.VolumeRegistry,
                 :get_system_volume,
                 []
               ) do
            {:ok, volume} ->
              volume.name == "_system" and volume.system == true

            _ ->
              false
          end
        end
      end
    end

    test "replication factor equals cluster size after all nodes joined", %{cluster: cluster} do
      assert_eventually timeout: 30_000 do
        case PeerCluster.rpc(
               cluster,
               :node1,
               NeonFS.Core.VolumeRegistry,
               :get_system_volume,
               []
             ) do
          {:ok, volume} -> volume.durability.factor == 3
          _ -> false
        end
      end
    end
  end

  describe "cross-node read/write" do
    test "write on one node is readable from another", %{cluster: cluster} do
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.SystemVolume, :write, [
          "/test/cross-node.txt",
          "hello from node1"
        ])

      assert_eventually timeout: 30_000 do
        case PeerCluster.rpc(cluster, :node2, NeonFS.Core.SystemVolume, :read, [
               "/test/cross-node.txt"
             ]) do
          {:ok, "hello from node1"} -> true
          _ -> false
        end
      end

      assert_eventually timeout: 30_000 do
        case PeerCluster.rpc(cluster, :node3, NeonFS.Core.SystemVolume, :read, [
               "/test/cross-node.txt"
             ]) do
          {:ok, "hello from node1"} -> true
          _ -> false
        end
      end
    end
  end

  describe "guard enforcement" do
    test "system volume cannot be deleted", %{cluster: cluster} do
      # Wait for system volume to be visible on node2
      assert_eventually timeout: 30_000 do
        match?(
          {:ok, _},
          PeerCluster.rpc(
            cluster,
            :node2,
            NeonFS.Core.VolumeRegistry,
            :get_system_volume,
            []
          )
        )
      end

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node2, NeonFS.Core.VolumeRegistry, :get_system_volume, [])

      result =
        PeerCluster.rpc(cluster, :node2, NeonFS.Core.VolumeRegistry, :delete, [volume.id])

      assert {:error, :system_volume} = result
    end
  end

  describe "list filtering" do
    test "system volume excluded from default list, included with option", %{cluster: cluster} do
      # Create a user volume
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "user-volume",
          %{}
        ])

      # Default list should only show user volume
      default_list =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :list, [])

      names = Enum.map(default_list, & &1.name)
      assert "user-volume" in names
      refute "_system" in names

      # List with include_system should show both
      full_list =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :list, [
          [include_system: true]
        ])

      full_names = Enum.map(full_list, & &1.name)
      assert "user-volume" in full_names
      assert "_system" in full_names
    end
  end

  describe "node failure resilience" do
    @describetag cluster_mode: :per_test

    setup %{cluster: cluster} do
      :ok = init_multi_node_cluster(cluster)
      %{}
    end

    test "system volume data accessible after single node failure", %{cluster: cluster} do
      # Write data to system volume
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.SystemVolume, :write, [
          "/test/resilience.txt",
          "survives failure"
        ])

      # Verify readable from node3 before stopping it
      assert_eventually timeout: 30_000 do
        case PeerCluster.rpc(cluster, :node3, NeonFS.Core.SystemVolume, :read, [
               "/test/resilience.txt"
             ]) do
          {:ok, "survives failure"} -> true
          _ -> false
        end
      end

      # Stop node3
      :ok = PeerCluster.stop_node(cluster, :node3)

      # Wait for failure detection
      node3_info = PeerCluster.get_node!(cluster, :node3)

      assert_eventually timeout: 5_000 do
        nodes = PeerCluster.rpc(cluster, :node1, Node, :list, [])
        node3_info.node not in nodes
      end

      # Data should still be readable from surviving nodes
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.Core.SystemVolume, :read, [
               "/test/resilience.txt"
             ]) do
          {:ok, "survives failure"} -> true
          _ -> false
        end
      end

      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(cluster, :node2, NeonFS.Core.SystemVolume, :read, [
               "/test/resilience.txt"
             ]) do
          {:ok, "survives failure"} -> true
          _ -> false
        end
      end
    end
  end

  describe "log retention" do
    test "prunes old files across the cluster", %{cluster: cluster} do
      today = Date.utc_today()
      old_date = today |> Date.add(-100) |> Date.to_iso8601()
      recent_date = today |> Date.add(-10) |> Date.to_iso8601()

      # Write audit files with old and recent dates
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.SystemVolume, :write, [
          "/audit/intents/#{old_date}.jsonl",
          "old intent log"
        ])

      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.SystemVolume, :write, [
          "/audit/intents/#{recent_date}.jsonl",
          "recent intent log"
        ])

      # Run retention prune from node2
      :ok =
        PeerCluster.rpc(cluster, :node2, NeonFS.Core.SystemVolume.Retention, :prune, [])

      # Old file should be pruned, recent file should remain
      assert_eventually timeout: 30_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.Core.SystemVolume, :list, [
               "/audit/intents"
             ]) do
          {:ok, files} ->
            "#{recent_date}.jsonl" in files and "#{old_date}.jsonl" not in files

          _ ->
            false
        end
      end
    end
  end
end
