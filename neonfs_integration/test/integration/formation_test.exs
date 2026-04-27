defmodule NeonFS.Integration.FormationTest do
  use NeonFS.TestSupport.ClusterCase, async: false

  @moduletag timeout: 180_000

  describe "3-node auto-bootstrap" do
    @describetag nodes: 3

    @tag :skip
    test "forms a cluster autonomously", %{cluster: cluster} do
      # Stop the pre-made cluster — we need to start fresh with formation config
      PeerCluster.stop_cluster(cluster)

      cluster =
        PeerCluster.start_cluster!(3,
          formation: [cluster_name: "auto-test"]
        )

      # Connect nodes so they can discover each other
      PeerCluster.connect_nodes(cluster)

      # Phase 1: Event-driven wait for all nodes to be fully started
      # (ReadySignal joined :pg — no polling)
      wait_for_full_mesh(cluster)

      # Phase 2: Formation starts after ReadySignal — poll for completion
      # (Ra cluster creation can take 60-90s on slow CI runners)
      assert_eventually timeout: 120_000 do
        Enum.all?(cluster.nodes, fn node_info ->
          PeerCluster.rpc(cluster, node_info.name, NeonFS.Cluster.State, :exists?, []) == true
        end)
      end

      # Verify cluster name matches on at least the init node
      {:ok, state} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Cluster.State, :load, [])

      assert state.cluster_name == "auto-test"

      # The init node should have itself in ra_cluster_members
      assert state.ra_cluster_members != []

      on_exit(fn -> PeerCluster.stop_cluster(cluster) end)
    end
  end

  describe "idempotent restart" do
    @describetag nodes: 1

    test "formation does not run when cluster.json already exists", %{cluster: cluster} do
      # First, manually init the cluster
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["existing"])

      # Verify cluster.json exists
      assert PeerCluster.rpc(cluster, :node1, NeonFS.Cluster.State, :exists?, []) == true

      node1_atom = PeerCluster.get_node!(cluster, :node1).node

      # Start Formation via GenServer.start (not start_link) to avoid the
      # RPC handler being linked to the short-lived Formation process.
      # Formation exits :normal in handle_continue when cluster.json exists.
      {:ok, _pid} =
        PeerCluster.rpc(cluster, :node1, GenServer, :start, [
          NeonFS.Cluster.Formation,
          [
            cluster_name: "should-not-overwrite",
            bootstrap_expect: 1,
            bootstrap_peers: [node1_atom],
            bootstrap_timeout: 5_000
          ],
          [name: NeonFS.Cluster.Formation]
        ])

      # Wait until the Formation process has exited on the remote node
      assert_eventually timeout: 5_000 do
        PeerCluster.rpc(cluster, :node1, Process, :whereis, [NeonFS.Cluster.Formation]) == nil
      end

      # Verify cluster name was NOT overwritten
      {:ok, state} = PeerCluster.rpc(cluster, :node1, NeonFS.Cluster.State, :load, [])
      assert state.cluster_name == "existing"
    end
  end

  describe "orphaned data refusal" do
    @describetag nodes: 1

    test "formation refuses when orphaned data exists", %{cluster: cluster} do
      node1_atom = PeerCluster.get_node!(cluster, :node1).node

      # Create orphaned blob data on the node (actual files, not just directories)
      blob_dir =
        PeerCluster.rpc(cluster, :node1, Application, :get_env, [
          :neonfs_core,
          :blob_store_base_dir
        ])

      prefix_dir = Path.join(blob_dir, "ab")
      PeerCluster.rpc(cluster, :node1, File, :mkdir_p!, [prefix_dir])

      PeerCluster.rpc(cluster, :node1, File, :write!, [
        Path.join(prefix_dir, "abcdef1234567890"),
        "orphaned chunk data"
      ])

      # Start Formation via GenServer.start (not start_link) to avoid
      # the shutdown exit propagating to the RPC handler process.
      # init/1 returns {:ok, state, {:continue, ...}} so GenServer.start
      # returns {:ok, pid}. The {:stop, ...} happens in handle_continue.
      {:ok, _pid} =
        PeerCluster.rpc(cluster, :node1, GenServer, :start, [
          NeonFS.Cluster.Formation,
          [
            cluster_name: "should-not-form",
            bootstrap_expect: 1,
            bootstrap_peers: [node1_atom],
            bootstrap_timeout: 5_000
          ],
          [name: NeonFS.Cluster.Formation]
        ])

      # Wait for Formation to exit (it stops in handle_continue)
      assert_eventually timeout: 5_000 do
        PeerCluster.rpc(cluster, :node1, Process, :whereis, [NeonFS.Cluster.Formation]) == nil
      end

      # Verify Formation did NOT create cluster.json
      refute PeerCluster.rpc(cluster, :node1, NeonFS.Cluster.State, :exists?, [])
    end
  end

  describe "single-node bootstrap" do
    @describetag nodes: 1

    test "bootstrap_expect: 1 initialises without waiting", %{cluster: cluster} do
      # Stop the pre-made cluster — start fresh with formation
      PeerCluster.stop_cluster(cluster)

      cluster =
        PeerCluster.start_cluster!(1,
          formation: [
            cluster_name: "solo-test",
            bootstrap_expect: 1
          ]
        )

      # Single node doesn't need connect_nodes, but call it anyway for consistency
      PeerCluster.connect_nodes(cluster)

      # Event-driven wait for node readiness, then brief poll for formation
      wait_for_full_mesh(cluster)

      assert_eventually timeout: 30_000 do
        PeerCluster.rpc(cluster, :node1, NeonFS.Cluster.State, :exists?, []) == true
      end

      {:ok, state} = PeerCluster.rpc(cluster, :node1, NeonFS.Cluster.State, :load, [])
      assert state.cluster_name == "solo-test"

      on_exit(fn -> PeerCluster.stop_cluster(cluster) end)
    end
  end
end
