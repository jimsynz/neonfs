defmodule NeonFS.Integration.CSIAttachFailoverTest do
  @moduledoc """
  Proves the guarantee behind CSI's single-attach enforcement for block
  volumes: an attachment is an exclusive `NamespaceCoordinator` claim held
  by a pid on the attached node, so a node that dies releases it and the
  volume can attach elsewhere without operator intervention.

  That is the reason the attachment is Ra-backed and holder-monitored
  rather than a row in a table, and it cannot be shown in `neonfs_csi`'s
  own tests: it needs a second node, and it needs the first one to die.

  The test drives `NeonFS.CSI.AttachRegistry` on the peers rather than the
  gRPC endpoint. The registry is where the claim is taken and released; a
  kubelet and a socket in front of it would add setup without adding
  coverage of the thing being proven.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 180_000
  @moduletag :integration
  # Three nodes, not two: killing one of two leaves Ra without a majority,
  # so the coordinator could commit nothing and the test would be measuring
  # lost quorum rather than the failover it means to.
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "csi-attach-failover")
    :ok
  end

  test "a dead node releases its block attachment", %{cluster: cluster} do
    volume_id = "blk-#{System.unique_integer([:positive])}"

    # Kill a node that is not the Ra leader. Losing the leader is a
    # different scenario — the coordinator applying the release is a
    # casualty of the same kill — and mixing the two would leave a failure
    # ambiguous.
    {victim, survivor} = pick_victim(cluster)

    prepare_node(cluster, victim, "worker-victim")
    prepare_node(cluster, survivor, "worker-survivor")

    assert {:ok, context} = claim(cluster, victim, volume_id, "worker-victim")
    assert context["neonfs.attached_node"] == "worker-victim"

    # While the victim holds it, the survivor cannot take the device. This
    # is the enforcement, and it has to hold across nodes rather than
    # within one.
    assert {:error, {:attached_elsewhere, _holder}} =
             claim(cluster, survivor, volume_id, "worker-survivor")

    PeerCluster.stop_node(cluster, victim)

    # No unpublish ran. The holder's own monitor died with its node, so
    # what releases the claim is a surviving coordinator noticing the node
    # left — which is what makes failover work when the node holding a
    # device is gone rather than politely detaching.
    assert :ok =
             wait_until(
               fn ->
                 match?({:ok, _context}, claim(cluster, survivor, volume_id, "worker-survivor"))
               end,
               timeout: 30_000
             ),
           "the survivor could not take the attachment after the holder's node died"
  end

  defp pick_victim(cluster) do
    {_name, leader} =
      PeerCluster.rpc(cluster, :node3, :ra_leaderboard, :lookup_leader, [:neonfs_meta])

    Enum.find_value([{:node1, :node2}, {:node2, :node1}], fn {victim, survivor} ->
      if PeerCluster.get_node!(cluster, victim).node != leader, do: {victim, survivor}
    end)
  end

  # Each peer needs a holder pid to hang claims on, and a way for the
  # resolver to map the CSI node_id back to the BEAM node. Both are the
  # real thing: the plugin registers itself as a `:csi` service the way a
  # node-mode plugin does, and the resolver reads the cluster's own
  # registry rather than a stub.
  defp prepare_node(cluster, node_name, node_id) do
    # `start`, not `start_link`: the RPC worker that runs this exits the
    # moment the call returns, and a linked holder would go with it —
    # taking the very claims it exists to keep alive.
    {:ok, _pid} =
      PeerCluster.rpc(cluster, node_name, GenServer, :start, [
        NeonFS.CSI.AttachHolder,
        [],
        [name: NeonFS.CSI.AttachHolder]
      ])

    :ok =
      PeerCluster.rpc(cluster, node_name, Application, :put_env, [
        :neonfs_csi,
        :service_list_fn,
        {NeonFS.Core.ServiceRegistry, :list, []}
      ])

    # These peers are core nodes, so the coordinator is local to them — the
    # Router path is for a controller running as its own interface node.
    :ok =
      PeerCluster.rpc(cluster, node_name, Application, :put_env, [
        :neonfs_csi,
        :coordinator_call_fn,
        NeonFS.Core.NamespaceCoordinator
      ])

    beam_node = PeerCluster.get_node!(cluster, node_name).node

    info = %NeonFS.Client.ServiceInfo{
      node: beam_node,
      type: :csi,
      metadata: %{mode: :node, node_id: node_id}
    }

    :ok = PeerCluster.rpc(cluster, node_name, NeonFS.Core.ServiceRegistry, :register, [info])

    :ok =
      wait_until(fn ->
        match?(
          {:ok, ^beam_node},
          PeerCluster.rpc(cluster, node_name, NeonFS.CSI.NodeResolver, :beam_node, [node_id])
        )
      end)
  end

  defp claim(cluster, node_name, volume_id, node_id) do
    PeerCluster.rpc(cluster, node_name, NeonFS.CSI.AttachRegistry, :claim, [volume_id, node_id])
  end
end
