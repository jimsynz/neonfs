defmodule NeonFS.Integration.ServiceRegistryTest do
  @moduledoc """
  Cross-node consistency regression for `NeonFS.Core.ServiceRegistry`.

  Pre-#349, every core node kept its own ETS cache of services hydrated
  from Ra on startup and updated on local writes. That produced a
  stale-follower window: a `deregister_service` command applied on the
  leader replicated to followers via Ra, but followers only refreshed
  their local ETS on restart — so `list/0` on a follower could keep
  returning a recently-deregistered service until its next boot.

  With #349, every read goes through `RaSupervisor.local_query/2`, so
  a registration / deregistration committed by any node is immediately
  visible to every other node on their next read.

  This test is the canonical cross-node consistency check for the
  whole #341 bug class — other manager slices (ACL, S3 credentials,
  Escalation) follow the same pattern.
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Client.ServiceInfo
  alias NeonFS.Core.ServiceRegistry

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "service-registry-test")
    %{}
  end

  describe "cross-node visibility" do
    test "a service registered on node1 is visible on node2 and node3", %{cluster: cluster} do
      service_node = :"registry-consistency-#{System.unique_integer([:positive])}@localhost"
      info = ServiceInfo.new(service_node, :fuse)

      :ok = PeerCluster.rpc(cluster, :node1, ServiceRegistry, :register, [info])

      assert_eventually timeout: 10_000 do
        node2_sees?(cluster, service_node, :fuse) and node3_sees?(cluster, service_node, :fuse)
      end
    end

    test "a deregister on node1 is visible on node2 and node3", %{cluster: cluster} do
      service_node = :"registry-consistency-dereg-#{System.unique_integer([:positive])}@localhost"
      info = ServiceInfo.new(service_node, :s3)

      :ok = PeerCluster.rpc(cluster, :node1, ServiceRegistry, :register, [info])

      assert_eventually timeout: 10_000 do
        node2_sees?(cluster, service_node, :s3)
      end

      :ok = PeerCluster.rpc(cluster, :node1, ServiceRegistry, :deregister, [service_node, :s3])

      assert_eventually timeout: 10_000 do
        not node2_sees?(cluster, service_node, :s3) and
          not node3_sees?(cluster, service_node, :s3)
      end
    end
  end

  defp node2_sees?(cluster, node_name, type),
    do: service_visible?(cluster, :node2, node_name, type)

  defp node3_sees?(cluster, node_name, type),
    do: service_visible?(cluster, :node3, node_name, type)

  defp service_visible?(cluster, peer, node_name, type) do
    case PeerCluster.rpc(cluster, peer, ServiceRegistry, :get, [node_name, type]) do
      {:ok, %{node: ^node_name, type: ^type}} -> true
      _ -> false
    end
  end
end
