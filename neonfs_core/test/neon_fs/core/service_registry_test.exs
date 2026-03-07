defmodule NeonFS.Core.ServiceRegistryTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Client.ServiceInfo
  alias NeonFS.Core.ServiceRegistry

  setup do
    start_service_registry()
    :ok
  end

  test "connected_nodes_by_type/1 returns only connected nodes of the requested type" do
    {:ok, core_peer, core_node} = :peer.start_link(%{name: :neonfs_core_registry_peer})
    {:ok, nfs_peer, nfs_node} = :peer.start_link(%{name: :neonfs_nfs_registry_peer})

    on_exit(fn ->
      safe_stop_peer(core_peer)
      safe_stop_peer(nfs_peer)
    end)

    :ok = ServiceRegistry.register(ServiceInfo.new(core_node, :core))
    :ok = ServiceRegistry.register(ServiceInfo.new(nfs_node, :nfs))
    :ok = ServiceRegistry.register(ServiceInfo.new(:neonfs_core_disconnected@localhost, :core))

    assert ServiceRegistry.connected_nodes_by_type(:core) == [core_node]
    assert ServiceRegistry.connected_nodes_by_type(:nfs) == [nfs_node]
  end

  test "stores multiple services for the same node independently" do
    shared_node = :shared_services@localhost

    :ok = ServiceRegistry.register(ServiceInfo.new(shared_node, :core))
    :ok = ServiceRegistry.register(ServiceInfo.new(shared_node, :nfs))

    assert {:ok, core_service} = ServiceRegistry.get(shared_node, :core)
    assert {:ok, nfs_service} = ServiceRegistry.get(shared_node, :nfs)
    assert core_service.type == :core
    assert nfs_service.type == :nfs

    assert Enum.map(ServiceRegistry.list_by_node(shared_node), & &1.type) == [:core, :nfs]

    assert :ok = ServiceRegistry.deregister(shared_node, :nfs)
    assert {:ok, _core_service} = ServiceRegistry.get(shared_node, :core)
    assert {:error, :not_found} = ServiceRegistry.get(shared_node, :nfs)
  end

  defp safe_stop_peer(peer) do
    :peer.stop(peer)
  catch
    :exit, _ -> :ok
  end
end
