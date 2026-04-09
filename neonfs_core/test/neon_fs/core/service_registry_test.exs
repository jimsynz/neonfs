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
    core_port = allocate_port()
    nfs_port = allocate_port()

    peer_ports_env =
      "neonfs_core_registry_peer@localhost:#{core_port},neonfs_nfs_registry_peer@localhost:#{nfs_port}"

    System.put_env("NEONFS_PEER_PORTS", peer_ports_env)

    {core_peer, core_node} =
      start_test_peer(:neonfs_core_registry_peer, core_port, peer_ports_env)

    {nfs_peer, nfs_node} = start_test_peer(:neonfs_nfs_registry_peer, nfs_port, peer_ports_env)

    on_exit(fn ->
      System.delete_env("NEONFS_PEER_PORTS")
      safe_stop_peer(core_peer)
      safe_stop_peer(nfs_peer)
    end)

    Node.connect(core_node)
    Node.connect(nfs_node)

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

  defp start_test_peer(name, dist_port, peer_ports_env) do
    code_paths =
      :code.get_path()
      |> Enum.flat_map(fn path -> [~c"-pa", path] end)

    args =
      [~c"-start_epmd", ~c"false", ~c"-epmd_module", ~c"Elixir.NeonFS.Epmd"] ++ code_paths

    env = [
      {~c"NEONFS_DIST_PORT", ~c"#{dist_port}"},
      {~c"NEONFS_PEER_PORTS", to_charlist(peer_ports_env)}
    ]

    {:ok, peer, node} =
      :peer.start_link(%{
        name: name,
        host: ~c"localhost",
        args: args,
        env: env,
        connection: 0,
        wait_boot: 30_000
      })

    {peer, node}
  end

  defp allocate_port do
    {:ok, socket} = :gen_tcp.listen(0, reuseaddr: true)
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end

  defp safe_stop_peer(peer) do
    :peer.stop(peer)
  catch
    :exit, _ -> :ok
  end
end
