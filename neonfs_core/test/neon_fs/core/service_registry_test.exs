defmodule NeonFS.Core.ServiceRegistryTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase
  use Mimic

  require Logger

  alias NeonFS.Client.ServiceInfo
  alias NeonFS.Core.{NodeRegistry, RaServer, RaSupervisor, ServiceRegistry}

  @moduletag :tmp_dir

  @peer_boot_attempts 5
  @peer_boot_backoff_ms 500

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    ensure_node_named()
    start_ra()
    :ok = RaServer.init_cluster()

    start_service_registry()

    on_exit(fn -> cleanup_test_dirs() end)

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

  test "ignores a nodedown for the local node and keeps its services registered" do
    :ok = ServiceRegistry.register(ServiceInfo.new(Node.self(), :core))
    assert {:ok, _} = ServiceRegistry.get(Node.self(), :core)

    send(
      Process.whereis(ServiceRegistry),
      {:nodedown, Node.self(), [nodedown_reason: :net_kernel_terminated]}
    )

    # Drain the mailbox past the nodedown message before asserting.
    :sys.get_state(ServiceRegistry)

    assert {:ok, _} = ServiceRegistry.get(Node.self(), :core),
           "the local core service must survive a spurious self-nodedown (#1049)"
  end

  test "list/0 stamps :draining on services whose node is draining (#1324)" do
    :ok = ServiceRegistry.register(ServiceInfo.new(Node.self(), :core))
    :ok = NodeRegistry.set_status(Node.self(), :draining)

    entry =
      ServiceRegistry.list()
      |> Enum.find(&(&1.node == Node.self() and &1.type == :core))

    assert entry.status == :draining
  end

  describe "a failed Ra write" do
    setup do
      Mimic.allow(RaSupervisor, self(), Process.whereis(ServiceRegistry))
      :ok
    end

    test "register/1 reports it to the caller instead of replying :ok" do
      stub(RaSupervisor, :command, fn _cmd, _timeout -> {:timeout, Node.self()} end)

      assert {:error, :timeout} =
               ServiceRegistry.register(ServiceInfo.new(:unreplicated@localhost, :nfs))
    end

    test "deregister/2 reports it to the caller instead of replying :ok" do
      stub(RaSupervisor, :command, fn _cmd, _timeout -> {:error, :ra_unavailable} end)

      assert {:error, :ra_unavailable} =
               ServiceRegistry.deregister(:unreplicated@localhost, :nfs)
    end

    # `:ra.process_command/3` answers `{:ok, Reply, Leader}` once the command
    # commits, whatever `Reply` is — so a state-machine rejection arrives under
    # an outer `:ok` and matching only that tag reports a phantom success.
    test "register/1 reports a state-machine rejection nested inside an :ok" do
      stub(RaSupervisor, :command, fn _cmd, _timeout ->
        {:ok, {:error, :rejected_by_machine}, Node.self()}
      end)

      assert {:error, :rejected_by_machine} =
               ServiceRegistry.register(ServiceInfo.new(:unreplicated@localhost, :nfs))
    end

    test "the registry survives it and keeps answering" do
      stub(RaSupervisor, :command, fn _cmd, _timeout -> {:timeout, Node.self()} end)

      assert {:error, :timeout} =
               ServiceRegistry.register(ServiceInfo.new(:unreplicated@localhost, :nfs))

      assert Process.alive?(Process.whereis(ServiceRegistry))
      assert is_list(ServiceRegistry.list())
    end
  end

  test "list/0 stamps :maintenance on services whose node is cordoned (#1376)" do
    :ok = ServiceRegistry.register(ServiceInfo.new(Node.self(), :core))
    :ok = NodeRegistry.set_status(Node.self(), :maintenance)

    entry =
      ServiceRegistry.list()
      |> Enum.find(&(&1.node == Node.self() and &1.type == :core))

    assert entry.status == :maintenance
  end

  defp start_test_peer(name, dist_port, peer_ports_env) do
    code_paths =
      :code.get_path()
      |> Enum.flat_map(fn path -> [~c"-pa", path] end)

    args =
      [~c"-start_epmd", ~c"false", ~c"-epmd_module", ~c"Elixir.NeonFS.Epmd"] ++ code_paths

    env = [
      {~c"NEONFS_DIST_PORT", Integer.to_charlist(dist_port)},
      {~c"NEONFS_PEER_PORTS", String.to_charlist(peer_ports_env)}
    ]

    peer_opts = %{
      name: name,
      host: ~c"localhost",
      args: args,
      env: env,
      # Standard I/O rather than a TCP control port: with a port the origin
      # listens and waits for the peer to dial back, and OTP hard-codes that
      # accept at 60s (`ACCEPT_TIMEOUT` in `peer.erl`). A saturated runner
      # that overruns it burns a minute and then fails boot with
      # `{:inet_async, :timeout}`, which no retry budget can avoid because
      # backoff only lengthens the gaps between attempts. Standard I/O opens
      # no listen socket, so the ceiling does not exist, and peer lifetime is
      # tied to the origin's port so an abandoned peer cannot outlive the run.
      connection: :standard_io,
      wait_boot: 60_000
    }

    {:ok, peer, node} = start_peer_with_retry(peer_opts, @peer_boot_attempts)

    {peer, node}
  end

  # `:peer.start/1` rather than `start_link/1`: a linked peer dying during
  # bring-up propagates an EXIT to the test process and fails the whole
  # module. `safe_stop_peer/1` in `on_exit` still bounds peer lifetime.
  defp start_peer_with_retry(peer_opts, attempts_left) do
    case :peer.start(peer_opts) do
      {:ok, peer, node} ->
        {:ok, peer, node}

      {:error, reason} = error ->
        if retry_boot?(attempts_left, reason) do
          backoff_and_retry_boot(peer_opts, attempts_left, reason)
        else
          error
        end
    end
  catch
    # `:peer.start/1` exits rather than returning `{:error, _}` for every boot
    # failure that is not an argument error: `wait_boot` expiring exits
    # `:timeout`, and a peer that dies mid-boot exits `{:boot_failed, reason}`.
    :exit, reason ->
      if retry_boot?(attempts_left, reason) do
        backoff_and_retry_boot(peer_opts, attempts_left, reason)
      else
        :erlang.raise(:exit, reason, __STACKTRACE__)
      end
  end

  defp retry_boot?(attempts_left, reason),
    do: attempts_left > 1 and transient_boot_error?(reason)

  defp backoff_and_retry_boot(peer_opts, attempts_left, reason) do
    Logger.warning(
      "peer boot failed transiently (#{inspect(reason)}), " <>
        "retrying (#{attempts_left - 1} attempt(s) left)"
    )

    Process.sleep(@peer_boot_backoff_ms * Integer.pow(2, @peer_boot_attempts - attempts_left))
    start_peer_with_retry(peer_opts, attempts_left - 1)
  end

  defp transient_boot_error?({:boot_failed, reason}), do: transient_boot_error?(reason)
  defp transient_boot_error?(:timeout), do: true
  defp transient_boot_error?(:tcp_closed), do: true
  defp transient_boot_error?({:inet_async, :timeout}), do: true
  defp transient_boot_error?(_reason), do: false

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
