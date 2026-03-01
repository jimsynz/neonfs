defmodule NeonFS.Transport.PoolManagerTest do
  use ExUnit.Case, async: false

  alias NeonFS.Transport.{ConnPool, PoolManager, PoolSupervisor, TLS}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    # Generate CA + server + client certs
    {ca_cert, ca_key} = TLS.generate_ca("pool-manager-test")

    server_key = TLS.generate_node_key()
    server_csr = TLS.create_csr(server_key, "echo-server")
    server_cert = TLS.sign_csr(server_csr, "localhost", ca_cert, ca_key, 100)

    client_key = TLS.generate_node_key()
    client_csr = TLS.create_csr(client_key, "test-client")
    client_cert = TLS.sign_csr(client_csr, "localhost", ca_cert, ca_key, 101)

    ca_certfile = Path.join(tmp_dir, "ca.crt")
    server_certfile = Path.join(tmp_dir, "server.crt")
    server_keyfile = Path.join(tmp_dir, "server.key")
    client_certfile = Path.join(tmp_dir, "client.crt")
    client_keyfile = Path.join(tmp_dir, "client.key")

    File.write!(ca_certfile, TLS.encode_cert(ca_cert))
    File.write!(server_certfile, TLS.encode_cert(server_cert))
    File.write!(server_keyfile, TLS.encode_key(server_key))
    File.write!(client_certfile, TLS.encode_cert(client_cert))
    File.write!(client_keyfile, TLS.encode_key(client_key))

    # Start TLS echo server
    server_ssl_opts = [
      {:certs_keys, [%{certfile: server_certfile, keyfile: server_keyfile}]},
      {:cacertfile, ca_certfile},
      {:verify, :verify_peer},
      {:fail_if_no_peer_cert, true},
      {:versions, [:"tlsv1.3"]},
      :binary,
      {:packet, 4},
      {:active, false},
      {:reuseaddr, true}
    ]

    {:ok, listen_socket} = :ssl.listen(0, server_ssl_opts)
    {:ok, {_addr, port}} = :ssl.sockname(listen_socket)
    spawn_link(fn -> accept_loop(listen_socket) end)

    on_exit(fn -> :ssl.close(listen_socket) end)

    # Client SSL options for ConnPool
    client_ssl_opts = [
      {:certs_keys, [%{certfile: client_certfile, keyfile: client_keyfile}]},
      {:cacertfile, ca_certfile},
      {:verify, :verify_peer},
      {:versions, [:"tlsv1.3"]},
      :binary,
      {:packet, 4},
      {:active, false}
    ]

    # Start PoolSupervisor with unique name
    unique = System.unique_integer([:positive])
    sup_name = :"pool_sup_#{unique}"
    start_supervised!({PoolSupervisor, name: sup_name})

    %{
      port: port,
      client_ssl_opts: client_ssl_opts,
      pool_supervisor: sup_name,
      tmp_dir: tmp_dir
    }
  end

  describe "start_link/1" do
    test "starts the GenServer and creates ETS table", ctx do
      pm = start_pool_manager(ctx)
      assert Process.alive?(pm)
      assert :ets.info(:neonfs_transport_pools) != :undefined
    end
  end

  describe "ensure_pool/2" do
    test "creates a ConnPool for a peer", ctx do
      start_pool_manager(ctx)
      endpoint = {~c"localhost", ctx.port}

      assert {:ok, pid} = PoolManager.ensure_pool(:peer@host1, endpoint)
      assert is_pid(pid)
      assert Process.alive?(pid)
    end

    test "returns existing pool if already created", ctx do
      start_pool_manager(ctx)
      endpoint = {~c"localhost", ctx.port}

      assert {:ok, pid1} = PoolManager.ensure_pool(:peer@host1, endpoint)
      assert {:ok, pid2} = PoolManager.ensure_pool(:peer@host1, endpoint)
      assert pid1 == pid2
    end

    test "pool is functional after creation", ctx do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :transport, :conn_pool, :worker_connected]
        ])

      start_pool_manager(ctx)
      endpoint = {~c"localhost", ctx.port}

      {:ok, pool} = PoolManager.ensure_pool(:peer@host1, endpoint)

      # Wait for at least one async TLS connection to complete
      assert_receive {[:neonfs, :transport, :conn_pool, :worker_connected], ^ref, %{}, _}, 2_000

      message = {:test, make_ref()}
      assert ConnPool.execute(pool, message) == message
    end
  end

  describe "get_pool/1" do
    test "returns {:ok, pid} for existing pool", ctx do
      start_pool_manager(ctx)
      endpoint = {~c"localhost", ctx.port}

      {:ok, pid} = PoolManager.ensure_pool(:peer@host1, endpoint)
      assert {:ok, ^pid} = PoolManager.get_pool(:peer@host1)
    end

    test "returns {:error, :no_pool} for unknown peer", ctx do
      start_pool_manager(ctx)
      assert {:error, :no_pool} = PoolManager.get_pool(:unknown@host)
    end

    test "returns {:error, :no_pool} before PoolManager starts" do
      assert {:error, :no_pool} = PoolManager.get_pool(:unknown@host)
    end
  end

  describe "remove_pool/1" do
    test "stops the ConnPool and removes the ETS entry", ctx do
      start_pool_manager(ctx)
      endpoint = {~c"localhost", ctx.port}

      {:ok, pid} = PoolManager.ensure_pool(:peer@host1, endpoint)
      assert Process.alive?(pid)

      assert :ok = PoolManager.remove_pool(:peer@host1)

      # DynamicSupervisor.terminate_child/2 is synchronous — pool is already stopped
      refute Process.alive?(pid)
      assert {:error, :no_pool} = PoolManager.get_pool(:peer@host1)
    end

    test "removing non-existent pool is a no-op", ctx do
      start_pool_manager(ctx)
      assert :ok = PoolManager.remove_pool(:nonexistent@host)
    end
  end

  describe "nodedown handling" do
    test "pool removed on :nodedown event", ctx do
      pm = start_pool_manager(ctx)
      endpoint = {~c"localhost", ctx.port}

      {:ok, _pid} = PoolManager.ensure_pool(:departed@host1, endpoint)
      assert {:ok, _} = PoolManager.get_pool(:departed@host1)

      # Simulate :nodedown
      send(pm, {:nodedown, :departed@host1, []})
      :sys.get_state(pm)

      assert {:error, :no_pool} = PoolManager.get_pool(:departed@host1)
    end
  end

  describe "pool crash recovery" do
    test "ETS entry cleaned up when pool crashes", ctx do
      pm = start_pool_manager(ctx)
      endpoint = {~c"localhost", ctx.port}

      {:ok, pid} = PoolManager.ensure_pool(:crash_peer@host, endpoint)

      # Kill the pool process and wait for the monitor DOWN to be processed
      Process.exit(pid, :kill)
      :sys.get_state(pm)

      # ETS entry should be cleaned up
      assert {:error, :no_pool} = PoolManager.get_pool(:crash_peer@host)

      # Verify the GenServer is still alive
      assert Process.alive?(pm)
    end
  end

  describe "advertise_endpoint/1" do
    test "returns {host, port} with configured address" do
      Application.put_env(:neonfs_client, :data_transfer, advertise: ~c"10.0.1.5")
      on_exit(fn -> Application.delete_env(:neonfs_client, :data_transfer) end)

      assert {~c"10.0.1.5", 44_831} = PoolManager.advertise_endpoint(44_831)
    end

    test "auto-detects address when not configured" do
      Application.delete_env(:neonfs_client, :data_transfer)
      on_exit(fn -> Application.delete_env(:neonfs_client, :data_transfer) end)

      {host, port} = PoolManager.advertise_endpoint(12_345)
      assert port == 12_345
      # Host should be a tuple (IPv4) or charlist
      assert is_tuple(host) or is_list(host)
    end
  end

  describe "discovery refresh" do
    test "creates pools for discovered peers with data_endpoint metadata", ctx do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :transport, :pool_manager, :discovery_refresh]
        ])

      # Start PoolManager with a short refresh interval
      pm = start_pool_manager(ctx, discovery_refresh_interval: 100)

      # Mock Discovery by inserting service info into the Discovery ETS table
      setup_discovery_ets([
        {:peer_a@host1, :core, %{data_endpoint: {~c"localhost", ctx.port}}},
        {:peer_b@host2, :core, %{data_endpoint: {~c"localhost", ctx.port}}}
      ])

      # Wait for discovery refresh to fire
      assert_receive {[:neonfs, :transport, :pool_manager, :discovery_refresh], ^ref, _, _}, 1_000

      assert {:ok, _} = PoolManager.get_pool(:peer_a@host1)
      assert {:ok, _} = PoolManager.get_pool(:peer_b@host2)

      # Verify the manager is still alive
      assert Process.alive?(pm)
    end
  end

  # Helpers

  defp start_pool_manager(ctx, extra_opts \\ []) do
    opts =
      [
        pool_size: 1,
        worker_idle_timeout: 30_000,
        discovery_refresh_interval: Keyword.get(extra_opts, :discovery_refresh_interval, 60_000),
        pool_supervisor: ctx.pool_supervisor,
        ssl_opts: ctx.client_ssl_opts
      ] ++ extra_opts

    start_supervised!({PoolManager, opts})
  end

  defp setup_discovery_ets(services) do
    # Ensure the Discovery ETS table exists
    table = :neonfs_client_services

    unless :ets.info(table) != :undefined do
      :ets.new(table, [:named_table, :set, :public, read_concurrency: true])
    end

    # Group by type and insert
    by_type =
      services
      |> Enum.group_by(fn {_node, type, _meta} -> type end, fn {node, type, meta} ->
        %NeonFS.Client.ServiceInfo{
          node: node,
          type: type,
          registered_at: DateTime.utc_now(),
          metadata: meta,
          status: :online
        }
      end)

    for {type, type_services} <- by_type do
      :ets.insert(table, {{:by_type, type}, type_services})
    end
  end

  # Echo server helpers

  defp accept_loop(listen_socket) do
    case :ssl.transport_accept(listen_socket) do
      {:ok, transport_socket} ->
        handshake_and_spawn(transport_socket)
        accept_loop(listen_socket)

      {:error, _} ->
        :ok
    end
  end

  defp handshake_and_spawn(transport_socket) do
    case :ssl.handshake(transport_socket, 10_000) do
      {:ok, socket} ->
        pid =
          spawn(fn ->
            receive do
              :ready -> echo_loop(socket)
            end
          end)

        :ssl.controlling_process(socket, pid)
        send(pid, :ready)

      {:error, _} ->
        :ok
    end
  end

  defp echo_loop(socket) do
    case :ssl.recv(socket, 0, 30_000) do
      {:ok, data} ->
        :ssl.send(socket, data)
        echo_loop(socket)

      {:error, _} ->
        :ssl.close(socket)
    end
  end
end
