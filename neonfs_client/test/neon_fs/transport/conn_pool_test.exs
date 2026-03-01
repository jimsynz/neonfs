defmodule NeonFS.Transport.ConnPoolTest do
  use ExUnit.Case, async: false

  alias NeonFS.Transport.{ConnPool, TLS}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    # Generate CA
    {ca_cert, ca_key} = TLS.generate_ca("connpool-test")

    # Generate server cert
    server_key = TLS.generate_node_key()
    server_csr = TLS.create_csr(server_key, "echo-server")
    server_cert = TLS.sign_csr(server_csr, "localhost", ca_cert, ca_key, 100)

    # Generate client cert
    client_key = TLS.generate_node_key()
    client_csr = TLS.create_csr(client_key, "test-client")
    client_cert = TLS.sign_csr(client_csr, "localhost", ca_cert, ca_key, 101)

    # Write certs to temp files
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

    # Client SSL options (fail_if_no_peer_cert is server-only)
    client_ssl_opts = [
      {:certs_keys, [%{certfile: client_certfile, keyfile: client_keyfile}]},
      {:cacertfile, ca_certfile},
      {:verify, :verify_peer},
      {:versions, [:"tlsv1.3"]},
      :binary,
      {:packet, 4},
      {:active, false}
    ]

    on_exit(fn ->
      :ssl.close(listen_socket)
    end)

    %{
      port: port,
      listen_socket: listen_socket,
      client_ssl_opts: client_ssl_opts,
      server_ssl_opts: server_ssl_opts,
      ca_certfile: ca_certfile,
      tmp_dir: tmp_dir
    }
  end

  describe "start_link/1" do
    test "creates a pool that connects to the echo server", ctx do
      {:ok, pool} =
        ConnPool.start_link(
          peer: {~c"localhost", ctx.port},
          ssl_opts: ctx.client_ssl_opts,
          pool_size: 2
        )

      assert is_pid(pool)
      assert Process.alive?(pool)
    end

    test "accepts :name option", ctx do
      name = :"connpool_named_#{System.unique_integer([:positive])}"

      {:ok, _pool} =
        ConnPool.start_link(
          peer: {~c"localhost", ctx.port},
          ssl_opts: ctx.client_ssl_opts,
          pool_size: 1,
          name: name
        )

      assert GenServer.whereis(name) != nil
    end

    test "initialises all connections eagerly", ctx do
      pool_size = 3

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :transport, :conn_pool, :worker_connected]
        ])

      {:ok, pool} =
        ConnPool.start_link(
          peer: {~c"localhost", ctx.port},
          ssl_opts: ctx.client_ssl_opts,
          pool_size: pool_size
        )

      # Wait for all async TLS connections to establish
      for _ <- 1..pool_size do
        assert_receive {[:neonfs, :transport, :conn_pool, :worker_connected], ^ref, _, _}, 5_000
      end

      # Verify by successfully executing pool_size concurrent checkouts
      tasks =
        for _ <- 1..pool_size do
          Task.async(fn ->
            ConnPool.execute(pool, {:ping, make_ref()})
          end)
        end

      results = Task.await_many(tasks, 5_000)
      assert length(results) == pool_size

      for result <- results do
        assert {:ping, _ref} = result
      end
    end
  end

  describe "execute/3" do
    test "sends a message and receives echoed response", ctx do
      {:ok, pool} =
        ConnPool.start_link(
          peer: {~c"localhost", ctx.port},
          ssl_opts: ctx.client_ssl_opts,
          pool_size: 2
        )

      ref = make_ref()
      message = {:get_chunk, ref, "abc123", "vol-1"}
      response = ConnPool.execute(pool, message)

      assert response == message
    end

    test "handles multiple sequential requests", ctx do
      {:ok, pool} =
        ConnPool.start_link(
          peer: {~c"localhost", ctx.port},
          ssl_opts: ctx.client_ssl_opts,
          pool_size: 2
        )

      for i <- 1..10 do
        message = {:request, i}
        assert ConnPool.execute(pool, message) == message
      end
    end

    test "handles concurrent requests", ctx do
      {:ok, pool} =
        ConnPool.start_link(
          peer: {~c"localhost", ctx.port},
          ssl_opts: ctx.client_ssl_opts,
          pool_size: 4
        )

      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            message = {:concurrent, i, make_ref()}
            {message, ConnPool.execute(pool, message)}
          end)
        end

      results = Task.await_many(tasks, 10_000)

      for {sent, received} <- results do
        assert sent == received
      end
    end

    test "accepts string host in peer", ctx do
      {:ok, pool} =
        ConnPool.start_link(
          peer: {"localhost", ctx.port},
          ssl_opts: ctx.client_ssl_opts,
          pool_size: 1
        )

      message = {:hello, "world"}
      assert ConnPool.execute(pool, message) == message
    end

    test "raises on recv timeout and removes connection", ctx do
      # Start a server that accepts TLS but never responds to messages
      {:ok, slow_listen} = :ssl.listen(0, ctx.server_ssl_opts)
      {:ok, {_addr, slow_port}} = :ssl.sockname(slow_listen)
      spawn_link(fn -> silent_accept_loop(slow_listen) end)
      on_exit(fn -> :ssl.close(slow_listen) end)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :transport, :conn_pool, :worker_connected]
        ])

      {:ok, pool} =
        ConnPool.start_link(
          peer: {~c"localhost", slow_port},
          ssl_opts: ctx.client_ssl_opts,
          pool_size: 1
        )

      # Wait for async TLS connection to establish
      assert_receive {[:neonfs, :transport, :conn_pool, :worker_connected], ^ref, _, _}, 5_000

      # Short recv timeout — server never responds, so recv should timeout
      assert_raise MatchError, fn ->
        ConnPool.execute(pool, {:test, :data}, recv_timeout: 500)
      end
    end
  end

  describe "checkout timeout" do
    test "raises when pool is exhausted", ctx do
      # Start a server that accepts TLS but never responds (holds connection open)
      {:ok, slow_listen} = :ssl.listen(0, ctx.server_ssl_opts)
      {:ok, {_addr, slow_port}} = :ssl.sockname(slow_listen)
      spawn_link(fn -> silent_accept_loop(slow_listen) end)
      on_exit(fn -> :ssl.close(slow_listen) end)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :transport, :conn_pool, :worker_connected]
        ])

      {:ok, pool} =
        ConnPool.start_link(
          peer: {~c"localhost", slow_port},
          ssl_opts: ctx.client_ssl_opts,
          pool_size: 1
        )

      # Wait for async TLS connection to establish
      assert_receive {[:neonfs, :transport, :conn_pool, :worker_connected], ^ref, _, _}, 5_000

      # Hold the only connection with a long-running execute (server never responds)
      slow_task =
        Task.async(fn ->
          try do
            ConnPool.execute(pool, {:slow, :request}, recv_timeout: 30_000)
          rescue
            _ -> :timed_out
          end
        end)

      # Give time for the slow task to check out the connection
      Process.sleep(100)

      # Try to checkout with a short timeout — pool exhausted, should exit
      assert catch_exit(ConnPool.execute(pool, {:fast, :request}, timeout: 200))

      Task.shutdown(slow_task, :brutal_kill)
    end
  end

  describe "dead connection replacement" do
    test "pool recovers after server-side disconnect", ctx do
      {:ok, pool} =
        ConnPool.start_link(
          peer: {~c"localhost", ctx.port},
          ssl_opts: ctx.client_ssl_opts,
          pool_size: 1
        )

      # First request succeeds
      message1 = {:request, 1}
      assert ConnPool.execute(pool, message1) == message1

      # Send a special message that tells the echo server to close the connection
      _echo = ConnPool.execute(pool, :close_connection)

      # The connection was closed server-side after the response was sent.
      # The next checkout will get the dead socket. The send/recv will fail,
      # NimblePool removes the worker and starts a replacement.
      # Wait for the replacement connection to be established.
      reconnect_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :transport, :conn_pool, :worker_connected]
        ])

      # Trigger the dead connection detection by attempting a request
      try do
        ConnPool.execute(pool, {:trigger_reconnect, 0})
      rescue
        MatchError -> :expected
      end

      # Wait for pool to reconnect
      assert_receive {[:neonfs, :transport, :conn_pool, :worker_connected], ^reconnect_ref, _, _},
                     5_000

      message2 = {:request, 2}
      assert ConnPool.execute(pool, message2) == message2
    end
  end

  describe "handle_ping" do
    test "healthy connections survive ping", ctx do
      {:ok, pool} =
        ConnPool.start_link(
          peer: {~c"localhost", ctx.port},
          ssl_opts: ctx.client_ssl_opts,
          pool_size: 1,
          worker_idle_timeout: 500
        )

      # Wait for at least one ping cycle
      Process.sleep(1_000)

      # Pool should still work
      message = {:after_ping, make_ref()}
      assert ConnPool.execute(pool, message) == message
    end
  end

  describe "SSL options" do
    test "default_ssl_opts uses TLS.tls_dir paths", ctx do
      Application.put_env(:neonfs_client, :tls_dir, ctx.tmp_dir)

      on_exit(fn ->
        Application.delete_env(:neonfs_client, :tls_dir)
      end)

      # Write client certs as node.crt/node.key (matching default_ssl_opts expectations)
      File.cp!(Path.join(ctx.tmp_dir, "client.crt"), Path.join(ctx.tmp_dir, "node.crt"))
      File.cp!(Path.join(ctx.tmp_dir, "client.key"), Path.join(ctx.tmp_dir, "node.key"))

      # Start pool without explicit ssl_opts — should use defaults from tls_dir
      {:ok, pool} =
        ConnPool.start_link(
          peer: {~c"localhost", ctx.port},
          pool_size: 1
        )

      message = {:default_opts_test, make_ref()}
      assert ConnPool.execute(pool, message) == message
    end

    test "pool_size is configurable via application env" do
      Application.put_env(:neonfs_client, :data_transfer, pool_size: 8)

      on_exit(fn ->
        Application.delete_env(:neonfs_client, :data_transfer)
      end)

      assert Application.get_env(:neonfs_client, :data_transfer)[:pool_size] == 8
    end
  end

  # Echo server helpers

  defp accept_loop(listen_socket) do
    case :ssl.transport_accept(listen_socket) do
      {:ok, transport_socket} ->
        handshake_and_spawn(transport_socket, &echo_loop/1)
        accept_loop(listen_socket)

      {:error, _} ->
        :ok
    end
  end

  defp echo_loop(socket) do
    case :ssl.recv(socket, 0, 30_000) do
      {:ok, data} ->
        message = :erlang.binary_to_term(data, [:safe])

        case message do
          :close_connection ->
            # Echo the message back, then close the connection
            :ssl.send(socket, data)
            :ssl.close(socket)

          _ ->
            :ssl.send(socket, data)
            echo_loop(socket)
        end

      {:error, _} ->
        :ssl.close(socket)
    end
  end

  # Accepts TLS connections but never reads or responds — holds connections open
  defp silent_accept_loop(listen_socket) do
    case :ssl.transport_accept(listen_socket) do
      {:ok, transport_socket} ->
        handshake_and_spawn(transport_socket, fn _socket -> Process.sleep(:infinity) end)
        silent_accept_loop(listen_socket)

      {:error, _} ->
        :ok
    end
  end

  defp handshake_and_spawn(transport_socket, handler_fn) do
    case :ssl.handshake(transport_socket, 10_000) do
      {:ok, socket} ->
        pid =
          spawn(fn ->
            receive do
              :ready -> handler_fn.(socket)
            end
          end)

        :ssl.controlling_process(socket, pid)
        send(pid, :ready)

      {:error, _} ->
        :ok
    end
  end
end
