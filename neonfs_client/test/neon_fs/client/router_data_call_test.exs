defmodule NeonFS.Client.RouterDataCallTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.Router
  alias NeonFS.Transport.{PoolManager, PoolSupervisor, TLS}

  @moduletag :tmp_dir

  # Tests that mock PoolManager via ETS directly (no real TLS)

  describe "data_call/4 with no pool" do
    test "returns {:error, :no_data_endpoint} when no pool exists" do
      assert {:error, :no_data_endpoint} =
               Router.data_call(:unknown@host, :get_chunk, hash: "abc", volume_id: "vol1")
    end
  end

  # Tests using a real TLS echo server + PoolManager + ConnPool

  setup %{tmp_dir: tmp_dir} do
    # Generate CA + server + client certs
    {ca_cert, ca_key} = TLS.generate_ca("data-call-test")

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

    # Start TLS echo server that echoes protocol responses
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
    test_pid = self()
    spawn_link(fn -> accept_loop(listen_socket, test_pid) end)

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
    sup_name = :"pool_sup_dc_#{unique}"
    start_supervised!({PoolSupervisor, name: sup_name})

    # Start PoolManager
    pm_opts = [
      pool_size: 1,
      worker_idle_timeout: 30_000,
      discovery_refresh_interval: 60_000,
      pool_supervisor: sup_name,
      ssl_opts: client_ssl_opts
    ]

    start_supervised!({PoolManager, pm_opts})

    %{
      port: port,
      client_ssl_opts: client_ssl_opts,
      pool_supervisor: sup_name
    }
  end

  describe "data_call/4 with :put_chunk" do
    test "builds correct message and returns :ok on success", ctx do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :transport, :conn_pool, :worker_connected]
        ])

      endpoint = {~c"localhost", ctx.port}
      {:ok, _pool} = PoolManager.ensure_pool(:put_peer@host, endpoint)

      assert_receive {[:neonfs, :transport, :conn_pool, :worker_connected], ^ref, _, _}, 5_000

      assert :ok =
               Router.data_call(:put_peer@host, :put_chunk,
                 hash: "sha256:abc",
                 volume_id: "vol1",
                 write_id: "w1",
                 tier: :hot,
                 data: "chunk_bytes"
               )
    end
  end

  describe "data_call/4 with :get_chunk" do
    test "returns {:ok, chunk_bytes} on success", ctx do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :transport, :conn_pool, :worker_connected]
        ])

      endpoint = {~c"localhost", ctx.port}
      {:ok, _pool} = PoolManager.ensure_pool(:get_peer@host, endpoint)

      assert_receive {[:neonfs, :transport, :conn_pool, :worker_connected], ^ref, _, _}, 5_000

      assert {:ok, "chunk_data_here"} =
               Router.data_call(:get_peer@host, :get_chunk,
                 hash: "sha256:def",
                 volume_id: "vol2"
               )
    end
  end

  describe "data_call/4 with :has_chunk" do
    test "returns {:ok, %{tier: tier, size: size}} on success", ctx do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :transport, :conn_pool, :worker_connected]
        ])

      endpoint = {~c"localhost", ctx.port}
      {:ok, _pool} = PoolManager.ensure_pool(:has_peer@host, endpoint)

      assert_receive {[:neonfs, :transport, :conn_pool, :worker_connected], ^ref, _, _}, 5_000

      assert {:ok, %{tier: :hot, size: 1024}} =
               Router.data_call(:has_peer@host, :has_chunk, hash: "sha256:ghi")
    end
  end

  describe "data_call/4 error responses" do
    test "returns {:error, reason} when remote returns error", ctx do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :transport, :conn_pool, :worker_connected]
        ])

      endpoint = {~c"localhost", ctx.port}
      {:ok, _pool} = PoolManager.ensure_pool(:err_peer@host, endpoint)

      assert_receive {[:neonfs, :transport, :conn_pool, :worker_connected], ^ref, _, _}, 5_000

      # get_chunk with hash starting with "notfound:" triggers :not_found in our echo server
      assert {:error, :not_found} =
               Router.data_call(:err_peer@host, :get_chunk,
                 hash: "notfound:xyz",
                 volume_id: "vol1"
               )
    end

    test "returns {:error, :ref_mismatch} when response ref doesn't match", ctx do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :transport, :conn_pool, :worker_connected]
        ])

      endpoint = {~c"localhost", ctx.port}
      {:ok, _pool} = PoolManager.ensure_pool(:mismatch_peer@host, endpoint)

      assert_receive {[:neonfs, :transport, :conn_pool, :worker_connected], ^ref, _, _}, 5_000

      # get_chunk with hash starting with "badref:" triggers a wrong ref response
      assert {:error, :ref_mismatch} =
               Router.data_call(:mismatch_peer@host, :get_chunk,
                 hash: "badref:xyz",
                 volume_id: "vol1"
               )
    end
  end

  # Protocol-aware echo server helpers
  # This server understands the data transfer protocol and returns appropriate responses

  defp accept_loop(listen_socket, test_pid) do
    case :ssl.transport_accept(listen_socket) do
      {:ok, transport_socket} ->
        handshake_and_spawn(transport_socket, test_pid)
        accept_loop(listen_socket, test_pid)

      {:error, _} ->
        :ok
    end
  end

  defp handshake_and_spawn(transport_socket, _test_pid) do
    case :ssl.handshake(transport_socket, 10_000) do
      {:ok, socket} ->
        pid =
          spawn(fn ->
            receive do
              :ready -> protocol_loop(socket)
            end
          end)

        :ssl.controlling_process(socket, pid)
        send(pid, :ready)

      {:error, _} ->
        :ok
    end
  end

  defp protocol_loop(socket) do
    case :ssl.recv(socket, 0, 30_000) do
      {:ok, data} ->
        message = :erlang.binary_to_term(data, [:safe])
        response = handle_protocol_message(message)
        :ssl.send(socket, :erlang.term_to_binary(response))
        protocol_loop(socket)

      {:error, _} ->
        :ssl.close(socket)
    end
  end

  # Simulate the Handler's protocol responses
  defp handle_protocol_message({:put_chunk, ref, _hash, _volume_id, _write_id, _tier, _data}) do
    {:ok, ref}
  end

  defp handle_protocol_message({:get_chunk, ref, "notfound:" <> _, _volume_id, _tier}) do
    {:error, ref, :not_found}
  end

  defp handle_protocol_message({:get_chunk, _ref, "badref:" <> _, _volume_id, _tier}) do
    {:ok, make_ref(), "wrong_data"}
  end

  defp handle_protocol_message({:get_chunk, ref, _hash, _volume_id, _tier}) do
    {:ok, ref, "chunk_data_here"}
  end

  defp handle_protocol_message({:get_chunk, ref, "notfound:" <> _, _volume_id}) do
    {:error, ref, :not_found}
  end

  defp handle_protocol_message({:get_chunk, _ref, "badref:" <> _, _volume_id}) do
    {:ok, make_ref(), "wrong_data"}
  end

  defp handle_protocol_message({:get_chunk, ref, _hash, _volume_id}) do
    {:ok, ref, "chunk_data_here"}
  end

  defp handle_protocol_message({:has_chunk, ref, _hash}) do
    {:ok, ref, :hot, 1024}
  end
end
