defmodule NeonFS.Transport.ListenerTest do
  use ExUnit.Case, async: false

  alias NeonFS.Transport.{HandlerSupervisor, Listener, StubBlobStore, TLS}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    {ca_cert, ca_key} = TLS.generate_ca("listener-test")

    server_key = TLS.generate_node_key()
    server_csr = TLS.create_csr(server_key, "listener-server")
    server_cert = TLS.sign_csr(server_csr, "localhost", ca_cert, ca_key, 100)

    client_key = TLS.generate_node_key()
    client_csr = TLS.create_csr(client_key, "listener-client")
    client_cert = TLS.sign_csr(client_csr, "localhost", ca_cert, ca_key, 101)

    {wrong_ca_cert, _wrong_ca_key} = TLS.generate_ca("wrong-ca")

    ca_certfile = Path.join(tmp_dir, "ca.crt")
    server_certfile = Path.join(tmp_dir, "server.crt")
    server_keyfile = Path.join(tmp_dir, "server.key")
    client_certfile = Path.join(tmp_dir, "client.crt")
    client_keyfile = Path.join(tmp_dir, "client.key")
    wrong_ca_certfile = Path.join(tmp_dir, "wrong_ca.crt")

    File.write!(ca_certfile, TLS.encode_cert(ca_cert))
    File.write!(server_certfile, TLS.encode_cert(server_cert))
    File.write!(server_keyfile, TLS.encode_key(server_key))
    File.write!(client_certfile, TLS.encode_cert(client_cert))
    File.write!(client_keyfile, TLS.encode_key(client_key))
    File.write!(wrong_ca_certfile, TLS.encode_cert(wrong_ca_cert))

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

    client_ssl_opts = [
      {:certs_keys, [%{certfile: client_certfile, keyfile: client_keyfile}]},
      {:cacertfile, ca_certfile},
      {:verify, :verify_peer},
      {:versions, [:"tlsv1.3"]},
      :binary,
      {:packet, 4},
      {:active, false}
    ]

    unique = System.unique_integer([:positive])
    sup_name = :"handler_sup_#{unique}"
    listener_name = :"listener_#{unique}"

    start_supervised!({HandlerSupervisor, name: sup_name})
    start_supervised!(StubBlobStore)

    %{
      server_ssl_opts: server_ssl_opts,
      client_ssl_opts: client_ssl_opts,
      wrong_ca_certfile: wrong_ca_certfile,
      client_certfile: client_certfile,
      client_keyfile: client_keyfile,
      ca_certfile: ca_certfile,
      sup_name: sup_name,
      listener_name: listener_name
    }
  end

  describe "start_link/1" do
    test "binds to an OS-assigned port", ctx do
      port = start_listener(ctx)
      assert port > 0
    end

    test "registers with given name", ctx do
      start_listener(ctx)
      assert GenServer.whereis(ctx.listener_name) != nil
    end
  end

  describe "get_port/1" do
    test "returns the actual listening port", ctx do
      port = start_listener(ctx)
      assert is_integer(port)
      assert port > 0
    end
  end

  describe "accepting connections" do
    test "accepts a TLS connection", ctx do
      port = start_listener(ctx)

      assert {:ok, client} = :ssl.connect(~c"localhost", port, ctx.client_ssl_opts)

      :ssl.close(client)
    end

    test "accepts multiple concurrent connections", ctx do
      port = start_listener(ctx)

      clients =
        for _ <- 1..4 do
          {:ok, client} = :ssl.connect(~c"localhost", port, ctx.client_ssl_opts)
          client
        end

      assert length(clients) == 4

      for client <- clients do
        :ssl.close(client)
      end
    end

    test "failed TLS handshake does not crash acceptor", ctx do
      port = start_listener(ctx)

      wrong_client_opts = [
        {:certs_keys, [%{certfile: ctx.client_certfile, keyfile: ctx.client_keyfile}]},
        {:cacertfile, ctx.wrong_ca_certfile},
        {:verify, :verify_peer},
        {:versions, [:"tlsv1.3"]},
        :binary,
        {:packet, 4},
        {:active, false}
      ]

      assert {:error, _} = :ssl.connect(~c"localhost", port, wrong_client_opts, 5_000)

      # Wait for the failed handshake to process
      Process.sleep(200)

      # Listener should still be alive
      assert Process.alive?(GenServer.whereis(ctx.listener_name))

      # Good client should still connect
      {:ok, client} = :ssl.connect(~c"localhost", port, ctx.client_ssl_opts)
      :ssl.close(client)
    end
  end

  describe "message dispatch" do
    test "dispatches put_chunk and returns response", ctx do
      port = start_listener(ctx)

      {:ok, client} = :ssl.connect(~c"localhost", port, ctx.client_ssl_opts)
      Process.sleep(100)

      ref = make_ref()
      message = {:put_chunk, ref, <<0::256>>, "vol-1", "w-1", :hot, "hello"}
      :ok = :ssl.send(client, :erlang.term_to_binary(message))
      {:ok, response_bytes} = :ssl.recv(client, 0, 5_000)
      response = :erlang.binary_to_term(response_bytes, [:safe])

      assert {:ok, ^ref} = response

      :ssl.close(client)
    end

    test "dispatches get_chunk and returns response", ctx do
      data = "get chunk test data"
      hash = :crypto.hash(:sha256, data)
      StubBlobStore.seed(hash, data)

      port = start_listener(ctx)

      {:ok, client} = :ssl.connect(~c"localhost", port, ctx.client_ssl_opts)
      Process.sleep(100)

      ref = make_ref()
      message = {:get_chunk, ref, hash, "vol-1"}
      :ok = :ssl.send(client, :erlang.term_to_binary(message))
      {:ok, response_bytes} = :ssl.recv(client, 0, 5_000)
      response = :erlang.binary_to_term(response_bytes, [:safe])

      assert {:ok, ^ref, ^data} = response

      :ssl.close(client)
    end

    test "dispatches has_chunk and returns response", ctx do
      data = "has chunk test"
      hash = :crypto.hash(:sha256, data)
      StubBlobStore.seed(hash, data, :hot)

      port = start_listener(ctx)

      {:ok, client} = :ssl.connect(~c"localhost", port, ctx.client_ssl_opts)
      Process.sleep(100)

      ref = make_ref()
      message = {:has_chunk, ref, hash}
      :ok = :ssl.send(client, :erlang.term_to_binary(message))
      {:ok, response_bytes} = :ssl.recv(client, 0, 5_000)
      response = :erlang.binary_to_term(response_bytes, [:safe])

      assert {:ok, ^ref, :hot, size} = response
      assert size == byte_size(data)

      :ssl.close(client)
    end
  end

  # Helpers

  defp start_listener(ctx, extra_opts \\ []) do
    opts =
      [
        port: 0,
        ssl_opts: ctx.server_ssl_opts,
        handler_supervisor: ctx.sup_name,
        dispatch_module: StubBlobStore,
        name: ctx.listener_name
      ] ++ extra_opts

    start_supervised!({Listener, opts})
    Listener.get_port(ctx.listener_name)
  end
end
