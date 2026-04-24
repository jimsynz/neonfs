defmodule NeonFS.Transport.HandlerTest do
  use ExUnit.Case, async: false

  alias NeonFS.Transport.{Handler, StubBlobStore, TLS}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    {ca_cert, ca_key} = TLS.generate_ca("handler-test")

    server_key = TLS.generate_node_key()
    server_csr = TLS.create_csr(server_key, "handler-server")
    server_cert = TLS.sign_csr(server_csr, "localhost", ca_cert, ca_key, 100)

    client_key = TLS.generate_node_key()
    client_csr = TLS.create_csr(client_key, "handler-client")
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

    {:ok, listen_socket} = :ssl.listen(0, server_ssl_opts)
    {:ok, {_addr, port}} = :ssl.sockname(listen_socket)

    start_supervised!(StubBlobStore)

    on_exit(fn -> :ssl.close(listen_socket) end)

    %{
      listen_socket: listen_socket,
      port: port,
      client_ssl_opts: client_ssl_opts
    }
  end

  describe "put_chunk dispatch" do
    test "7-tuple (legacy) stores a chunk as-is and returns {:ok, ref}", ctx do
      {client, _handler} = connect_and_start_handler(ctx)

      ref = make_ref()
      message = {:put_chunk, ref, <<0::256>>, "vol-1", "write-1", :hot, "hello world"}
      response = send_and_recv(client, message)

      assert {:ok, ^ref} = response

      # The legacy path invokes write_chunk with empty opts — no
      # compression / encryption resolution takes place. The stub
      # records the opts it received for inspection.
      assert StubBlobStore.last_write_opts() == []

      :ssl.close(client)
    end

    test "8-tuple (new) triggers volume-opts resolution before write", ctx do
      # Seed a stub volume with zstd compression configured.
      StubBlobStore.put_volume_opts("vol-1", compression: "zstd", compression_level: 5)

      {client, _handler} = connect_and_start_handler(ctx)

      ref = make_ref()
      payload = "plaintext payload"

      message =
        {:put_chunk, ref, <<0::256>>, "vol-1", "drive-1", "write-1", :hot, payload}

      response = send_and_recv(client, message)

      assert {:ok, ^ref, codec} = response
      assert codec.compression == :zstd
      assert codec.crypto == nil
      assert codec.original_size == byte_size(payload)

      # The new path invokes write_chunk with the resolved volume opts.
      assert StubBlobStore.last_write_opts() == [compression: "zstd", compression_level: 5]

      :ssl.close(client)
    end

    test "8-tuple with unknown volume_id falls back to empty opts", ctx do
      # No volume-opts seeded — stub's resolver returns [] for unknown
      # volume ids, matching the production BlobStore behaviour.
      {client, _handler} = connect_and_start_handler(ctx)

      ref = make_ref()

      message =
        {:put_chunk, ref, <<0::256>>, "vol-unknown", "drive-1", "write-1", :hot, "data"}

      response = send_and_recv(client, message)

      assert {:ok, ^ref, %{compression: :none, crypto: nil, original_size: 4}} = response
      assert StubBlobStore.last_write_opts() == []

      :ssl.close(client)
    end

    test "8-tuple response carries a :locations list for the replica fan-out (#478)", ctx do
      # Dispatches that don't export replicate_after_put/5 (the stub
      # falls in this category) yield a single-entry `:locations`
      # list: the just-written local node. The handler always
      # populates the key so the interface-side ChunkWriter can rely
      # on it.
      {client, _handler} = connect_and_start_handler(ctx)

      ref = make_ref()

      message =
        {:put_chunk, ref, <<0::256>>, "vol-fallback", "drive-x", "write-1", :hot, "bytes"}

      response = send_and_recv(client, message)

      assert {:ok, ^ref, %{locations: [location]}} = response
      assert location.node == Node.self()
      assert location.drive_id == "drive-x"
      assert location.tier == :hot

      :ssl.close(client)
    end

    test "8-tuple propagates {:error, _} from resolve_put_chunk_opts/1 without writing", ctx do
      # Encrypted volume whose KeyManager.get_current_key/1 would fail
      # surfaces `{:error, reason}` from resolve_put_chunk_opts/1 in
      # production. The handler must abort the write and send the reason
      # back — storing plaintext on an encrypted volume would leak data.
      StubBlobStore.put_volume_opts("vol-missing-key", {:error, :key_not_found})

      {client, _handler} = connect_and_start_handler(ctx)

      # Leave a sentinel on `last_write_opts` so we can confirm no
      # write happened during the failing dispatch — any write call
      # from the handler would overwrite this marker.
      StubBlobStore.put_volume_opts("vol-ok", marker: :pre_test)
      ref_ok = make_ref()
      ok_msg = {:put_chunk, ref_ok, <<0::256>>, "vol-ok", "drive-1", "write-ok", :hot, "ok data"}
      assert {:ok, ^ref_ok, _codec} = send_and_recv(client, ok_msg)
      assert StubBlobStore.last_write_opts() == [marker: :pre_test]

      ref = make_ref()

      message =
        {:put_chunk, ref, <<0::256>>, "vol-missing-key", "drive-1", "write-1", :hot,
         "plaintext should never land"}

      response = send_and_recv(client, message)

      assert {:error, ^ref, :key_not_found} = response

      # The sentinel from the previous successful dispatch is still in
      # place — confirming the handler did NOT call write_chunk for the
      # failing volume.
      assert StubBlobStore.last_write_opts() == [marker: :pre_test]

      :ssl.close(client)
    end
  end

  describe "get_chunk dispatch" do
    test "returns chunk data for existing chunk", ctx do
      data = "test chunk data"
      hash = :crypto.hash(:sha256, data)
      StubBlobStore.seed(hash, data)

      {client, _handler} = connect_and_start_handler(ctx)

      ref = make_ref()
      message = {:get_chunk, ref, hash, "vol-1"}
      response = send_and_recv(client, message)

      assert {:ok, ^ref, ^data} = response

      :ssl.close(client)
    end

    test "returns {:error, ref, :not_found} for missing chunk", ctx do
      {client, _handler} = connect_and_start_handler(ctx)

      ref = make_ref()
      missing_hash = :crypto.hash(:sha256, "nonexistent")
      message = {:get_chunk, ref, missing_hash, "vol-1"}
      response = send_and_recv(client, message)

      assert {:error, ^ref, :not_found} = response

      :ssl.close(client)
    end

    test "supports 5-element get_chunk with tier", ctx do
      data = "tier-aware chunk"
      hash = :crypto.hash(:sha256, data)
      StubBlobStore.seed(hash, data)

      {client, _handler} = connect_and_start_handler(ctx)

      ref = make_ref()
      message = {:get_chunk, ref, hash, "vol-1", "warm"}
      response = send_and_recv(client, message)

      assert {:ok, ^ref, ^data} = response

      :ssl.close(client)
    end
  end

  describe "has_chunk dispatch" do
    test "returns tier and size for existing chunk", ctx do
      data = "chunk for has_chunk"
      hash = :crypto.hash(:sha256, data)
      StubBlobStore.seed(hash, data, :hot)

      {client, _handler} = connect_and_start_handler(ctx)

      ref = make_ref()
      message = {:has_chunk, ref, hash}
      response = send_and_recv(client, message)

      assert {:ok, ^ref, :hot, size} = response
      assert size == byte_size(data)

      :ssl.close(client)
    end

    test "returns {:error, ref, :not_found} for missing chunk", ctx do
      {client, _handler} = connect_and_start_handler(ctx)

      ref = make_ref()
      missing_hash = :crypto.hash(:sha256, "not here")
      message = {:has_chunk, ref, missing_hash}
      response = send_and_recv(client, message)

      assert {:error, ^ref, :not_found} = response

      :ssl.close(client)
    end
  end

  describe "connection lifecycle" do
    test "stops on ssl_closed", ctx do
      {client, handler} = connect_and_start_handler(ctx)

      monitor = Process.monitor(handler)
      :ssl.close(client)

      assert_receive {:DOWN, ^monitor, :process, ^handler, :normal}, 5_000
    end

    test "re-arms active mode after processing batch", ctx do
      {client, _handler} = connect_and_start_handler(ctx)

      # Send more than @active_n (10) messages to trigger ssl_passive
      for i <- 1..15 do
        ref = make_ref()
        message = {:put_chunk, ref, <<i::256>>, "vol-1", "w-#{i}", :hot, "chunk_#{i}"}
        :ok = :ssl.send(client, :erlang.term_to_binary(message))
      end

      # All should get responses (proving ssl_passive was re-armed)
      for _ <- 1..15 do
        {:ok, response_bytes} = :ssl.recv(client, 0, 5_000)
        response = :erlang.binary_to_term(response_bytes, [:safe])
        assert {:ok, _ref} = response
      end

      :ssl.close(client)
    end
  end

  # Helpers

  defp connect_and_start_handler(ctx) do
    parent = self()

    task =
      Task.async(fn ->
        {:ok, client} = :ssl.connect(~c"localhost", ctx.port, ctx.client_ssl_opts)
        # Transfer controlling process to the test process before this Task exits,
        # otherwise the socket closes when the Task terminates.
        :ok = :ssl.controlling_process(client, parent)
        client
      end)

    {:ok, transport_socket} = :ssl.transport_accept(ctx.listen_socket)
    {:ok, server_socket} = :ssl.handshake(transport_socket, 10_000)

    client_socket = Task.await(task)

    {:ok, handler} =
      GenServer.start_link(Handler,
        socket: server_socket,
        dispatch_module: StubBlobStore
      )

    :ssl.controlling_process(server_socket, handler)
    send(handler, :activate)

    {client_socket, handler}
  end

  defp send_and_recv(client_socket, message) do
    :ok = :ssl.send(client_socket, :erlang.term_to_binary(message))
    {:ok, response_bytes} = :ssl.recv(client_socket, 0, 5_000)
    :erlang.binary_to_term(response_bytes, [:safe])
  end
end
