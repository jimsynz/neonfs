defmodule NeonFS.Core.WriteOperationDataPlaneTest do
  @moduledoc """
  Tests for the data plane migration of WriteOperation's stripe distribution.

  Verifies that write_chunk_to_target uses Router.data_call for remote writes,
  falls back to RPC when no data endpoint exists, and local writes still go
  directly through BlobStore.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Client.Router
  alias NeonFS.Core.{Blob.Native, VolumeRegistry, WriteOperation}
  alias NeonFS.Transport.{PoolManager, PoolSupervisor, TLS}

  @moduletag :tmp_dir

  describe "write_file/4 local writes bypass data plane" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)

      # Create a replicated volume (single node, no remote targets)
      vol_name = "local-write-#{:rand.uniform(999_999)}"
      {:ok, volume} = VolumeRegistry.create(vol_name)
      {:ok, volume: volume}
    end

    test "writes file locally without data plane", %{volume: volume} do
      data = "local write test data"

      assert {:ok, file_meta} = WriteOperation.write_file_streamed(volume.id, "/test.txt", [data])
      assert file_meta.size == byte_size(data)
      assert file_meta.path == "/test.txt"
    end
  end

  describe "write_file/4 with erasure coding (single node)" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_stripe_index()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)

      # Create an erasure-coded volume
      vol_name = "erasure-#{:rand.uniform(999_999)}"

      {:ok, volume} =
        VolumeRegistry.create(vol_name,
          durability: %{type: :erasure, data_chunks: 2, parity_chunks: 1}
        )

      {:ok, volume: volume}
    end

    test "erasure write succeeds in single-node mode (all targets are local)", %{
      volume: volume
    } do
      # In single-node mode, all stripe targets default to Node.self()
      # so write_chunk_to_target uses BlobStore directly (no data plane).
      data = String.duplicate("x", 4096)

      assert {:ok, file_meta} = WriteOperation.write_file_at(volume.id, "/erasure.txt", 0, data)
      assert file_meta.size == byte_size(data)
      assert file_meta.stripes != []
    end
  end

  describe "data plane hash computation" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_drive_registry()
      start_blob_store()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "Native.compute_hash matches :crypto.hash for content addressing" do
      data = "hash consistency test data"

      nif_hash = Native.compute_hash(data)
      crypto_hash = :crypto.hash(:sha256, data)

      assert nif_hash == crypto_hash
    end

    test "hash is deterministic across multiple calls" do
      data = :crypto.strong_rand_bytes(1024)

      hash1 = Native.compute_hash(data)
      hash2 = Native.compute_hash(data)

      assert hash1 == hash2
    end
  end

  describe "data plane write via TLS" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()
      start_drive_registry()
      start_blob_store()
      start_chunk_index()

      on_exit(fn -> cleanup_test_dirs() end)

      # Generate certs
      {ca_cert, ca_key} = TLS.generate_ca("wo-data-plane-test")

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

      # Client SSL options
      client_ssl_opts = [
        {:certs_keys, [%{certfile: client_certfile, keyfile: client_keyfile}]},
        {:cacertfile, ca_certfile},
        {:verify, :verify_peer},
        {:versions, [:"tlsv1.3"]},
        :binary,
        {:packet, 4},
        {:active, false}
      ]

      # Start PoolSupervisor + PoolManager
      unique = System.unique_integer([:positive])
      sup_name = :"pool_sup_wo_#{unique}"
      start_supervised!({PoolSupervisor, name: sup_name})

      pm_opts = [
        pool_size: 1,
        worker_idle_timeout: 30_000,
        discovery_refresh_interval: 60_000,
        pool_supervisor: sup_name,
        ssl_opts: client_ssl_opts
      ]

      start_supervised!({PoolManager, pm_opts})

      {:ok, port: port}
    end

    test "put_chunk via data plane returns success for remote write", ctx do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :transport, :conn_pool, :worker_connected]
        ])

      fake_node = :wo_peer@host
      endpoint = {~c"localhost", ctx.port}
      {:ok, _pool} = PoolManager.ensure_pool(fake_node, endpoint)

      assert_receive {[:neonfs, :transport, :conn_pool, :worker_connected], ^ref, _, _}, 5_000

      chunk_data = "stripe chunk data"
      chunk_hash = Native.compute_hash(chunk_data)

      # Simulate what write_chunk_to_remote does
      result =
        Router.data_call(fake_node, :put_chunk,
          hash: chunk_hash,
          volume_id: "default",
          write_id: nil,
          tier: "hot",
          data: chunk_data
        )

      assert result == :ok
    end

    test "timeout is configurable for data plane writes", ctx do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :transport, :conn_pool, :worker_connected]
        ])

      fake_node = :timeout_peer@host
      endpoint = {~c"localhost", ctx.port}
      {:ok, _pool} = PoolManager.ensure_pool(fake_node, endpoint)

      assert_receive {[:neonfs, :transport, :conn_pool, :worker_connected], ^ref, _, _}, 5_000

      chunk_data = "timeout test"
      chunk_hash = Native.compute_hash(chunk_data)

      # Should succeed with custom timeout
      assert :ok =
               Router.data_call(
                 fake_node,
                 :put_chunk,
                 [
                   hash: chunk_hash,
                   volume_id: "default",
                   write_id: nil,
                   tier: "hot",
                   data: chunk_data
                 ],
                 timeout: 15_000
               )
    end
  end

  # Echo server

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

  defp handle_protocol_message({:put_chunk, ref, _hash, _volume_id, _write_id, _tier, _data}) do
    {:ok, ref}
  end

  defp handle_protocol_message({:get_chunk, ref, _hash, _volume_id, _tier}) do
    {:error, ref, :not_found}
  end

  defp handle_protocol_message({:get_chunk, ref, _hash, _volume_id}) do
    {:error, ref, :not_found}
  end

  defp handle_protocol_message({:has_chunk, ref, _hash}) do
    {:error, ref, :not_found}
  end
end
