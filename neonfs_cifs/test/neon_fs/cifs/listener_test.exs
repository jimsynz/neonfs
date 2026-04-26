defmodule NeonFS.CIFS.ListenerTest do
  use ExUnit.Case, async: true

  alias NeonFS.CIFS.Listener

  doctest Listener

  defmodule EchoHandler do
    @moduledoc false
    use ThousandIsland.Handler

    @impl ThousandIsland.Handler
    def handle_data(data, socket, state) do
      ThousandIsland.Socket.send(socket, data)
      {:continue, state}
    end
  end

  describe "encode/1" do
    test "prefixes a 4-byte big-endian length to the ETF body" do
      iodata = Listener.encode({:hello, "world"})
      bytes = IO.iodata_to_binary(iodata)

      <<len::big-32, body::binary>> = bytes
      assert byte_size(body) == len
      assert :erlang.binary_to_term(body) == {:hello, "world"}
    end

    test "round-trips a complex term verbatim through decode/1" do
      term = %{volume: "vol-a", entries: [{1, "a"}, {2, "b"}], eof: true}
      <<_len::big-32, body::binary>> = IO.iodata_to_binary(Listener.encode(term))
      assert {:ok, ^term} = Listener.decode(body)
    end
  end

  describe "decode/1" do
    test "returns :badetf on garbage" do
      assert {:error, :badetf} = Listener.decode(<<0, 0, 0, 0>>)
    end
  end

  describe "child_spec/1 with :socket_path" do
    setup do
      # AF_UNIX `sun_path` is capped at ~108 bytes, so we can't use
      # ExUnit's `:tmp_dir` (which nests under the test module name).
      socket_path =
        Path.join(
          System.tmp_dir!(),
          "neonfs_cifs_listener_test_#{System.unique_integer([:positive])}.sock"
        )

      on_exit(fn -> _ = File.rm(socket_path) end)
      {:ok, socket_path: socket_path}
    end

    test "binds a Unix domain socket and round-trips a packet:4 frame", %{
      socket_path: socket_path
    } do
      start_supervised!(Listener.child_spec(socket_path: socket_path, handler: EchoHandler))

      assert File.exists?(socket_path), "expected listener to bind UDS at #{socket_path}"

      {:ok, client} =
        :gen_tcp.connect({:local, socket_path}, 0, [:binary, active: false, packet: 4])

      payload = :erlang.term_to_binary({:ping, "hi"})
      :ok = :gen_tcp.send(client, payload)
      {:ok, echoed} = :gen_tcp.recv(client, 0, 5_000)
      :gen_tcp.close(client)

      assert :erlang.binary_to_term(echoed) == {:ping, "hi"}
    end
  end
end
