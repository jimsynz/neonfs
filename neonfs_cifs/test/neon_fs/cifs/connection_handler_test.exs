defmodule NeonFS.CIFS.ConnectionHandlerTest do
  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.CIFS.Listener

  setup :set_mimic_global
  setup :verify_on_exit!

  setup do
    {:ok, server} =
      start_supervised(Listener.child_spec(tcp_port: 0, handler: NeonFS.CIFS.ConnectionHandler))

    {:ok, {_addr, port}} = ThousandIsland.listener_info(server)
    {:ok, server: server, port: port}
  end

  describe "end-to-end framed ETF round-trip" do
    test "connect then stat round-trips through the listener", %{port: port} do
      stub(NeonFS.Client, :core_call, fn NeonFS.Core.FileIndex,
                                         :get_by_path,
                                         ["vol-a", "/hello"] ->
        {:ok, %{size: 5, mode: 0o100644, accessed_at: 1, modified_at: 2, changed_at: 3}}
      end)

      {:ok, sock} =
        :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false])

      :ok = :gen_tcp.send(sock, frame_body({:connect, %{"volume" => "vol-a"}}))
      assert {:ok, body1} = :gen_tcp.recv(sock, 0, 5_000)
      assert :erlang.binary_to_term(body1) == {:ok, %{}}

      :ok = :gen_tcp.send(sock, frame_body({:stat, %{"path" => "/hello"}}))
      assert {:ok, body2} = :gen_tcp.recv(sock, 0, 5_000)

      assert {:ok,
              %{
                stat: %{
                  size: 5,
                  mode: 0o100644,
                  atime: 1,
                  mtime: 2,
                  ctime: 3,
                  kind: :file
                }
              }} = :erlang.binary_to_term(body2)

      :gen_tcp.close(sock)
    end

    test "an undecodable frame returns :einval without dropping the connection", %{port: port} do
      {:ok, sock} =
        :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false])

      :ok = :gen_tcp.send(sock, "garbage")
      assert {:ok, body} = :gen_tcp.recv(sock, 0, 5_000)
      assert :erlang.binary_to_term(body) == {:error, :einval}

      :gen_tcp.close(sock)
    end
  end

  defp frame_body(term) do
    # The kernel's `packet: 4` strips the 4-byte length prefix on
    # send too, so we hand it the bare ETF body.
    :erlang.term_to_binary(term)
  end
end
