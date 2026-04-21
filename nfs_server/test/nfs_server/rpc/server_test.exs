defmodule NFSServer.RPC.ServerTest do
  use ExUnit.Case, async: false

  alias NFSServer.RPC.{Auth, RecordMarking, Server}
  alias NFSServer.XDR

  defmodule EchoHandler do
    @behaviour NFSServer.RPC.Handler

    @impl true
    def handle_call(0, _args, _auth, _ctx), do: {:ok, <<>>}
    def handle_call(1, args, _auth, _ctx), do: {:ok, args}
    def handle_call(_, _args, _auth, _ctx), do: :proc_unavail
  end

  setup do
    {:ok, pid} =
      start_supervised(
        {Server,
         port: 0,
         bind: "127.0.0.1",
         programs: %{500_001 => %{1 => EchoHandler}},
         name: :"server_test_#{System.unique_integer([:positive])}"}
      )

    port = Server.port(pid)
    {:ok, server: pid, port: port}
  end

  defp call_bytes(prog, vers, proc, args \\ <<>>) do
    body =
      XDR.encode_uint(1234) <>
        XDR.encode_int(0) <>
        XDR.encode_uint(2) <>
        XDR.encode_uint(prog) <>
        XDR.encode_uint(vers) <>
        XDR.encode_uint(proc) <>
        Auth.encode_opaque_auth(%Auth.None{}) <>
        Auth.encode_opaque_auth(%Auth.None{}) <>
        args

    RecordMarking.encode(body)
  end

  defp recv_message(socket) do
    {:ok, header} = :gen_tcp.recv(socket, 4)
    <<_last::1, len::31>> = header
    {:ok, body} = :gen_tcp.recv(socket, len)
    body
  end

  describe "end-to-end RPC over TCP" do
    test "PMAPPROC_NULL ping over the auto-registered portmapper", %{port: port} do
      {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
      :ok = :gen_tcp.send(sock, call_bytes(100_000, 2, 0))

      reply_body = recv_message(sock)

      # xid=1234 echoed, msg_type=REPLY=1, accepted=0, verf flavor=0
      # body=0 (verf opaque len), accept_stat=SUCCESS=0, no body.
      assert <<1234::32, 1::32, 0::32, 0::32, 0::32, 0::32>> = reply_body

      :gen_tcp.close(sock)
    end

    test "echo handler returns the args back as the success body", %{port: port} do
      {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
      :ok = :gen_tcp.send(sock, call_bytes(500_001, 1, 1, "hello-world"))

      reply_body = recv_message(sock)

      # Skip header bytes (24 = xid + msg_type + accepted + verf
      # flavor + verf len + accept_stat) and assert tail equals the
      # echoed args.
      <<_::binary-size(24), echoed::binary>> = reply_body
      assert echoed == "hello-world"

      :gen_tcp.close(sock)
    end

    test "unknown program returns PROG_UNAVAIL", %{port: port} do
      {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
      :ok = :gen_tcp.send(sock, call_bytes(404_404, 1, 0))

      reply_body = recv_message(sock)
      # accept_stat=PROG_UNAVAIL=1
      assert <<1234::32, 1::32, 0::32, 0::32, 0::32, 1::32>> = reply_body

      :gen_tcp.close(sock)
    end

    test "GETPORT for NFSv3/TCP returns the listener's actual port", %{port: port} do
      {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])

      args =
        XDR.encode_uint(100_003) <>
          XDR.encode_uint(3) <>
          XDR.encode_uint(6) <>
          XDR.encode_uint(0)

      :ok = :gen_tcp.send(sock, call_bytes(100_000, 2, 3, args))

      reply_body = recv_message(sock)

      # Header bytes 0..23 are envelope, byte 24..27 is the
      # XDR-encoded port number.
      <<_::binary-size(24), reported_port::32>> = reply_body
      # NFS handler isn't registered in this test, so portmap returns 0.
      assert reported_port == 0

      :gen_tcp.close(sock)
    end
  end

  describe "end-to-end with NFSv3 program registered" do
    setup do
      {:ok, pid} =
        start_supervised(
          {Server,
           port: 0,
           bind: "127.0.0.1",
           programs: %{100_003 => %{3 => EchoHandler}, 500_001 => %{1 => EchoHandler}},
           name: :"server_with_nfs_#{System.unique_integer([:positive])}"},
          id: :nfs_server_advertised
        )

      port = Server.port(pid)
      {:ok, server: pid, port: port}
    end

    test "GETPORT advertises the listener's port for NFSv3", %{port: port} do
      {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])

      args =
        XDR.encode_uint(100_003) <>
          XDR.encode_uint(3) <>
          XDR.encode_uint(6) <>
          XDR.encode_uint(0)

      :ok = :gen_tcp.send(sock, call_bytes(100_000, 2, 3, args))

      <<_::binary-size(24), reported_port::32>> = recv_message(sock)
      assert reported_port == port

      :gen_tcp.close(sock)
    end
  end
end
