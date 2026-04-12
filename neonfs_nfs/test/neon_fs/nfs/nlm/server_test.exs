defmodule NeonFS.NFS.NLM.ServerTest do
  use ExUnit.Case, async: false

  alias NeonFS.NFS.NLM.Server, as: NLMServer
  alias NeonFS.NFS.RPC.XDR

  @nlm_program 100_021
  @nlm_version 4

  setup do
    core_call_fn = fn
      NeonFS.Core.LockManager, :lock, _ -> :ok
      NeonFS.Core.LockManager, :unlock, _ -> :ok
      NeonFS.Core.LockManager, :test_lock, _ -> :ok
      _, _, _ -> {:error, :not_implemented}
    end

    {:ok, server} =
      start_supervised(
        {NLMServer,
         port: 0,
         bind_address: "127.0.0.1",
         handler_opts: [core_call_fn: core_call_fn],
         name: :"nlm_test_#{System.unique_integer([:positive])}"}
      )

    port = NLMServer.port(server)
    %{port: port}
  end

  test "accepts TCP connections and responds to NLM4_NULL", %{port: port} do
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])

    rpc_call = build_rpc_call(1, 0, <<>>)
    frame = <<1::1, byte_size(rpc_call)::big-unsigned-31, rpc_call::binary>>

    :ok = :gen_tcp.send(socket, frame)
    {:ok, response} = :gen_tcp.recv(socket, 0, 5_000)

    # Strip record marking
    <<1::1, _len::big-unsigned-31, reply::binary>> = response

    assert <<
             # xid
             1::big-32,
             # reply
             1::big-32,
             # accepted
             0::big-32,
             # verf AUTH_NONE
             0::big-32,
             0::big-32,
             # SUCCESS
             0::big-32,
             _body::binary
           >> = reply

    :gen_tcp.close(socket)
  end

  test "handles NLM4_LOCK over TCP", %{port: port} do
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])

    lock_body = encode_lockargs("cookie42", false, true, make_lock(), false, 1)
    rpc_call = build_rpc_call(2, 2, lock_body)
    frame = <<1::1, byte_size(rpc_call)::big-unsigned-31, rpc_call::binary>>

    :ok = :gen_tcp.send(socket, frame)
    {:ok, response} = :gen_tcp.recv(socket, 0, 5_000)

    <<1::1, _len::big-unsigned-31, reply::binary>> = response

    # Verify accepted reply
    assert <<
             2::big-32,
             1::big-32,
             0::big-32,
             0::big-32,
             0::big-32,
             0::big-32,
             body::binary
           >> = reply

    # Verify NLM4_GRANTED (0) in the response
    assert {:ok, "cookie42", rest} = XDR.decode_opaque(body)
    assert {:ok, 0, _rest} = XDR.decode_int(rest)

    :gen_tcp.close(socket)
  end

  test "handles multiple requests on the same connection", %{port: port} do
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])

    for xid <- 1..3 do
      rpc_call = build_rpc_call(xid, 0, <<>>)
      frame = <<1::1, byte_size(rpc_call)::big-unsigned-31, rpc_call::binary>>

      :ok = :gen_tcp.send(socket, frame)
      {:ok, response} = :gen_tcp.recv(socket, 0, 5_000)
      <<1::1, _len::big-unsigned-31, reply::binary>> = response

      assert <<^xid::big-32, 1::big-32, _rest::binary>> = reply
    end

    :gen_tcp.close(socket)
  end

  test "returns PROG_MISMATCH for wrong version", %{port: port} do
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])

    rpc_call =
      <<
        10::big-32,
        0::big-32,
        2::big-32,
        @nlm_program::big-32,
        # wrong version
        3::big-32,
        0::big-32,
        0::big-32,
        0::big-32,
        0::big-32,
        0::big-32
      >>

    frame = <<1::1, byte_size(rpc_call)::big-unsigned-31, rpc_call::binary>>

    :ok = :gen_tcp.send(socket, frame)
    {:ok, response} = :gen_tcp.recv(socket, 0, 5_000)
    <<1::1, _len::big-unsigned-31, reply::binary>> = response

    assert <<
             10::big-32,
             1::big-32,
             0::big-32,
             0::big-32,
             0::big-32,
             # PROG_MISMATCH
             2::big-32,
             4::big-32,
             4::big-32
           >> = reply

    :gen_tcp.close(socket)
  end

  ## Helpers

  defp build_rpc_call(xid, procedure, body) do
    <<
      xid::big-32,
      # CALL
      0::big-32,
      # RPC version 2
      2::big-32,
      @nlm_program::big-32,
      @nlm_version::big-32,
      procedure::big-32,
      # cred: AUTH_NONE
      0::big-32,
      0::big-32,
      # verf: AUTH_NONE
      0::big-32,
      0::big-32,
      body::binary
    >>
  end

  defp make_lock do
    fh = <<1::little-64, 0::128>>

    %{
      caller_name: "testhost",
      fh: fh,
      oh: <<1, 2, 3>>,
      svid: 100,
      offset: 0,
      length: 1024
    }
  end

  defp encode_lock(lock) do
    XDR.encode_string(lock.caller_name) <>
      XDR.encode_opaque(lock.fh) <>
      XDR.encode_opaque(lock.oh) <>
      XDR.encode_int(lock.svid) <>
      XDR.encode_hyper_uint(lock.offset) <>
      XDR.encode_hyper_uint(lock.length)
  end

  defp encode_lockargs(cookie, block, exclusive, lock, reclaim, state) do
    XDR.encode_opaque(cookie) <>
      XDR.encode_bool(block) <>
      XDR.encode_bool(exclusive) <>
      encode_lock(lock) <>
      XDR.encode_bool(reclaim) <>
      XDR.encode_int(state)
  end
end
