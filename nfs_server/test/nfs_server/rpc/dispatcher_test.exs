defmodule NFSServer.RPC.DispatcherTest do
  use ExUnit.Case, async: true

  alias NFSServer.RPC.{Auth, Dispatcher, Message}
  alias NFSServer.XDR

  defmodule EchoHandler do
    @behaviour NFSServer.RPC.Handler

    @impl true
    def handle_call(0, _args, _auth, _ctx), do: {:ok, <<>>}
    def handle_call(1, args, _auth, _ctx), do: {:ok, args}
    def handle_call(2, _args, _auth, _ctx), do: :garbage_args
    def handle_call(3, _args, _auth, _ctx), do: :system_err
    def handle_call(_, _args, _auth, _ctx), do: :proc_unavail
  end

  defmodule CrashHandler do
    @behaviour NFSServer.RPC.Handler

    @impl true
    def handle_call(_, _args, _auth, _ctx), do: raise("boom")
  end

  # Echoes the ctx fields back in the reply body so tests can assert
  # what the dispatcher surfaced to the handler.
  defmodule CtxEchoHandler do
    @behaviour NFSServer.RPC.Handler

    @impl true
    def handle_call(_proc, _args, _auth, ctx) do
      {:ok, :erlang.term_to_binary(Map.get(ctx, :peer, :absent))}
    end
  end

  defp call(prog, vers, proc, args \\ <<>>, rpcvers \\ 2) do
    %Message.Call{
      xid: 42,
      rpcvers: rpcvers,
      prog: prog,
      vers: vers,
      proc: proc,
      cred: %Auth.None{},
      verf: %Auth.None{},
      args: args
    }
  end

  defp programs do
    %{
      100_500 => %{1 => EchoHandler, 2 => EchoHandler},
      999_999 => %{1 => CrashHandler},
      555_555 => %{1 => CtxEchoHandler}
    }
  end

  describe "RPC version mismatch" do
    test "returns RPC_MISMATCH for any version other than 2" do
      reply = Dispatcher.dispatch(call(100_500, 1, 0, <<>>, 3), programs())
      assert %Message.DeniedReply{xid: 42, reason: {:rpc_mismatch, 2, 2}} = reply
    end
  end

  describe "program lookup" do
    test "returns PROG_UNAVAIL for an unknown program" do
      reply = Dispatcher.dispatch(call(404_404, 1, 0), programs())
      assert %Message.AcceptedReply{stat: :prog_unavail} = reply
    end

    test "returns PROG_MISMATCH with min/max for the wrong version" do
      reply = Dispatcher.dispatch(call(100_500, 99, 0), programs())
      assert %Message.AcceptedReply{stat: {:prog_mismatch, 1, 2}} = reply
    end
  end

  describe "successful dispatch" do
    test "returns SUCCESS with the handler-encoded body" do
      reply = Dispatcher.dispatch(call(100_500, 1, 1, "echo-payload"), programs())

      assert %Message.AcceptedReply{
               stat: :success,
               body: "echo-payload",
               verf: %Auth.None{}
             } = reply
    end
  end

  describe "ctx.peer plumbing (#1217)" do
    test "surfaces the extras' peer address to the handler" do
      reply = Dispatcher.dispatch(call(555_555, 1, 0), programs(), %{peer: {10, 0, 0, 5}})

      assert %Message.AcceptedReply{stat: :success, body: body} = reply
      assert :erlang.binary_to_term(body) == {10, 0, 0, 5}
    end

    test "leaves peer absent when no extras are passed (handlers Map.get a default)" do
      reply = Dispatcher.dispatch(call(555_555, 1, 0), programs())

      assert %Message.AcceptedReply{stat: :success, body: body} = reply
      # `ctx` carries no `:peer` key on the bare dispatch/2 path; the TCP
      # server always supplies `%{peer: …}` (possibly nil) in production.
      assert :erlang.binary_to_term(body) == :absent
    end
  end

  describe "handler error mapping" do
    test "returns PROC_UNAVAIL when the handler says so" do
      reply = Dispatcher.dispatch(call(100_500, 1, 99), programs())
      assert %Message.AcceptedReply{stat: :proc_unavail} = reply
    end

    test "returns GARBAGE_ARGS when the handler says so" do
      reply = Dispatcher.dispatch(call(100_500, 1, 2), programs())
      assert %Message.AcceptedReply{stat: :garbage_args} = reply
    end

    test "returns SYSTEM_ERR when the handler crashes" do
      reply = Dispatcher.dispatch(call(999_999, 1, 0), programs())
      assert %Message.AcceptedReply{stat: :system_err} = reply
    end
  end

  describe "encode_reply round-trip" do
    test "an accepted SUCCESS reply encodes the xid, msg_type, accept stat and body" do
      reply = %Message.AcceptedReply{
        xid: 7,
        verf: %Auth.None{},
        stat: :success,
        body: XDR.encode_uint(123)
      }

      bytes = reply |> Message.encode_reply() |> IO.iodata_to_binary()

      assert <<7::32, 1::32, 0::32, 0::32, 0::32, 0::32, 123::32>> = bytes
    end
  end
end
