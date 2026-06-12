defmodule NFSServer.RPC.Dispatcher do
  @moduledoc """
  Map an incoming `NFSServer.RPC.Message.Call` to the registered
  handler for its `{program, version}` pair, then wrap the result in
  the appropriate `NFSServer.RPC.Message.AcceptedReply` /
  `DeniedReply`.

  The dispatcher is **stateless** — handlers are passed in via the
  `programs` map. The TCP server holds the registry; this module
  is pure-function so it's trivial to unit-test.

  ## RPC error mapping

    * RPC version != 2  → `RPC_MISMATCH` (denied)
    * Unknown program   → `PROG_UNAVAIL`
    * Wrong version of a known program → `PROG_MISMATCH`
    * Handler returns `:proc_unavail`  → `PROC_UNAVAIL`
    * Handler returns `:garbage_args`  → `GARBAGE_ARGS`
    * Handler returns `:system_err` or raises → `SYSTEM_ERR`
    * Handler returns `{:ok, body}`    → `SUCCESS` with `body`
  """

  alias NFSServer.RPC.{Auth, Message}
  require Logger

  @typedoc """
  Registry of installed handlers:

      %{
        100000 => %{2 => SomeModule},  # portmapper
        100003 => %{3 => Nfs3Handler}  # NFSv3
      }
  """
  @type programs :: %{
          required(non_neg_integer()) => %{
            required(non_neg_integer()) => module()
          }
        }

  @doc """
  Dispatch a single call. Returns a `Message.reply()` ready to be
  encoded with `Message.encode_reply/1`.

  `extras` is merged into the handler `ctx` map — the TCP server uses
  it to surface the connection's client address as `ctx.peer` so
  handlers can apply per-export host filtering (#1217).
  """
  @spec dispatch(Message.Call.t(), programs(), map()) :: Message.reply()
  def dispatch(%Message.Call{} = call, programs, extras \\ %{}) do
    if Message.rpc_version_ok?(call) do
      dispatch_program(call, programs, extras)
    else
      %Message.DeniedReply{
        xid: call.xid,
        reason: {:rpc_mismatch, Message.rpc_version(), Message.rpc_version()}
      }
    end
  end

  defp dispatch_program(%Message.Call{prog: prog, vers: vers} = call, programs, extras) do
    case Map.fetch(programs, prog) do
      :error ->
        accepted_reply(call, :prog_unavail)

      {:ok, versions} ->
        case Map.fetch(versions, vers) do
          {:ok, handler} -> invoke_handler(call, handler, extras)
          :error -> prog_mismatch_reply(call, versions)
        end
    end
  end

  defp invoke_handler(%Message.Call{} = call, handler, extras) do
    ctx = Map.merge(%{call: call}, extras)

    result =
      try do
        handler.handle_call(call.proc, call.args, call.cred, ctx)
      rescue
        e ->
          Logger.warning("RPC handler raised", reason: inspect(e), kind: handler)
          :system_err
      catch
        kind, reason ->
          Logger.warning("RPC handler exited",
            reason: inspect({kind, reason}),
            kind: handler
          )

          :system_err
      end

    case result do
      {:ok, body} -> accepted_reply(call, :success, body)
      :proc_unavail -> accepted_reply(call, :proc_unavail)
      :garbage_args -> accepted_reply(call, :garbage_args)
      :system_err -> accepted_reply(call, :system_err)
    end
  end

  defp prog_mismatch_reply(%Message.Call{} = call, versions) do
    {low, high} = version_bounds(versions)
    accepted_reply(call, {:prog_mismatch, low, high})
  end

  defp version_bounds(versions) do
    keys = Map.keys(versions)
    {Enum.min(keys), Enum.max(keys)}
  end

  defp accepted_reply(call, stat, body \\ <<>>) do
    %Message.AcceptedReply{
      xid: call.xid,
      verf: %Auth.None{},
      stat: stat,
      body: body
    }
  end
end
