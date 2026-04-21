defmodule NFSServer.RPC.Message do
  @moduledoc """
  ONC RPC v2 call and reply envelopes — [RFC 5531 §9](https://www.rfc-editor.org/rfc/rfc5531#section-9).

  An RPC message is one of:

      msg_type CALL = 0
      msg_type REPLY = 1

  Calls carry the program/version/procedure to invoke and the
  caller's credentials; replies carry an outcome (accepted vs.
  denied) and either the procedure's result body or a status code.

  This module decodes incoming wire bytes into the structs below and
  encodes outgoing replies back to wire bytes. The procedure-specific
  argument and result bodies are NOT decoded here — they're left as
  binaries for the program-specific handler to parse.
  """

  alias NFSServer.RPC.Auth
  alias NFSServer.XDR

  @rpc_version 2
  @msg_call 0
  @msg_reply 1

  @reply_accepted 0
  @reply_denied 1

  @accept_success 0
  @accept_prog_unavail 1
  @accept_prog_mismatch 2
  @accept_proc_unavail 3
  @accept_garbage_args 4
  @accept_system_err 5

  @reject_rpc_mismatch 0
  @reject_auth_error 1

  defmodule Call do
    @moduledoc """
    Decoded RPC call. `args` is the unparsed payload — pass it to
    the program handler verbatim.
    """
    defstruct [:xid, :rpcvers, :prog, :vers, :proc, :cred, :verf, :args]

    @type t :: %__MODULE__{
            xid: non_neg_integer(),
            rpcvers: non_neg_integer(),
            prog: non_neg_integer(),
            vers: non_neg_integer(),
            proc: non_neg_integer(),
            cred: NFSServer.RPC.Auth.credential(),
            verf: NFSServer.RPC.Auth.credential(),
            args: binary()
          }
  end

  defmodule AcceptedReply do
    @moduledoc """
    Successful or per-procedure-error reply. `body` is the
    procedure's encoded result for `:success`, or the relevant
    payload (e.g. `{:prog_mismatch, low, high}`) for the error
    cases.
    """
    defstruct [:xid, :verf, :stat, :body]

    @type stat ::
            :success
            | :prog_unavail
            | {:prog_mismatch, non_neg_integer(), non_neg_integer()}
            | :proc_unavail
            | :garbage_args
            | :system_err

    @type t :: %__MODULE__{
            xid: non_neg_integer(),
            verf: NFSServer.RPC.Auth.credential(),
            stat: stat(),
            body: binary()
          }
  end

  defmodule DeniedReply do
    @moduledoc """
    Reply rejected at the RPC layer (RPC version mismatch or auth
    failure). `body` carries the relevant tuple.
    """
    defstruct [:xid, :reason]

    @type reason ::
            {:rpc_mismatch, non_neg_integer(), non_neg_integer()}
            | {:auth_error, non_neg_integer()}

    @type t :: %__MODULE__{xid: non_neg_integer(), reason: reason()}
  end

  @typedoc "Decoded reply, of either accepted or denied flavour."
  @type reply :: AcceptedReply.t() | DeniedReply.t()

  # ——— Decode ——————————————————————————————————————————————————

  @doc """
  Decode an RPC call message. Returns `{:ok, call}` on success.

  This is the only message type a server expects to receive; replies
  are produced via `encode_reply/1`.
  """
  @spec decode_call(binary()) :: {:ok, Call.t()} | {:error, term()}
  def decode_call(binary) when is_binary(binary) do
    with {:ok, xid, r1} <- XDR.decode_uint(binary),
         {:ok, msg_type, r2} <- XDR.decode_int(r1),
         :ok <- expect_msg_call(msg_type),
         {:ok, rpcvers, r3} <- XDR.decode_uint(r2),
         {:ok, prog, r4} <- XDR.decode_uint(r3),
         {:ok, vers, r5} <- XDR.decode_uint(r4),
         {:ok, proc, r6} <- XDR.decode_uint(r5),
         {:ok, cred, r7} <- Auth.decode_opaque_auth(r6),
         {:ok, verf, args} <- Auth.decode_opaque_auth(r7) do
      {:ok,
       %Call{
         xid: xid,
         rpcvers: rpcvers,
         prog: prog,
         vers: vers,
         proc: proc,
         cred: cred,
         verf: verf,
         args: args
       }}
    end
  end

  defp expect_msg_call(@msg_call), do: :ok
  defp expect_msg_call(other), do: {:error, {:unexpected_msg_type, other}}

  @doc "Quick predicate — is the message version this server speaks?"
  @spec rpc_version_ok?(Call.t()) :: boolean()
  def rpc_version_ok?(%Call{rpcvers: @rpc_version}), do: true
  def rpc_version_ok?(%Call{}), do: false

  @doc "Numeric RPC version this server implements (always 2)."
  @spec rpc_version() :: 2
  def rpc_version, do: @rpc_version

  # ——— Encode (replies) ————————————————————————————————————————

  @doc """
  Encode a reply (accepted or denied) to wire bytes. The result is
  the body — pass through `RPC.RecordMarking.encode/1` before
  writing to the socket.
  """
  @spec encode_reply(reply()) :: binary()
  def encode_reply(%AcceptedReply{} = reply) do
    XDR.encode_uint(reply.xid) <>
      XDR.encode_int(@msg_reply) <>
      XDR.encode_int(@reply_accepted) <>
      Auth.encode_opaque_auth(reply.verf) <>
      encode_accept_stat(reply.stat, reply.body)
  end

  def encode_reply(%DeniedReply{xid: xid, reason: reason}) do
    XDR.encode_uint(xid) <>
      XDR.encode_int(@msg_reply) <>
      XDR.encode_int(@reply_denied) <>
      encode_reject(reason)
  end

  defp encode_accept_stat(:success, body),
    do: XDR.encode_int(@accept_success) <> body

  defp encode_accept_stat(:prog_unavail, _body),
    do: XDR.encode_int(@accept_prog_unavail)

  defp encode_accept_stat({:prog_mismatch, low, high}, _body) do
    XDR.encode_int(@accept_prog_mismatch) <>
      XDR.encode_uint(low) <>
      XDR.encode_uint(high)
  end

  defp encode_accept_stat(:proc_unavail, _body),
    do: XDR.encode_int(@accept_proc_unavail)

  defp encode_accept_stat(:garbage_args, _body),
    do: XDR.encode_int(@accept_garbage_args)

  defp encode_accept_stat(:system_err, _body),
    do: XDR.encode_int(@accept_system_err)

  defp encode_reject({:rpc_mismatch, low, high}) do
    XDR.encode_int(@reject_rpc_mismatch) <>
      XDR.encode_uint(low) <>
      XDR.encode_uint(high)
  end

  defp encode_reject({:auth_error, stat}) do
    XDR.encode_int(@reject_auth_error) <>
      XDR.encode_uint(stat)
  end
end
