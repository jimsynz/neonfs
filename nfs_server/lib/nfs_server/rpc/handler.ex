defmodule NFSServer.RPC.Handler do
  @moduledoc """
  Behaviour for ONC RPC program handlers.

  A handler is a module that implements `handle_call/4` and is
  registered with `NFSServer.RPC.Dispatcher` for one or more
  `{program, version}` pairs. The dispatcher peels the RPC envelope,
  validates the credential flavor, and invokes:

      handle_call(proc, args_binary, auth_context, ctx)

  The handler decodes `args_binary` (typically with `NFSServer.XDR`
  helpers), produces a result, and returns one of the result tuples
  below. The dispatcher wraps the result in the RPC reply envelope.

  ## Return values

    * `{:ok, encoded_body}` — successful reply, `encoded_body` is the
      already-XDR-encoded result body.
    * `:proc_unavail` — the procedure number isn't implemented; the
      dispatcher returns `PROC_UNAVAIL`.
    * `:garbage_args` — `args_binary` couldn't be decoded; the
      dispatcher returns `GARBAGE_ARGS`.
    * `:system_err` — handler crashed or a downstream system error;
      the dispatcher returns `SYSTEM_ERR`.

  The `ctx` argument is currently a `%{call: NFSServer.RPC.Message.Call.t()}`
  map carrying the original call envelope. Future fields (e.g. peer
  address, TLS info) can be added without breaking handlers.
  """

  alias NFSServer.RPC.{Auth, Message}

  @typedoc "The reply body — handler-encoded XDR or a structured error."
  @type result ::
          {:ok, body :: binary()}
          | :proc_unavail
          | :garbage_args
          | :system_err

  @type ctx :: %{call: Message.Call.t()}

  @callback handle_call(
              proc :: non_neg_integer(),
              args :: binary(),
              auth :: Auth.credential(),
              ctx()
            ) :: result()
end
