defmodule NFSServer.RPC.Portmapper do
  @moduledoc """
  Minimal portmap v2 (program 100000) handler — enough to satisfy
  `mount -t nfs` clients that go through `rpcbind` before they
  contact the NFS server directly.

  We register the same TCP listener for both portmap and NFS, so a
  GETPORT for `(NFS=100003, version 3, TCP=6)` returns the same port
  this server is listening on. Other portmap procedures return
  `PROC_UNAVAIL`.

  ## Procedure numbers (RFC 1833 §3.2)

      PMAPPROC_NULL    = 0   # ping
      PMAPPROC_SET     = 1   # not implemented
      PMAPPROC_UNSET   = 2   # not implemented
      PMAPPROC_GETPORT = 3   # implemented
      PMAPPROC_DUMP    = 4   # not implemented
      PMAPPROC_CALLIT  = 5   # not implemented
  """

  @behaviour NFSServer.RPC.Handler

  alias NFSServer.XDR

  @program 100_000
  @version 2

  @proc_null 0
  @proc_getport 3

  @doc "Numeric program id (always 100000)."
  @spec program() :: 100_000
  def program, do: @program

  @doc "Numeric version (always 2)."
  @spec version() :: 2
  def version, do: @version

  @impl true
  def handle_call(@proc_null, _args, _auth, _ctx), do: {:ok, <<>>}

  def handle_call(@proc_getport, args, _auth, ctx) do
    case decode_mapping(args) do
      {:ok, prog, vers, proto, _port} ->
        port = lookup_port(prog, vers, proto, ctx)
        {:ok, XDR.encode_uint(port)}

      :error ->
        :garbage_args
    end
  end

  def handle_call(_proc, _args, _auth, _ctx), do: :proc_unavail

  defp decode_mapping(binary) do
    with {:ok, prog, r1} <- XDR.decode_uint(binary),
         {:ok, vers, r2} <- XDR.decode_uint(r1),
         {:ok, proto, r3} <- XDR.decode_uint(r2),
         {:ok, port, _} <- XDR.decode_uint(r3) do
      {:ok, prog, vers, proto, port}
    else
      _ -> :error
    end
  end

  # The dispatcher passes the full call envelope through `ctx`. We
  # use it to fetch the listening port that was stashed there by
  # `RPC.Server` — that way the portmapper always reports the port
  # the server is actually bound on, even when bound to an ephemeral
  # port in tests.
  #
  # `port` defaults to 0 ("not registered") when no NFS port is
  # known for the requested mapping.
  defp lookup_port(prog, vers, proto, ctx) do
    mappings = Map.get(ctx, :portmap_mappings, %{})

    case Map.fetch(mappings, {prog, vers, proto}) do
      {:ok, port} -> port
      :error -> 0
    end
  end
end
