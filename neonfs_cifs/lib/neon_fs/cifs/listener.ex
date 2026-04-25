defmodule NeonFS.CIFS.Listener do
  @moduledoc """
  ThousandIsland-based UDS / TCP listener for the Samba VFS protocol.

  Each accepted connection runs in its own process under
  ThousandIsland's `Task.Supervisor`. The connection handler module
  (`NeonFS.CIFS.ConnectionHandler` by default) implements the
  `ThousandIsland.Handler` behaviour and owns per-connection state
  (open file/dir handle table, the bound NeonFS volume, etc.).

  ## Framing

  Wire frames are 4-byte big-endian length-prefixed:

      <<length::big-32, term_to_binary(message)::binary>>

  We use ThousandIsland's transport `packet: 4` mode so the listener
  delivers one decoded payload per `handle_data/3` call. Encoding on
  the way out goes through `Listener.encode/1` (or directly via
  `:gen_tcp.send/2`, which respects `packet: 4`).

  Configuration is via `child_spec/1` keyword opts:

    * `:socket_path` — bind a Unix domain socket at this path. The
      parent directory is created and any pre-existing socket file
      is removed first. Default is taken from
      `NeonFS.CIFS.Supervisor`.
    * `:tcp_port` — alternative to `:socket_path`; bind a TCP
      listener on `127.0.0.1:port` (test mode).
    * `:handler` — `ThousandIsland.Handler` module. Defaults to
      `NeonFS.CIFS.ConnectionHandler`.
  """

  alias NeonFS.CIFS.ConnectionHandler

  @doc """
  Build the supervised child spec to embed under
  `NeonFS.CIFS.Supervisor` (or anywhere else a child spec is
  expected — e.g. tests pulling it under `start_supervised!/1`).

  Returns a `{ThousandIsland, opts}` tuple — the supervisor coerces
  it into the expanded child-spec map automatically. We don't return
  a fully-expanded `Supervisor.child_spec()` because ThousandIsland
  owns its own start/2 wiring and we don't want to re-derive it
  here.
  """
  @spec child_spec(keyword()) :: {module(), keyword()}
  def child_spec(opts) do
    handler = Keyword.get(opts, :handler, ConnectionHandler)
    transport_module = transport_module(opts)
    transport_options = transport_options(opts)

    {
      ThousandIsland,
      # `packet: 4` is set both on the listening socket and on each
      # accepted socket so 4-byte length-prefixed frames are
      # auto-assembled by the kernel before our handler sees them.
      transport_module: transport_module,
      transport_options: transport_options,
      handler_module: handler,
      read_timeout: :infinity
    }
  end

  @doc """
  Encode `message` as a wire-ready iolist (4-byte big-endian length
  prefix + ETF body). Public so test clients can build frames.
  """
  @spec encode(term()) :: iodata()
  def encode(message) do
    body = :erlang.term_to_binary(message)
    [<<byte_size(body)::big-32>>, body]
  end

  @doc """
  Decode a wire frame's body — pure ETF deserialisation. The
  4-byte length prefix has already been stripped by the
  `packet: 4`-configured socket. Returns `{:ok, term}` or
  `{:error, :badetf}` on failure.
  """
  @spec decode(binary()) :: {:ok, term()} | {:error, :badetf}
  def decode(body) when is_binary(body) do
    {:ok, :erlang.binary_to_term(body, [:safe])}
  rescue
    ArgumentError -> {:error, :badetf}
  end

  defp transport_module(opts) do
    cond do
      Keyword.has_key?(opts, :socket_path) -> ThousandIsland.Transports.Unix
      Keyword.has_key?(opts, :tcp_port) -> ThousandIsland.Transports.TCP
      true -> ThousandIsland.Transports.Unix
    end
  end

  defp transport_options(opts) do
    base = [packet: 4]

    cond do
      path = Keyword.get(opts, :socket_path) ->
        path |> Path.dirname() |> File.mkdir_p!()
        File.rm(path)
        base ++ [path: path]

      port = Keyword.get(opts, :tcp_port) ->
        base ++ [port: port, ip: {127, 0, 0, 1}]

      true ->
        base
    end
  end
end
