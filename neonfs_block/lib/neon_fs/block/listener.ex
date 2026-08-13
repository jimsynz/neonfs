defmodule NeonFS.Block.Listener do
  @moduledoc """
  ThousandIsland listener for the NBD server.

  Each accepted connection runs `NeonFS.Block.ConnectionHandler` in its own
  process. The socket is `packet: :raw`: NBD frames its own messages, and the
  headers are fixed-width rather than length-prefixed, so there is nothing for
  the kernel to assemble.

  Binds `127.0.0.1` by default per `AGENTS.md`'s listener posture — NBD has no
  authentication of its own, so anything reachable on the port can attach any
  export this node can resolve. Widening the bind means confining the port to
  a trusted network.
  """

  alias NeonFS.Block.ConnectionHandler

  @default_bind "127.0.0.1"
  # The IANA-assigned NBD port.
  @default_port 10_809

  @doc """
  Child spec for the listener, taking its bind and port from options and
  falling back to application environment.
  """
  @spec child_spec(keyword()) :: {module(), keyword()}
  def child_spec(opts \\ []) do
    {
      ThousandIsland,
      port: Keyword.get_lazy(opts, :port, &configured_port/0),
      handler_module: Keyword.get(opts, :handler, ConnectionHandler),
      transport_options: [ip: bind_address(opts), packet: :raw]
    }
  end

  @doc """
  The configured bind address, as an address tuple.
  """
  @spec bind_address(keyword()) :: :inet.ip_address()
  def bind_address(opts \\ []) do
    opts
    |> Keyword.get_lazy(:bind, &configured_bind/0)
    |> parse_address()
  end

  @doc """
  The configured port, which is what a client has to dial.
  """
  @spec port() :: :inet.port_number()
  def port, do: configured_port()

  defp configured_bind, do: Application.get_env(:neonfs_block, :bind, @default_bind)
  defp configured_port, do: Application.get_env(:neonfs_block, :port, @default_port)

  defp parse_address(address) when is_tuple(address), do: address

  defp parse_address(address) when is_binary(address) do
    case address |> to_charlist() |> :inet.parse_address() do
      {:ok, parsed} -> parsed
      {:error, :einval} -> raise ArgumentError, "invalid NBD bind address: #{inspect(address)}"
    end
  end
end
