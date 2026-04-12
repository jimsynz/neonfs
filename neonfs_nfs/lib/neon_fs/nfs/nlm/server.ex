defmodule NeonFS.NFS.NLM.Server do
  @moduledoc """
  NLM v4 TCP server.

  Listens for NLM (Network Lock Manager, program 100021) connections on a
  configurable port (default 4045) and spawns a `NeonFS.NFS.RPC.Transport`
  process per connection.

  ## Configuration

      config :neonfs_nfs,
        nlm_port: 4045,
        nlm_bind: "0.0.0.0"

  Clients connect using: `mount -t nfs -o vers=3,tcp,nlm_port=4045 host:/volume /mnt`
  """

  use GenServer

  require Logger

  alias NeonFS.NFS.NLM.Handler
  alias NeonFS.NFS.RPC.Transport

  defmodule State do
    @moduledoc false

    @type t :: %__MODULE__{
            listen_socket: :gen_tcp.socket() | nil,
            port: non_neg_integer() | nil,
            bind_address: String.t() | nil,
            handler_opts: keyword(),
            connections: [pid()]
          }

    defstruct [:listen_socket, :port, :bind_address, :handler_opts, connections: []]
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  Returns the port the NLM server is listening on.
  """
  @spec port(GenServer.server()) :: non_neg_integer()
  def port(server \\ __MODULE__) do
    GenServer.call(server, :port)
  end

  @impl true
  def init(opts) do
    port = Keyword.get(opts, :port, nlm_port())
    bind_address = Keyword.get(opts, :bind_address, nlm_bind_address())
    handler_opts = Keyword.get(opts, :handler_opts, [])

    ip = parse_bind_address(bind_address)

    tcp_opts = [
      :binary,
      active: false,
      reuseaddr: true,
      ip: ip,
      packet: :raw,
      nodelay: true
    ]

    case :gen_tcp.listen(port, tcp_opts) do
      {:ok, listen_socket} ->
        {:ok, actual_port} = :inet.port(listen_socket)

        Logger.info("NLM server listening",
          bind_address: bind_address,
          port: actual_port,
          component: :nlm
        )

        :telemetry.execute([:neonfs, :nlm, :server, :started], %{}, %{port: actual_port})

        send(self(), :accept)

        {:ok,
         %State{
           listen_socket: listen_socket,
           port: actual_port,
           bind_address: bind_address,
           handler_opts: handler_opts
         }}

      {:error, reason} ->
        Logger.error("NLM server failed to listen",
          bind_address: bind_address,
          port: port,
          reason: inspect(reason),
          component: :nlm
        )

        {:stop, {:listen_failed, reason}}
    end
  end

  @impl true
  def handle_call(:port, _from, state) do
    {:reply, state.port, state}
  end

  @impl true
  def handle_info(:accept, state) do
    case :gen_tcp.accept(state.listen_socket, 100) do
      {:ok, client_socket} ->
        {:ok, {peer_addr, peer_port}} = :inet.peername(client_socket)

        Logger.debug("NLM connection accepted",
          peer: "#{:inet.ntoa(peer_addr)}:#{peer_port}",
          component: :nlm
        )

        :telemetry.execute([:neonfs, :nlm, :connection, :accepted], %{count: 1}, %{})

        {:ok, transport} =
          Transport.start_link(
            socket: client_socket,
            handler: Handler,
            handler_opts: state.handler_opts
          )

        :gen_tcp.controlling_process(client_socket, transport)
        :inet.setopts(client_socket, active: :once)

        send(self(), :accept)
        {:noreply, %{state | connections: [transport | state.connections]}}

      {:error, :timeout} ->
        send(self(), :accept)
        {:noreply, state}

      {:error, :closed} ->
        Logger.info("NLM server: listen socket closed", component: :nlm)
        {:stop, :normal, state}

      {:error, reason} ->
        Logger.error("NLM accept error", reason: inspect(reason), component: :nlm)
        send(self(), :accept)
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {:noreply, %{state | connections: List.delete(state.connections, pid)}}
  end

  @impl true
  def terminate(_reason, state) do
    if state.listen_socket do
      :gen_tcp.close(state.listen_socket)
    end

    :telemetry.execute([:neonfs, :nlm, :server, :stopped], %{}, %{})
    :ok
  end

  ## Private

  defp nlm_port do
    Application.get_env(:neonfs_nfs, :nlm_port, 4045)
  end

  defp nlm_bind_address do
    Application.get_env(:neonfs_nfs, :nlm_bind, "0.0.0.0")
  end

  defp parse_bind_address(address) when is_binary(address) do
    {:ok, ip} = :inet.parse_address(String.to_charlist(address))
    ip
  end
end
