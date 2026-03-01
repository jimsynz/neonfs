defmodule NeonFS.Transport.Listener do
  @moduledoc """
  TLS listener for inbound data transfer connections.

  Opens a TLS listening socket with mutual TLS verification and spawns
  configurable acceptor tasks. Accepted connections are handed to
  `NeonFS.Transport.Handler` processes supervised under
  `NeonFS.Transport.HandlerSupervisor`.

  ## Options

    * `:port` — port to listen on (default: 0, OS-assigned)
    * `:bind` — IP address to bind to (default: `{0,0,0,0,0,0,0,0}` for dual-stack)
    * `:num_acceptors` — number of concurrent acceptor tasks (default: 4)
    * `:ssl_opts` — TLS options list; defaults to options from `NeonFS.Transport.TLS.tls_dir/0`
    * `:handler_supervisor` — supervisor for Handler processes
      (default: `NeonFS.Transport.HandlerSupervisor`)
    * `:dispatch_module` — dispatch module for Handlers
      (default: `NeonFS.Core.BlobStore`)
    * `:name` — registered name (default: `NeonFS.Transport.Listener`)

  """

  use GenServer

  require Logger

  alias NeonFS.Transport.{HandlerSupervisor, TLS}

  @default_num_acceptors 4
  @handshake_timeout 10_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Returns the actual listening port."
  @spec get_port() :: :inet.port_number()
  def get_port, do: get_port(__MODULE__)

  @spec get_port(GenServer.server()) :: :inet.port_number()
  def get_port(server), do: GenServer.call(server, :get_port)

  @doc """
  Rebinds the listener with fresh TLS certificates.

  Closes the existing socket (if any), stops acceptors, and opens a new
  TLS listening socket using the current certificates on disk. This is
  called after `cluster_init` or `join_cluster` writes node certificates.
  """
  @spec rebind() :: :ok | {:error, term()}
  def rebind, do: rebind(__MODULE__)

  @spec rebind(GenServer.server()) :: :ok | {:error, term()}
  def rebind(server), do: GenServer.call(server, :rebind, 10_000)

  # GenServer callbacks

  @impl GenServer
  def init(opts) do
    port = Keyword.get(opts, :port, 0)
    bind = Keyword.get(opts, :bind, {0, 0, 0, 0, 0, 0, 0, 0})
    num_acceptors = Keyword.get(opts, :num_acceptors, @default_num_acceptors)
    ssl_opts = Keyword.get_lazy(opts, :ssl_opts, &default_ssl_opts/0)
    handler_supervisor = Keyword.get(opts, :handler_supervisor, HandlerSupervisor)
    dispatch_module = Keyword.get(opts, :dispatch_module, NeonFS.Core.BlobStore)

    config = %{
      port: port,
      bind: bind,
      num_acceptors: num_acceptors,
      handler_supervisor: handler_supervisor,
      dispatch_module: dispatch_module
    }

    listen_opts = [{:ip, bind} | ssl_opts]

    case :ssl.listen(port, listen_opts) do
      {:ok, listen_socket} ->
        start_accepting(listen_socket, config)

      {:error, reason} ->
        Logger.info("Transport listener not started", reason: inspect(reason))
        {:ok, %{listen_socket: nil, port: 0, acceptors: [], config: config}}
    end
  end

  @impl GenServer
  def handle_call(:get_port, _from, state) do
    {:reply, state.port, state}
  end

  def handle_call(:rebind, _from, state) do
    close_listener(state)

    config = state.config
    ssl_opts = default_ssl_opts()
    listen_opts = [{:ip, config.bind} | ssl_opts]

    case :ssl.listen(config.port, listen_opts) do
      {:ok, listen_socket} ->
        new_state = rebind_with_socket(listen_socket, config)
        {:reply, :ok, new_state}

      {:error, reason} ->
        Logger.warning("Transport listener rebind failed", reason: inspect(reason))
        {:reply, {:error, reason}, %{state | listen_socket: nil, port: 0, acceptors: []}}
    end
  end

  @impl GenServer
  def terminate(_reason, %{listen_socket: nil}), do: :ok

  def terminate(_reason, state) do
    :ssl.close(state.listen_socket)
  end

  # Private functions

  defp rebind_with_socket(listen_socket, config) do
    {:ok, {_addr, actual_port}} = :ssl.sockname(listen_socket)
    acceptors = spawn_acceptors(listen_socket, config)
    Logger.info("Transport listener rebound", port: actual_port)
    %{listen_socket: listen_socket, port: actual_port, acceptors: acceptors, config: config}
  end

  defp start_accepting(listen_socket, config) do
    {:ok, {_addr, actual_port}} = :ssl.sockname(listen_socket)
    acceptors = spawn_acceptors(listen_socket, config)

    Logger.info("Transport listener started",
      port: actual_port,
      num_acceptors: config.num_acceptors
    )

    {:ok,
     %{listen_socket: listen_socket, port: actual_port, acceptors: acceptors, config: config}}
  end

  defp spawn_acceptors(listen_socket, config) do
    acceptor_config = %{
      listen_socket: listen_socket,
      handler_supervisor: config.handler_supervisor,
      dispatch_module: config.dispatch_module
    }

    for _ <- 1..config.num_acceptors do
      spawn_link(fn -> accept_loop(acceptor_config) end)
    end
  end

  defp close_listener(%{listen_socket: nil}), do: :ok

  defp close_listener(%{listen_socket: socket, acceptors: acceptors}) do
    for pid <- acceptors, Process.alive?(pid), do: Process.exit(pid, :shutdown)
    :ssl.close(socket)
  end

  defp accept_loop(config) do
    case :ssl.transport_accept(config.listen_socket) do
      {:ok, transport_socket} ->
        handle_accept(transport_socket, config)
        accept_loop(config)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        Logger.warning("Transport accept failed", reason: inspect(reason))
        :ok
    end
  end

  defp handle_accept(transport_socket, config) do
    case :ssl.handshake(transport_socket, @handshake_timeout) do
      {:ok, socket} ->
        handler_opts = [socket: socket, dispatch_module: config.dispatch_module]

        case HandlerSupervisor.start_handler(config.handler_supervisor, handler_opts) do
          {:ok, pid} ->
            :ssl.controlling_process(socket, pid)
            send(pid, :activate)

            :telemetry.execute(
              [:neonfs, :transport, :listener, :connection_accepted],
              %{},
              %{}
            )

          {:error, reason} ->
            Logger.warning("Failed to start handler", reason: inspect(reason))
            :ssl.close(socket)
        end

      {:error, reason} ->
        :telemetry.execute(
          [:neonfs, :transport, :listener, :connection_error],
          %{},
          %{reason: reason}
        )

        Logger.warning("TLS handshake failed", reason: inspect(reason))
    end
  end

  defp default_ssl_opts do
    tls_dir = TLS.tls_dir()

    [
      {:certs_keys,
       [%{certfile: Path.join(tls_dir, "node.crt"), keyfile: Path.join(tls_dir, "node.key")}]},
      {:cacertfile, Path.join(tls_dir, "ca.crt")},
      {:verify, :verify_peer},
      {:fail_if_no_peer_cert, true},
      {:versions, [:"tlsv1.3"]},
      :binary,
      {:packet, 4},
      {:active, false},
      {:reuseaddr, true}
    ]
  end
end
