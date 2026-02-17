defmodule NeonFS.Transport.ConnPool do
  @moduledoc """
  Connection pool for persistent TLS connections to a single peer node.

  Implements the `NimblePool` behaviour to manage a pool of TLS connections.
  Each peer node gets its own ConnPool instance (managed by PoolManager).
  The pool uses cluster CA certificates from Phase 8 for mutual TLS authentication.

  ## Usage

      {:ok, pool} = ConnPool.start_link(
        peer: {~c"10.0.1.5", 44831},
        ssl_opts: custom_ssl_opts
      )

      response = ConnPool.execute(pool, {:get_chunk, ref, hash, volume_id})
  """

  alias NeonFS.Transport.TLS

  @behaviour NimblePool

  @default_pool_size 4
  @default_worker_idle_timeout 30_000
  @default_checkout_timeout 30_000
  @default_recv_timeout 30_000

  @type peer :: {:ssl.host(), :inet.port_number()}

  @doc """
  Starts a connection pool linked to the current process.

  ## Options

    * `:peer` (required) — `{host, port}` tuple for the remote peer
    * `:ssl_opts` — TLS options list; defaults to options derived from
      `NeonFS.Transport.TLS.tls_dir/0`
    * `:pool_size` — number of connections per peer (default: 4, configurable
      via `:neonfs_client, :data_transfer, :pool_size`)
    * `:worker_idle_timeout` — idle health-check interval in ms (default: 30_000)
    * `:name` — optional registered name for the pool

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {peer, opts} = Keyword.pop!(opts, :peer)
    {ssl_opts, opts} = Keyword.pop_lazy(opts, :ssl_opts, &default_ssl_opts/0)
    {pool_size, opts} = Keyword.pop(opts, :pool_size, configured_pool_size())

    {worker_idle_timeout, opts} =
      Keyword.pop(opts, :worker_idle_timeout, @default_worker_idle_timeout)

    {name, _opts} = Keyword.pop(opts, :name)

    nimble_opts = [
      worker: {__MODULE__, %{peer: peer, ssl_opts: ssl_opts}},
      pool_size: pool_size,
      lazy: false,
      worker_idle_timeout: worker_idle_timeout
    ]

    nimble_opts = if name, do: Keyword.put(nimble_opts, :name, name), else: nimble_opts

    NimblePool.start_link(nimble_opts)
  end

  @doc """
  Checks out a connection, sends a serialised message, receives and deserialises
  the response, then checks the connection back in.

  ## Options

    * `:timeout` — checkout timeout in ms (default: 30_000)
    * `:recv_timeout` — recv timeout in ms (default: 30_000)

  Returns the deserialised response term on success, or `{:error, reason}` if
  send/recv fails (the connection is removed from the pool in that case).
  """
  @spec execute(GenServer.server(), term(), keyword()) :: term()
  def execute(pool, message, opts \\ []) do
    checkout_timeout = Keyword.get(opts, :timeout, @default_checkout_timeout)
    recv_timeout = Keyword.get(opts, :recv_timeout, @default_recv_timeout)

    NimblePool.checkout!(
      pool,
      :checkout,
      fn _command, socket ->
        data = :erlang.term_to_binary(message)
        :ok = :ssl.send(socket, data)
        {:ok, response_bytes} = :ssl.recv(socket, 0, recv_timeout)
        response = :erlang.binary_to_term(response_bytes, [:safe])
        {response, socket}
      end,
      checkout_timeout
    )
  end

  # NimblePool callbacks

  @impl NimblePool
  def init_pool(pool_state) do
    {:ok, Map.put(pool_state, :pool_pid, self())}
  end

  @impl NimblePool
  def init_worker(%{peer: {host, port}, ssl_opts: ssl_opts, pool_pid: pool_pid} = pool_state) do
    {:async,
     fn ->
       host_charlist = if is_binary(host), do: String.to_charlist(host), else: host
       {:ok, socket} = :ssl.connect(host_charlist, port, ssl_opts)
       # Transfer controlling process to the pool before this Task exits,
       # otherwise the socket is closed when the Task terminates.
       :ok = :ssl.controlling_process(socket, pool_pid)
       socket
     end, pool_state}
  end

  @impl NimblePool
  def handle_checkout(:checkout, _from, socket, pool_state) do
    {:ok, socket, socket, pool_state}
  end

  @impl NimblePool
  def handle_checkin(socket, _from, _old_socket, pool_state) do
    {:ok, socket, pool_state}
  end

  @impl NimblePool
  def handle_info({:ssl_closed, _}, _socket) do
    {:remove, :closed}
  end

  def handle_info({:ssl_error, _, _reason}, _socket) do
    {:remove, :error}
  end

  @impl NimblePool
  def handle_ping(socket, _pool_state) do
    case :ssl.connection_information(socket) do
      {:ok, _} -> {:ok, socket}
      {:error, _} -> {:remove, :stale}
    end
  end

  @impl NimblePool
  def terminate_worker(_reason, socket, pool_state) do
    :ssl.close(socket)
    {:ok, pool_state}
  end

  defp configured_pool_size do
    Application.get_env(:neonfs_client, :data_transfer, [])
    |> Keyword.get(:pool_size, @default_pool_size)
  end

  defp default_ssl_opts do
    tls_dir = TLS.tls_dir()

    [
      {:certs_keys,
       [%{certfile: Path.join(tls_dir, "node.crt"), keyfile: Path.join(tls_dir, "node.key")}]},
      {:cacertfile, Path.join(tls_dir, "ca.crt")},
      {:verify, :verify_peer},
      # Nodes connect by IP but certs are issued for hostnames. Since all certs
      # are signed by the cluster CA, cert chain verification is sufficient —
      # hostname checking is not needed for this internal PKI.
      {:server_name_indication, :disable},
      {:versions, [:"tlsv1.3"]},
      :binary,
      {:packet, 4},
      {:active, false}
    ]
  end
end
