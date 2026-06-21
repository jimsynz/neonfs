defmodule NeonFS.Client.Join do
  @moduledoc """
  Joining-side cluster join flow (#1160).

  Any node that depends on `neonfs_client` can join a cluster with an
  invite token — interface nodes (NFS, S3, WebDAV, FUSE, Docker, …)
  exactly like core nodes, just registering as a service instead of
  joining Ra. The serving side (`accept_join`, invite validation, CSR
  signing) lives in `NeonFS.Cluster.Join` on core nodes; this module
  only ever talks to it over HTTP or Erlang distribution.

  Two entry points:

  - `join_cluster/4` — production flow: HTTP invite redemption against
    the via node's `/api/cluster/redeem-invite` endpoint (no working
    TLS distribution required), credential install, distribution
    restart, then the join completes via distribution RPC.
  - `join_cluster_rpc/4` — direct-RPC flow for integration tests and
    pre-connected nodes (no metrics HTTP server required).

  Core layers its Ra membership and data-plane activation on top via
  the `:finalize_hook` option — a 1-arity function receiving the saved
  `NeonFS.Cluster.State`, invoked after the join has been persisted.
  """

  alias NeonFS.Client.{InviteCrypto, ServiceType}
  alias NeonFS.Cluster.State
  alias NeonFS.TLSDistConfig
  alias NeonFS.Transport.{Listener, PoolManager, TLS}

  require Logger

  import NeonFS.Client.ServiceType, only: [is_service_type: 1]

  # The serving-side module on core nodes; referenced only as an RPC
  # target, never as a compile-time dependency.
  @core_join_module NeonFS.Cluster.Join

  # Default port for the core node's metrics HTTP server, which serves
  # the invite-redemption endpoint. Mirrors `:neonfs_core` `:metrics_port`.
  @default_redeem_port 9568

  @doc """
  Requests cluster membership from an existing node using an invite token.

  Contacts the via node over HTTP to redeem the invite token and obtain
  cluster credentials, then starts TLS distribution with the cluster
  certificates and completes the join via Erlang distribution.

  ## Parameters
  - `token` - The invite token provided by the existing cluster
  - `via_address` - HTTP address of an existing cluster member (e.g., "node1:9568")
  - `type` - Service type for this node (default: `:core`). Non-core types
    skip Ra cluster membership but are registered in ServiceRegistry.
  - `opts` - `:finalize_hook` — 1-arity function receiving the saved
    `State` after the join persists; core passes its Ra-membership and
    data-plane activation here.

  ## Returns
  - `{:ok, :joining}` once the invite is redeemed and credentials are stored.
    The join then completes asynchronously: the node restarts TLS distribution
    (to load the cluster cert), connects to the via node, and finalizes.
    Callers confirm completion by polling `cluster status` (#1033).
  - `{:error, reason}` on a synchronous failure (invalid token, unreachable via
    node, already in a cluster).
  """
  @spec join_cluster(String.t(), String.t(), ServiceType.t(), keyword()) ::
          {:ok, :joining} | {:error, term()}
  def join_cluster(token, via_address, type \\ :core, opts \\ [])
      when is_binary(token) and is_binary(via_address) and is_service_type(type) do
    this_node = Node.self()
    node_name = Atom.to_string(this_node)
    node_key = TLS.generate_node_key()
    csr = TLS.create_csr(node_key, node_name)

    with :ok <- validate_not_in_cluster(),
         {:ok, credentials} <- request_join_http(via_address, token, csr, node_name),
         :ok <- store_credentials(credentials, node_key),
         :ok <- TLSDistConfig.regenerate(TLS.tls_dir()) do
      # Connecting to the via node over TLS distribution requires the cluster
      # cert we just wrote, which only goes live after a distribution restart —
      # and that restart drops the CLI's connection. So the rest of the join
      # (restart → connect → finalize) runs in a detached process; the CLI
      # reconnects and validates via `cluster status` (#1033). Synchronous
      # failures (bad token, unreachable via node) are still reported directly.
      finalize_join_async(credentials, token, this_node, type, opts)
      {:ok, :joining}
    end
  end

  @doc """
  Join a cluster via direct RPC (for testing or pre-connected nodes).

  Unlike `join_cluster/4` which uses HTTP for credential exchange, this
  function uses direct Erlang RPC to the via node. This requires that
  the joining node is already connected to the via node.

  Used by integration tests where the metrics HTTP server is not running.
  """
  @spec join_cluster_rpc(String.t(), atom(), ServiceType.t(), keyword()) ::
          {:ok, State.t()} | {:error, term()}
  def join_cluster_rpc(token, via_node, type \\ :core, opts \\ [])
      when is_binary(token) and is_atom(via_node) and is_service_type(type) do
    this_node = Node.self()
    node_name = Atom.to_string(this_node)
    node_key = TLS.generate_node_key()
    csr = TLS.create_csr(node_key, node_name)
    data_endpoint = local_data_endpoint()

    with :ok <- validate_not_in_cluster(),
         {:ok, cluster_info} <-
           request_join_rpc(via_node, token, this_node, type, csr, data_endpoint),
         {:ok, state} <- build_cluster_state(cluster_info, type),
         :ok <- State.save(state),
         :ok <- store_rpc_tls(cluster_info, node_key) do
      run_finalize_hook(opts, state)
      {:ok, state}
    end
  end

  @doc """
  Builds the joining node's `ra_cluster_members`. Core nodes include themselves;
  the advertised members already contain the joiner, so the result is deduped to
  avoid a duplicate self entry. Non-core nodes are not Ra members.
  """
  @spec joiner_ra_members([atom()], atom(), ServiceType.t()) :: [atom()]
  def joiner_ra_members(ra_members, this_node, type) do
    if ServiceType.core?(type) do
      Enum.uniq([this_node | ra_members])
    else
      Enum.uniq(ra_members)
    end
  end

  @doc """
  The local node's advertised data-plane endpoint, or `nil` when the
  transport listener isn't running.
  """
  @spec local_data_endpoint() :: term() | nil
  def local_data_endpoint do
    case Process.whereis(Listener) do
      nil ->
        nil

      _pid ->
        port = Listener.get_port()
        if port > 0, do: PoolManager.advertise_endpoint(port)
    end
  rescue
    _ -> nil
  end

  @doc """
  The local node's Erlang distribution listen port (from
  `$NEONFS_DIST_PORT`), or 0 when unset.
  """
  @spec local_dist_port() :: non_neg_integer()
  def local_dist_port do
    case System.get_env("NEONFS_DIST_PORT") do
      nil -> 0
      port_str -> String.to_integer(port_str)
    end
  rescue
    _ -> 0
  end

  @doc """
  Appends the default redeem port (#{@default_redeem_port}) to a via address
  that omits one, so a bare `host` reaches the core metrics HTTP server
  instead of defaulting to port 80 (#1346).

  An address that already carries a port — `host:9568` or bracketed IPv6
  `[::1]:9568` — is returned unchanged.
  """
  @spec ensure_redeem_port(String.t()) :: String.t()
  def ensure_redeem_port("[" <> _ = address) do
    if String.contains?(address, "]:"),
      do: address,
      else: address <> ":#{@default_redeem_port}"
  end

  def ensure_redeem_port(address) do
    if String.contains?(address, ":"),
      do: address,
      else: address <> ":#{@default_redeem_port}"
  end

  # ── Async finalize (HTTP flow) ────────────────────────────────────

  # Detached: survives both the CLI disconnect and the distribution restart it
  # triggers (it's a local, unlinked process). The 500 ms delay lets the CLI's
  # "joining" ack flush before distribution drops.
  defp finalize_join_async(credentials, token, this_node, type, opts) do
    spawn(fn -> do_finalize_join(credentials, token, this_node, type, opts) end)
    :ok
  end

  defp do_finalize_join(credentials, token, this_node, type, opts) do
    Process.sleep(500)

    case run_finalize_join(credentials, token, this_node, type, opts) do
      :ok ->
        :ok

      other ->
        Logger.error("cluster join finalize failed", reason: inspect(other))
        :telemetry.execute([:neonfs, :cluster, :join_failed], %{}, %{reason: inspect(other)})
    end
  end

  defp run_finalize_join(credentials, token, this_node, type, opts) do
    with :ok <- TLSDistConfig.restart_distribution(),
         :ok <- TLSDistConfig.await_distribution(10_000),
         :ok <- activate_cluster_distribution(credentials),
         {:ok, cluster_info} <-
           complete_join_via_distribution(credentials, token, this_node, type),
         {:ok, state} <- build_cluster_state(cluster_info, type),
         :ok <- State.save(state) do
      run_finalize_hook(opts, state)
      :telemetry.execute([:neonfs, :cluster, :join_completed], %{}, %{type: type})
      :ok
    end
  end

  defp run_finalize_hook(opts, state) do
    case Keyword.get(opts, :finalize_hook) do
      hook when is_function(hook, 1) -> hook.(state)
      nil -> :ok
    end
  end

  # ── HTTP invite redemption ────────────────────────────────────────

  defp request_join_http(via_address, token, csr, node_name) do
    :inets.start()
    csr_pem = TLS.encode_csr(csr)
    {random, expiry} = parse_token_parts(token)
    proof = :crypto.mac(:hmac, :sha256, token, csr_pem) |> Base.encode64()

    body =
      %{
        "csr_pem" => csr_pem,
        "token_random" => random,
        "token_expiry" => expiry,
        "proof" => proof,
        "node_name" => node_name,
        "dist_port" => local_dist_port()
      }
      |> :json.encode()
      |> IO.iodata_to_binary()

    url = ~c"http://#{ensure_redeem_port(via_address)}/api/cluster/redeem-invite"
    request = {url, [], ~c"application/json", body}

    case :httpc.request(:post, request, [{:timeout, 30_000}], []) do
      {:ok, {{_, 200, _}, _headers, response_body}} ->
        response_binary = IO.iodata_to_binary(response_body)
        InviteCrypto.decrypt_response(response_binary, token)

      {:ok, {{_, status, _}, _, response_body}} ->
        Logger.error(
          "Invite redemption HTTP error: status=#{status} body=#{IO.iodata_to_binary(response_body)}"
        )

        {:error, {:http_error, status}}

      {:error, reason} ->
        Logger.error("Invite redemption HTTP failed", reason: inspect(reason))
        {:error, {:http_failed, reason}}
    end
  end

  defp parse_token_parts(token) do
    case String.split(token, "_") do
      ["nfs", "inv", random, expiry, _signature] -> {random, expiry}
    end
  end

  # ── Credential install ────────────────────────────────────────────

  defp store_credentials(credentials, node_key) do
    ca_cert = TLS.decode_cert!(credentials["ca_cert_pem"])
    node_cert = TLS.decode_cert!(credentials["node_cert_pem"])
    TLS.write_local_tls(ca_cert, node_cert, node_key)
    :ok
  end

  defp store_rpc_tls(%{node_cert_pem: nil}, _node_key), do: :ok
  defp store_rpc_tls(%{ca_cert_pem: nil}, _node_key), do: :ok

  defp store_rpc_tls(%{node_cert_pem: node_cert_pem, ca_cert_pem: ca_cert_pem}, node_key) do
    ca_cert = TLS.decode_cert!(ca_cert_pem)
    node_cert = TLS.decode_cert!(node_cert_pem)
    TLS.write_local_tls(ca_cert, node_cert, node_key)
    :ok
  end

  defp store_rpc_tls(_cluster_info, _node_key), do: :ok

  # ── Distribution + serving-side RPC ───────────────────────────────

  defp activate_cluster_distribution(credentials) do
    via_node = credentials["via_node"] |> String.to_atom()
    via_dist_port = credentials["via_dist_port"]

    if is_integer(via_dist_port) and via_dist_port > 0 do
      System.put_env("NEONFS_PEER_PORTS", "#{via_node}:#{via_dist_port}")
    end

    case Node.connect(via_node) do
      true ->
        :ok

      false ->
        Logger.error("Failed to connect to via node: #{via_node}")
        {:error, {:connect_failed, via_node}}
    end
  end

  defp complete_join_via_distribution(credentials, token, this_node, type) do
    via_node = credentials["via_node"] |> String.to_atom()

    request_join_rpc(via_node, token, this_node, type, nil, local_data_endpoint())
  end

  defp request_join_rpc(via_node, token, this_node, type, csr, data_endpoint) do
    case :rpc.call(via_node, @core_join_module, :accept_join, [
           token,
           this_node,
           type,
           csr,
           data_endpoint,
           local_dist_port()
         ]) do
      {:ok, cluster_info} ->
        {:ok, cluster_info}

      {:error, reason} ->
        {:error, {:join_rejected, reason}}

      {:badrpc, reason} ->
        {:error, {:rpc_failed, reason}}
    end
  end

  # ── Local state construction ──────────────────────────────────────

  defp validate_not_in_cluster do
    if State.exists?() do
      {:error, :already_in_cluster}
    else
      :ok
    end
  end

  defp build_cluster_state(cluster_info, type) do
    this_node = Node.self()

    node_info = %{
      id: generate_node_id(),
      name: this_node,
      joined_at: DateTime.utc_now(),
      dist_port: local_dist_port()
    }

    created_at =
      case DateTime.from_iso8601(cluster_info.created_at) do
        {:ok, dt, _offset} -> dt
        _ -> DateTime.utc_now()
      end

    known_peers =
      cluster_info.known_peers
      |> Enum.map(fn peer ->
        %{
          id: peer["id"] || peer.id,
          name: parse_atom(peer["name"] || peer.name),
          last_seen: parse_datetime(peer["last_seen"] || peer.last_seen),
          dist_port: peer["dist_port"] || peer[:dist_port] || 0
        }
      end)
      |> State.sanitise_peers(this_node)

    ra_members = Enum.map(cluster_info.ra_cluster_members, &parse_atom/1)
    ra_cluster_members = joiner_ra_members(ra_members, this_node, type)

    state = %State{
      cluster_id: cluster_info.cluster_id,
      cluster_name: cluster_info.cluster_name,
      created_at: created_at,
      master_key: cluster_info.master_key,
      this_node: node_info,
      known_peers: known_peers,
      ra_cluster_members: ra_cluster_members,
      node_type: type
    }

    {:ok, state}
  end

  defp generate_node_id do
    :crypto.strong_rand_bytes(5)
    |> Base.encode32(case: :lower, padding: false)
    |> binary_part(0, 8)
  end

  defp parse_atom(value) when is_atom(value), do: value
  defp parse_atom(value) when is_binary(value), do: String.to_atom(value)

  defp parse_datetime(%DateTime{} = dt), do: dt

  defp parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _offset} -> dt
      _ -> DateTime.utc_now()
    end
  end
end
