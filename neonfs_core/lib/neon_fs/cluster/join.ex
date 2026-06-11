defmodule NeonFS.Cluster.Join do
  @moduledoc """
  Serving side of the cluster join flow, plus core's layering on the
  client join.

  The joining-side machinery (HTTP invite redemption, credential
  install, distribution restart, state persistence) lives in
  `NeonFS.Client.Join` so any node type can join (#1160). This module:

  - serves join requests on existing cluster members (`accept_join/6`),
  - wraps the client join with core's finalize hook (Ra membership,
    quorum-ring rebuild, data-plane activation).
  """

  alias NeonFS.Client.Join, as: ClientJoin
  alias NeonFS.Client.{ServiceInfo, ServiceType}
  alias NeonFS.Cluster.{Invite, State}

  alias NeonFS.Core.{
    AuditLog,
    CertificateAuthority,
    DriveManager,
    RaServer,
    ServiceRegistry,
    VolumeRegistry
  }

  alias NeonFS.Core.Supervisor, as: CoreSupervisor
  alias NeonFS.Transport.{Listener, PoolManager, TLS}

  require Logger

  import NeonFS.Client.ServiceType, only: [is_service_type: 1]

  @cluster_name :neonfs_meta

  @type join_params :: %{
          token: String.t(),
          via_node: atom()
        }

  @doc """
  Requests cluster membership from an existing node using an invite token.

  This function should be called on the joining node. It contacts the via
  node over HTTP to redeem the invite token and obtain cluster credentials,
  then starts TLS distribution with the cluster certificates and completes
  the join via Erlang distribution.

  ## Parameters
  - `token` - The invite token provided by the existing cluster
  - `via_address` - HTTP address of an existing cluster member (e.g., "node1:9568")
  - `type` - Service type for this node (default: `:core`). Non-core types
    skip Ra cluster membership but are registered in ServiceRegistry.

  ## Returns
  - `{:ok, :joining}` once the invite is redeemed and credentials are stored.
    The join then completes asynchronously: the node restarts TLS distribution
    (to load the cluster cert), connects to the via node, and joins Ra. Callers
    confirm completion by polling `cluster status` (#1033).
  - `{:error, reason}` on a synchronous failure (invalid token, unreachable via
    node, already in a cluster).

  ## Examples

      iex> NeonFS.Cluster.Join.join_cluster("nfs_inv_...", "node1:9568")
      {:ok, :joining}
  """
  @spec join_cluster(String.t(), String.t(), ServiceType.t()) ::
          {:ok, :joining} | {:error, term()}
  def join_cluster(token, via_address, type \\ :core)
      when is_binary(token) and is_binary(via_address) and is_service_type(type) do
    ClientJoin.join_cluster(token, via_address, type,
      finalize_hook: fn state ->
        activate_data_plane()
        finalize_core_membership(state, via_address, type)
      end
    )
  end

  defp finalize_core_membership(state, via_address, type) do
    if ServiceType.core?(type) do
      _ = join_ra_cluster(state)
      rebuild_quorum_ring_on_cores()
    end

    log_join_completed(state, via_address)
  end

  defp rebuild_quorum_ring_on_cores do
    CoreSupervisor.rebuild_quorum_ring()

    for node <- ServiceRegistry.connected_nodes_by_type(:core) do
      try do
        :erpc.call(node, CoreSupervisor, :rebuild_quorum_ring, [], 5_000)
      catch
        _, _ -> :ok
      end
    end

    :ok
  end

  defp log_join_completed(state, via_address) do
    AuditLog.log_event(
      event_type: :node_joined,
      actor_uid: 0,
      resource: Atom.to_string(state.this_node.name),
      details: %{
        cluster_id: state.cluster_id,
        node_type: Atom.to_string(state.node_type),
        via_address: via_address
      }
    )
  end

  @doc """
  Join a cluster via direct RPC (for testing or pre-connected nodes).

  Unlike `join_cluster/3` which uses HTTP for credential exchange, this
  function uses direct Erlang RPC to the via node. This requires that
  the joining node is already connected to the via node.

  Used by integration tests where the metrics HTTP server is not running.
  """
  @spec join_cluster_rpc(String.t(), atom(), ServiceType.t()) ::
          {:ok, State.t()} | {:error, term()}
  def join_cluster_rpc(token, via_node, type \\ :core)
      when is_binary(token) and is_atom(via_node) and is_service_type(type) do
    ClientJoin.join_cluster_rpc(token, via_node, type,
      finalize_hook: fn state ->
        activate_data_plane()

        if ServiceType.core?(type) do
          schedule_ra_join_async(state)
        end
      end
    )
  end

  # Schedule the Ra join to happen asynchronously after a delay.
  # This ensures the RPC response is sent before any Logger messages are emitted.
  # The delay gives time for the CLI to receive the response and close the connection.
  defp schedule_ra_join_async(state) do
    spawn(fn ->
      # Wait for the RPC response to be sent and connection to close
      receive do
      after
        500 -> :ok
      end

      # Now safe to log and do Ra operations
      case join_ra_cluster(state) do
        :ok ->
          Logger.info("Successfully joined cluster", cluster_name: state.cluster_name)

        {:error, reason} ->
          Logger.error("Failed to join Ra cluster", reason: inspect(reason))
      end
    end)

    :ok
  end

  @doc """
  Accepts a join request from a new node (called via RPC on existing cluster member).

  This function should be called on the existing cluster node.

  ## Parameters
  - `token` - The invite token from the joining node
  - `joining_node` - The node name of the joining node
  - `type` - Service type of the joining node (default: `:core`). Non-core
    types are registered in ServiceRegistry but not added to Ra cluster.
  - `csr` - The node's CSR for certificate issuance

  ## Returns
  - `{:ok, cluster_info}` containing cluster details and TLS certs for the joining node
  - `{:error, reason}` on failure
  """
  @spec accept_join(
          String.t(),
          atom(),
          ServiceType.t(),
          TLS.csr() | nil,
          term(),
          non_neg_integer()
        ) ::
          {:ok, map()} | {:error, term()}
  def accept_join(
        token,
        joining_node,
        type \\ :core,
        csr \\ nil,
        data_endpoint \\ nil,
        dist_port \\ 0
      )
      when is_binary(token) and is_atom(joining_node) and is_service_type(type) do
    hostname = joining_node |> Atom.to_string() |> String.split("@") |> List.last()

    with :ok <- Invite.validate_invite(token),
         {:ok, state} <- State.load(),
         {:ok, node_cert_pem, ca_cert_pem} <- sign_joining_node_csr(csr, hostname),
         {:ok, updated_state, joiner_peer} <-
           add_peer_to_state(state, joining_node, type, dist_port),
         :ok <- State.save(updated_state),
         :ok <- maybe_add_to_ra_cluster(joining_node, type) do
      register_service(joining_node, type, data_endpoint)
      maybe_adjust_system_volume_replication(updated_state, type)
      propagate_new_peer(state, joining_node, joiner_peer, type)

      cluster_info = %{
        cluster_id: state.cluster_id,
        cluster_name: state.cluster_name,
        created_at: DateTime.to_iso8601(state.created_at),
        master_key: state.master_key,
        known_peers:
          [self_as_peer(state.this_node) | state.known_peers]
          |> State.sanitise_peers(joining_node)
          |> Enum.map(&peer_to_wire/1),
        ra_cluster_members: Enum.map(updated_state.ra_cluster_members, &Atom.to_string/1),
        node_cert_pem: node_cert_pem,
        ca_cert_pem: ca_cert_pem,
        via_dist_port: ClientJoin.local_dist_port()
      }

      Logger.info("Accepted join request", type: type, joining_node: joining_node)
      {:ok, cluster_info}
    end
  end

  @doc """
  Adds `peer` to this node's persisted `known_peers`, deduped and excluding this
  node itself. Invoked via RPC on existing core peers when a new node joins, so
  every core node holds the full peer set and can resolve every other node's
  distribution port (#1060/#1061). Idempotent.
  """
  @spec add_known_peer(State.peer_info()) :: :ok | {:error, term()}
  def add_known_peer(peer) do
    with {:ok, state} <- State.load() do
      State.save(%{
        state
        | known_peers: State.sanitise_peers([peer | state.known_peers], state.this_node.name)
      })
    end
  end

  # Private functions

  defp self_as_peer(node_info) do
    %{
      id: node_info.id,
      name: node_info.name,
      last_seen: DateTime.utc_now(),
      dist_port: node_info[:dist_port] || 0
    }
  end

  defp peer_to_wire(peer) do
    %{
      id: peer.id,
      name: Atom.to_string(peer.name),
      last_seen: DateTime.to_iso8601(peer.last_seen),
      dist_port: peer[:dist_port] || 0
    }
  end

  defp activate_data_plane do
    case Listener.rebind() do
      :ok ->
        ServiceRegistry.refresh_self()
        broadcast_data_endpoint()
        create_peer_pools()

      {:error, reason} ->
        Logger.warning("Failed to activate data plane", reason: inspect(reason))
    end
  catch
    _, _ -> :ok
  end

  defp broadcast_data_endpoint do
    port = Listener.get_port()

    if port > 0 do
      endpoint = PoolManager.advertise_endpoint(port)
      this_node = Node.self()

      info =
        ServiceInfo.new(this_node, :core, metadata: %{data_endpoint: endpoint})

      for node <- ServiceRegistry.connected_nodes_by_type(:core) do
        try do
          :erpc.call(node, ServiceRegistry, :register, [info], 5_000)

          # Only create a pool if the remote node has its data plane active
          # (port > 0 means it has TLS certs and can make outbound connections)
          remote_port = :erpc.call(node, Listener, :get_port, [], 5_000)

          if remote_port > 0 do
            :erpc.call(node, PoolManager, :ensure_pool, [this_node, endpoint], 5_000)
          end
        catch
          _, _ -> :ok
        end
      end
    end
  rescue
    _ -> :ok
  end

  defp create_peer_pools do
    for peer <- ServiceRegistry.connected_nodes_by_type(:core) do
      try do
        case :erpc.call(peer, ServiceRegistry, :get, [peer, :core], 5_000) do
          {:ok, info} ->
            endpoint = Map.get(info.metadata || %{}, :data_endpoint)

            if endpoint do
              PoolManager.ensure_pool(peer, endpoint)
            end

          _ ->
            :ok
        end
      catch
        _, _ -> :ok
      end
    end
  rescue
    _ -> :ok
  end

  defp sign_joining_node_csr(nil, _hostname), do: {:ok, nil, nil}

  defp sign_joining_node_csr(csr, hostname) do
    if TLS.valid_csr_format?(csr) and TLS.validate_csr(csr) do
      case CertificateAuthority.sign_node_csr(csr, hostname) do
        {:ok, node_cert, ca_cert} ->
          {:ok, TLS.encode_cert(node_cert), TLS.encode_cert(ca_cert)}

        {:error, reason} ->
          {:error, {:cert_signing_failed, reason}}
      end
    else
      {:error, {:cert_signing_failed, :invalid_csr}}
    end
  end

  defp generate_node_id do
    :crypto.strong_rand_bytes(5)
    |> Base.encode32(case: :lower, padding: false)
    |> binary_part(0, 8)
  end

  defp add_peer_to_state(%State{} = state, joining_node, type, dist_port) do
    node_id = generate_node_id()

    peer_info = %{
      id: node_id,
      name: joining_node,
      last_seen: DateTime.utc_now(),
      dist_port: dist_port
    }

    # Only core nodes are added to ra_cluster_members
    ra_cluster_members =
      if ServiceType.core?(type) do
        [joining_node | state.ra_cluster_members]
      else
        state.ra_cluster_members
      end

    updated_state = %{
      state
      | known_peers: State.sanitise_peers([peer_info | state.known_peers], state.this_node.name),
        ra_cluster_members: Enum.uniq(ra_cluster_members)
    }

    {:ok, updated_state, peer_info}
  end

  # Tell the existing core peers about the newly-joined core node so every core
  # node holds the complete peer set and the distribution mesh closes. Without
  # this, a node that joined earlier never learns about later joiners and cannot
  # resolve their dist port (#1060/#1061). Best-effort: a peer that is briefly
  # unreachable picks the entry up on its next restart from its own join state.
  defp propagate_new_peer(%State{} = state, joining_node, joiner_peer, type) do
    if ServiceType.core?(type) do
      state.ra_cluster_members
      |> Enum.reject(&(&1 == Node.self() or &1 == joining_node))
      |> Enum.each(fn peer_node ->
        try do
          :erpc.call(peer_node, __MODULE__, :add_known_peer, [joiner_peer], 5_000)
        catch
          _, _ -> :ok
        end
      end)
    end

    :ok
  end

  defp maybe_add_to_ra_cluster(joining_node, type) do
    if ServiceType.core?(type) do
      add_to_ra_cluster(joining_node)
    else
      :ok
    end
  end

  # Bump the system volume's replication factor when a new core joins,
  # but never shrink it — `neonfs cluster init --system-replicas N`
  # seeds the factor at the operator's chosen value, and a 2-node join
  # arriving later should not knock it back to 2.
  defp maybe_adjust_system_volume_replication(updated_state, type) do
    if ServiceType.core?(type) do
      adjust_system_volume_replication_for_core_join(updated_state)
    end
  end

  defp adjust_system_volume_replication_for_core_join(updated_state) do
    core_count = length(updated_state.ra_cluster_members)
    current_factor = current_system_volume_factor()

    if core_count > current_factor do
      apply_system_volume_replication(core_count)
    end
  end

  defp apply_system_volume_replication(core_count) do
    case VolumeRegistry.adjust_system_volume_replication(core_count) do
      {:ok, _volume} ->
        Logger.info("System volume replication factor adjusted",
          core_count: core_count
        )

      {:error, reason} ->
        Logger.warning("Failed to adjust system volume replication",
          core_count: core_count,
          reason: inspect(reason)
        )
    end
  catch
    # The adjustment is best-effort during a join — `{:error, _}` already
    # only warns. A `GenServer.call` timeout arrives as an exit, which
    # otherwise escapes the `case` and fails the whole join even though
    # the VolumeRegistry finishes the adjustment server-side (the
    # caller's timeout only drops the reply). Treat it the same as an
    # error return (#1154).
    :exit, reason ->
      Logger.warning("Failed to adjust system volume replication",
        core_count: core_count,
        reason: inspect(reason)
      )
  end

  defp current_system_volume_factor do
    case VolumeRegistry.get_system_volume() do
      {:ok, %{durability: %{factor: factor}}} -> factor
      _ -> 0
    end
  end

  defp register_service(joining_node, type, data_endpoint) do
    metadata =
      if data_endpoint do
        %{data_endpoint: data_endpoint}
      else
        %{}
      end

    info = ServiceInfo.new(joining_node, type, metadata: metadata)

    case ServiceRegistry.register(info) do
      :ok ->
        Logger.info("Registered service", type: type, joining_node: joining_node)

      {:error, reason} ->
        Logger.warning("Failed to register service",
          joining_node: joining_node,
          reason: inspect(reason)
        )
    end
  end

  defp add_to_ra_cluster(joining_node) do
    this_node = Node.self()
    server_id = {@cluster_name, this_node}
    new_server_id = {@cluster_name, joining_node}

    # Wait for the Ra server to be ready and have a leader
    case wait_for_leader(server_id) do
      :ok ->
        # Check if already a member (e.g., pre-added by Formation init node)
        if ra_member?(server_id, new_server_id) do
          Logger.info("Node already in Ra cluster", joining_node: joining_node)
          :ok
        else
          # Ra only allows one cluster membership change at a time.
          # Retry with backoff if a change is already in progress.
          add_member_with_retry(server_id, new_server_id, joining_node, 0, 10)
        end

      {:error, reason} ->
        Logger.error("Ra cluster not ready", reason: inspect(reason))
        {:error, {:ra_not_ready, reason}}
    end
  end

  defp ra_member?(server_id, target_server_id) do
    case :ra.members(server_id, 5_000) do
      {:ok, members, _leader} -> target_server_id in members
      _ -> false
    end
  end

  # Retry add_member with exponential backoff for :cluster_change_not_permitted
  defp add_member_with_retry(server_id, new_server_id, joining_node, attempt, max_attempts) do
    case :ra.add_member(server_id, new_server_id, 30_000) do
      {:ok, _, _leader} ->
        Logger.info("Added node to Ra cluster", joining_node: joining_node)
        :ok

      {:timeout, _} ->
        Logger.warning("Timeout adding node to Ra cluster", joining_node: joining_node)
        {:error, :ra_add_timeout}

      {:error, :cluster_change_not_permitted} when attempt < max_attempts ->
        # Another cluster change is in progress, wait and retry
        backoff = min(100 * :math.pow(2, attempt), 5000) |> trunc()

        Logger.debug("Cluster change in progress, retrying",
          backoff_ms: backoff,
          attempt: attempt + 1,
          max_attempts: max_attempts
        )

        receive do
        after
          backoff -> :ok
        end

        add_member_with_retry(server_id, new_server_id, joining_node, attempt + 1, max_attempts)

      {:error, reason} ->
        Logger.error("Failed to add node to Ra cluster",
          joining_node: joining_node,
          reason: inspect(reason)
        )

        {:error, {:ra_add_failed, reason}}
    end
  end

  # Wait for the Ra cluster to have a leader
  # With cluster membership changes, leader election can take longer
  # 100 attempts * 200ms = 20 seconds max wait
  defp wait_for_leader(server_id, attempts \\ 0, max_attempts \\ 100) do
    if attempts >= max_attempts do
      {:error, :no_leader}
    else
      case :ra.members(server_id) do
        {:ok, _members, leader} when leader != :undefined ->
          Logger.debug("Ra cluster has leader", leader: leader)
          :ok

        {:ok, _members, :undefined} ->
          # No leader yet, wait and retry
          receive do
          after
            200 -> :ok
          end

          wait_for_leader(server_id, attempts + 1, max_attempts)

        {:error, _reason} ->
          # Error querying members, wait and retry
          receive do
          after
            200 -> :ok
          end

          wait_for_leader(server_id, attempts + 1, max_attempts)

        {:timeout, _} ->
          # Timeout, wait and retry
          receive do
          after
            200 -> :ok
          end

          wait_for_leader(server_id, attempts + 1, max_attempts)
      end
    end
  end

  defp join_ra_cluster(state) do
    # When joining a cluster, we need to reconfigure the local Ra server
    # to join the existing cluster. The Ra server was started as a single-node
    # cluster initially, but now needs to be part of the existing cluster.

    this_node = Node.self()

    existing_members =
      state.ra_cluster_members
      |> Enum.filter(&(&1 != this_node))

    Logger.info("Node joining Ra cluster",
      node: this_node,
      existing_members: existing_members
    )

    # Call RaServer to reconfigure for cluster join
    case RaServer.join_cluster(existing_members) do
      :ok ->
        Logger.info("Ra server successfully joined cluster")
        # The joining node's startup-configured drives live in
        # `DriveRegistry`'s ETS but never make it into the
        # Ra-replicated bootstrap-layer drives table — that's the
        # same gap `Cluster.Init.do_init_cluster/1` plugged for the
        # bootstrap node via #890. Without this call,
        # `Volume.Provisioner` only ever sees the bootstrap node's
        # drives, so `min_copies > 1` durabilities can't be
        # satisfied even on multi-node clusters.
        :ok = DriveManager.register_local_drives_in_bootstrap()
        :ok

      {:error, reason} ->
        Logger.error("Failed to join Ra cluster", reason: inspect(reason))
        {:error, {:ra_join_failed, reason}}
    end
  end
end
