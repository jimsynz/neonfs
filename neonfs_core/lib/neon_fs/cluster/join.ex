defmodule NeonFS.Cluster.Join do
  @moduledoc """
  Node join flow for adding nodes to an existing cluster.

  This module handles both sides of the join process:
  - Creating an invite token on an existing cluster node
  - Joining a cluster using an invite token
  """

  alias NeonFS.Client.{ServiceInfo, ServiceType}
  alias NeonFS.Cluster.{Invite, State}
  alias NeonFS.Core.{CertificateAuthority, RaServer, ServiceRegistry, VolumeRegistry}
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

  This function should be called on the joining node.

  ## Parameters
  - `token` - The invite token provided by the existing cluster
  - `via_node` - The node name of an existing cluster member (e.g., :neonfs_core@node1)
  - `type` - Service type for this node (default: `:core`). Non-core types
    skip Ra cluster membership but are registered in ServiceRegistry.

  ## Returns
  - `{:ok, state}` on successful join
  - `{:error, reason}` on failure

  ## Examples

      iex> NeonFS.Cluster.Join.join_cluster("nfs_inv_...", :neonfs_core@node1)
      {:ok, %NeonFS.Cluster.State{}}

      iex> NeonFS.Cluster.Join.join_cluster("nfs_inv_...", :neonfs_core@node1, :fuse)
      {:ok, %NeonFS.Cluster.State{}}
  """
  @spec join_cluster(String.t(), atom(), ServiceType.t()) ::
          {:ok, State.t()} | {:error, term()}
  def join_cluster(token, via_node, type \\ :core)
      when is_binary(token) and is_atom(via_node) and is_service_type(type) do
    this_node = Node.self()
    node_name = Atom.to_string(this_node)
    node_key = TLS.generate_node_key()
    csr = TLS.create_csr(node_key, node_name)

    data_endpoint = local_data_endpoint()

    with :ok <- validate_not_in_cluster(),
         {:ok, cluster_info} <-
           request_join(via_node, token, this_node, type, csr, data_endpoint),
         {:ok, state} <- build_cluster_state(cluster_info, type),
         :ok <- State.save(state),
         :ok <- store_local_tls(cluster_info, node_key) do
      activate_data_plane()

      # Only core nodes join the Ra cluster
      if ServiceType.core?(type) do
        schedule_ra_join_async(state)
      end

      {:ok, state}
    end
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
  @spec accept_join(String.t(), atom(), ServiceType.t(), TLS.csr() | nil, term()) ::
          {:ok, map()} | {:error, term()}
  def accept_join(token, joining_node, type \\ :core, csr \\ nil, data_endpoint \\ nil)
      when is_binary(token) and is_atom(joining_node) and is_service_type(type) do
    hostname = joining_node |> Atom.to_string() |> String.split("@") |> List.last()

    with :ok <- Invite.validate_invite(token),
         {:ok, state} <- State.load(),
         {:ok, node_cert_pem, ca_cert_pem} <- sign_joining_node_csr(csr, hostname),
         {:ok, updated_state} <- add_peer_to_state(state, joining_node, type),
         :ok <- State.save(updated_state),
         :ok <- maybe_add_to_ra_cluster(joining_node, type) do
      # Register in ServiceRegistry for all node types
      register_service(joining_node, type, data_endpoint)

      # Adjust system volume replication for core node joins (non-fatal)
      maybe_adjust_system_volume_replication(updated_state, type)

      cluster_info = %{
        cluster_id: state.cluster_id,
        cluster_name: state.cluster_name,
        # Convert DateTime to ISO8601 string for RPC transport
        created_at: DateTime.to_iso8601(state.created_at),
        master_key: state.master_key,
        # Convert peer info to serialisable format
        known_peers:
          Enum.map(updated_state.known_peers, fn peer ->
            %{
              id: peer.id,
              name: Atom.to_string(peer.name),
              last_seen: DateTime.to_iso8601(peer.last_seen)
            }
          end),
        ra_cluster_members: Enum.map(updated_state.ra_cluster_members, &Atom.to_string/1),
        node_cert_pem: node_cert_pem,
        ca_cert_pem: ca_cert_pem
      }

      Logger.info("Accepted join request", type: type, joining_node: joining_node)
      {:ok, cluster_info}
    end
  end

  # Private functions

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

      for node <- Node.list() do
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
    for peer <- Node.list() do
      try do
        case :erpc.call(peer, ServiceRegistry, :get, [peer], 5_000) do
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

  defp store_local_tls(%{node_cert_pem: nil}, _node_key), do: :ok
  defp store_local_tls(%{ca_cert_pem: nil}, _node_key), do: :ok

  defp store_local_tls(%{node_cert_pem: node_cert_pem, ca_cert_pem: ca_cert_pem}, node_key) do
    ca_cert = TLS.decode_cert!(ca_cert_pem)
    node_cert = TLS.decode_cert!(node_cert_pem)
    TLS.write_local_tls(ca_cert, node_cert, node_key)
    :ok
  end

  defp store_local_tls(_cluster_info, _node_key), do: :ok

  defp validate_not_in_cluster do
    if State.exists?() do
      {:error, :already_in_cluster}
    else
      :ok
    end
  end

  defp request_join(via_node, token, this_node, type, csr, data_endpoint) do
    case :rpc.call(via_node, __MODULE__, :accept_join, [
           token,
           this_node,
           type,
           csr,
           data_endpoint
         ]) do
      {:ok, cluster_info} ->
        {:ok, cluster_info}

      {:error, reason} ->
        {:error, {:join_rejected, reason}}

      {:badrpc, reason} ->
        {:error, {:rpc_failed, reason}}
    end
  end

  defp build_cluster_state(cluster_info, type) do
    this_node = Node.self()
    node_id = generate_node_id()

    node_info = %{
      id: node_id,
      name: this_node,
      joined_at: DateTime.utc_now()
    }

    # Parse created_at from ISO8601 string
    created_at =
      case DateTime.from_iso8601(cluster_info.created_at) do
        {:ok, dt, _offset} -> dt
        _ -> DateTime.utc_now()
      end

    # Convert known_peers back to proper format
    known_peers =
      Enum.map(cluster_info.known_peers, fn peer ->
        %{
          id: peer["id"] || peer.id,
          name: parse_atom(peer["name"] || peer.name),
          last_seen: parse_datetime(peer["last_seen"] || peer.last_seen)
        }
      end)

    # Convert ra_cluster_members back to atoms
    ra_members =
      Enum.map(cluster_info.ra_cluster_members, fn member ->
        parse_atom(member)
      end)

    # Non-core nodes don't add themselves to ra_cluster_members
    ra_cluster_members =
      if ServiceType.core?(type) do
        [this_node | ra_members]
      else
        ra_members
      end

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

  # Parse a value that might be a string or atom into an atom
  defp parse_atom(value) when is_atom(value), do: value
  defp parse_atom(value) when is_binary(value), do: String.to_atom(value)

  # Parse a value that might be a DateTime or ISO8601 string into DateTime
  defp parse_datetime(%DateTime{} = dt), do: dt

  defp parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _offset} -> dt
      _ -> DateTime.utc_now()
    end
  end

  defp add_peer_to_state(%State{} = state, joining_node, type) do
    node_id = generate_node_id()

    peer_info = %{
      id: node_id,
      name: joining_node,
      last_seen: DateTime.utc_now()
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
      | known_peers: [peer_info | state.known_peers],
        ra_cluster_members: ra_cluster_members
    }

    {:ok, updated_state}
  end

  defp maybe_add_to_ra_cluster(joining_node, type) do
    if ServiceType.core?(type) do
      add_to_ra_cluster(joining_node)
    else
      :ok
    end
  end

  defp maybe_adjust_system_volume_replication(updated_state, type) do
    if ServiceType.core?(type) do
      core_count = length(updated_state.ra_cluster_members)

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
        :ok

      {:error, reason} ->
        Logger.error("Failed to join Ra cluster", reason: inspect(reason))
        {:error, {:ra_join_failed, reason}}
    end
  end

  defp local_data_endpoint do
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
end
