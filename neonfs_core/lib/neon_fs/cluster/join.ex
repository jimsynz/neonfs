defmodule NeonFS.Cluster.Join do
  @moduledoc """
  Node join flow for adding nodes to an existing cluster.

  This module handles both sides of the join process:
  - Creating an invite token on an existing cluster node
  - Joining a cluster using an invite token
  """

  alias NeonFS.Client.{ServiceInfo, ServiceType}
  alias NeonFS.Cluster.{Invite, State}
  alias NeonFS.Core.{RaServer, ServiceRegistry}

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

    with :ok <- validate_not_in_cluster(),
         {:ok, cluster_info} <- request_join(via_node, token, this_node, type),
         {:ok, state} <- build_cluster_state(cluster_info, type),
         :ok <- State.save(state) do
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
      Process.sleep(500)

      # Now safe to log and do Ra operations
      case join_ra_cluster(state) do
        :ok ->
          Logger.info("Successfully joined cluster #{state.cluster_name}")

        {:error, reason} ->
          Logger.error("Failed to join Ra cluster: #{inspect(reason)}")
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

  ## Returns
  - `{:ok, cluster_info}` containing cluster details for the joining node
  - `{:error, reason}` on failure
  """
  @spec accept_join(String.t(), atom(), ServiceType.t()) :: {:ok, map()} | {:error, term()}
  def accept_join(token, joining_node, type \\ :core)
      when is_binary(token) and is_atom(joining_node) and is_service_type(type) do
    with :ok <- Invite.validate_invite(token),
         {:ok, state} <- State.load(),
         {:ok, updated_state} <- add_peer_to_state(state, joining_node, type),
         :ok <- State.save(updated_state),
         :ok <- maybe_add_to_ra_cluster(joining_node, type) do
      # Register in ServiceRegistry for all node types
      register_service(joining_node, type)

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
        ra_cluster_members: Enum.map(updated_state.ra_cluster_members, &Atom.to_string/1)
      }

      Logger.info("Accepted #{type} join request from #{inspect(joining_node)}")
      {:ok, cluster_info}
    end
  end

  # Private functions

  defp validate_not_in_cluster do
    if State.exists?() do
      {:error, :already_in_cluster}
    else
      :ok
    end
  end

  defp request_join(via_node, token, this_node, type) do
    case :rpc.call(via_node, __MODULE__, :accept_join, [token, this_node, type]) do
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

  defp register_service(joining_node, type) do
    info = ServiceInfo.new(joining_node, type)

    case ServiceRegistry.register(info) do
      :ok ->
        Logger.info("Registered #{type} service for #{inspect(joining_node)}")

      {:error, reason} ->
        Logger.warning(
          "Failed to register service for #{inspect(joining_node)}: #{inspect(reason)}"
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
        # Ra only allows one cluster membership change at a time.
        # Retry with backoff if a change is already in progress.
        add_member_with_retry(server_id, new_server_id, joining_node, 0, 10)

      {:error, reason} ->
        Logger.error("Ra cluster not ready: #{inspect(reason)}")
        {:error, {:ra_not_ready, reason}}
    end
  end

  # Retry add_member with exponential backoff for :cluster_change_not_permitted
  defp add_member_with_retry(server_id, new_server_id, joining_node, attempt, max_attempts) do
    case :ra.add_member(server_id, new_server_id, 30_000) do
      {:ok, _, _leader} ->
        Logger.info("Added #{inspect(joining_node)} to Ra cluster")
        :ok

      {:timeout, _} ->
        Logger.warning("Timeout adding #{inspect(joining_node)} to Ra cluster")
        {:error, :ra_add_timeout}

      {:error, :cluster_change_not_permitted} when attempt < max_attempts ->
        # Another cluster change is in progress, wait and retry
        backoff = min(100 * :math.pow(2, attempt), 5000) |> trunc()

        Logger.debug(
          "Cluster change in progress, retrying in #{backoff}ms (attempt #{attempt + 1}/#{max_attempts})"
        )

        Process.sleep(backoff)
        add_member_with_retry(server_id, new_server_id, joining_node, attempt + 1, max_attempts)

      {:error, reason} ->
        Logger.error("Failed to add #{inspect(joining_node)} to Ra cluster: #{inspect(reason)}")

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
          Logger.debug("Ra cluster has leader: #{inspect(leader)}")
          :ok

        {:ok, _members, :undefined} ->
          # No leader yet, wait and retry
          Process.sleep(200)
          wait_for_leader(server_id, attempts + 1, max_attempts)

        {:error, _reason} ->
          # Error querying members, wait and retry
          Process.sleep(200)
          wait_for_leader(server_id, attempts + 1, max_attempts)

        {:timeout, _} ->
          # Timeout, wait and retry
          Process.sleep(200)
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

    Logger.info(
      "Node #{inspect(this_node)} joining Ra cluster. " <>
        "Existing members: #{inspect(existing_members)}"
    )

    # Call RaServer to reconfigure for cluster join
    case RaServer.join_cluster(existing_members) do
      :ok ->
        Logger.info("Ra server successfully joined cluster")
        :ok

      {:error, reason} ->
        Logger.error("Failed to join Ra cluster: #{inspect(reason)}")
        {:error, {:ra_join_failed, reason}}
    end
  end
end
