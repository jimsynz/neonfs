defmodule NeonFS.Cluster.Join do
  @moduledoc """
  Node join flow for adding nodes to an existing cluster.

  This module handles both sides of the join process:
  - Creating an invite token on an existing cluster node
  - Joining a cluster using an invite token
  """

  alias NeonFS.Cluster.{Invite, State}

  require Logger

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

  ## Returns
  - `{:ok, state}` on successful join
  - `{:error, reason}` on failure

  ## Examples

      iex> NeonFS.Cluster.Join.join_cluster("nfs_inv_...", :neonfs_core@node1)
      {:ok, %NeonFS.Cluster.State{}}
  """
  @spec join_cluster(String.t(), atom()) :: {:ok, State.t()} | {:error, term()}
  def join_cluster(token, via_node) when is_binary(token) and is_atom(via_node) do
    this_node = Node.self()

    with :ok <- validate_not_in_cluster(),
         {:ok, cluster_info} <- request_join(via_node, token, this_node),
         {:ok, state} <- build_cluster_state(cluster_info),
         :ok <- State.save(state),
         :ok <- join_ra_cluster(state) do
      Logger.info("Successfully joined cluster #{state.cluster_name}")
      {:ok, state}
    end
  end

  @doc """
  Accepts a join request from a new node (called via RPC on existing cluster member).

  This function should be called on the existing cluster node.

  ## Parameters
  - `token` - The invite token from the joining node
  - `joining_node` - The node name of the joining node

  ## Returns
  - `{:ok, cluster_info}` containing cluster details for the joining node
  - `{:error, reason}` on failure
  """
  @spec accept_join(String.t(), atom()) :: {:ok, map()} | {:error, term()}
  def accept_join(token, joining_node) when is_binary(token) and is_atom(joining_node) do
    with :ok <- Invite.validate_invite(token),
         {:ok, state} <- State.load(),
         {:ok, updated_state} <- add_peer_to_state(state, joining_node),
         :ok <- State.save(updated_state),
         :ok <- add_to_ra_cluster(joining_node) do
      cluster_info = %{
        cluster_id: state.cluster_id,
        cluster_name: state.cluster_name,
        created_at: state.created_at,
        master_key: state.master_key,
        known_peers: updated_state.known_peers,
        ra_cluster_members: updated_state.ra_cluster_members
      }

      Logger.info("Accepted join request from #{inspect(joining_node)}")
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

  defp request_join(via_node, token, this_node) do
    case :rpc.call(via_node, __MODULE__, :accept_join, [token, this_node]) do
      {:ok, cluster_info} ->
        {:ok, cluster_info}

      {:error, reason} ->
        {:error, {:join_rejected, reason}}

      {:badrpc, reason} ->
        {:error, {:rpc_failed, reason}}
    end
  end

  defp build_cluster_state(cluster_info) do
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

    state = %State{
      cluster_id: cluster_info.cluster_id,
      cluster_name: cluster_info.cluster_name,
      created_at: created_at,
      master_key: cluster_info.master_key,
      this_node: node_info,
      known_peers: cluster_info.known_peers,
      ra_cluster_members: [this_node | cluster_info.ra_cluster_members]
    }

    {:ok, state}
  end

  defp generate_node_id do
    :crypto.strong_rand_bytes(5)
    |> Base.encode32(case: :lower, padding: false)
    |> binary_part(0, 8)
  end

  defp add_peer_to_state(%State{} = state, joining_node) do
    node_id = generate_node_id()

    peer_info = %{
      id: node_id,
      name: joining_node,
      last_seen: DateTime.utc_now()
    }

    updated_state = %{
      state
      | known_peers: [peer_info | state.known_peers],
        ra_cluster_members: [joining_node | state.ra_cluster_members]
    }

    {:ok, updated_state}
  end

  defp add_to_ra_cluster(joining_node) do
    this_node = Node.self()
    server_id = {@cluster_name, this_node}
    new_server_id = {@cluster_name, joining_node}

    case :ra.add_member(server_id, new_server_id) do
      {:ok, _, _leader} ->
        Logger.info("Added #{inspect(joining_node)} to Ra cluster")
        :ok

      {:timeout, _} ->
        Logger.warning("Timeout adding #{inspect(joining_node)} to Ra cluster")
        {:error, :ra_add_timeout}

      {:error, reason} ->
        Logger.error("Failed to add #{inspect(joining_node)} to Ra cluster: #{inspect(reason)}")
        {:error, {:ra_add_failed, reason}}
    end
  end

  defp join_ra_cluster(_state) do
    # When a new node joins, it needs to start its Ra server
    # The Ra server will be started automatically by RaServer GenServer
    # We just need to wait for it to sync with the existing cluster

    this_node = Node.self()
    server_id = {@cluster_name, this_node}

    # Check if Ra server is running
    case :ra.restart_server(server_id) do
      {:ok, _} ->
        Logger.info("Ra server restarted and joined cluster")
        :ok

      {:error, :not_started} ->
        # Server not started yet, this is expected on first join
        # RaServer will start it automatically
        Logger.info("Ra server will be started by RaServer GenServer")
        :ok

      {:error, reason} ->
        Logger.error("Failed to restart Ra server: #{inspect(reason)}")
        {:error, {:ra_join_failed, reason}}
    end
  end
end
