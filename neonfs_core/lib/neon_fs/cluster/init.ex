defmodule NeonFS.Cluster.Init do
  @moduledoc """
  Cluster initialization logic.

  Handles the creation of a new NeonFS cluster on the first node.
  This includes:
  - Generating unique cluster and node IDs
  - Generating cryptographic master key (for future encryption)
  - Persisting cluster state to disk
  - Bootstrapping single-node Ra cluster
  """

  alias NeonFS.Cluster.State
  alias NeonFS.Core.RaServer

  @doc """
  Initializes a new cluster with the given name.

  Returns `{:ok, cluster_id}` on success, or `{:error, reason}` on failure.

  ## Errors
  - `{:error, :already_initialised}` - cluster state already exists
  - `{:error, :node_not_named}` - Erlang node not named (required for Ra)
  - `{:error, :ra_start_failed}` - Ra cluster failed to start
  """
  @spec init_cluster(String.t()) :: {:ok, String.t()} | {:error, term()}
  def init_cluster(cluster_name) do
    cond do
      State.exists?() ->
        {:error, :already_initialised}

      Node.self() == :nonode@nohost ->
        {:error, :node_not_named}

      true ->
        do_init_cluster(cluster_name)
    end
  end

  # Private implementation

  defp do_init_cluster(cluster_name) do
    cluster_id = generate_cluster_id()
    node_id = generate_node_id()
    master_key = generate_master_key()
    node_name = Node.self()

    node_info = %{
      id: node_id,
      name: node_name,
      joined_at: DateTime.utc_now()
    }

    state = State.new(cluster_id, cluster_name, master_key, node_info)

    # Save cluster state first
    with :ok <- State.save(state),
         # Then start Ra as a founding single-node cluster
         :ok <- RaServer.init_cluster() do
      {:ok, cluster_id}
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Generates a unique cluster ID with prefix 'clust_'.
  """
  @spec generate_cluster_id() :: String.t()
  def generate_cluster_id do
    random_suffix =
      :crypto.strong_rand_bytes(6)
      |> Base.encode32(case: :lower, padding: false)
      |> binary_part(0, 8)

    "clust_#{random_suffix}"
  end

  @doc """
  Generates a unique node ID with prefix 'node_'.
  """
  @spec generate_node_id() :: String.t()
  def generate_node_id do
    random_suffix =
      :crypto.strong_rand_bytes(6)
      |> Base.encode32(case: :lower, padding: false)
      |> binary_part(0, 8)

    "node_#{random_suffix}"
  end

  @doc """
  Generates a cryptographic master key.
  Returns 256-bit (32 byte) key encoded as base64.
  This key will be used for encryption in Phase 5.
  """
  @spec generate_master_key() :: String.t()
  def generate_master_key do
    :crypto.strong_rand_bytes(32)
    |> Base.encode64()
  end
end
