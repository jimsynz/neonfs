defmodule NeonFS.Cluster.Init do
  @moduledoc """
  Cluster initialization logic.

  Handles the creation of a new NeonFS cluster on the first node.
  This includes:
  - Generating unique cluster and node IDs
  - Generating cryptographic master key (for future encryption)
  - Persisting cluster state to disk
  - Bootstrapping single-node Ra cluster
  - Creating the system volume and writing cluster identity
  - Generating the cluster CA and issuing the first node certificate
  """

  alias NeonFS.Cluster.State
  alias NeonFS.Core.{CertificateAuthority, RaServer, SystemVolume, VolumeRegistry}
  alias NeonFS.Transport.TLS

  @doc """
  Initializes a new cluster with the given name.

  Returns `{:ok, cluster_id}` on success, or `{:error, reason}` on failure.

  The init sequence is:
  1. Generate cluster/node IDs and master key
  2. Save cluster state to disk
  3. Bootstrap single-node Ra cluster
  4. Create system volume
  5. Write cluster identity file
  6. Generate cluster CA
  7. Issue first node certificate

  ## Errors
  - `{:error, :already_initialised}` - cluster state already exists
  - `{:error, :node_not_named}` - Erlang node not named (required for Ra)
  - `{:error, :ra_start_failed}` - Ra cluster failed to start
  - `{:error, :system_volume_failed}` - system volume creation failed
  - `{:error, :identity_write_failed}` - cluster identity write failed
  - `{:error, {:ca_init_failed, reason}}` - CA initialisation failed
  - `{:error, {:node_cert_failed, reason}}` - first node certificate issuance failed
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

    with :ok <- State.save(state),
         :ok <- RaServer.init_cluster(),
         {:ok, _volume} <- create_system_volume(),
         :ok <- write_cluster_identity(cluster_name),
         {:ok, _ca_cert, _ca_key} <- init_cluster_ca(cluster_name),
         :ok <- issue_first_node_cert() do
      {:ok, cluster_id}
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp create_system_volume do
    case VolumeRegistry.create_system_volume() do
      {:ok, volume} -> {:ok, volume}
      {:error, :already_exists} -> VolumeRegistry.get_system_volume()
      {:error, reason} -> {:error, {:system_volume_failed, reason}}
    end
  end

  defp write_cluster_identity(cluster_name) do
    identity = %{
      cluster_name: cluster_name,
      initialized_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      format_version: 1
    }

    json = identity |> :json.format() |> IO.iodata_to_binary()

    case SystemVolume.write("/cluster/identity.json", json) do
      :ok -> :ok
      {:error, reason} -> {:error, {:identity_write_failed, reason}}
    end
  end

  defp init_cluster_ca(cluster_name) do
    case CertificateAuthority.init_ca(cluster_name) do
      {:ok, ca_cert, ca_key} -> {:ok, ca_cert, ca_key}
      {:error, reason} -> {:error, {:ca_init_failed, reason}}
    end
  end

  defp issue_first_node_cert do
    node_name = Atom.to_string(Node.self())
    hostname = node_hostname()
    node_key = TLS.generate_node_key()
    csr = TLS.create_csr(node_key, node_name)

    case CertificateAuthority.sign_node_csr(csr, hostname) do
      {:ok, node_cert, ca_cert} ->
        TLS.write_local_tls(ca_cert, node_cert, node_key)
        :ok

      {:error, reason} ->
        {:error, {:node_cert_failed, reason}}
    end
  end

  defp node_hostname do
    Node.self()
    |> Atom.to_string()
    |> String.split("@")
    |> List.last()
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
