defmodule NeonFS.Cluster.State do
  @moduledoc """
  Cluster state structure and persistence.

  Cluster state is persisted to disk as JSON at /var/lib/neonfs/meta/cluster.json
  and contains cluster identity, node information, and Ra cluster membership.
  """

  alias __MODULE__

  @type node_info :: %{
          id: String.t(),
          name: atom(),
          joined_at: DateTime.t()
        }

  @type peer_info :: %{
          id: String.t(),
          name: atom(),
          last_seen: DateTime.t()
        }

  @type t :: %__MODULE__{
          cluster_id: String.t(),
          cluster_name: String.t(),
          created_at: DateTime.t(),
          master_key: String.t(),
          this_node: node_info(),
          known_peers: [peer_info()],
          ra_cluster_members: [atom()]
        }

  @enforce_keys [:cluster_id, :cluster_name, :created_at, :master_key, :this_node]
  defstruct [
    :cluster_id,
    :cluster_name,
    :created_at,
    :master_key,
    :this_node,
    known_peers: [],
    ra_cluster_members: []
  ]

  @doc """
  Creates a new cluster state.
  """
  @spec new(String.t(), String.t(), String.t(), node_info()) :: t()
  def new(cluster_id, cluster_name, master_key, node_info) do
    %State{
      cluster_id: cluster_id,
      cluster_name: cluster_name,
      created_at: DateTime.utc_now(),
      master_key: master_key,
      this_node: node_info,
      known_peers: [],
      ra_cluster_members: [node_info.name]
    }
  end

  @doc """
  Returns the path to the cluster state file.
  """
  @spec state_file_path() :: String.t()
  def state_file_path do
    meta_dir = Application.get_env(:neonfs_core, :meta_dir, "/var/lib/neonfs/meta")
    Path.join(meta_dir, "cluster.json")
  end

  @doc """
  Checks if cluster state file exists.
  """
  @spec exists?() :: boolean()
  def exists? do
    File.exists?(state_file_path())
  end

  @doc """
  Saves cluster state to disk as JSON.
  Uses atomic write pattern: write to temp file, sync, rename.
  """
  @spec save(t()) :: :ok | {:error, term()}
  def save(%State{} = state) do
    path = state_file_path()

    # Ensure directory exists
    path
    |> Path.dirname()
    |> File.mkdir_p!()

    # Convert to JSON-serialisable map
    data = %{
      "cluster_id" => state.cluster_id,
      "cluster_name" => state.cluster_name,
      "created_at" => DateTime.to_iso8601(state.created_at),
      "master_key" => state.master_key,
      "this_node" => %{
        "id" => state.this_node.id,
        "name" => Atom.to_string(state.this_node.name),
        "joined_at" => DateTime.to_iso8601(state.this_node.joined_at)
      },
      "known_peers" =>
        Enum.map(state.known_peers, fn peer ->
          %{
            "id" => peer.id,
            "name" => Atom.to_string(peer.name),
            "last_seen" => DateTime.to_iso8601(peer.last_seen)
          }
        end),
      "ra_cluster_members" => Enum.map(state.ra_cluster_members, &Atom.to_string/1)
    }

    json = :json.format(data)

    # Atomic write
    temp_path = "#{path}.tmp"

    with :ok <- File.write(temp_path, json),
         :ok <- sync_file(temp_path) do
      File.rename(temp_path, path)
    end
  end

  @doc """
  Loads cluster state from disk.
  """
  @spec load() :: {:ok, t()} | {:error, :not_found | :invalid_json | term()}
  def load do
    path = state_file_path()

    if File.exists?(path) do
      case File.read(path) do
        {:ok, content} ->
          parse_json(content)

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:error, :not_found}
    end
  end

  # Private helpers

  defp sync_file(path) do
    case File.open(path, [:read, :write]) do
      {:ok, device} ->
        result = :file.datasync(device)
        :ok = File.close(device)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_json(content) do
    try do
      data = :json.decode(content)
      parse_state(data)
    rescue
      _ -> {:error, :invalid_json}
    end
  end

  defp parse_state(data) do
    state = %State{
      cluster_id: data["cluster_id"],
      cluster_name: data["cluster_name"],
      created_at: parse_datetime!(data["created_at"]),
      master_key: data["master_key"],
      this_node: %{
        id: data["this_node"]["id"],
        name: String.to_atom(data["this_node"]["name"]),
        joined_at: parse_datetime!(data["this_node"]["joined_at"])
      },
      known_peers:
        Enum.map(data["known_peers"] || [], fn peer ->
          %{
            id: peer["id"],
            name: String.to_atom(peer["name"]),
            last_seen: parse_datetime!(peer["last_seen"])
          }
        end),
      ra_cluster_members: Enum.map(data["ra_cluster_members"] || [], &String.to_atom/1)
    }

    {:ok, state}
  rescue
    _ -> {:error, :invalid_json}
  end

  defp parse_datetime!(iso8601) do
    case DateTime.from_iso8601(iso8601) do
      {:ok, datetime, _offset} -> datetime
      _ -> raise "Invalid datetime"
    end
  end
end
