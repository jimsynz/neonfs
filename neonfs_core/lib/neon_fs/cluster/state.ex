defmodule NeonFS.Cluster.State do
  @moduledoc """
  Cluster state structure and persistence.

  Cluster state is persisted to disk as JSON at /var/lib/neonfs/meta/cluster.json
  and contains cluster identity, node information, and Ra cluster membership.
  """

  alias __MODULE__
  alias __MODULE__.Validator

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

  @type drive_config :: %{
          id: String.t(),
          path: String.t(),
          tier: String.t(),
          capacity: String.t()
        }

  @type t :: %__MODULE__{
          cluster_id: String.t(),
          cluster_name: String.t(),
          created_at: DateTime.t(),
          drives: [drive_config()],
          master_key: String.t(),
          min_peers_for_operation: pos_integer(),
          this_node: node_info(),
          known_peers: [peer_info()],
          metrics: map(),
          peer_connect_timeout: pos_integer(),
          peer_sync_interval: pos_integer(),
          ra_cluster_members: [atom()],
          node_type: atom(),
          gc: map(),
          scrub: map(),
          startup_peer_timeout: pos_integer(),
          worker: map()
        }

  @enforce_keys [:cluster_id, :cluster_name, :created_at, :master_key, :this_node]
  defstruct [
    :cluster_id,
    :cluster_name,
    :created_at,
    :master_key,
    :this_node,
    drives: [],
    gc: %{},
    known_peers: [],
    metrics: %{},
    min_peers_for_operation: 1,
    peer_connect_timeout: 10_000,
    peer_sync_interval: 30_000,
    ra_cluster_members: [],
    node_type: :core,
    scrub: %{},
    startup_peer_timeout: 30_000,
    worker: %{}
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
      "drives" =>
        Enum.map(state.drives, fn drive ->
          %{
            "id" => to_string(drive[:id] || drive["id"]),
            "path" => to_string(drive[:path] || drive["path"]),
            "tier" => to_string(drive[:tier] || drive["tier"]),
            "capacity" => to_string(drive[:capacity] || drive["capacity"] || "0")
          }
        end),
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
      "metrics" => serialise_map_config(state.metrics),
      "ra_cluster_members" => Enum.map(state.ra_cluster_members, &Atom.to_string/1),
      "min_peers_for_operation" => state.min_peers_for_operation,
      "node_type" => Atom.to_string(state.node_type),
      "peer_connect_timeout" => state.peer_connect_timeout,
      "peer_sync_interval" => state.peer_sync_interval,
      "gc" => serialise_map_config(state.gc),
      "scrub" => serialise_map_config(state.scrub),
      "startup_peer_timeout" => state.startup_peer_timeout,
      "worker" => serialise_worker_config(state.worker)
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
  @spec load() ::
          {:ok, t()}
          | {:error,
             :not_found | :invalid_json | {:validation_failed, [Validator.error()]} | term()}
  def load do
    path = state_file_path()

    with :ok <- check_file_exists(path),
         {:ok, content} <- File.read(path),
         {:ok, state} <- parse_json(content) do
      validate_loaded_state(state)
    end
  end

  @doc """
  Updates the drives list in the persisted cluster state.

  Loads the current state, replaces the drives field, and saves.
  """
  @spec update_drives([drive_config()]) :: :ok | {:error, term()}
  def update_drives(drives) when is_list(drives) do
    case load() do
      {:ok, state} ->
        save(%{state | drives: drives})

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Updates the worker config in the persisted cluster state.

  Merges new values into the existing worker config map, preserving
  any keys not present in the update. Keys should be strings matching
  the JSON field names (e.g. `"max_concurrent"`).
  """
  @spec update_worker_config(map()) :: :ok | {:error, term()}
  def update_worker_config(new_config) when is_map(new_config) do
    case load() do
      {:ok, state} ->
        merged = Map.merge(state.worker, new_config)
        save(%{state | worker: merged})

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private helpers

  defp check_file_exists(path) do
    if File.exists?(path), do: :ok, else: {:error, :not_found}
  end

  defp validate_loaded_state(state) do
    case Validator.validate(state) do
      :ok -> {:ok, state}
      {:error, errors} -> {:error, {:validation_failed, errors}}
    end
  end

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
    data = :json.decode(content)
    parse_state(data)
  rescue
    _ -> {:error, :invalid_json}
  end

  defp parse_state(data) do
    state =
      %State{
        cluster_id: data["cluster_id"],
        cluster_name: data["cluster_name"],
        created_at: parse_datetime!(data["created_at"]),
        drives: parse_drives(data["drives"]),
        master_key: data["master_key"],
        known_peers: parse_peers(data["known_peers"]),
        ra_cluster_members: Enum.map(data["ra_cluster_members"] || [], &String.to_atom/1),
        this_node: parse_node_info(data["this_node"])
      }
      |> merge_config_fields(data)

    {:ok, state}
  rescue
    _ -> {:error, :invalid_json}
  end

  defp merge_config_fields(state, data) do
    %{
      state
      | gc: data["gc"] || %{},
        metrics: data["metrics"] || %{},
        min_peers_for_operation: data["min_peers_for_operation"] || 1,
        node_type: parse_node_type(data["node_type"]),
        peer_connect_timeout: data["peer_connect_timeout"] || 10_000,
        peer_sync_interval: data["peer_sync_interval"] || 30_000,
        scrub: data["scrub"] || %{},
        startup_peer_timeout: data["startup_peer_timeout"] || 30_000,
        worker: data["worker"] || %{}
    }
  end

  defp parse_drives(nil), do: []

  defp parse_drives(drives) do
    Enum.map(drives, fn drive ->
      %{
        "id" => drive["id"],
        "path" => drive["path"],
        "tier" => drive["tier"],
        "capacity" => drive["capacity"] || "0"
      }
    end)
  end

  defp parse_node_info(node_data) do
    %{
      id: node_data["id"],
      name: String.to_atom(node_data["name"]),
      joined_at: parse_datetime!(node_data["joined_at"])
    }
  end

  defp parse_node_type(nil), do: :core
  defp parse_node_type(type) when is_binary(type), do: String.to_existing_atom(type)
  defp parse_node_type(type) when is_atom(type), do: type

  defp parse_peers(nil), do: []

  defp parse_peers(peers) do
    Enum.map(peers, fn peer ->
      %{
        id: peer["id"],
        name: String.to_atom(peer["name"]),
        last_seen: parse_datetime!(peer["last_seen"])
      }
    end)
  end

  defp parse_datetime!(iso8601) do
    case DateTime.from_iso8601(iso8601) do
      {:ok, datetime, _offset} -> datetime
      _ -> raise "Invalid datetime"
    end
  end

  defp serialise_map_config(map) when map == %{}, do: %{}

  defp serialise_map_config(map) when is_map(map) do
    map
    |> Enum.map(fn {k, v} -> {to_string(k), v} end)
    |> Map.new()
  end

  defp serialise_worker_config(worker), do: serialise_map_config(worker)
end
