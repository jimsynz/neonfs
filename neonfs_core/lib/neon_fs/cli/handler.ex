defmodule NeonFS.CLI.Handler do
  @moduledoc """
  RPC handler for CLI commands. Called via Erlang distribution.

  This module provides the daemon-side interface for CLI operations, converting
  internal data structures to serializable maps that can be sent across the
  Erlang distribution protocol.

  All functions return `{:ok, data}` or `{:error, reason}` tuples where data
  consists only of serializable terms (maps, lists, atoms, strings, numbers).
  """

  alias NeonFS.Cluster.{Init, Invite, Join, State}
  alias NeonFS.Core.{Volume, VolumeRegistry}

  @doc """
  Returns cluster status information.

  ## Returns
  - `{:ok, map}` - Status map with cluster information
  """
  @spec cluster_status() :: {:ok, map()}
  def cluster_status do
    {:ok,
     %{
       name: get_cluster_name(),
       node: Atom.to_string(Node.self()),
       status: :running,
       volumes: count_volumes(),
       uptime_seconds: get_uptime()
     }}
  end

  @doc """
  Initializes a new cluster with the given name.

  ## Parameters
  - `cluster_name` - Name for the new cluster (string)

  ## Returns
  - `{:ok, map}` - Success map with cluster_id
  - `{:error, reason}` - Error tuple
  """
  @spec cluster_init(String.t()) :: {:ok, map()} | {:error, term()}
  def cluster_init(cluster_name) when is_binary(cluster_name) do
    case Init.init_cluster(cluster_name) do
      {:ok, cluster_id} ->
        # Load the state to get full details
        case State.load() do
          {:ok, state} ->
            {:ok,
             %{
               cluster_id: cluster_id,
               cluster_name: state.cluster_name,
               node_id: state.this_node.id,
               node_name: Atom.to_string(state.this_node.name),
               created_at: DateTime.to_iso8601(state.created_at)
             }}

          {:error, _} ->
            # Fallback if load fails
            {:ok, %{cluster_id: cluster_id}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Creates an invite token for joining nodes.

  ## Parameters
  - `expires_in` - Duration in seconds the token is valid for

  ## Returns
  - `{:ok, %{token: string}}` - Success map with invite token
  - `{:error, reason}` - Error tuple
  """
  @spec create_invite(pos_integer()) :: {:ok, map()} | {:error, term()}
  def create_invite(expires_in) when is_integer(expires_in) and expires_in > 0 do
    case Invite.create_invite(expires_in) do
      {:ok, token} ->
        {:ok, %{token: token}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Joins an existing cluster using an invite token.

  ## Parameters
  - `token` - Invite token from existing cluster
  - `via_node` - Node name of existing cluster member (string)

  ## Returns
  - `{:ok, map}` - Success map with cluster information
  - `{:error, reason}` - Error tuple
  """
  @spec join_cluster(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def join_cluster(token, via_node_str)
      when is_binary(token) and is_binary(via_node_str) do
    # Convert node name string to atom
    via_node = String.to_atom(via_node_str)

    case Join.join_cluster(token, via_node) do
      {:ok, state} ->
        {:ok,
         %{
           cluster_id: state.cluster_id,
           cluster_name: state.cluster_name,
           node_id: state.this_node.id,
           node_name: Atom.to_string(state.this_node.name),
           joined_at: DateTime.to_iso8601(state.this_node.joined_at),
           known_peers:
             Enum.map(state.known_peers, fn peer ->
               %{
                 id: peer.id,
                 name: Atom.to_string(peer.name),
                 last_seen: DateTime.to_iso8601(peer.last_seen)
               }
             end)
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Lists all volumes in the cluster.

  ## Returns
  - `{:ok, [map]}` - List of volume maps
  """
  @spec list_volumes() :: {:ok, [map()]}
  def list_volumes do
    volumes =
      VolumeRegistry.list()
      |> Enum.map(&volume_to_map/1)

    {:ok, volumes}
  end

  @doc """
  Creates a new volume with the given name and configuration.

  ## Parameters
  - `name` - Volume name (string)
  - `config` - Configuration map with optional keys:
    - `:owner` - Owner string
    - `:durability` - Durability config map
    - `:write_ack` - Write acknowledgment level (`:local`, `:quorum`, `:all`)
    - `:initial_tier` - Initial storage tier (`:hot`, `:warm`, `:cold`)
    - `:compression` - Compression config map
    - `:verification` - Verification config map

  ## Returns
  - `{:ok, map}` - Created volume as map
  - `{:error, reason}` - Error tuple
  """
  @spec create_volume(String.t(), map()) :: {:ok, map()} | {:error, term()}
  def create_volume(name, config) when is_binary(name) and is_map(config) do
    # Convert map to keyword list for VolumeRegistry
    opts = map_to_opts(config)

    case VolumeRegistry.create(name, opts) do
      {:ok, volume} -> {:ok, volume_to_map(volume)}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Deletes a volume by name.

  ## Parameters
  - `name` - Volume name (string)

  ## Returns
  - `{:ok, %{}}` - Success with empty map
  - `{:error, reason}` - Error tuple
  """
  @spec delete_volume(String.t()) :: {:ok, map()} | {:error, term()}
  def delete_volume(name) when is_binary(name) do
    case VolumeRegistry.get_by_name(name) do
      {:ok, volume} ->
        case VolumeRegistry.delete(volume.id) do
          :ok -> {:ok, %{}}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Gets volume details by name.

  ## Parameters
  - `name` - Volume name (string)

  ## Returns
  - `{:ok, map}` - Volume details as map
  - `{:error, reason}` - Error tuple
  """
  @spec get_volume(String.t()) :: {:ok, map()} | {:error, term()}
  def get_volume(name) when is_binary(name) do
    case VolumeRegistry.get_by_name(name) do
      {:ok, volume} -> {:ok, volume_to_map(volume)}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Mounts a volume at the specified path.

  Requires the neonfs_fuse application to be running on the local node.

  ## Parameters
  - `volume_name` - Volume name (string)
  - `mount_point` - Mount point path (string)
  - `options` - Mount options map (currently unused)

  ## Returns
  - `{:ok, map}` - Mount info as map
  - `{:error, reason}` - Error tuple
  """
  @spec mount(String.t(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def mount(volume_name, mount_point, options)
      when is_binary(volume_name) and is_binary(mount_point) and is_map(options) do
    with {:ok, fuse_node} <- get_fuse_node(),
         opts = map_to_opts(options),
         {:ok, mount_id} <- rpc_mount(fuse_node, volume_name, mount_point, opts),
         {:ok, mount_info} <- rpc_get_mount(fuse_node, mount_id) do
      {:ok, mount_info_to_map(mount_info)}
    end
  end

  @doc """
  Unmounts a filesystem by mount ID or path.

  Requires the neonfs_fuse application to be running on the local node.

  ## Parameters
  - `mount_id_or_path` - Mount ID (string) or mount point path (string)

  ## Returns
  - `{:ok, %{}}` - Success with empty map
  - `{:error, reason}` - Error tuple
  """
  @spec unmount(String.t()) :: {:ok, map()} | {:error, term()}
  def unmount(mount_id_or_path) when is_binary(mount_id_or_path) do
    with {:ok, fuse_node} <- get_fuse_node() do
      mount_id_or_path
      |> do_unmount(fuse_node)
      |> format_unmount_result()
    end
  end

  @doc """
  Lists all active mounts.

  Requires the neonfs_fuse application to be running on the local node.

  ## Returns
  - `{:ok, [map]}` - List of mount info maps
  """
  @spec list_mounts() :: {:ok, [map()]}
  def list_mounts do
    with {:ok, fuse_node} <- get_fuse_node(),
         {:ok, mounts} <- rpc_list_mounts(fuse_node) do
      {:ok, Enum.map(mounts, &mount_info_to_map/1)}
    end
  end

  # Private helper functions

  # Get the FUSE node and verify it's reachable
  defp get_fuse_node do
    fuse_node = Application.get_env(:neonfs_core, :fuse_node, :neonfs_fuse@localhost)

    case :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :__info__, [:module]) do
      {:badrpc, _} -> {:error, :fuse_not_available}
      NeonFS.FUSE.MountManager -> {:ok, fuse_node}
    end
  end

  # RPC wrappers for MountManager operations
  defp rpc_mount(fuse_node, volume_name, mount_point, opts) do
    case :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :mount, [volume_name, mount_point, opts]) do
      {:badrpc, reason} -> {:error, {:rpc_failed, reason}}
      result -> result
    end
  end

  defp rpc_unmount(fuse_node, mount_id) do
    case :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :unmount, [mount_id]) do
      {:badrpc, reason} -> {:error, {:rpc_failed, reason}}
      result -> result
    end
  end

  defp rpc_list_mounts(fuse_node) do
    case :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :list_mounts, []) do
      {:badrpc, reason} -> {:error, {:rpc_failed, reason}}
      mounts when is_list(mounts) -> {:ok, mounts}
      result -> result
    end
  end

  defp rpc_get_mount(fuse_node, mount_id) do
    case :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :get_mount, [mount_id]) do
      {:badrpc, reason} -> {:error, {:rpc_failed, reason}}
      result -> result
    end
  end

  defp rpc_get_mount_by_path(fuse_node, path) do
    case :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :get_mount_by_path, [path]) do
      {:badrpc, reason} -> {:error, {:rpc_failed, reason}}
      result -> result
    end
  end

  defp get_cluster_name do
    # For Phase 1, use the node name as cluster name
    # Phase 2 will have proper cluster naming via Ra
    Node.self()
    |> Atom.to_string()
    |> String.split("@")
    |> List.first()
    |> Kernel.||("neonfs")
  end

  defp count_volumes do
    VolumeRegistry.list()
    |> length()
  end

  defp get_uptime do
    # Return uptime in seconds
    {uptime_ms, _} = :erlang.statistics(:wall_clock)
    div(uptime_ms, 1000)
  end

  defp volume_to_map(%Volume{} = volume) do
    %{
      id: volume.id,
      name: volume.name,
      owner: volume.owner,
      durability: volume.durability,
      write_ack: volume.write_ack,
      initial_tier: volume.initial_tier,
      compression: volume.compression,
      verification: volume.verification,
      logical_size: volume.logical_size,
      physical_size: volume.physical_size,
      chunk_count: volume.chunk_count,
      created_at: DateTime.to_iso8601(volume.created_at),
      updated_at: DateTime.to_iso8601(volume.updated_at)
    }
  end

  defp do_unmount(mount_id_or_path, fuse_node) do
    # Try mount_id first, then try path
    case rpc_get_mount(fuse_node, mount_id_or_path) do
      {:ok, _mount} ->
        rpc_unmount(fuse_node, mount_id_or_path)

      {:error, :not_found} ->
        case rpc_get_mount_by_path(fuse_node, mount_id_or_path) do
          {:ok, mount} ->
            rpc_unmount(fuse_node, mount.id)

          {:error, :not_found} ->
            {:error, :mount_not_found}
        end
    end
  end

  defp format_unmount_result(:ok), do: {:ok, %{}}
  defp format_unmount_result({:error, reason}), do: {:error, reason}

  defp mount_info_to_map(mount_info) do
    # Convert mount info to map, excluding PIDs and references
    %{
      id: mount_info.id,
      volume_name: mount_info.volume_name,
      mount_point: mount_info.mount_point,
      started_at: DateTime.to_iso8601(mount_info.started_at)
    }
  end

  defp map_to_opts(map) when is_map(map) do
    map
    |> Enum.map(fn {k, v} -> {String.to_existing_atom(k), v} end)
    |> Enum.into([])
  rescue
    # If key doesn't exist as atom, use string key directly
    ArgumentError ->
      Enum.into(map, [])
  end
end
