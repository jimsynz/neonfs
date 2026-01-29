defmodule NeonFS.CLI.Handler do
  @moduledoc """
  RPC handler for CLI commands. Called via Erlang distribution.

  This module provides the daemon-side interface for CLI operations, converting
  internal data structures to serializable maps that can be sent across the
  Erlang distribution protocol.

  All functions return `{:ok, data}` or `{:error, reason}` tuples where data
  consists only of serializable terms (maps, lists, atoms, strings, numbers).
  """

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
       uptime: get_uptime()
     }}
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
    with_fuse_manager(fn manager ->
      # Convert options map to keyword list
      opts = map_to_opts(options)

      case manager.mount(volume_name, mount_point, opts) do
        {:ok, mount_info} -> {:ok, mount_info_to_map(mount_info)}
        {:error, reason} -> {:error, reason}
      end
    end)
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
    with_fuse_manager(fn manager ->
      mount_id_or_path
      |> do_unmount(manager)
      |> format_unmount_result()
    end)
  end

  @doc """
  Lists all active mounts.

  Requires the neonfs_fuse application to be running on the local node.

  ## Returns
  - `{:ok, [map]}` - List of mount info maps
  """
  @spec list_mounts() :: {:ok, [map()]}
  def list_mounts do
    with_fuse_manager(fn manager ->
      mounts =
        manager.list_mounts()
        |> Enum.map(&mount_info_to_map/1)

      {:ok, mounts}
    end)
  end

  # Private helper functions

  # Conditionally execute FUSE operations if the application is available
  #
  # TODO: Phase 1 uses Code.ensure_loaded?/1 which only checks the local VM.
  # Per spec/architecture.md, neonfs_core and neonfs_fuse are separate Erlang nodes
  # (e.g., neonfs_core@localhost and neonfs_fuse@localhost) that communicate via
  # Erlang distribution. For proper multi-node/container deployment, this should use:
  #
  #   :rpc.call(fuse_node_name(), NeonFS.FUSE.MountManager, function, args)
  #
  # where fuse_node_name() returns the configured FUSE node name (from config or
  # service discovery). The current implementation works for Phase 1 where both apps
  # run in the same node, but will need updating for separate container deployment.
  defp with_fuse_manager(fun) do
    fuse_node = Application.get_env(:neonfs_core, :fuse_node, :neonfs_fuse@localhost)

    # Check if FUSE node is reachable via RPC
    case :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :__info__, [:module]) do
      {:badrpc, _} ->
        # FUSE node not available (not running or not reachable)
        {:error, :fuse_not_available}

      NeonFS.FUSE.MountManager ->
        # FUSE module exists, create RPC wrapper for manager operations
        rpc_manager = %{
          mount: fn volume_name, mount_point, opts ->
            :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :mount, [
              volume_name,
              mount_point,
              opts
            ])
          end,
          unmount: fn mount_id ->
            :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :unmount, [mount_id])
          end,
          list_mounts: fn ->
            :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :list_mounts, [])
          end,
          get_mount: fn mount_id ->
            :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :get_mount, [mount_id])
          end,
          get_mount_by_path: fn path ->
            :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :get_mount_by_path, [path])
          end
        }

        fun.(rpc_manager)
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

  defp do_unmount(mount_id_or_path, manager) do
    # Try mount_id first, then try path
    case manager.get_mount(mount_id_or_path) do
      {:ok, _mount} ->
        manager.unmount(mount_id_or_path)

      {:error, :not_found} ->
        case manager.get_mount_by_path(mount_id_or_path) do
          {:ok, mount} ->
            manager.unmount(mount.id)

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
