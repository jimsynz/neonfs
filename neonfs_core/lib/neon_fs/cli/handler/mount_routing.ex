defmodule NeonFS.CLI.Handler.MountRouting do
  @moduledoc """
  CLI command handlers that route to the FUSE and NFS interface nodes:
  FUSE mount/unmount/list and NFS export/unexport/list plus the NFS
  mount-parameter resolver.

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates its `mount/3`, `unmount/1`, `list_mounts/0`, `nfs_export/3`,
  `nfs_unexport/1`, `handle_nfs_mount_request/1` and `nfs_list_exports/0`
  RPC entry points here, so the CLI wire contract is unchanged.

  FUSE node discovery first honours the mounting client's host: the CLI
  puts its own hostname in the mount `options` (`"host"`), and a FUSE
  node on that host is preferred so the mount lands where the operator
  ran the command even when the RPC is handled by a core-only node
  elsewhere (#1359). Failing that it prefers the local node, then
  `ServiceRegistry`, then connected peers, then the configured fallback —
  a FUSE mount is only visible on its own host, so the operator's own
  node is the right default (#1358). NFS discovery checks
  `ServiceRegistry` first, since an export is cluster state served by
  every NFS node. RPC wrappers carry bounded timeouts so a misbehaving
  interface node can't hang the CLI.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.{ServiceRegistry, VolumeRegistry}
  alias NeonFS.Error.{NotFound, Unavailable, VolumeNotFound}

  # RPC wrappers for MountManager operations. Bounded timeouts so a misbehaving
  # FUSE node can't hang the calling CLI command indefinitely (#1035) — a stuck
  # mount surfaces as a clear error rather than an unbounded wait.
  @mount_rpc_timeout 60_000
  @unmount_rpc_timeout 30_000

  @doc """
  Mounts a volume via FUSE on a discovered FUSE node.

  ## Parameters
  - `volume_name` - Volume name (string)
  - `mount_point` - Mount point path (string)
  - `options` - Mount options map. A `"host"` entry (the CLI's own
    hostname) routes the mount to a FUSE node on that host (#1359); it is
    consumed here and not forwarded to the FUSE node. `"allow_other"` /
    `"allow_root"` booleans are forwarded to the FUSE node's
    `MountManager` to widen mount access (#1574).

  ## Returns
  - `{:ok, map}` - Mount info as map
  - `{:error, reason}` - Error tuple
  """
  @spec mount(String.t(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def mount(volume_name, mount_point, options)
      when is_binary(volume_name) and is_binary(mount_point) and is_map(options) do
    set_cli_metadata()
    {client_host, mount_options} = pop_client_host(options)

    with :ok <- require_cluster(),
         {:ok, fuse_node} <- get_fuse_node(client_host),
         opts = fuse_mount_opts(mount_options),
         {:ok, mount_id} <- rpc_mount(fuse_node, volume_name, mount_point, opts),
         {:ok, mount_info} <- rpc_get_mount(fuse_node, mount_id) do
      {:ok, mount_info_to_map(mount_info, fuse_node)}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
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
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, fuse_node} <- get_fuse_node(),
         {:ok, _} <- do_unmount(mount_id_or_path, fuse_node) do
      {:ok, %{}}
    else
      {:error, :mount_not_found} ->
        {:error,
         NotFound.exception(
           message:
             "No mount matches '#{mount_id_or_path}' (tried mount id, mount point, " <>
               "and volume name). Use `neonfs fuse list` to see active mounts."
         )}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Lists all active mounts across the cluster.

  Queries all discovered FUSE nodes and aggregates their mounts.

  ## Returns
  - `{:ok, [map]}` - List of mount info maps with node field
  """
  @spec list_mounts() :: {:ok, [map()]}
  def list_mounts do
    set_cli_metadata()

    with :ok <- require_cluster() do
      fuse_nodes = get_all_fuse_nodes()

      if Enum.empty?(fuse_nodes) do
        {:error, wrap_error(Unavailable.exception(message: "FUSE service not available"))}
      else
        mounts =
          fuse_nodes
          |> Enum.flat_map(&collect_node_mounts/1)
          |> Enum.sort_by(& &1.node)

        {:ok, mounts}
      end
    end
  end

  @doc """
  Exports a volume via NFS.

  Sets the volume's `nfs_export` flag in cluster state (#1175). Every
  running NFS node observes the change via volume lifecycle events and
  serves the export — no node targeting involved.

  ## Parameters
  - `volume_name` - Volume name (string)
  - `allowed_ips` - optional list of IP/CIDR strings; only these clients
    may mount and access the export. An empty list (the default) means
    allow all (#1217).
  - `root_squash` - optional boolean (default `true`); when set, a remote
    uid 0 is mapped to `nobody` before authorisation so it can't act as
    the volume owner (#1216). Pass `false` for `no_root_squash`.

  ## Returns
  - `{:ok, map}` - Export info as map
  - `{:error, reason}` - Error tuple
  """
  @spec nfs_export(String.t(), [String.t()], boolean()) :: {:ok, map()} | {:error, term()}
  def nfs_export(volume_name, allowed_ips \\ [], root_squash \\ true)
      when is_binary(volume_name) and is_list(allowed_ips) and is_boolean(root_squash) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, updated} <-
           VolumeRegistry.update(volume.id,
             nfs_export: true,
             nfs_allowed_ips: allowed_ips,
             nfs_root_squash: root_squash
           ) do
      {:ok, nfs_export_to_map(updated)}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Unexports a volume from NFS by volume name.

  Clears the volume's `nfs_export` flag in cluster state; every NFS
  node observes the change and stops serving the export.

  ## Parameters
  - `volume_name` - Volume name (string)

  ## Returns
  - `{:ok, %{}}` - Success with empty map
  - `{:error, reason}` - Error tuple
  """
  @spec nfs_unexport(String.t()) :: {:ok, map()} | {:error, term()}
  def nfs_unexport(volume_name) when is_binary(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, _updated} <- clear_nfs_export(volume) do
      {:ok, %{}}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, :not_exported} ->
        {:error, NotFound.exception(message: "NFS export not found: #{volume_name}")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Resolves the NFS mount parameters for `volume_name` so the CLI can
  perform the `mount.nfs` syscall locally as the calling user (#847).

  Returns the server address, port, and export path (always
  `"/<volume_name>"`) for the volume's NFS export. The export must
  exist — call `nfs_export/1` first if needed. The CLI's
  `neonfs nfs mount` subcommand consumes this map.

  ## Returns

  - `{:ok, %{server_address, port, export_path, volume_name, node}}` —
    mount params ready to feed to `mount.nfs`.
  - `{:error, NeonFS.Error.VolumeNotFound{}}` — unknown volume name.
  - `{:error, NeonFS.Error.NotFound{}}` — volume exists but isn't
    NFS-exported.
  - `{:error, NeonFS.Error.Unavailable{}}` — no reachable NFS node.
  """
  @spec handle_nfs_mount_request(binary()) :: {:ok, map()} | {:error, term()}
  def handle_nfs_mount_request(volume_name) when is_binary(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- fetch_volume(volume_name),
         :ok <- ensure_nfs_exported(volume),
         {:ok, nfs_node} <- get_nfs_node() do
      {server_address, port} = rpc_nfs_bind_info(nfs_node)

      {:ok,
       %{
         volume_name: volume_name,
         node: Atom.to_string(nfs_node),
         server_address: server_address,
         port: port,
         export_path: "/" <> volume_name
       }}
    else
      {:error, :not_exported} ->
        {:error,
         wrap_error(
           NotFound.exception(message: "no NFS export for volume #{inspect(volume_name)}")
         )}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Lists all active NFS exports across the cluster.

  Exports are cluster state (volumes with the `nfs_export` flag set),
  served by every running NFS node — the result contains one row per
  export per reachable NFS node. When no NFS node is reachable the
  exports are still listed, with placeholder endpoint fields.

  ## Returns
  - `{:ok, [map]}` - List of export info maps with node field
  """
  @spec nfs_list_exports() :: {:ok, [map()]} | {:error, term()}
  def nfs_list_exports do
    set_cli_metadata()

    with :ok <- require_cluster() do
      exported = Enum.filter(VolumeRegistry.list(), & &1.nfs_export)

      rows =
        case get_all_nfs_nodes() do
          [] -> Enum.map(exported, &nfs_export_row(&1, "-", "-", 2049))
          nfs_nodes -> Enum.flat_map(nfs_nodes, &node_export_rows(&1, exported))
        end

      {:ok, Enum.sort_by(rows, &{&1.volume_name, &1.node})}
    end
  end

  # Private

  defp get_all_fuse_nodes do
    registry_nodes =
      try do
        ServiceRegistry.list_by_type(:fuse)
        |> Enum.map(& &1.node)
      rescue
        ArgumentError -> []
      end

    local_node =
      case check_local_fuse() do
        :available -> [Node.self()]
        :not_available -> []
      end

    connected_fuse_nodes =
      Node.list()
      |> Enum.filter(&fuse_node?/1)

    (registry_nodes ++ local_node ++ connected_fuse_nodes)
    |> Enum.uniq()
  end

  # Get all reachable NFS nodes in the cluster
  defp get_all_nfs_nodes do
    registry_nodes =
      try do
        ServiceRegistry.list_by_type(:nfs)
        |> Enum.map(& &1.node)
      rescue
        ArgumentError -> []
      end

    local_node =
      case check_local_nfs() do
        :available -> [Node.self()]
        :not_available -> []
      end

    connected_nfs_nodes =
      Node.list()
      |> Enum.filter(&nfs_node?/1)

    (registry_nodes ++ local_node ++ connected_nfs_nodes)
    |> Enum.uniq()
  end

  # Get the FUSE node and verify it's reachable.
  # A FUSE node co-located with the mounting client wins outright: the mount
  # point only exists on the operator's host, so when the CLI tells us its
  # hostname we honour it even if the RPC landed on a core-only node elsewhere
  # (#1359). Otherwise checks in order: local node, ServiceRegistry, connected
  # nodes, configured fallback. The local node is preferred because a FUSE mount
  # is only visible on its own host: on an all-omnibus cluster every node
  # registers a `:fuse` service, so routing to the registry head would mount on
  # an arbitrary node — not the one the operator ran the CLI on (#1358).
  defp get_fuse_node(client_host) do
    case same_host_fuse_node(client_host) do
      {:ok, fuse_node} -> {:ok, fuse_node}
      :not_found -> get_fuse_node()
    end
  end

  defp get_fuse_node do
    with :not_available <- check_local_fuse(),
         :not_found <- discover_fuse_node_from_registry(),
         :not_found <- discover_fuse_node() do
      check_configured_fuse_node()
    else
      :available -> {:ok, Node.self()}
      {:ok, fuse_node} -> {:ok, fuse_node}
    end
  end

  # Prefer a known FUSE node whose host matches the mounting client's. No
  # match (the common all-`@localhost` single-host case, or an unknown host)
  # falls through to the default discovery chain.
  defp same_host_fuse_node(nil), do: :not_found

  defp same_host_fuse_node(client_host) do
    get_all_fuse_nodes()
    |> Enum.find(fn node -> node_host(node) == client_host end)
    |> case do
      nil -> :not_found
      fuse_node -> {:ok, fuse_node}
    end
  end

  defp pop_client_host(options) do
    case Map.pop(options, "host") do
      {host, rest} when is_binary(host) and host != "" -> {host, rest}
      {_absent_or_blank, rest} -> {nil, rest}
    end
  end

  # Whitelist the known FUSE mount flags into the keyword opts forwarded to
  # the FUSE node's `MountManager` (#1574). The generic string→atom coercion
  # can't be used here: `:allow_other` / `:allow_root` are only referenced in
  # the FUSE app, so on a core-only node they aren't in the atom table and
  # `String.to_existing_atom/1` would raise — silently dropping the flags.
  # Naming the atoms here keeps them deterministically resolvable on core.
  @fuse_mount_flags [:allow_other, :allow_root]

  defp fuse_mount_opts(options) do
    Enum.reduce(@fuse_mount_flags, [], fn flag, acc ->
      if Map.get(options, Atom.to_string(flag)) == true do
        [{flag, true} | acc]
      else
        acc
      end
    end)
  end

  # Get the NFS node and verify it's reachable
  # Checks in order: ServiceRegistry, local node, connected nodes, configured fallback
  defp get_nfs_node do
    with :not_found <- discover_nfs_node_from_registry(),
         :not_available <- check_local_nfs(),
         :not_found <- discover_nfs_node() do
      check_configured_nfs_node()
    else
      :available -> {:ok, Node.self()}
      {:ok, nfs_node} -> {:ok, nfs_node}
    end
  end

  # Try ServiceRegistry first — this is the authoritative source
  defp discover_fuse_node_from_registry do
    case ServiceRegistry.list_by_type(:fuse) do
      [first | _] -> {:ok, first.node}
      [] -> :not_found
    end
  rescue
    # ServiceRegistry may not be started yet
    ArgumentError -> :not_found
  end

  # Try ServiceRegistry first — this is the authoritative source
  defp discover_nfs_node_from_registry do
    case ServiceRegistry.list_by_type(:nfs) do
      [first | _] -> {:ok, first.node}
      [] -> :not_found
    end
  rescue
    # ServiceRegistry may not be started yet
    ArgumentError -> :not_found
  end

  # Check if FUSE MountManager is available on the local node
  defp check_local_fuse do
    case Process.whereis(NeonFS.FUSE.MountManager) do
      nil -> :not_available
      _pid -> :available
    end
  end

  # Check if NFS ExportManager is available on the local node
  defp check_local_nfs do
    case Process.whereis(NeonFS.NFS.ExportManager) do
      nil -> :not_available
      _pid -> :available
    end
  end

  # Discover FUSE node by looking for connected nodes with neonfs_fuse prefix
  defp discover_fuse_node do
    Node.list()
    |> Enum.find(&fuse_node?/1)
    |> case do
      nil -> :not_found
      fuse_node -> {:ok, fuse_node}
    end
  end

  # Discover NFS node by looking for connected nodes with neonfs_nfs prefix
  defp discover_nfs_node do
    Node.list()
    |> Enum.find(&nfs_node?/1)
    |> case do
      nil -> :not_found
      nfs_node -> {:ok, nfs_node}
    end
  end

  defp fuse_node?(node) do
    node |> Atom.to_string() |> String.starts_with?("neonfs_fuse@")
  end

  defp nfs_node?(node) do
    node |> Atom.to_string() |> String.starts_with?("neonfs_nfs@")
  end

  # Fall back to configured node (for backwards compatibility)
  defp check_configured_fuse_node do
    fuse_node = Application.get_env(:neonfs_core, :fuse_node, :neonfs_fuse@localhost)

    case :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :__info__, [:module]) do
      {:badrpc, _} ->
        {:error, Unavailable.exception(message: "FUSE service not available")}

      NeonFS.FUSE.MountManager ->
        {:ok, fuse_node}
    end
  end

  # Fall back to configured node
  defp check_configured_nfs_node do
    nfs_node = Application.get_env(:neonfs_core, :nfs_node, :neonfs_nfs@localhost)

    case :rpc.call(nfs_node, NeonFS.NFS.ExportManager, :__info__, [:module]) do
      {:badrpc, _} ->
        {:error, Unavailable.exception(message: "NFS service not available")}

      NeonFS.NFS.ExportManager ->
        {:ok, nfs_node}
    end
  end

  defp rpc_mount(fuse_node, volume_name, mount_point, opts) do
    fuse_rpc(fuse_node, :mount, [volume_name, mount_point, opts], @mount_rpc_timeout)
  end

  defp rpc_unmount(fuse_node, mount_id) do
    fuse_rpc(fuse_node, :unmount, [mount_id], @unmount_rpc_timeout)
  end

  defp rpc_list_mounts(fuse_node) do
    case fuse_rpc(fuse_node, :list_mounts, [], :infinity) do
      {:error, _} = error -> error
      mounts when is_list(mounts) -> {:ok, mounts}
      result -> result
    end
  end

  defp rpc_get_mount(fuse_node, mount_id) do
    fuse_rpc(fuse_node, :get_mount, [mount_id], :infinity)
  end

  defp rpc_get_mount_by_volume_name(fuse_node, volume_name) do
    fuse_rpc(fuse_node, :get_mount_by_volume_name, [volume_name], :infinity)
  end

  defp rpc_get_mount_by_path(fuse_node, path) do
    fuse_rpc(fuse_node, :get_mount_by_path, [path], :infinity)
  end

  # Single dispatch point for MountManager RPCs. The RPC module is read from
  # app env (defaulting to `:rpc`) so tests can inject a stub and assert which
  # node a mount was routed to (#1358).
  defp fuse_rpc(fuse_node, fun, args, timeout) do
    rpc_mod = Application.get_env(:neonfs_core, :fuse_rpc_mod, :rpc)

    case rpc_mod.call(fuse_node, NeonFS.FUSE.MountManager, fun, args, timeout) do
      {:badrpc, reason} ->
        {:error, Unavailable.exception(message: "FUSE RPC failed: #{inspect(reason)}")}

      result ->
        result
    end
  end

  defp clear_nfs_export(%{nfs_export: false}), do: {:error, :not_exported}
  defp clear_nfs_export(volume), do: VolumeRegistry.update(volume.id, nfs_export: false)

  defp ensure_nfs_exported(%{nfs_export: true}), do: :ok
  defp ensure_nfs_exported(_volume), do: {:error, :not_exported}

  defp rpc_nfs_bind_info(nfs_node) do
    host =
      case :rpc.call(nfs_node, Application, :get_env, [:neonfs_nfs, :bind_address]) do
        {:badrpc, _} -> node_host(nfs_node)
        nil -> node_host(nfs_node)
        address when address in ["0.0.0.0", "::"] -> node_host(nfs_node)
        address -> address
      end

    port =
      case :rpc.call(nfs_node, Application, :get_env, [:neonfs_nfs, :port]) do
        {:badrpc, _} -> 2049
        nil -> 2049
        p -> p
      end

    {host, port}
  end

  defp node_host(node) do
    node
    |> Atom.to_string()
    |> String.split("@")
    |> List.last()
  end

  # Tries the supplied identifier as a mount-id, then as a mount
  # point, then as a volume name. The volume-name lookup is the
  # newest gate (#1016) — operators frequently typed
  # `neonfs fuse unmount <volume>` and got an unhelpful "Mount not
  # found" when the daemon only resolved mount ids and paths.
  defp do_unmount(mount_id_or_path, fuse_node) do
    case rpc_get_mount(fuse_node, mount_id_or_path) do
      {:ok, _mount} ->
        wrap_unmount_result(rpc_unmount(fuse_node, mount_id_or_path))

      {:error, :not_found} ->
        unmount_by_path_or_volume(mount_id_or_path, fuse_node)
    end
  end

  defp unmount_by_path_or_volume(mount_id_or_path, fuse_node) do
    case rpc_get_mount_by_path(fuse_node, mount_id_or_path) do
      {:ok, mount} ->
        wrap_unmount_result(rpc_unmount(fuse_node, mount.id))

      {:error, :not_found} ->
        unmount_by_volume_name(mount_id_or_path, fuse_node)
    end
  end

  defp unmount_by_volume_name(volume_name, fuse_node) do
    case rpc_get_mount_by_volume_name(fuse_node, volume_name) do
      {:ok, mount} -> wrap_unmount_result(rpc_unmount(fuse_node, mount.id))
      {:error, :not_found} -> {:error, :mount_not_found}
    end
  end

  defp wrap_unmount_result(:ok), do: {:ok, %{}}
  defp wrap_unmount_result({:error, _} = err), do: err

  defp collect_node_mounts(fuse_node) do
    case rpc_list_mounts(fuse_node) do
      {:ok, node_mounts} ->
        Enum.map(node_mounts, &mount_info_to_map(&1, fuse_node))

      {:error, _} ->
        []
    end
  end

  defp mount_info_to_map(mount_info, fuse_node) do
    # Convert mount info to map, excluding PIDs and references
    %{
      id: mount_info.id,
      node: Atom.to_string(fuse_node),
      volume_name: mount_info.volume_name,
      mount_point: mount_info.mount_point,
      started_at: DateTime.to_iso8601(mount_info.started_at)
    }
  end

  # Best-effort endpoint hint for CLI output: exports are served by
  # every NFS node, so any reachable one will do for the mount-command
  # hint. Degrades to placeholders when none is up — the export itself
  # is still valid cluster state.
  defp nfs_export_to_map(volume) do
    case get_nfs_node() do
      {:ok, nfs_node} ->
        {server_address, port} = rpc_nfs_bind_info(nfs_node)
        nfs_export_row(volume, Atom.to_string(nfs_node), server_address, port)

      {:error, _} ->
        nfs_export_row(volume, "-", "-", 2049)
    end
  end

  defp nfs_export_row(volume, node_name, server_address, port) do
    %{
      node: node_name,
      volume_name: volume.name,
      exported_at: DateTime.to_iso8601(volume.updated_at),
      server_address: server_address,
      port: port,
      allowed_ips: volume.nfs_allowed_ips,
      root_squash: volume.nfs_root_squash
    }
  end

  defp node_export_rows(nfs_node, exported) do
    {server_address, port} = rpc_nfs_bind_info(nfs_node)
    Enum.map(exported, &nfs_export_row(&1, Atom.to_string(nfs_node), server_address, port))
  end
end
