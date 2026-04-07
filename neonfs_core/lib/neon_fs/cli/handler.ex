defmodule NeonFS.CLI.Handler do
  @moduledoc """
  RPC handler for CLI commands. Called via Erlang distribution.

  This module provides the daemon-side interface for CLI operations, converting
  internal data structures to serializable maps that can be sent across the
  Erlang distribution protocol.

  All functions return `{:ok, data}` or `{:error, reason}` tuples where data
  consists only of serializable terms (maps, lists, atoms, strings, numbers).
  """

  require Logger

  alias NeonFS.Client.HealthCheck, as: ClientHealthCheck
  alias NeonFS.Cluster.{Init, Invite, Join, State}

  alias NeonFS.Core.{
    ACLManager,
    AuditLog,
    Authorise,
    BackgroundWorker,
    CertificateAuthority,
    DriveManager,
    JobTracker,
    KeyManager,
    KeyRotation,
    RaSupervisor,
    ServiceRegistry,
    StorageMetrics,
    Volume,
    VolumeACL,
    VolumeEncryption,
    VolumeRegistry
  }

  alias NeonFS.Core.Job

  alias NeonFS.Error.{
    Internal,
    Invalid,
    InvalidConfig,
    NotFound,
    PermissionDenied,
    Unavailable,
    VolumeNotFound
  }

  @doc """
  Returns cluster status information.

  ## Returns
  - `{:ok, map}` - Status map with cluster information
  """
  @spec cluster_status() :: {:ok, map()}
  def cluster_status do
    set_cli_metadata()

    if State.exists?() do
      {:ok,
       %{
         name: get_cluster_name(),
         node: Atom.to_string(Node.self()),
         status: :running,
         volumes: count_volumes(),
         uptime_seconds: get_uptime()
       }}
    else
      {:ok,
       %{
         name: nil,
         node: Atom.to_string(Node.self()),
         status: :not_initialised,
         volumes: 0,
         uptime_seconds: get_uptime()
       }}
    end
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
    set_cli_metadata()

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

      {:error, :already_initialised} ->
        {:error, Invalid.exception(message: "Cluster already initialised")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Returns cluster CA information.

  ## Returns
  - `{:ok, map}` - CA info with subject, algorithm, validity dates, serial counter
  - `{:error, :ca_not_initialized}` - CA hasn't been created yet
  """
  @spec handle_ca_info() :: {:ok, map()} | {:error, term()}
  def handle_ca_info do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case CertificateAuthority.ca_info() do
        {:ok, info} ->
          {:ok,
           %{
             subject: info.subject,
             algorithm: info.algorithm,
             valid_from: DateTime.to_iso8601(info.valid_from),
             valid_to: DateTime.to_iso8601(info.valid_to),
             current_serial: info.current_serial,
             nodes_issued: info.nodes_issued
           }}

        {:error, _} ->
          {:error, Unavailable.exception(message: "Certificate authority not initialised")}
      end
    end
  end

  @doc """
  Lists all issued node certificates with their status.

  ## Returns
  - `{:ok, [map]}` - List of certificate info maps
  - `{:error, :ca_not_initialized}` - CA hasn't been created yet
  """
  @spec handle_ca_list() :: {:ok, [map()]} | {:error, term()}
  def handle_ca_list do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case CertificateAuthority.list_issued() do
        {:ok, certs} ->
          {:ok,
           Enum.map(certs, fn cert ->
             %{
               node_name: cert.node_name,
               hostname: cert.hostname,
               serial: cert.serial,
               expires: DateTime.to_iso8601(cert.not_after),
               status: if(cert.revoked, do: "revoked", else: "valid")
             }
           end)}

        {:error, _} ->
          {:error, Unavailable.exception(message: "Certificate authority not initialised")}
      end
    end
  end

  @doc """
  Revokes a node's certificate by node name.

  Looks up the node in the issued certificates list and revokes its certificate.

  ## Parameters
  - `node_name` - The node name (as it appears in the certificate subject CN)

  ## Returns
  - `{:ok, map}` - Revocation result with serial number
  - `{:error, :node_not_found}` - No certificate found for the given node
  - `{:error, :ca_not_initialized}` - CA hasn't been created yet
  """
  @spec handle_ca_revoke(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_ca_revoke(node_name) when is_binary(node_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, certs} <- map_ca_error(CertificateAuthority.list_issued()),
         {:ok, cert} <- find_cert_by_node(certs, node_name),
         :ok <- CertificateAuthority.revoke_certificate(cert.serial, :cessation_of_operation) do
      {:ok, %{serial: cert.serial, node_name: cert.node_name, status: "revoked"}}
    else
      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Rotates the cluster CA.

  CA rotation is a rare, disruptive operation that reissues all node
  certificates. It requires a dual-CA transition period and rolling
  reissuance across the cluster.

  Not yet implemented — returns a structured error.
  """
  @spec handle_ca_rotate() :: {:error, Exception.t()}
  def handle_ca_rotate do
    set_cli_metadata()

    with :ok <- require_cluster() do
      {:error, Unavailable.exception(message: "CA rotation not yet implemented")}
    end
  end

  @doc """
  Creates an invite token for joining nodes.

  ## Parameters
  - `expires_in` - Duration in seconds the token is valid for

  ## Returns
  - `{:ok, %{"token" => string}}` - Success map with invite token
  - `{:error, reason}` - Error tuple
  """
  @spec create_invite(pos_integer()) :: {:ok, map()} | {:error, term()}
  def create_invite(expires_in) when is_integer(expires_in) and expires_in > 0 do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case Invite.create_invite(expires_in) do
        {:ok, token} ->
          {:ok, %{"token" => token}}

        {:error, :cluster_not_initialized} ->
          {:error, Unavailable.exception(message: "Cluster not initialised")}

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Joins an existing cluster using an invite token.

  ## Parameters
  - `token` - Invite token from existing cluster
  - `via_node` - Node name of existing cluster member (string)
  - `type` - Service type string (e.g. "core", "fuse"). Defaults to "core".

  ## Returns
  - `{:ok, map}` - Success map with cluster information
  - `{:error, reason}` - Error tuple
  """
  @spec join_cluster(String.t(), String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def join_cluster(token, via_address, type_str \\ "core")
      when is_binary(token) and is_binary(via_address) and is_binary(type_str) do
    set_cli_metadata()
    type = String.to_existing_atom(type_str)

    case Join.join_cluster(token, via_address, type) do
      {:ok, state} ->
        # Rebuild the quorum metadata ring on all nodes to include the new member
        rebuild_quorum_ring_on_all_nodes()

        AuditLog.log_event(
          event_type: :node_joined,
          actor_uid: 0,
          resource: Atom.to_string(Node.self()),
          details: %{
            cluster_id: state.cluster_id,
            node_type: Atom.to_string(state.node_type),
            via_address: via_address
          }
        )

        {:ok,
         %{
           "cluster_id" => state.cluster_id,
           "cluster_name" => state.cluster_name,
           "created_at" => DateTime.to_iso8601(state.created_at),
           "node_id" => state.this_node.id,
           "node_name" => Atom.to_string(state.this_node.name),
           "node_type" => Atom.to_string(state.node_type),
           "joined_at" => DateTime.to_iso8601(state.this_node.joined_at),
           "known_peers" =>
             Enum.map(state.known_peers, fn peer ->
               %{
                 "id" => peer.id,
                 "name" => Atom.to_string(peer.name),
                 "last_seen" => DateTime.to_iso8601(peer.last_seen)
               }
             end)
         }}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Lists all registered services in the cluster.

  ## Returns
  - `{:ok, [map]}` - List of service info maps
  """
  @spec list_services() :: {:ok, [map()]}
  def list_services do
    set_cli_metadata()

    with :ok <- require_cluster() do
      services =
        ServiceRegistry.list()
        |> Enum.map(&service_info_to_map/1)

      {:ok, services}
    end
  end

  @doc """
  Lists volumes in the cluster.

  ## Parameters
  - `filters` - Optional filter map:
    - `"all"` - When `true`, includes system volumes (default: excluded)

  ## Returns
  - `{:ok, [map]}` - List of volume maps
  """
  @spec list_volumes(map()) :: {:ok, [map()]}
  def list_volumes(filters \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      opts =
        case Map.get(filters, "all") do
          true -> [include_system: true]
          _ -> []
        end

      volumes =
        VolumeRegistry.list(opts)
        |> Enum.map(&volume_to_map/1)

      {:ok, volumes}
    end
  end

  @doc """
  Creates a new volume with the given name and configuration.

  ## Parameters
  - `name` - Volume name (string)
  - `config` - Configuration map with optional keys:
    - `:owner` - Owner string
    - `:durability` - Durability config map
    - `:write_ack` - Write acknowledgment level (`:local`, `:quorum`, `:all`)
    - `:tiering` - Tiering config map (`:initial_tier`, `:promotion_threshold`, `:demotion_delay`)
    - `:caching` - Caching config map (`:transformed_chunks`, `:reconstructed_stripes`, `:remote_chunks`)
    - `:io_weight` - I/O scheduling weight (positive integer)
    - `:compression` - Compression config map
    - `:verification` - Verification config map

  ## Returns
  - `{:ok, map}` - Created volume as map
  - `{:error, reason}` - Error tuple
  """
  @spec create_volume(String.t(), map()) :: {:ok, map()} | {:error, term()}
  def create_volume(name, config) when is_binary(name) and is_map(config) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      opts = map_to_opts(config)
      owner_uid = Keyword.get(opts, :owner_uid, 0)
      owner_gid = Keyword.get(opts, :owner_gid, 0)

      with {:ok, parsed_opts} <- parse_durability_opt(opts),
           {:ok, enc_opts} <- parse_encryption_opt(parsed_opts),
           final_opts = merge_verification_defaults(enc_opts),
           {:ok, volume} <- VolumeRegistry.create(name, final_opts),
           :ok <- setup_encryption_if_needed(volume) do
        create_initial_acl(volume.id, owner_uid, owner_gid)

        AuditLog.log_event(
          event_type: :volume_created,
          actor_uid: owner_uid,
          resource: volume.id,
          details: %{name: name}
        )

        {:ok, volume_to_map(volume)}
      else
        {:error, :already_exists} ->
          {:error, Invalid.exception(message: "Volume '#{name}' already exists")}

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Updates an existing volume's configuration.

  ## Parameters
  - `name` - Volume name (string)
  - `config` - Configuration map with string keys. Supported fields:
    - `"atime_mode"` - POSIX atime mode (`"noatime"` or `"relatime"`)
    - `"compression"` - Compression config map (e.g. `%{"algorithm" => "none"}`)
    - `"io_weight"` - I/O scheduling weight (positive integer)
    - `"write_ack"` - Write acknowledgement level (`"local"`, `"quorum"`, `"all"`)
    - `"initial_tier"` / `"promotion_threshold"` / `"demotion_delay"` - Tiering sub-fields
    - `"transformed_chunks"` / `"reconstructed_stripes"` / `"remote_chunks"` - Caching sub-fields
    - `"on_read"` / `"sampling_rate"` / `"scrub_interval"` - Verification sub-fields
    - `"metadata_replicas"` / `"read_quorum"` / `"write_quorum"` - Metadata consistency sub-fields

  Immutable fields (`durability`, `encryption`, `name`, `id`) are rejected.
  Nested sub-fields are merged with the volume's current configuration.

  ## Returns
  - `{:ok, map}` - Updated volume as map
  - `{:error, reason}` - Error tuple
  """
  @spec update_volume(String.t(), map()) :: {:ok, map()} | {:error, term()}
  def update_volume(name, config) when is_binary(name) and is_map(config) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(name),
         :ok <- reject_immutable_updates(config),
         opts = build_update_opts(config, volume),
         {:ok, updated} <- VolumeRegistry.update(volume.id, opts) do
      :telemetry.execute(
        [:neonfs, :cli, :volume_updated],
        %{},
        %{volume_id: volume.id, name: name, fields: Map.keys(config)}
      )

      AuditLog.log_event(
        event_type: :volume_updated,
        actor_uid: 0,
        resource: volume.id,
        details: %{name: name, fields: Map.keys(config)}
      )

      {:ok, volume_to_map(updated)}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
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
  def delete_volume(name), do: delete_volume(name, [])

  @spec delete_volume(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def delete_volume(name, opts) when is_binary(name) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      uid = Keyword.get(opts, :uid, 0)
      gids = Keyword.get(opts, :gids, [])

      with {:ok, volume} <- VolumeRegistry.get_by_name(name),
           :ok <- Authorise.check(uid, gids, :admin, {:volume, volume.id}),
           :ok <- VolumeRegistry.delete(volume.id) do
        cleanup_volume_acl(volume.id)

        AuditLog.log_event(
          event_type: :volume_deleted,
          actor_uid: uid,
          resource: volume.id,
          details: %{name: name}
        )

        {:ok, %{}}
      else
        {:error, :not_found} ->
          {:error, VolumeNotFound.exception(volume_name: name)}

        {:error, :permission_denied} ->
          {:error, PermissionDenied.exception(operation: :admin)}

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
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
    set_cli_metadata()

    with :ok <- require_cluster() do
      case VolumeRegistry.get_by_name(name) do
        {:ok, volume} ->
          {:ok, volume_to_map(volume)}

        {:error, :not_found} ->
          {:error, VolumeNotFound.exception(volume_name: name)}

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
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
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, fuse_node} <- get_fuse_node(),
         opts = map_to_opts(options),
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
        {:error, NotFound.exception(message: "Mount not found: #{mount_id_or_path}")}

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

  Requires the neonfs_nfs application to be running on a reachable node.

  ## Parameters
  - `volume_name` - Volume name (string)

  ## Returns
  - `{:ok, map}` - Export info as map
  - `{:error, reason}` - Error tuple
  """
  @spec nfs_mount(String.t()) :: {:ok, map()} | {:error, term()}
  def nfs_mount(volume_name) when is_binary(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, nfs_node} <- get_nfs_node(),
         {:ok, _volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, export_id} <- rpc_nfs_export(nfs_node, volume_name),
         {:ok, export_info} <- rpc_nfs_get_export(nfs_node, export_id) do
      {:ok, nfs_export_to_map(export_info, nfs_node)}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Unexports a volume from NFS by export ID or volume name.

  Requires the neonfs_nfs application to be running on a reachable node.

  ## Parameters
  - `export_id_or_volume` - Export ID (string) or volume name (string)

  ## Returns
  - `{:ok, %{}}` - Success with empty map
  - `{:error, reason}` - Error tuple
  """
  @spec nfs_unmount(String.t()) :: {:ok, map()} | {:error, term()}
  def nfs_unmount(export_id_or_volume) when is_binary(export_id_or_volume) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, nfs_node} <- get_nfs_node(),
         {:ok, _} <- do_nfs_unmount(export_id_or_volume, nfs_node) do
      {:ok, %{}}
    else
      {:error, :export_not_found} ->
        {:error, NotFound.exception(message: "NFS export not found: #{export_id_or_volume}")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Lists all active NFS exports across the cluster.

  Queries all discovered NFS nodes and aggregates their exports.

  ## Returns
  - `{:ok, [map]}` - List of export info maps with node field
  """
  @spec nfs_list_mounts() :: {:ok, [map()]} | {:error, term()}
  def nfs_list_mounts do
    set_cli_metadata()

    with :ok <- require_cluster() do
      nfs_nodes = get_all_nfs_nodes()

      if Enum.empty?(nfs_nodes) do
        {:error, wrap_error(Unavailable.exception(message: "NFS service not available"))}
      else
        exports =
          nfs_nodes
          |> Enum.flat_map(&collect_node_nfs_exports/1)
          |> Enum.sort_by(& &1.node)

        {:ok, exports}
      end
    end
  end

  @doc """
  Starts key rotation for a volume.

  ## Parameters
  - `volume_name` - Volume name (string)

  ## Returns
  - `{:ok, map}` - Rotation info with from_version, to_version, total_chunks
  - `{:error, reason}` - Error tuple
  """
  @spec rotate_volume_key(String.t()) :: {:ok, map()} | {:error, term()}
  def rotate_volume_key(volume_name) when is_binary(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, result} <- KeyRotation.start_rotation(volume.id) do
      {:ok, result}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Returns the current key rotation status for a volume.

  ## Parameters
  - `volume_name` - Volume name (string)

  ## Returns
  - `{:ok, map}` - Rotation state with progress
  - `{:error, :no_rotation}` - No rotation in progress
  - `{:error, reason}` - Error tuple
  """
  @spec rotation_status(String.t()) :: {:ok, map()} | {:error, term()}
  def rotation_status(volume_name) when is_binary(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, result} <- KeyRotation.rotation_status(volume.id) do
      {:ok, result}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, :no_rotation} ->
        {:error, NotFound.exception(message: "No key rotation in progress for '#{volume_name}'")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Sets extended ACL entries on a file or directory.

  ## Parameters
  - `volume_name` - Volume name (string)
  - `path` - File path within the volume
  - `acl_entries` - List of ACL entry maps

  ## Returns
  - `{:ok, %{}}` - Success
  - `{:error, reason}` - Error tuple
  """
  @spec handle_set_file_acl(String.t(), String.t(), [map()]) :: {:ok, map()} | {:error, term()}
  def handle_set_file_acl(volume_name, path, acl_entries) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         :ok <- ACLManager.set_file_acl(volume.id, path, acl_entries) do
      {:ok, %{}}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Gets the file ACL (mode + extended entries) for a file or directory.

  ## Parameters
  - `volume_name` - Volume name (string)
  - `path` - File path within the volume

  ## Returns
  - `{:ok, map}` - ACL info with mode, uid, gid, acl_entries, default_acl
  - `{:error, reason}` - Error tuple
  """
  @spec handle_get_file_acl(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def handle_get_file_acl(volume_name, path) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, result} <- ACLManager.get_file_acl(volume.id, path) do
      {:ok, result}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Sets the default ACL for a directory.

  ## Parameters
  - `volume_name` - Volume name (string)
  - `path` - Directory path within the volume
  - `default_acl` - List of ACL entry maps to inherit

  ## Returns
  - `{:ok, %{}}` - Success
  - `{:error, reason}` - Error tuple
  """
  @spec handle_set_default_acl(String.t(), String.t(), [map()]) :: {:ok, map()} | {:error, term()}
  def handle_set_default_acl(volume_name, path, default_acl) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         :ok <- ACLManager.set_default_acl(volume.id, path, default_acl) do
      {:ok, %{}}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Lists audit log events with optional filters.

  ## Parameters
  - `filters` - Map with optional keys:
    - `"type"` - Event type string (e.g. "volume_created")
    - `"actor_uid"` - Actor UID integer
    - `"since"` - ISO 8601 datetime string
    - `"until"` - ISO 8601 datetime string
    - `"limit"` - Maximum number of results (default: 100)

  ## Returns
  - `{:ok, [map]}` - List of audit event maps
  """
  @spec handle_audit_list(map()) :: {:ok, [map()]}
  def handle_audit_list(filters \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      query_opts = parse_audit_filters(filters)
      local_events = AuditLog.query(query_opts)
      remote_events = collect_remote_audit_events(query_opts)

      events =
        (local_events ++ remote_events)
        |> Enum.sort_by(& &1.timestamp, {:desc, DateTime})
        |> maybe_limit_events(query_opts)
        |> Enum.map(&audit_event_to_map/1)

      {:ok, events}
    end
  end

  @doc """
  Grants permissions to a principal on a volume.

  ## Parameters
  - `volume_name` - Volume name (string)
  - `principal_str` - Principal string, e.g. "uid:1000" or "gid:100"
  - `permissions` - List of permission strings, e.g. ["read", "write"]

  ## Returns
  - `{:ok, %{}}` - Success
  - `{:error, reason}` - Error tuple
  """
  @spec handle_acl_grant(String.t(), String.t(), [String.t()]) :: {:ok, map()} | {:error, term()}
  def handle_acl_grant(volume_name, principal_str, permissions) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, principal} <- parse_principal(principal_str),
         {:ok, perms} <- parse_permissions(permissions),
         :ok <- ACLManager.grant(volume.id, principal, perms) do
      AuditLog.log_event(
        event_type: :acl_grant,
        actor_uid: 0,
        resource: volume.id,
        details: %{principal: principal_str, permissions: permissions}
      )

      {:ok, %{}}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Revokes all permissions for a principal on a volume.

  ## Parameters
  - `volume_name` - Volume name (string)
  - `principal_str` - Principal string, e.g. "uid:1000" or "gid:100"

  ## Returns
  - `{:ok, %{}}` - Success
  - `{:error, reason}` - Error tuple
  """
  @spec handle_acl_revoke(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def handle_acl_revoke(volume_name, principal_str) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, principal} <- parse_principal(principal_str),
         :ok <- ACLManager.revoke(volume.id, principal) do
      AuditLog.log_event(
        event_type: :acl_revoke,
        actor_uid: 0,
        resource: volume.id,
        details: %{principal: principal_str}
      )

      {:ok, %{}}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Shows the ACL for a volume.

  ## Parameters
  - `volume_name` - Volume name (string)

  ## Returns
  - `{:ok, map}` - ACL info with owner_uid, owner_gid, entries
  - `{:error, reason}` - Error tuple
  """
  @spec handle_acl_show(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_acl_show(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, acl} <- ACLManager.get_volume_acl(volume.id) do
      {:ok, volume_acl_to_map(acl)}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Adds a drive to the local node.

  ## Parameters
  - `config` - Drive config map with keys: "path", "tier", "capacity", optional "id"

  ## Returns
  - `{:ok, map}` - Drive info as map
  - `{:error, reason}` - Error tuple
  """
  @spec handle_add_drive(map()) :: {:ok, map()} | {:error, term()}
  def handle_add_drive(config) when is_map(config) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case DriveManager.add_drive(config) do
        {:ok, drive_map} -> {:ok, drive_map}
        {:error, reason} -> {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Removes a drive from the local node.

  ## Parameters
  - `drive_id` - Drive identifier (string)
  - `force` - Whether to force removal even if drive has data (boolean)

  ## Returns
  - `{:ok, %{}}` - Success
  - `{:error, reason}` - Error tuple
  """
  @spec handle_remove_drive(String.t(), boolean()) :: {:ok, map()} | {:error, term()}
  def handle_remove_drive(drive_id, force \\ false) when is_binary(drive_id) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case DriveManager.remove_drive(drive_id, force: force) do
        :ok -> {:ok, %{}}
        {:error, reason} -> {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Lists drives across the cluster.

  ## Parameters
  - `filters` - Optional filter map:
    - `"node"` - Node name string to filter by (e.g. "neonfs_core@host1")

  ## Returns
  - `{:ok, [map]}` - List of drive info maps
  """
  @spec handle_list_drives(map()) :: {:ok, [map()]}
  def handle_list_drives(filters \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      opts =
        case Map.get(filters, "node") do
          nil -> []
          node_name -> [node: String.to_atom(node_name)]
        end

      {:ok, DriveManager.list_all_drives(opts)}
    end
  end

  @doc """
  Starts evacuation of all data from a drive.

  ## Parameters
  - `node_name` - Node name string (e.g. "neonfs-core@host")
  - `drive_id` - Drive identifier
  - `opts` - Options map with optional keys:
    - `"any_tier"` - Allow migration to any tier (default: false)

  ## Returns
  - `{:ok, map}` - Job info map
  - `{:error, reason}` - Error tuple
  """
  @spec handle_evacuate_drive(String.t(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def handle_evacuate_drive(node_name, drive_id, opts \\ %{})
      when is_binary(node_name) and is_binary(drive_id) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      node = String.to_atom(node_name)
      any_tier = Map.get(opts, "any_tier", false)

      case NeonFS.Core.DriveEvacuation.start_evacuation(node, drive_id, any_tier: any_tier) do
        {:ok, job} -> {:ok, job_to_map(job)}
        {:error, reason} -> {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Returns the evacuation status for a drive.

  ## Parameters
  - `drive_id` - Drive identifier

  ## Returns
  - `{:ok, map}` - Status map
  - `{:error, reason}` - Error tuple
  """
  @spec handle_evacuation_status(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_evacuation_status(drive_id) when is_binary(drive_id) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case NeonFS.Core.DriveEvacuation.evacuation_status(drive_id) do
        {:ok, status} ->
          {:ok,
           %{
             job_id: status.job_id,
             status: Atom.to_string(status.status),
             progress_total: status.progress.total,
             progress_completed: status.progress.completed,
             progress_description: status.progress.description,
             drive_id: status.drive_id,
             node: if(status.node, do: Atom.to_string(status.node)),
             any_tier: status.any_tier
           }}

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Starts a cluster-wide rebalance operation.

  ## Parameters
  - `opts` - Options map with optional keys:
    - `"tier"` - Specific tier to rebalance (e.g. "hot", "warm", "cold")
    - `"threshold"` - Balance tolerance as string float (default: "0.10")
    - `"batch_size"` - Chunks per migration batch as string integer (default: "50")

  ## Returns
  - `{:ok, map}` - Job info map
  - `{:error, reason}` - Error tuple
  """
  @spec handle_rebalance(map()) :: {:ok, map()} | {:error, term()}
  def handle_rebalance(opts \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      rebalance_opts =
        []
        |> parse_tier_opt(Map.get(opts, "tier"))
        |> parse_float_opt(:threshold, Map.get(opts, "threshold"))
        |> parse_int_opt(:batch_size, Map.get(opts, "batch_size"))

      case NeonFS.Core.ClusterRebalance.start_rebalance(rebalance_opts) do
        {:ok, job} -> {:ok, job_to_map(job)}
        {:error, reason} -> {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Returns the status of an active or recent rebalance operation.

  ## Returns
  - `{:ok, map}` - Status map with progress info
  - `{:error, :no_rebalance}` - No rebalance in progress
  """
  @spec handle_rebalance_status() :: {:ok, map()} | {:error, term()}
  def handle_rebalance_status do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case NeonFS.Core.ClusterRebalance.rebalance_status() do
        {:ok, status} ->
          {:ok,
           %{
             job_id: status.job_id,
             status: Atom.to_string(status.status),
             progress_total: status.progress.total,
             progress_completed: status.progress.completed,
             progress_description: status.progress.description,
             tiers: Enum.map(status.tiers, &Atom.to_string/1),
             threshold: status.threshold
           }}

        {:error, :no_rebalance} ->
          {:error, NotFound.exception(message: "No rebalance in progress")}

        {:error, reason} ->
          {:error, wrap_error(reason)}
      end
    end
  end

  @doc """
  Returns cluster-wide storage capacity information.

  ## Returns
  - `{:ok, map}` - Capacity info with per-drive breakdown
  """
  @spec handle_storage_stats() :: {:ok, map()}
  def handle_storage_stats do
    set_cli_metadata()

    with :ok <- require_cluster() do
      stats = StorageMetrics.cluster_capacity()

      drives =
        Enum.map(stats.drives, fn d ->
          %{
            node: Atom.to_string(d.node),
            drive_id: d.drive_id,
            tier: Atom.to_string(d.tier),
            capacity_bytes: serialise_capacity(d.capacity_bytes),
            used_bytes: d.used_bytes,
            available_bytes: serialise_capacity(d.available_bytes),
            state: Atom.to_string(d.state)
          }
        end)

      {:ok,
       %{
         drives: drives,
         total_capacity: serialise_capacity(stats.total_capacity),
         total_used: stats.total_used,
         total_available: serialise_capacity(stats.total_available)
       }}
    end
  end

  defp serialise_capacity(:unlimited), do: "unlimited"
  defp serialise_capacity(n) when is_integer(n), do: n

  @doc """
  Starts a garbage collection job.

  ## Parameters
  - `opts` - Options map with optional keys:
    - `"volume"` - Volume name to scope collection to

  ## Returns
  - `{:ok, map}` - Job info map
  - `{:error, :not_found}` - Volume name doesn't exist
  """
  @spec handle_gc_collect(map()) :: {:ok, map()} | {:error, term()}
  def handle_gc_collect(opts \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, params} <- resolve_gc_params(opts),
         {:ok, job} <- JobTracker.create(NeonFS.Core.Job.Runners.GarbageCollection, params) do
      {:ok, job_to_map(job)}
    else
      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Returns recent garbage collection jobs across the cluster.

  ## Returns
  - `{:ok, [map]}` - List of GC job maps, most recent first
  """
  @spec handle_gc_status() :: {:ok, [map()]}
  def handle_gc_status do
    set_cli_metadata()

    with :ok <- require_cluster() do
      jobs = JobTracker.list_cluster(type: NeonFS.Core.Job.Runners.GarbageCollection)
      {:ok, Enum.map(jobs, &job_to_map/1)}
    end
  end

  @doc """
  Starts an integrity scrub job.

  ## Parameters
  - `opts` - Options map with optional keys:
    - `"volume"` - Volume name to scope scrubbing to

  ## Returns
  - `{:ok, map}` - Job info map
  - `{:error, :not_found}` - Volume name doesn't exist
  """
  @spec handle_scrub_start(map()) :: {:ok, map()} | {:error, term()}
  def handle_scrub_start(opts \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, params} <- resolve_scrub_params(opts),
         {:ok, job} <- JobTracker.create(NeonFS.Core.Job.Runners.Scrub, params) do
      {:ok, job_to_map(job)}
    else
      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Returns recent scrub jobs across the cluster.

  ## Returns
  - `{:ok, [map]}` - List of scrub job maps, most recent first
  """
  @spec handle_scrub_status() :: {:ok, [map()]}
  def handle_scrub_status do
    set_cli_metadata()

    with :ok <- require_cluster() do
      jobs = JobTracker.list_cluster(type: NeonFS.Core.Job.Runners.Scrub)
      {:ok, Enum.map(jobs, &job_to_map/1)}
    end
  end

  @doc """
  Lists background jobs with optional filters.

  ## Parameters
  - `filters` - Map with optional keys:
    - `"cluster"` - Whether to query all nodes (default: true)
    - `"status"` - Status string or list (e.g. "running")
    - `"type"` - Job type label (e.g. "key-rotation")

  ## Returns
  - `{:ok, [map]}` - List of job maps
  """
  @spec handle_list_jobs(map()) :: {:ok, [map()]}
  def handle_list_jobs(filters \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      cluster_wide = Map.get(filters, "cluster", true)
      parsed_filters = parse_job_filters(filters)

      jobs =
        if cluster_wide do
          JobTracker.list_cluster(parsed_filters)
        else
          JobTracker.list(parsed_filters)
        end

      {:ok, Enum.map(jobs, &job_to_map/1)}
    end
  end

  @doc """
  Gets a job by ID.

  ## Parameters
  - `job_id` - Job identifier (string)

  ## Returns
  - `{:ok, map}` - Job details as map
  - `{:error, reason}` - Error tuple
  """
  @spec handle_get_job(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_get_job(job_id) when is_binary(job_id) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case JobTracker.get(job_id) do
        {:ok, job} -> {:ok, job_to_map(job)}
        {:error, :not_found} -> {:error, NotFound.exception(message: "Job not found: #{job_id}")}
      end
    end
  end

  @doc """
  Cancels a running or pending job.

  ## Parameters
  - `job_id` - Job identifier (string)

  ## Returns
  - `{:ok, %{}}` - Success
  - `{:error, reason}` - Error tuple
  """
  @spec handle_cancel_job(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_cancel_job(job_id) when is_binary(job_id) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      case JobTracker.cancel(job_id) do
        :ok ->
          {:ok, %{}}

        {:error, :not_found} ->
          {:error, NotFound.exception(message: "Job not found: #{job_id}")}

        {:error, :already_terminal} ->
          {:error, Invalid.exception(message: "Job already finished: #{job_id}")}
      end
    end
  end

  @doc """
  Returns background worker status across the cluster.

  ## Returns
  - `{:ok, [map]}` - List of per-node worker status maps
  """
  @spec handle_worker_status() :: {:ok, [map()]}
  def handle_worker_status do
    set_cli_metadata()

    with :ok <- require_cluster() do
      local_status = worker_status_map(Node.self(), BackgroundWorker.status())
      remote_statuses = collect_remote_worker_statuses()

      statuses =
        [local_status | remote_statuses]
        |> Enum.sort_by(& &1.node)

      {:ok, statuses}
    end
  end

  @doc """
  Reconfigures the background worker with new settings.

  Accepts a map with string keys matching `cluster.json` field names.
  Validates that values are positive integers before applying.
  Persists changes to `cluster.json` so they survive restarts.

  ## Parameters
  - `config` - Map with optional keys: `"max_concurrent"`, `"max_per_minute"`, `"drive_concurrency"`

  ## Returns
  - `{:ok, map}` - New worker config after applying changes
  - `{:error, reason}` - Validation or persistence error
  """
  @spec handle_worker_configure(map()) :: {:ok, map()} | {:error, term()}
  def handle_worker_configure(config) when is_map(config) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, changes} <- validate_worker_config(config),
         :ok <- BackgroundWorker.reconfigure(changes),
         :ok <- persist_worker_config(config) do
      status = BackgroundWorker.status()

      {:ok,
       %{
         max_concurrent: status.max_concurrent,
         max_per_minute: status.max_per_minute,
         drive_concurrency: status.drive_concurrency
       }}
    else
      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Returns node health status from the HealthCheck subsystem.

  ## Returns
  - `{:ok, map}` - Health report with node, status, checked_at, and per-subsystem checks
  """
  @spec handle_node_status() :: {:ok, map()}
  def handle_node_status do
    set_cli_metadata()
    ClientHealthCheck.handle_node_status()
  end

  @doc """
  Returns a list of all nodes in the cluster with their roles and uptimes.

  Combines ServiceRegistry entries with Ra membership to determine leader/follower
  roles for core nodes. Non-core nodes use their service type as their role.

  ## Returns
  - `{:ok, [map]}` - List of node info maps sorted by node name
  """
  @spec handle_node_list() :: {:ok, [map()]}
  def handle_node_list do
    set_cli_metadata()

    with :ok <- require_cluster() do
      {ra_members, leader} = get_ra_membership()

      nodes =
        ServiceRegistry.list()
        |> Enum.map(&service_to_node_info(&1, ra_members, leader))
        |> Enum.sort_by(& &1.node)

      {:ok, nodes}
    end
  end

  # Private helper functions

  defp require_cluster do
    if State.exists?() do
      :ok
    else
      {:error,
       NotFound.exception(
         message:
           "Cluster not initialised. Run 'neonfs cluster init' or 'neonfs cluster join' first."
       )}
    end
  end

  defp set_cli_metadata do
    Logger.metadata(
      component: :cli,
      request_id: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    )
  end

  # Wraps a legacy error reason into a structured NeonFS.Error.
  # Already-structured errors (Splode exceptions with a class field) pass through unchanged.
  defp wrap_error(%{__exception__: true, class: _} = error), do: error

  defp wrap_error(reason) when is_binary(reason),
    do: Internal.exception(message: reason)

  defp wrap_error(reason) when is_atom(reason),
    do: Internal.exception(message: Atom.to_string(reason))

  defp wrap_error(reason),
    do: Internal.exception(message: inspect(reason))

  @worker_config_keys %{
    "max_concurrent" => :max_concurrent,
    "max_per_minute" => :max_per_minute,
    "drive_concurrency" => :drive_concurrency
  }

  defp validate_worker_config(config) do
    changes =
      config
      |> Map.take(Map.keys(@worker_config_keys))
      |> Enum.reduce_while([], fn {key, value}, acc ->
        case validate_positive_integer(key, value) do
          {:ok, int_value} ->
            {:cont, [{@worker_config_keys[key], int_value} | acc]}

          {:error, _} = err ->
            {:halt, err}
        end
      end)

    case changes do
      {:error, _} = err ->
        err

      list when is_list(list) and list == [] ->
        {:error, InvalidConfig.exception(reason: "no valid settings provided")}

      list when is_list(list) ->
        {:ok, list}
    end
  end

  defp validate_positive_integer(_key, value) when is_integer(value) and value > 0 do
    {:ok, value}
  end

  defp validate_positive_integer(key, value) when is_integer(value) do
    {:error,
     InvalidConfig.exception(
       field: String.to_atom(key),
       reason: "must be a positive integer, got: #{value}"
     )}
  end

  defp validate_positive_integer(key, value) do
    {:error,
     InvalidConfig.exception(
       field: String.to_atom(key),
       reason: "must be a positive integer, got: #{inspect(value)}"
     )}
  end

  defp persist_worker_config(config) do
    json_config =
      config
      |> Map.take(Map.keys(@worker_config_keys))

    State.update_worker_config(json_config)
  end

  defp job_to_map(%Job{} = job) do
    %{
      id: job.id,
      type: job.type.label(),
      node: Atom.to_string(job.node),
      status: Atom.to_string(job.status),
      progress_total: job.progress.total,
      progress_completed: job.progress.completed,
      progress_description: job.progress.description,
      params: serialise_params(job.params),
      error: if(job.error, do: inspect(job.error)),
      created_at: DateTime.to_iso8601(job.created_at),
      started_at: if(job.started_at, do: DateTime.to_iso8601(job.started_at)),
      updated_at: DateTime.to_iso8601(job.updated_at),
      completed_at: if(job.completed_at, do: DateTime.to_iso8601(job.completed_at))
    }
  end

  defp serialise_params(params) when is_map(params) do
    Map.new(params, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), serialise_param_value(v)}
      {k, v} -> {to_string(k), serialise_param_value(v)}
    end)
  end

  defp serialise_param_value(v) when is_atom(v), do: Atom.to_string(v)
  defp serialise_param_value(v) when is_binary(v), do: v
  defp serialise_param_value(v) when is_number(v), do: v
  defp serialise_param_value(v), do: inspect(v)

  defp parse_tier_opt(opts, nil), do: opts

  defp parse_tier_opt(opts, tier) when is_binary(tier) do
    Keyword.put(opts, :tier, String.to_existing_atom(tier))
  rescue
    ArgumentError -> opts
  end

  defp parse_float_opt(opts, _key, nil), do: opts

  defp parse_float_opt(opts, key, value) when is_binary(value) do
    case Float.parse(value) do
      {f, ""} -> Keyword.put(opts, key, f)
      _ -> opts
    end
  end

  defp parse_float_opt(opts, key, value) when is_float(value) do
    Keyword.put(opts, key, value)
  end

  defp parse_float_opt(opts, _key, _value), do: opts

  defp parse_int_opt(opts, _key, nil), do: opts

  defp parse_int_opt(opts, key, value) when is_binary(value) do
    case Integer.parse(value) do
      {n, ""} when n > 0 -> Keyword.put(opts, key, n)
      _ -> opts
    end
  end

  defp parse_int_opt(opts, key, value) when is_integer(value) and value > 0 do
    Keyword.put(opts, key, value)
  end

  defp parse_int_opt(opts, _key, _value), do: opts

  defp parse_job_filters(filters) do
    []
    |> parse_job_status_filter(Map.get(filters, "status"))
    |> parse_job_type_filter(Map.get(filters, "type"))
  end

  defp parse_job_status_filter(opts, nil), do: opts

  defp parse_job_status_filter(opts, status) when is_binary(status) do
    Keyword.put(opts, :status, String.to_existing_atom(status))
  rescue
    ArgumentError -> opts
  end

  defp parse_job_status_filter(opts, statuses) when is_list(statuses) do
    atoms =
      statuses
      |> Enum.map(fn s ->
        try do
          String.to_existing_atom(s)
        rescue
          ArgumentError -> nil
        end
      end)
      |> Enum.reject(&is_nil/1)

    if atoms != [], do: Keyword.put(opts, :status, atoms), else: opts
  end

  defp parse_job_status_filter(opts, _), do: opts

  defp parse_job_type_filter(opts, nil), do: opts

  defp parse_job_type_filter(opts, type_label) when is_binary(type_label) do
    # Find runner module by label
    # We search known runners; extensible as new runners are added
    known_runners = [
      NeonFS.Core.Job.Runners.ClusterRebalance,
      NeonFS.Core.Job.Runners.DriveEvacuation,
      NeonFS.Core.Job.Runners.GarbageCollection,
      NeonFS.Core.Job.Runners.KeyRotation,
      NeonFS.Core.Job.Runners.Scrub
    ]

    case Enum.find(known_runners, fn mod -> mod.label() == type_label end) do
      nil -> opts
      mod -> Keyword.put(opts, :type, mod)
    end
  end

  defp parse_job_type_filter(opts, _), do: opts

  defp resolve_gc_params(%{"volume" => volume_name}) when is_binary(volume_name) do
    case VolumeRegistry.get_by_name(volume_name) do
      {:ok, volume} ->
        {:ok, %{volume_id: volume.id}}

      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}
    end
  end

  defp resolve_gc_params(_), do: {:ok, %{}}

  defp resolve_scrub_params(%{"volume" => volume_name}) when is_binary(volume_name) do
    case VolumeRegistry.get_by_name(volume_name) do
      {:ok, volume} ->
        {:ok, %{volume_id: volume.id}}

      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}
    end
  end

  defp resolve_scrub_params(_), do: {:ok, %{}}

  # Get all reachable FUSE nodes in the cluster
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

  # Get the FUSE node and verify it's reachable
  # Checks in order: ServiceRegistry, local node, connected nodes, configured fallback
  defp get_fuse_node do
    with :not_found <- discover_fuse_node_from_registry(),
         :not_available <- check_local_fuse(),
         :not_found <- discover_fuse_node() do
      check_configured_fuse_node()
    else
      :available -> {:ok, Node.self()}
      {:ok, fuse_node} -> {:ok, fuse_node}
    end
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

  # RPC wrappers for MountManager operations
  defp rpc_mount(fuse_node, volume_name, mount_point, opts) do
    case :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :mount, [volume_name, mount_point, opts]) do
      {:badrpc, reason} ->
        {:error, Unavailable.exception(message: "FUSE RPC failed: #{inspect(reason)}")}

      result ->
        result
    end
  end

  defp rpc_unmount(fuse_node, mount_id) do
    case :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :unmount, [mount_id]) do
      {:badrpc, reason} ->
        {:error, Unavailable.exception(message: "FUSE RPC failed: #{inspect(reason)}")}

      result ->
        result
    end
  end

  defp rpc_list_mounts(fuse_node) do
    case :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :list_mounts, []) do
      {:badrpc, reason} ->
        {:error, Unavailable.exception(message: "FUSE RPC failed: #{inspect(reason)}")}

      mounts when is_list(mounts) ->
        {:ok, mounts}

      result ->
        result
    end
  end

  defp rpc_get_mount(fuse_node, mount_id) do
    case :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :get_mount, [mount_id]) do
      {:badrpc, reason} ->
        {:error, Unavailable.exception(message: "FUSE RPC failed: #{inspect(reason)}")}

      result ->
        result
    end
  end

  defp rpc_get_mount_by_path(fuse_node, path) do
    case :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :get_mount_by_path, [path]) do
      {:badrpc, reason} ->
        {:error, Unavailable.exception(message: "FUSE RPC failed: #{inspect(reason)}")}

      result ->
        result
    end
  end

  # RPC wrappers for NFS ExportManager operations
  defp rpc_nfs_export(nfs_node, volume_name) do
    case :rpc.call(nfs_node, NeonFS.NFS.ExportManager, :export, [volume_name]) do
      {:badrpc, reason} ->
        {:error, Unavailable.exception(message: "NFS RPC failed: #{inspect(reason)}")}

      result ->
        result
    end
  end

  defp rpc_nfs_unexport(nfs_node, export_id) do
    case :rpc.call(nfs_node, NeonFS.NFS.ExportManager, :unexport, [export_id]) do
      {:badrpc, reason} ->
        {:error, Unavailable.exception(message: "NFS RPC failed: #{inspect(reason)}")}

      result ->
        result
    end
  end

  defp rpc_nfs_list_exports(nfs_node) do
    case :rpc.call(nfs_node, NeonFS.NFS.ExportManager, :list_exports, []) do
      {:badrpc, reason} ->
        {:error, Unavailable.exception(message: "NFS RPC failed: #{inspect(reason)}")}

      exports when is_list(exports) ->
        {:ok, exports}

      result ->
        result
    end
  end

  defp rpc_nfs_get_export(nfs_node, export_id) do
    with {:ok, exports} <- rpc_nfs_list_exports(nfs_node) do
      case Enum.find(exports, &(&1.id == export_id)) do
        nil -> {:error, :not_found}
        export -> {:ok, export}
      end
    end
  end

  defp rpc_nfs_get_export_by_volume(nfs_node, volume_name) do
    with {:ok, exports} <- rpc_nfs_list_exports(nfs_node) do
      case Enum.find(exports, &(&1.volume_name == volume_name)) do
        nil -> {:error, :not_found}
        export -> {:ok, export}
      end
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

  defp service_to_node_info(service, ra_members, leader) do
    server_id = {RaSupervisor.cluster_name(), service.node}
    is_leader = server_id == leader

    role =
      cond do
        service.type != :core -> Atom.to_string(service.type)
        is_leader -> "leader"
        server_id in ra_members -> "follower"
        true -> Atom.to_string(service.type)
      end

    %{
      node: Atom.to_string(service.node),
      type: Atom.to_string(service.type),
      role: role,
      status: Atom.to_string(service.status),
      uptime_seconds: get_remote_uptime(service.node)
    }
  end

  defp get_ra_membership do
    case :ra.members(RaSupervisor.server_id(), 1_000) do
      {:ok, members, leader} -> {members, leader}
      _ -> {[], nil}
    end
  end

  defp get_remote_uptime(node) when node == node() do
    get_uptime()
  end

  defp get_remote_uptime(node) do
    case :rpc.call(node, :erlang, :statistics, [:wall_clock], 2_000) do
      {uptime_ms, _} -> div(uptime_ms, 1000)
      _ -> 0
    end
  end

  defp volume_to_map(%Volume{} = volume) do
    %{
      id: volume.id,
      name: volume.name,
      owner: volume.owner,
      atime_mode: volume.atime_mode,
      durability: volume.durability,
      durability_display: format_durability(volume.durability),
      write_ack: volume.write_ack,
      tiering: volume.tiering,
      caching: volume.caching,
      io_weight: volume.io_weight,
      compression: volume.compression,
      verification: volume.verification,
      metadata_consistency: volume.metadata_consistency,
      encryption: encryption_to_map(volume.encryption),
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
        wrap_unmount_result(rpc_unmount(fuse_node, mount_id_or_path))

      {:error, :not_found} ->
        case rpc_get_mount_by_path(fuse_node, mount_id_or_path) do
          {:ok, mount} ->
            wrap_unmount_result(rpc_unmount(fuse_node, mount.id))

          {:error, :not_found} ->
            {:error, :mount_not_found}
        end
    end
  end

  defp do_nfs_unmount(export_id_or_volume, nfs_node) do
    # Try export_id first, then volume name
    case rpc_nfs_get_export(nfs_node, export_id_or_volume) do
      {:ok, _export} ->
        wrap_unmount_result(rpc_nfs_unexport(nfs_node, export_id_or_volume))

      {:error, :not_found} ->
        case rpc_nfs_get_export_by_volume(nfs_node, export_id_or_volume) do
          {:ok, export} ->
            wrap_unmount_result(rpc_nfs_unexport(nfs_node, export.id))

          {:error, :not_found} ->
            {:error, :export_not_found}
        end
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

  defp collect_node_nfs_exports(nfs_node) do
    case rpc_nfs_list_exports(nfs_node) do
      {:ok, node_exports} ->
        Enum.map(node_exports, &nfs_export_to_map(&1, nfs_node))

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

  defp nfs_export_to_map(export_info, nfs_node) do
    %{
      id: export_info.id,
      node: Atom.to_string(nfs_node),
      volume_name: export_info.volume_name,
      exported_at: DateTime.to_iso8601(export_info.exported_at)
    }
  end

  defp service_info_to_map(info) do
    %{
      node: Atom.to_string(info.node),
      type: Atom.to_string(info.type),
      status: Atom.to_string(info.status),
      registered_at: DateTime.to_iso8601(info.registered_at),
      metadata: info.metadata
    }
  end

  defp parse_encryption_opt(opts) do
    case Keyword.get(opts, :encryption) do
      nil ->
        {:ok, opts}

      %VolumeEncryption{} ->
        {:ok, opts}

      %{mode: mode} when is_atom(mode) ->
        enc = build_encryption_config(mode)
        {:ok, Keyword.put(opts, :encryption, enc)}

      %{"mode" => mode} when is_binary(mode) ->
        enc = build_encryption_config(String.to_existing_atom(mode))
        {:ok, Keyword.put(opts, :encryption, enc)}

      _other ->
        {:ok, opts}
    end
  rescue
    ArgumentError ->
      {:error, InvalidConfig.exception(field: :encryption, reason: "invalid encryption mode")}
  end

  defp build_encryption_config(:none), do: VolumeEncryption.new(mode: :none)

  defp build_encryption_config(:server_side) do
    VolumeEncryption.new(mode: :server_side, current_key_version: 1)
  end

  defp setup_encryption_if_needed(volume) do
    if Volume.encrypted?(volume) do
      case KeyManager.setup_volume_encryption(volume.id) do
        {:ok, _version} -> :ok
        {:error, reason} -> {:error, reason}
      end
    else
      :ok
    end
  end

  defp merge_verification_defaults(opts) do
    case Keyword.get(opts, :verification) do
      nil ->
        opts

      config when is_map(config) ->
        Keyword.put(opts, :verification, Map.merge(Volume.default_verification(), config))
    end
  end

  defp parse_durability_opt(opts) do
    case Keyword.get(opts, :durability) do
      nil ->
        {:ok, opts}

      durability when is_binary(durability) ->
        case parse_durability(durability) do
          {:ok, config} -> {:ok, Keyword.put(opts, :durability, config)}
          {:error, _} = err -> err
        end

      _map ->
        {:ok, opts}
    end
  end

  defp parse_durability("replicate:" <> rest) do
    case Integer.parse(rest) do
      {n, ""} when n >= 1 ->
        {:ok, %{type: :replicate, factor: n, min_copies: max(1, n - 1)}}

      _ ->
        {:error, durability_format_error()}
    end
  end

  defp parse_durability("erasure:" <> rest) do
    parse_erasure_parts(String.split(rest, ":"))
  end

  defp parse_durability(_), do: {:error, durability_format_error()}

  defp parse_erasure_parts([d_str, p_str]) do
    with {d, ""} <- Integer.parse(d_str),
         {p, ""} <- Integer.parse(p_str),
         true <- d >= 1 and p >= 1 do
      {:ok, %{type: :erasure, data_chunks: d, parity_chunks: p}}
    else
      _ -> {:error, durability_format_error()}
    end
  end

  defp parse_erasure_parts(_), do: {:error, durability_format_error()}

  defp durability_format_error do
    InvalidConfig.exception(
      field: :durability,
      reason: "invalid format, use 'replicate:N' or 'erasure:D:P'"
    )
  end

  defp format_durability(%{type: :replicate, factor: factor}) do
    "replicate:#{factor}"
  end

  defp format_durability(%{type: :erasure, data_chunks: d, parity_chunks: p}) do
    overhead = (d + p) / d
    overhead_str = :erlang.float_to_binary(overhead, decimals: 2)
    "erasure:#{d}+#{p} (#{overhead_str}x overhead)"
  end

  defp format_durability(_), do: "unknown"

  defp map_to_opts(map) when is_map(map) do
    map
    |> Enum.map(fn {k, v} -> {String.to_existing_atom(k), v} end)
    |> Enum.into([])
  rescue
    # If key doesn't exist as atom, use string key directly
    ArgumentError ->
      Enum.into(map, [])
  end

  defp cleanup_volume_acl(volume_id) do
    ACLManager.delete_volume_acl(volume_id)
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp create_initial_acl(volume_id, owner_uid, owner_gid) do
    acl = VolumeACL.new(volume_id: volume_id, owner_uid: owner_uid, owner_gid: owner_gid)
    ACLManager.set_volume_acl(volume_id, acl)
  rescue
    _ -> Logger.warning("Failed to create initial ACL for volume", volume_id: volume_id)
  catch
    :exit, _ -> Logger.warning("Failed to create initial ACL for volume", volume_id: volume_id)
  end

  defp audit_event_to_map(%NeonFS.Core.AuditEvent{} = event) do
    %{
      id: event.id,
      timestamp: DateTime.to_iso8601(event.timestamp),
      event_type: Atom.to_string(event.event_type),
      actor_uid: event.actor_uid,
      actor_node: Atom.to_string(event.actor_node),
      resource: event.resource,
      details: event.details,
      outcome: Atom.to_string(event.outcome)
    }
  end

  defp parse_audit_filters(filters) do
    []
    |> maybe_add_filter(:event_type, parse_event_type(Map.get(filters, "type")))
    |> maybe_add_filter(:actor_uid, Map.get(filters, "actor_uid"))
    |> maybe_add_filter(:resource, Map.get(filters, "resource"))
    |> maybe_add_filter(:since, parse_datetime(Map.get(filters, "since")))
    |> maybe_add_filter(:until, parse_datetime(Map.get(filters, "until")))
    |> maybe_add_filter(:limit, Map.get(filters, "limit"))
  end

  defp maybe_add_filter(opts, _key, nil), do: opts
  defp maybe_add_filter(opts, key, value), do: Keyword.put(opts, key, value)

  defp parse_event_type(nil), do: nil

  defp parse_event_type(type) when is_binary(type) do
    String.to_existing_atom(type)
  rescue
    ArgumentError -> nil
  end

  defp parse_datetime(nil), do: nil

  defp parse_datetime(dt_string) when is_binary(dt_string) do
    case DateTime.from_iso8601(dt_string) do
      {:ok, dt, _offset} -> dt
      _ -> nil
    end
  end

  defp collect_remote_audit_events(query_opts) do
    for node <- ServiceRegistry.connected_nodes_by_type(:core), reduce: [] do
      acc ->
        case safe_remote_audit_query(node, query_opts) do
          events when is_list(events) -> events ++ acc
          _ -> acc
        end
    end
  end

  defp safe_remote_audit_query(node, query_opts) do
    :erpc.call(node, AuditLog, :query, [query_opts], 5_000)
  catch
    :exit, _ -> []
  end

  defp maybe_limit_events(events, query_opts) do
    limit = Keyword.get(query_opts, :limit, 100)
    Enum.take(events, limit)
  end

  defp worker_status_map(node, status) do
    %{
      node: Atom.to_string(node),
      max_concurrent: status.max_concurrent,
      max_per_minute: status.max_per_minute,
      drive_concurrency: status.drive_concurrency,
      queued: status.queued,
      running: status.running,
      completed_total: status.completed_total,
      by_priority: %{
        high: status.by_priority[:high] || 0,
        normal: status.by_priority[:normal] || 0,
        low: status.by_priority[:low] || 0
      }
    }
  end

  defp collect_remote_worker_statuses do
    for node <- ServiceRegistry.connected_nodes_by_type(:core), reduce: [] do
      acc ->
        case safe_remote_worker_status(node) do
          {:ok, status} -> [worker_status_map(node, status) | acc]
          _ -> acc
        end
    end
  end

  defp safe_remote_worker_status(node) do
    case :erpc.call(node, BackgroundWorker, :status, [], 5_000) do
      status when is_map(status) -> {:ok, status}
      other -> {:error, other}
    end
  catch
    :exit, _ -> {:error, :unreachable}
  end

  defp encryption_to_map(%VolumeEncryption{} = enc) do
    base = %{
      mode: Atom.to_string(enc.mode),
      current_key_version: enc.current_key_version
    }

    case enc.rotation do
      nil ->
        Map.put(base, :rotation, nil)

      rotation ->
        Map.put(base, :rotation, %{
          from_version: rotation.from_version,
          to_version: rotation.to_version,
          started_at: DateTime.to_iso8601(rotation.started_at),
          progress: rotation.progress
        })
    end
  end

  defp volume_acl_to_map(%VolumeACL{} = acl) do
    %{
      volume_id: acl.volume_id,
      owner_uid: acl.owner_uid,
      owner_gid: acl.owner_gid,
      entries:
        Enum.map(acl.entries, fn entry ->
          {type, id} = entry.principal

          %{
            principal: "#{type}:#{id}",
            permissions: entry.permissions |> MapSet.to_list() |> Enum.map(&Atom.to_string/1)
          }
        end)
    }
  end

  defp parse_principal("uid:" <> uid_str) do
    case Integer.parse(uid_str) do
      {uid, ""} when uid >= 0 -> {:ok, {:uid, uid}}
      _ -> {:error, Invalid.exception(message: "Invalid UID: #{uid_str}")}
    end
  end

  defp parse_principal("gid:" <> gid_str) do
    case Integer.parse(gid_str) do
      {gid, ""} when gid >= 0 -> {:ok, {:gid, gid}}
      _ -> {:error, Invalid.exception(message: "Invalid GID: #{gid_str}")}
    end
  end

  defp parse_principal(other) do
    {:error, Invalid.exception(message: "Invalid principal format: #{other}. Use uid:N or gid:N")}
  end

  defp parse_permissions(perm_strings) when is_list(perm_strings) do
    perms = Enum.map(perm_strings, &String.to_existing_atom/1)
    valid = [:read, :write, :admin]
    invalid = Enum.reject(perms, &(&1 in valid))

    if invalid == [] do
      {:ok, perms}
    else
      {:error, Invalid.exception(message: "Invalid permissions: #{inspect(invalid)}")}
    end
  rescue
    ArgumentError ->
      {:error, Invalid.exception(message: "Invalid permission name. Valid: read, write, admin")}
  end

  # Find a cert entry matching the given node name.
  # The node_name in cert metadata is the full X.500 subject (e.g. "/O=NeonFS/CN=node@host").
  # Match against the CN portion or the full subject.
  defp find_cert_by_node(certs, name) do
    case Enum.find(certs, fn cert ->
           cert.node_name == name or
             String.ends_with?(cert.node_name, "/CN=#{name}") or
             cert.hostname == name
         end) do
      nil ->
        {:error, NotFound.exception(message: "No certificate found for node '#{name}'")}

      cert ->
        {:ok, cert}
    end
  end

  defp map_ca_error({:ok, _} = ok), do: ok

  defp map_ca_error({:error, _}),
    do: {:error, Unavailable.exception(message: "Certificate authority not initialised")}

  @immutable_update_fields ~w(durability encryption name id)

  defp reject_immutable_updates(config) do
    found =
      config
      |> Map.keys()
      |> Enum.filter(&(&1 in @immutable_update_fields))

    if found == [] do
      :ok
    else
      {:error,
       InvalidConfig.exception(
         field: :immutable,
         reason: "cannot update immutable fields: #{Enum.join(found, ", ")}"
       )}
    end
  end

  @tiering_fields ~w(initial_tier promotion_threshold demotion_delay)
  @caching_fields ~w(transformed_chunks reconstructed_stripes remote_chunks)
  @verification_fields ~w(on_read sampling_rate scrub_interval)
  @metadata_consistency_fields ~w(metadata_replicas read_quorum write_quorum)

  defp build_update_opts(config, volume) do
    []
    |> maybe_put_simple(config, "atime_mode", :atime_mode, &coerce_atom/1)
    |> maybe_put_simple(config, "io_weight", :io_weight, &coerce_integer/1)
    |> maybe_put_simple(config, "write_ack", :write_ack, &coerce_atom/1)
    |> maybe_put_simple(config, "owner", :owner, &Function.identity/1)
    |> maybe_put_nested(config, @tiering_fields, :tiering, volume.tiering, &coerce_tiering/2)
    |> maybe_put_nested(config, @caching_fields, :caching, volume.caching, &coerce_caching/2)
    |> maybe_put_nested(
      config,
      @verification_fields,
      :verification,
      volume.verification,
      &coerce_verification/2
    )
    |> maybe_put_nested(
      config,
      @metadata_consistency_fields,
      :metadata_consistency,
      volume.metadata_consistency,
      &coerce_metadata_consistency/2
    )
    |> maybe_put_compression(config, volume)
  end

  defp maybe_put_simple(opts, config, string_key, opt_key, coerce_fn) do
    case Map.fetch(config, string_key) do
      {:ok, value} -> Keyword.put(opts, opt_key, coerce_fn.(value))
      :error -> opts
    end
  end

  defp maybe_put_nested(opts, config, field_names, opt_key, current, coerce_fn) do
    sub_config = Map.take(config, field_names)

    if map_size(sub_config) == 0 do
      opts
    else
      merged =
        Enum.reduce(sub_config, current || %{}, fn {k, v}, acc ->
          coerce_fn.(acc, {k, v})
        end)

      Keyword.put(opts, opt_key, merged)
    end
  end

  defp maybe_put_compression(opts, config, volume) do
    case Map.fetch(config, "compression") do
      {:ok, comp} when is_map(comp) ->
        coerced = coerce_compression_map(comp)
        merged = Map.merge(volume.compression, coerced)
        Keyword.put(opts, :compression, merged)

      {:ok, comp} ->
        Keyword.put(opts, :compression, %{algorithm: coerce_atom(comp)})

      :error ->
        opts
    end
  end

  defp coerce_compression_map(map) do
    map
    |> atomise_map()
    |> Map.new(fn
      {:algorithm, v} -> {:algorithm, coerce_atom(v)}
      {:level, v} -> {:level, coerce_integer(v)}
      {:min_size, v} -> {:min_size, coerce_integer(v)}
      other -> other
    end)
  end

  defp coerce_tiering(acc, {"initial_tier", v}), do: Map.put(acc, :initial_tier, coerce_atom(v))

  defp coerce_tiering(acc, {"promotion_threshold", v}),
    do: Map.put(acc, :promotion_threshold, coerce_integer(v))

  defp coerce_tiering(acc, {"demotion_delay", v}),
    do: Map.put(acc, :demotion_delay, coerce_integer(v))

  defp coerce_caching(acc, {"transformed_chunks", v}),
    do: Map.put(acc, :transformed_chunks, coerce_boolean(v))

  defp coerce_caching(acc, {"reconstructed_stripes", v}),
    do: Map.put(acc, :reconstructed_stripes, coerce_boolean(v))

  defp coerce_caching(acc, {"remote_chunks", v}),
    do: Map.put(acc, :remote_chunks, coerce_boolean(v))

  defp coerce_verification(acc, {"on_read", v}), do: Map.put(acc, :on_read, coerce_atom(v))

  defp coerce_verification(acc, {"sampling_rate", v}),
    do: Map.put(acc, :sampling_rate, coerce_float(v))

  defp coerce_verification(acc, {"scrub_interval", v}),
    do: Map.put(acc, :scrub_interval, coerce_integer(v))

  defp coerce_metadata_consistency(acc, {"metadata_replicas", v}),
    do: Map.put(acc, :replicas, coerce_integer(v))

  defp coerce_metadata_consistency(acc, {"read_quorum", v}),
    do: Map.put(acc, :read_quorum, coerce_integer(v))

  defp coerce_metadata_consistency(acc, {"write_quorum", v}),
    do: Map.put(acc, :write_quorum, coerce_integer(v))

  defp coerce_atom(v) when is_atom(v), do: v
  defp coerce_atom(v) when is_binary(v), do: String.to_existing_atom(v)

  defp coerce_integer(v) when is_integer(v), do: v
  defp coerce_integer(v) when is_binary(v), do: String.to_integer(v)

  defp coerce_float(v) when is_float(v), do: v
  defp coerce_float(v) when is_integer(v), do: v / 1
  defp coerce_float(v) when is_binary(v), do: String.to_float(v)

  defp coerce_boolean(v) when is_boolean(v), do: v
  defp coerce_boolean("true"), do: true
  defp coerce_boolean("false"), do: false

  defp atomise_map(map) when is_map(map) do
    Map.new(map, fn {k, v} -> {coerce_atom(k), v} end)
  end

  defp rebuild_quorum_ring_on_all_nodes do
    NeonFS.Core.Supervisor.rebuild_quorum_ring()

    for node <- ServiceRegistry.connected_nodes_by_type(:core) do
      try do
        :erpc.call(node, NeonFS.Core.Supervisor, :rebuild_quorum_ring, [], 5_000)
      catch
        _, _ -> :ok
      end
    end

    :ok
  end
end
