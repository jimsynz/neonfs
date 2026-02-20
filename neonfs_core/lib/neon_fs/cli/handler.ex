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

  alias NeonFS.Cluster.{Init, Invite, Join, State}

  alias NeonFS.Core.{
    ACLManager,
    AuditLog,
    Authorise,
    CertificateAuthority,
    DriveManager,
    JobTracker,
    KeyManager,
    KeyRotation,
    ServiceRegistry,
    Volume,
    VolumeACL,
    VolumeEncryption,
    VolumeRegistry
  }

  alias NeonFS.Core.Job

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
  Returns cluster CA information.

  ## Returns
  - `{:ok, map}` - CA info with subject, algorithm, validity dates, serial counter
  - `{:error, :ca_not_initialized}` - CA hasn't been created yet
  """
  @spec handle_ca_info() :: {:ok, map()} | {:error, term()}
  def handle_ca_info do
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
        {:error, :ca_not_initialized}
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
        {:error, :ca_not_initialized}
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
    with {:ok, certs} <- map_ca_error(CertificateAuthority.list_issued()),
         {:ok, cert} <- find_cert_by_node(certs, node_name),
         :ok <- CertificateAuthority.revoke_certificate(cert.serial, :cessation_of_operation) do
      {:ok, %{serial: cert.serial, node_name: cert.node_name, status: "revoked"}}
    end
  end

  @doc """
  Rotates the cluster CA.

  CA rotation is a rare, disruptive operation that reissues all node
  certificates. It requires a dual-CA transition period and rolling
  reissuance across the cluster.

  Not yet implemented — returns `{:error, :not_implemented}`.
  """
  @spec handle_ca_rotate() :: {:error, :not_implemented}
  def handle_ca_rotate do
    {:error, :not_implemented}
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
    case Invite.create_invite(expires_in) do
      {:ok, token} ->
        {:ok, %{"token" => token}}

      {:error, reason} ->
        {:error, reason}
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
  def join_cluster(token, via_node_str, type_str \\ "core")
      when is_binary(token) and is_binary(via_node_str) and is_binary(type_str) do
    via_node = String.to_atom(via_node_str)
    type = String.to_existing_atom(type_str)

    case Join.join_cluster(token, via_node, type) do
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
            via_node: via_node_str
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
        {:error, reason}
    end
  end

  @doc """
  Lists all registered services in the cluster.

  ## Returns
  - `{:ok, [map]}` - List of service info maps
  """
  @spec list_services() :: {:ok, [map()]}
  def list_services do
    services =
      ServiceRegistry.list()
      |> Enum.map(&service_info_to_map/1)

    {:ok, services}
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
    - `:tiering` - Tiering config map (`:initial_tier`, `:promotion_threshold`, `:demotion_delay`)
    - `:caching` - Caching config map (`:transformed_chunks`, `:reconstructed_stripes`, `:remote_chunks`, `:max_memory`)
    - `:io_weight` - I/O scheduling weight (positive integer)
    - `:compression` - Compression config map
    - `:verification` - Verification config map

  ## Returns
  - `{:ok, map}` - Created volume as map
  - `{:error, reason}` - Error tuple
  """
  @spec create_volume(String.t(), map()) :: {:ok, map()} | {:error, term()}
  def create_volume(name, config) when is_binary(name) and is_map(config) do
    opts = map_to_opts(config)
    owner_uid = Keyword.get(opts, :owner_uid, 0)
    owner_gid = Keyword.get(opts, :owner_gid, 0)

    with {:ok, parsed_opts} <- parse_durability_opt(opts),
         {:ok, final_opts} <- parse_encryption_opt(parsed_opts),
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
    with {:ok, volume} <- VolumeRegistry.get_by_name(volume_name) do
      KeyRotation.start_rotation(volume.id)
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
    with {:ok, volume} <- VolumeRegistry.get_by_name(volume_name) do
      KeyRotation.rotation_status(volume.id)
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
    with {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         :ok <- ACLManager.set_file_acl(volume.id, path, acl_entries) do
      {:ok, %{}}
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
    with {:ok, volume} <- VolumeRegistry.get_by_name(volume_name) do
      ACLManager.get_file_acl(volume.id, path)
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
    with {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         :ok <- ACLManager.set_default_acl(volume.id, path, default_acl) do
      {:ok, %{}}
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
    query_opts =
      filters
      |> parse_audit_filters()

    events =
      AuditLog.query(query_opts)
      |> Enum.map(&audit_event_to_map/1)

    {:ok, events}
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
    with {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
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
    with {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, principal} <- parse_principal(principal_str),
         :ok <- ACLManager.revoke(volume.id, principal) do
      AuditLog.log_event(
        event_type: :acl_revoke,
        actor_uid: 0,
        resource: volume.id,
        details: %{principal: principal_str}
      )

      {:ok, %{}}
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
    with {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, acl} <- ACLManager.get_volume_acl(volume.id) do
      {:ok, volume_acl_to_map(acl)}
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
    case DriveManager.add_drive(config) do
      {:ok, drive_map} -> {:ok, drive_map}
      {:error, reason} -> {:error, reason}
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
    case DriveManager.remove_drive(drive_id, force: force) do
      :ok -> {:ok, %{}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Lists all drives on the local node.

  ## Returns
  - `{:ok, [map]}` - List of drive info maps
  """
  @spec handle_list_drives() :: {:ok, [map()]}
  def handle_list_drives do
    {:ok, DriveManager.list_drives()}
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
    case JobTracker.get(job_id) do
      {:ok, job} -> {:ok, job_to_map(job)}
      {:error, :not_found} -> {:error, "Job not found: #{job_id}"}
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
    case JobTracker.cancel(job_id) do
      :ok -> {:ok, %{}}
      {:error, :not_found} -> {:error, "Job not found: #{job_id}"}
      {:error, :already_terminal} -> {:error, "Job already finished: #{job_id}"}
    end
  end

  # Private helper functions

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
    known_runners = [NeonFS.Core.Job.Runners.KeyRotation]

    case Enum.find(known_runners, fn mod -> mod.label() == type_label end) do
      nil -> opts
      mod -> Keyword.put(opts, :type, mod)
    end
  end

  defp parse_job_type_filter(opts, _), do: opts

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

  # Check if FUSE MountManager is available on the local node
  defp check_local_fuse do
    case Process.whereis(NeonFS.FUSE.MountManager) do
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

  defp fuse_node?(node) do
    node |> Atom.to_string() |> String.starts_with?("neonfs_fuse@")
  end

  # Fall back to configured node (for backwards compatibility)
  defp check_configured_fuse_node do
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
      durability_display: format_durability(volume.durability),
      write_ack: volume.write_ack,
      tiering: volume.tiering,
      caching: volume.caching,
      io_weight: volume.io_weight,
      compression: volume.compression,
      verification: volume.verification,
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
    ArgumentError -> {:error, "Invalid encryption mode"}
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
    "Invalid durability format. Use 'replicate:N' or 'erasure:D:P'"
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
    _ -> Logger.warning("Failed to create initial ACL for volume #{volume_id}")
  catch
    :exit, _ -> Logger.warning("Failed to create initial ACL for volume #{volume_id}")
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
      _ -> {:error, "Invalid UID: #{uid_str}"}
    end
  end

  defp parse_principal("gid:" <> gid_str) do
    case Integer.parse(gid_str) do
      {gid, ""} when gid >= 0 -> {:ok, {:gid, gid}}
      _ -> {:error, "Invalid GID: #{gid_str}"}
    end
  end

  defp parse_principal(other),
    do: {:error, "Invalid principal format: #{other}. Use uid:N or gid:N"}

  defp parse_permissions(perm_strings) when is_list(perm_strings) do
    perms = Enum.map(perm_strings, &String.to_existing_atom/1)
    valid = [:read, :write, :admin]
    invalid = Enum.reject(perms, &(&1 in valid))

    if invalid == [] do
      {:ok, perms}
    else
      {:error, "Invalid permissions: #{inspect(invalid)}"}
    end
  rescue
    ArgumentError -> {:error, "Invalid permission name. Valid: read, write, admin"}
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
      nil -> {:error, :node_not_found}
      cert -> {:ok, cert}
    end
  end

  defp map_ca_error({:ok, _} = ok), do: ok
  defp map_ca_error({:error, _}), do: {:error, :ca_not_initialized}

  defp rebuild_quorum_ring_on_all_nodes do
    NeonFS.Core.Supervisor.rebuild_quorum_ring()

    for node <- Node.list() do
      try do
        :erpc.call(node, NeonFS.Core.Supervisor, :rebuild_quorum_ring, [], 5_000)
      catch
        _, _ -> :ok
      end
    end

    :ok
  end
end
