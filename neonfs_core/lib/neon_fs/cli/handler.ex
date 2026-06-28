defmodule NeonFS.CLI.Handler do
  @moduledoc """
  RPC handler for CLI commands. Called via Erlang distribution.

  This module provides the daemon-side interface for CLI operations, converting
  internal data structures to serializable maps that can be sent across the
  Erlang distribution protocol.

  All functions return `{:ok, data}` or `{:error, reason}` tuples where data
  consists only of serializable terms (maps, lists, atoms, strings, numbers).

  This module is a pure dispatch facade: every entry point `defdelegate`s to a
  per-command-group handler under `NeonFS.CLI.Handler.*` (the split tracked by
  #1203). Shared helpers live in `NeonFS.CLI.Handler.Common`. Keeping the RPC
  surface here means the CLI wire contract is one stable module regardless of
  how the implementation is partitioned.
  """

  alias NeonFS.CLI.Handler.ACL, as: ACLHandler
  alias NeonFS.CLI.Handler.CA, as: CAHandler
  alias NeonFS.CLI.Handler.Cluster, as: ClusterHandler
  alias NeonFS.CLI.Handler.ClusterRecovery, as: ClusterRecoveryHandler
  alias NeonFS.CLI.Handler.Credential, as: CredentialHandler
  alias NeonFS.CLI.Handler.DR, as: DRHandler
  alias NeonFS.CLI.Handler.Drives, as: DrivesHandler
  alias NeonFS.CLI.Handler.Escalation, as: EscalationHandler
  alias NeonFS.CLI.Handler.Jobs, as: JobsHandler
  alias NeonFS.CLI.Handler.Maintenance, as: MaintenanceHandler
  alias NeonFS.CLI.Handler.MountRouting, as: MountRoutingHandler
  alias NeonFS.CLI.Handler.Node, as: NodeHandler
  alias NeonFS.CLI.Handler.S3, as: S3Handler
  alias NeonFS.CLI.Handler.ScrubRepair, as: ScrubRepairHandler
  alias NeonFS.CLI.Handler.Services, as: ServicesHandler
  alias NeonFS.CLI.Handler.Snapshots, as: SnapshotsHandler
  alias NeonFS.CLI.Handler.VolumeLifecycle, as: VolumeLifecycleHandler
  alias NeonFS.CLI.Handler.Volumes, as: VolumesHandler

  @doc """
  Returns cluster status information.

  ## Returns
  - `{:ok, map}` - Status map with cluster information
  """
  @spec cluster_status() :: {:ok, map()}
  defdelegate cluster_status(), to: ClusterHandler

  @doc """
  Initializes a new cluster with the given name.

  ## Parameters
  - `cluster_name` - Name for the new cluster (string)
  - `drive_config` (optional) - Map describing the first drive to register
    as part of bootstrap. Shape: `%{"path" => path, "tier" => "hot" |
    "warm" | "cold"}`. Without it the bootstrap falls back to drives
    registered via the `:neonfs_core, :drives` application environment;
    a freshly-installed daemon ships with none, so the CLI should always
    supply a drive.
  - `opts` (optional) - Map of bootstrap options:
    - `"system_replicas"` (positive integer, default `1`) - replication
      factor to seed the `_system` volume with. Use this on a cluster
      you intend to scale up so the system volume isn't stuck at
      `replicate:1` after the first node-join ratchet.

  ## Returns
  - `{:ok, map}` - Success map with cluster_id
  - `{:error, reason}` - Error tuple
  """
  @spec cluster_init(String.t(), map() | nil, map()) :: {:ok, map()} | {:error, term()}
  defdelegate cluster_init(cluster_name, drive_config \\ nil, opts \\ %{}), to: ClusterHandler

  @doc """
  Returns cluster CA information.

  ## Returns
  - `{:ok, map}` - CA info with subject, algorithm, validity dates, serial counter
  - `{:error, :ca_not_initialized}` - CA hasn't been created yet
  """
  @spec handle_ca_info() :: {:ok, map()} | {:error, term()}
  defdelegate handle_ca_info(), to: CAHandler

  @doc """
  Lists all issued node certificates with their status.

  ## Returns
  - `{:ok, [map]}` - List of certificate info maps
  - `{:error, :ca_not_initialized}` - CA hasn't been created yet
  """
  @spec handle_ca_list() :: {:ok, [map()]} | {:error, term()}
  defdelegate handle_ca_list(), to: CAHandler

  @doc """
  Permanently decommissions a node from the cluster.

  Composes the three decommission steps operators would otherwise run
  separately: refusing under unsafe conditions, revoking the node's
  certificate, and removing it from the Ra quorum membership.

  Refuses if the target node is the current Ra leader (the leader must
  step down first). Refuses if the target node still owns drives,
  unless `opts["force"]` is truthy — force-removing a node with
  resident chunks risks losing any chunk whose only replica was on
  that node.

  The Ra membership removal is a consensus operation against the
  current leader; on success the departed node can no longer rejoin
  without a fresh invite. Certificate revocation is best-effort — the
  Ra removal is the authoritative step.

  ## Parameters
  - `node_name` - Target node name, matching the Erlang node atom string
    (e.g. `"neonfs_core@host1"`) or the CN portion (`"host1"`).
  - `opts` - Map of options. `"force"` (boolean, default false) skips
    the drive-presence check.

  ## Returns
  - `{:ok, map}` - Result map with `node`, `status`, `remaining_members`,
    and `certificate_revoked` (boolean).
  - `{:error, reason}` - Error tuple if any safety check fails or the
    Ra operation does not complete.
  """
  @spec handle_remove_node(String.t(), map()) :: {:ok, map()} | {:error, Exception.t()}
  defdelegate handle_remove_node(node_name, opts \\ %{}), to: ClusterRecoveryHandler

  @doc """
  Begins graceful decommission: marks a node `:draining` (stopping new
  placement/routing work) and evacuates its drives (#1325). The operator
  runs `handle_remove_node/2` once the drives are empty.
  """
  @spec handle_drain_node(String.t(), map()) :: {:ok, map()} | {:error, Exception.t()}
  defdelegate handle_drain_node(node_name, opts \\ %{}), to: ClusterRecoveryHandler

  @doc """
  Reverses a drain — marks the node `:active` again (#1325) — to abort a
  decommission before the node is removed.
  """
  @spec handle_undrain_node(String.t()) :: {:ok, map()} | {:error, Exception.t()}
  defdelegate handle_undrain_node(node_name), to: ClusterRecoveryHandler

  @doc """
  Cordons a node for planned maintenance (#1376): marks it `:maintenance`
  (stopping new placement/routing work) without evacuating drives or
  changing Ra membership.
  """
  @spec handle_cordon_node(String.t()) :: {:ok, map()} | {:error, Exception.t()}
  defdelegate handle_cordon_node(node_name), to: ClusterRecoveryHandler

  @doc """
  Reverses a cordon — marks the node `:active` again (#1376).
  """
  @spec handle_uncordon_node(String.t()) :: {:ok, map()} | {:error, Exception.t()}
  defdelegate handle_uncordon_node(node_name), to: ClusterRecoveryHandler

  @doc """
  Runs the safety-gate pipeline for `neonfs cluster force-reset` and
  records the operator-acknowledged data-loss intent in the audit log.

  This is the first slice of the force-reset command (tracking issue
  #458). The Ra minority-recovery mutation itself is deferred to #473;
  when every safety gate passes this function writes a durable audit
  entry and then returns an "Ra mutation not yet implemented" error.
  Landing the gates and the audit entry on their own lets operators
  exercise the CLI and discover misuse *before* the dangerous mutation
  is available, and gives post-mortems a record of every accepted
  attempt.

  Safety gates (in order — fail fast, each returns a structured
  `{:error, _}`):

  1. `require_cluster/0`.
  2. `--yes-i-accept-data-loss` must be truthy.
  3. `keep` must be non-empty, and every name must resolve to a node
     currently in the Ra membership (`:ra.members/2`).
  4. Every `keep` node must be reachable and its `NeonFS.Client.HealthCheck`
     report must be `:healthy`.
  5. `keep` must be a **minority** of the current Ra membership — if it
     is a majority (or exactly half) Ra will elect normally and
     force-reset is not appropriate.
  6. Every departed member (current members minus `keep`) must currently
     fail `:net_adm.ping/1` and must have a `last_seen` timestamp in
     cluster state older than `min_unreachable_seconds`. Missing
     last-seen data is treated as "unknown duration" and refused — we
     cannot prove the member is gone for good.

  Gates that pass write nothing. Gates that fail write nothing. Only
  an all-gates-pass run writes the audit entry (so the log is a clean
  record of accepted intents, not a noise log of typos).

  ## Parameters

  - `opts` - String-keyed map:
    - `"keep"` - list of surviving node name strings (required, non-empty).
    - `"min_unreachable_seconds"` - grace window (default `1800`).
    - `"yes_i_accept_data_loss"` - must be `true`.

  ## Returns

  - `{:error, %Unavailable{...}}` on every path. The specific message
    identifies the gate that failed, or — on all-gates-pass — says the
    Ra mutation is not yet implemented and points at the follow-up
    issue.
  """
  @spec handle_force_reset(map()) :: {:ok, map()} | {:error, Exception.t()}
  defdelegate handle_force_reset(opts), to: ClusterRecoveryHandler

  @doc """
  Disaster-recovery reconstruction: walks every configured drive's
  on-disk root segments and rebuilds the bootstrap-layer Ra state.

  Use this when Ra logs are unrecoverable but the underlying volume
  data is intact. Drive identity files (#778) and root segment
  chunks (#780) are the source of truth; this handler discovers
  them via `Reconstruction.OnDisk` (#844) and submits the Ra
  commands `Reconstruction.reconstruct/2` (#841) emits.

  ## Opts (map keys)

  - `"yes"` — must be `true`. Refuses without explicit confirmation.
  - `"overwrite_ra_state"` — allow when bootstrap-layer
    `volume_roots` is non-empty. Without this, refuses if the
    cluster already has registered volumes (so a misfire on a
    healthy cluster is bounded).
  - `"dry_run"` — return the discovered drives + commands but skip
    submission. Doesn't require `"yes"`.

  ## Returns

  - `{:ok, %{drives:, volumes:, commands:, commands_submitted:,
    commands_failed:, warnings:}}`.
  - `{:error, exception}` when refused or when a hard failure
    aborts the run.
  """
  @spec handle_cluster_reconstruct_from_disk(map()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_cluster_reconstruct_from_disk(opts), to: ClusterRecoveryHandler

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
  defdelegate handle_ca_revoke(node_name), to: CAHandler

  @doc """
  Rotates the cluster CA.

  CA rotation is a rare, disruptive operation that reissues all node
  certificates. It requires a dual-CA transition period and rolling
  reissuance across the cluster.
  """
  @spec handle_ca_rotate(map()) :: {:ok, map()} | {:error, Exception.t()}
  defdelegate handle_ca_rotate(opts \\ %{}), to: CAHandler

  @doc """
  Creates an invite token for joining nodes.

  ## Parameters
  - `expires_in` - Duration in seconds the token is valid for

  ## Returns
  - `{:ok, %{"token" => string}}` - Success map with invite token
  - `{:error, reason}` - Error tuple
  """
  @spec create_invite(pos_integer()) :: {:ok, map()} | {:error, term()}
  defdelegate create_invite(expires_in), to: ClusterHandler

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
  defdelegate join_cluster(token, via_address, type_str \\ "core"), to: ClusterHandler

  @doc """
  Lists all registered services in the cluster.

  ## Returns
  - `{:ok, [map]}` - List of service info maps
  """
  @spec list_services() :: {:ok, [map()]}
  defdelegate list_services(), to: ServicesHandler

  @doc """
  Lists volumes in the cluster.

  ## Parameters
  - `filters` - Optional filter map:
    - `"all"` - When `true`, includes system volumes (default: excluded)

  ## Returns
  - `{:ok, [map]}` - List of volume maps
  """
  @spec list_volumes(map()) :: {:ok, [map()]}
  defdelegate list_volumes(filters \\ %{}), to: VolumesHandler

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
  defdelegate create_volume(name, config), to: VolumesHandler

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
  defdelegate update_volume(name, config), to: VolumesHandler

  @doc """
  Deletes a volume by name.

  ## Parameters
  - `name` - Volume name (string)

  ## Returns
  - `{:ok, %{}}` - Success with empty map
  - `{:error, reason}` - Error tuple
  """
  @spec delete_volume(String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate delete_volume(name), to: VolumesHandler

  @spec delete_volume(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate delete_volume(name, opts), to: VolumesHandler

  @doc """
  Gets volume details by name.

  ## Parameters
  - `name` - Volume name (string)

  ## Returns
  - `{:ok, map}` - Volume details as map
  - `{:error, reason}` - Error tuple
  """
  @spec get_volume(String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate get_volume(name), to: VolumesHandler

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
  defdelegate mount(volume_name, mount_point, options), to: MountRoutingHandler

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
  defdelegate unmount(mount_id_or_path), to: MountRoutingHandler

  @doc """
  Lists all active mounts across the cluster.

  Queries all discovered FUSE nodes and aggregates their mounts.

  ## Returns
  - `{:ok, [map]}` - List of mount info maps with node field
  """
  @spec list_mounts() :: {:ok, [map()]}
  defdelegate list_mounts(), to: MountRoutingHandler

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
  defdelegate nfs_export(volume_name, allowed_ips \\ [], root_squash \\ true),
    to: MountRoutingHandler

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
  defdelegate nfs_unexport(volume_name), to: MountRoutingHandler

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
  defdelegate handle_nfs_mount_request(volume_name), to: MountRoutingHandler

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
  defdelegate nfs_list_exports(), to: MountRoutingHandler

  @doc """
  Starts key rotation for a volume.

  ## Parameters
  - `volume_name` - Volume name (string)

  ## Returns
  - `{:ok, map}` - Rotation info with from_version, to_version, total_chunks
  - `{:error, reason}` - Error tuple
  """
  @spec rotate_volume_key(String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate rotate_volume_key(volume_name), to: VolumesHandler

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
  defdelegate rotation_status(volume_name), to: VolumesHandler

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
  defdelegate handle_set_file_acl(volume_name, path, acl_entries), to: ACLHandler

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
  defdelegate handle_get_file_acl(volume_name, path), to: ACLHandler

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
  defdelegate handle_set_default_acl(volume_name, path, default_acl), to: ACLHandler

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
  defdelegate handle_audit_list(filters \\ %{}), to: ACLHandler

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
  defdelegate handle_acl_grant(volume_name, principal_str, permissions), to: ACLHandler

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
  defdelegate handle_acl_revoke(volume_name, principal_str), to: ACLHandler

  @doc """
  Shows the ACL for a volume.

  ## Parameters
  - `volume_name` - Volume name (string)

  ## Returns
  - `{:ok, map}` - ACL info with owner_uid, owner_gid, entries
  - `{:error, reason}` - Error tuple
  """
  @spec handle_acl_show(String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_acl_show(volume_name), to: ACLHandler

  @doc """
  Adds a drive to the local node.

  ## Parameters
  - `config` - Drive config map with keys: "path", "tier", "capacity", optional "id"

  ## Returns
  - `{:ok, map}` - Drive info as map
  - `{:error, reason}` - Error tuple
  """
  @spec handle_add_drive(map()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_add_drive(config), to: DrivesHandler

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
  defdelegate handle_remove_drive(drive_id, force \\ false), to: DrivesHandler

  @doc """
  Lists drives across the cluster.

  ## Parameters
  - `filters` - Optional filter map:
    - `"node"` - Node name string to filter by (e.g. "neonfs_core@host1")

  ## Returns
  - `{:ok, [map]}` - List of drive info maps
  """
  @spec handle_list_drives(map()) :: {:ok, [map()]}
  defdelegate handle_list_drives(filters \\ %{}), to: DrivesHandler

  @doc """
  Starts evacuation of all data from a drive.

  Evacuation always prefers a same-tier target drive and falls back to
  any tier when none is available, so the call has no tier-related
  options.

  ## Parameters
  - `node_name` - Node name string (e.g. "neonfs-core@host")
  - `drive_id` - Drive identifier

  ## Returns
  - `{:ok, map}` - Job info map
  - `{:error, reason}` - Error tuple
  """
  @spec handle_evacuate_drive(String.t(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_evacuate_drive(node_name, drive_id, opts \\ %{}), to: DrivesHandler

  @doc """
  Returns the evacuation status for a drive.

  ## Parameters
  - `drive_id` - Drive identifier

  ## Returns
  - `{:ok, map}` - Status map
  - `{:error, reason}` - Error tuple
  """
  @spec handle_evacuation_status(String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_evacuation_status(drive_id), to: DrivesHandler

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
  defdelegate handle_rebalance(opts \\ %{}), to: DrivesHandler

  @doc """
  Returns the status of an active or recent rebalance operation.

  ## Returns
  - `{:ok, map}` - Status map with progress info
  - `{:error, :no_rebalance}` - No rebalance in progress
  """
  @spec handle_rebalance_status() :: {:ok, map()} | {:error, term()}
  defdelegate handle_rebalance_status(), to: DrivesHandler

  @doc """
  Returns cluster-wide storage capacity information.

  ## Returns
  - `{:ok, map}` - Capacity info with per-drive breakdown
  """
  @spec handle_storage_stats() :: {:ok, map()}
  defdelegate handle_storage_stats(), to: DrivesHandler

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
  defdelegate handle_gc_collect(opts \\ %{}), to: MaintenanceHandler

  @doc """
  Returns recent garbage collection jobs across the cluster.

  ## Returns
  - `{:ok, [map]}` - List of GC job maps, most recent first
  """
  @spec handle_gc_status() :: {:ok, [map()]}
  defdelegate handle_gc_status(), to: MaintenanceHandler

  @doc """
  Triggers an immediate garbage-collection job for the named volume.
  Returns `{:ok, job_map}` on success, `{:error, reason}` on failure
  (`:not_found` for an unknown volume name, `:already_running` if a
  GC job is already in flight for the volume).
  """
  @spec handle_volume_gc_now(binary()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_volume_gc_now(volume_name), to: MaintenanceHandler

  @doc """
  Updates `RootSegment.schedules.gc.interval_ms` for the named
  volume. `interval_ms` must be at least 60_000 (1 minute) — anything
  smaller would tick faster than the scheduler itself.
  """
  @spec handle_volume_gc_set_interval(binary(), pos_integer()) ::
          {:ok, map()} | {:error, term()}
  defdelegate handle_volume_gc_set_interval(volume_name, interval_ms), to: MaintenanceHandler

  @doc """
  Returns the current GC schedule for the named volume — interval,
  last_run, and the most recent (or running) GC job for that volume.
  """
  @spec handle_volume_gc_status(binary()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_volume_gc_status(volume_name), to: MaintenanceHandler

  @doc """
  Triggers an immediate scrub job for the named volume.
  """
  @spec handle_volume_scrub_now(binary()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_volume_scrub_now(volume_name), to: MaintenanceHandler

  @doc """
  Updates `RootSegment.schedules.scrub.interval_ms` for the named
  volume. Minimum 60_000 ms (1 minute).
  """
  @spec handle_volume_scrub_set_interval(binary(), pos_integer()) ::
          {:ok, map()} | {:error, term()}
  defdelegate handle_volume_scrub_set_interval(volume_name, interval_ms), to: MaintenanceHandler

  @doc """
  Returns the current scrub schedule for the named volume — interval,
  last_run, and the latest scrub job for that volume.
  """
  @spec handle_volume_scrub_status(binary()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_volume_scrub_status(volume_name), to: MaintenanceHandler

  @doc """
  Triggers an immediate anti-entropy job for the named volume (#922).
  """
  @spec handle_volume_anti_entropy_now(binary()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_volume_anti_entropy_now(volume_name), to: MaintenanceHandler

  @doc """
  Updates `RootSegment.schedules.anti_entropy.interval_ms` for the
  named volume (#922). Minimum 60_000 ms (1 minute).
  """
  @spec handle_volume_anti_entropy_set_interval(binary(), pos_integer()) ::
          {:ok, map()} | {:error, term()}
  defdelegate handle_volume_anti_entropy_set_interval(volume_name, interval_ms),
    to: MaintenanceHandler

  @doc """
  Returns the current anti-entropy schedule for the named volume —
  interval, last_run, and the latest job (#922).
  """
  @spec handle_volume_anti_entropy_status(binary()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_volume_anti_entropy_status(volume_name), to: MaintenanceHandler

  @doc """
  Snapshots the named volume's current root chunk (#962 / epic #959).

  ## Parameters
  - `volume_name` — volume name (string).
  - `opts` — map with optional `"name"` (human-readable snapshot label).

  ## Returns
  - `{:ok, snapshot_map}` — `%{id, volume_id, volume_name, name, root_chunk_hash_hex, created_at}`.
  - `{:error, reason}` — volume not found, snapshot create failed, etc.
  """
  @spec handle_volume_snapshot_create(binary(), map()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_volume_snapshot_create(volume_name, opts \\ %{}), to: SnapshotsHandler

  @doc """
  Lists every snapshot for the named volume, newest first.
  """
  @spec handle_volume_snapshot_list(binary()) :: {:ok, [map()]} | {:error, term()}
  defdelegate handle_volume_snapshot_list(volume_name), to: SnapshotsHandler

  @doc """
  Shows a single snapshot, addressed by ULID or by human-readable
  `:name` (if unique within the volume), scoped to the named volume.
  """
  @spec handle_volume_snapshot_show(binary(), binary()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_volume_snapshot_show(volume_name, snapshot_ref), to: SnapshotsHandler

  @doc """
  Deletes the snapshot's pin. Accepts ULID or human-readable name (if
  unique within the volume). Idempotent — deleting a missing ULID is a
  no-op; deleting by an unknown name returns `:not_found`. Chunk
  reclamation is the GC scheduler's job (#961).
  """
  @spec handle_volume_snapshot_delete(binary(), binary()) :: :ok | {:error, term()}
  defdelegate handle_volume_snapshot_delete(volume_name, snapshot_ref), to: SnapshotsHandler

  @doc """
  Promotes a snapshot to a new top-level volume (#964). The new
  volume's `volume_root` points at the snapshot's `root_chunk_hash`;
  no chunks are copied.

  ## Parameters
  - `source_volume_name` — source volume name (string).
  - `snapshot_ref` — snapshot ULID, or human-readable name when unique
    within the source volume.
  - `new_volume_name` — name for the new volume.
  - `opts` — currently unused; reserved for `--storage-policy`
    forwarding (#964 body).

  ## Returns
  - `{:ok, %{volume_id, volume_name, source_volume_id, source_volume_name,
    snapshot_id, root_chunk_hash_hex}}` on success.
  - `{:error, reason}` — volume not found, snapshot ambiguous, name
    collision, etc.
  """
  @spec handle_volume_promote(binary(), binary(), binary(), map()) ::
          {:ok, map()} | {:error, term()}
  defdelegate handle_volume_promote(
                source_volume_name,
                snapshot_ref,
                new_volume_name,
                opts \\ %{}
              ),
              to: VolumeLifecycleHandler

  @doc """
  Rollback a volume's live root to a snapshot (#963).

  ## Parameters

  - `volume_name` - Volume to restore.
  - `snapshot_ref` - Snapshot id or unique name on `volume_name`.
  - `opts` - Map with optional keys (`"safe"`, `"force"`).

  ## Returns

  - `{:ok, map}` - `previous_root_hex`, `new_root_hex`,
    `pre_restore_snapshot_id` (string id or `nil`).
  - `{:error, term}` - cluster not initialised, volume / snapshot
    not found, `:unreferenced_chunks` (live root not covered and
    neither `:safe` nor `:force` set), or a Ra error.
  """
  @spec handle_volume_restore(binary(), binary(), map()) ::
          {:ok, map()} | {:error, term()}
  defdelegate handle_volume_restore(volume_name, snapshot_ref, opts \\ %{}),
    to: VolumeLifecycleHandler

  @doc """
  Export a volume's live root as a TAR archive at `output_path`
  on the daemon's filesystem (#965).

  V1 scope — live root only, local output only. Snapshot export,
  ACL/xattr capture, and S3/file:// URL outputs land in follow-ups.
  """
  @spec handle_volume_export(binary(), binary(), map()) ::
          {:ok, map()} | {:error, term()}
  defdelegate handle_volume_export(volume_name, output_path, opts \\ %{}),
    to: VolumeLifecycleHandler

  @doc """
  Import a previously-exported tarball into a new volume named
  `new_volume_name` (#966).

  V1 scope — local input path only, default storage policy. S3/
  `file://` URLs, custom storage policy, and post-import
  verification land in follow-ups.
  """
  @spec handle_volume_import(binary(), binary()) ::
          {:ok, map()} | {:error, term()}
  defdelegate handle_volume_import(input_path, new_volume_name), to: VolumeLifecycleHandler

  @doc """
  Take a snapshot of `volume_name`, export it to `output_path`, then
  drop the snapshot (#968).

  Returns `{:ok, summary}` with `:path`, `:volume`, `:snapshot_id`,
  `:file_count`, `:byte_count`. On export failure the snapshot is
  left in place per #968's "retry without re-snapshotting" semantics.
  """
  @spec handle_backup_create(binary(), binary(), map()) ::
          {:ok, map()} | {:error, term()}
  defdelegate handle_backup_create(volume_name, output_path, opts \\ %{}),
    to: VolumeLifecycleHandler

  @doc """
  Read a backup's manifest without unpacking the body (#968).

  Returns the parsed manifest map verbatim — the CLI surfaces a
  human-readable view of the well-known fields.
  """
  @spec handle_backup_describe(binary()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_backup_describe(input_path), to: VolumeLifecycleHandler

  @doc """
  Restore a backup tarball at `input_path` into a brand-new volume
  named `new_volume_name` (#968). Accepts a `:passphrase` to decrypt an
  encrypted archive (#1004).
  """
  @spec handle_backup_restore(binary(), binary(), map()) ::
          {:ok, map()} | {:error, term()}
  defdelegate handle_backup_restore(input_path, new_volume_name, opts \\ %{}),
    to: VolumeLifecycleHandler

  @doc """
  Restore a chain of backup archives (full + incrementals, oldest-first)
  into a brand-new volume (#1003). Accepts a `:passphrase` to decrypt
  encrypted archives (#1004).
  """
  @spec handle_backup_restore_chain([binary()], binary(), map()) ::
          {:ok, map()} | {:error, term()}
  defdelegate handle_backup_restore_chain(archive_paths, new_volume_name, opts \\ %{}),
    to: VolumeLifecycleHandler

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
  defdelegate handle_scrub_start(opts \\ %{}), to: ScrubRepairHandler

  @doc """
  Returns recent scrub jobs across the cluster.

  ## Returns
  - `{:ok, [map]}` - List of scrub job maps, most recent first
  """
  @spec handle_scrub_status() :: {:ok, [map()]}
  defdelegate handle_scrub_status(), to: ScrubRepairHandler

  @doc """
  Starts a replica-repair pass.

  Without `"volume"` in opts, queues a pass for every volume via
  `ReplicaRepairScheduler.trigger_now(:all)`. With `"volume"`,
  queues a single-volume pass — same dedupe logic as the scheduler
  (skips if a job is already running for that volume).

  ## Returns
  - `{:ok, [map]}` — list of queued job maps
  - `{:ok, []}` — every target volume already has a running job
  - `{:error, reason}` — volume not found, cluster not initialised, etc.
  """
  @spec handle_repair_start(map()) :: {:ok, [map()]} | {:error, term()}
  defdelegate handle_repair_start(opts \\ %{}), to: ScrubRepairHandler

  @doc """
  Returns recent replica-repair jobs across the cluster, optionally
  filtered by volume.

  ## Returns
  - `{:ok, [map]}` — list of repair-job maps, most recent first
  """
  @spec handle_repair_status(map()) :: {:ok, [map()]} | {:error, term()}
  defdelegate handle_repair_status(opts \\ %{}), to: ScrubRepairHandler

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
  defdelegate handle_list_jobs(filters \\ %{}), to: JobsHandler

  @doc """
  Gets a job by ID.

  ## Parameters
  - `job_id` - Job identifier (string)

  ## Returns
  - `{:ok, map}` - Job details as map
  - `{:error, reason}` - Error tuple
  """
  @spec handle_get_job(String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_get_job(job_id), to: JobsHandler

  @doc """
  Cancels a running or pending job.

  ## Parameters
  - `job_id` - Job identifier (string)

  ## Returns
  - `{:ok, %{}}` - Success
  - `{:error, reason}` - Error tuple
  """
  @spec handle_cancel_job(String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_cancel_job(job_id), to: JobsHandler

  @doc """
  Returns background worker status across the cluster.

  ## Returns
  - `{:ok, [map]}` - List of per-node worker status maps
  """
  @spec handle_worker_status() :: {:ok, [map()]}
  defdelegate handle_worker_status(), to: JobsHandler

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
  defdelegate handle_worker_configure(config), to: JobsHandler

  @doc """
  Returns node health status from the HealthCheck subsystem.

  ## Returns
  - `{:ok, map}` - Health report with node, status, checked_at, and per-subsystem checks
  """
  @spec handle_node_status() :: {:ok, map()}
  defdelegate handle_node_status(), to: NodeHandler

  @doc """
  Returns a list of all nodes in the cluster with their roles and uptimes.

  Combines ServiceRegistry entries with Ra membership to determine leader/follower
  roles for core nodes. Non-core nodes use their service type as their role.

  ## Returns
  - `{:ok, [map]}` - List of node info maps sorted by node name
  """
  @spec handle_node_list() :: {:ok, [map()]}
  defdelegate handle_node_list(), to: NodeHandler

  # Credential management (interface-agnostic: S3 SigV4 + WebDAV Basic)

  @doc """
  Creates a new credential for the given user identity.

  ## Parameters
  - `identity` - User identity to associate with the credential

  ## Returns
  - `{:ok, map}` - Credential details including secret key (shown once)
  - `{:error, reason}` - Error tuple
  """
  @spec handle_credential_create(term()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_credential_create(identity), to: CredentialHandler

  @doc """
  Lists credentials, optionally filtered by identity.

  ## Parameters
  - `filters` - Optional map with `:identity` key

  ## Returns
  - `{:ok, [map]}` - List of credentials (secrets redacted)
  """
  @spec handle_credential_list(map()) :: {:ok, [map()]}
  defdelegate handle_credential_list(filters \\ %{}), to: CredentialHandler

  @doc """
  Deletes a credential by access key ID.

  ## Parameters
  - `access_key_id` - The access key ID to delete

  ## Returns
  - `{:ok, map}` - Empty map on success
  - `{:error, reason}` - Error tuple
  """
  @spec handle_credential_delete(String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_credential_delete(access_key_id), to: CredentialHandler

  @doc """
  Rotates the secret access key for a credential.

  ## Parameters
  - `access_key_id` - The access key ID to rotate

  ## Returns
  - `{:ok, map}` - Updated credential with new secret key (shown once)
  - `{:error, reason}` - Error tuple
  """
  @spec handle_credential_rotate(String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_credential_rotate(access_key_id), to: CredentialHandler

  @doc """
  Shows details of a single credential by access key ID.

  ## Parameters
  - `access_key_id` - The access key ID to look up

  ## Returns
  - `{:ok, map}` - Credential details (secret redacted)
  - `{:error, reason}` - Error tuple
  """
  @spec handle_credential_show(String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_credential_show(access_key_id), to: CredentialHandler

  # S3 bucket management (volumes exposed as S3 buckets)

  @doc """
  Lists all volumes available as S3 buckets.

  ## Returns
  - `{:ok, [map]}` - List of bucket info maps
  """
  @spec handle_s3_list_buckets() :: {:ok, [map()]}
  defdelegate handle_s3_list_buckets(), to: S3Handler

  @doc """
  Shows details of a single S3 bucket (volume).

  ## Parameters
  - `bucket_name` - The bucket (volume) name

  ## Returns
  - `{:ok, map}` - Bucket details
  - `{:error, reason}` - Error tuple
  """
  @spec handle_s3_show_bucket(String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_s3_show_bucket(bucket_name), to: S3Handler

  @doc """
  Lists escalations, optionally filtered by `:status` or `:category`.

  ## Parameters
  - `filters` - Optional map with string or atom keys `status` and `category`.

  ## Returns
  - `{:ok, [map]}` - Serialisable list of escalation records.
  """
  @spec handle_escalation_list(map()) :: {:ok, [map()]} | {:error, term()}
  defdelegate handle_escalation_list(filters \\ %{}), to: EscalationHandler

  @doc """
  Resolves a pending escalation by choosing one of its options.
  """
  @spec handle_escalation_resolve(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_escalation_resolve(id, choice), to: EscalationHandler

  @doc """
  Fetches a single escalation by ID.
  """
  @spec handle_escalation_show(String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_escalation_show(id), to: EscalationHandler

  @doc """
  Triggers an immediate DR snapshot. Used by `neonfs dr snapshot create`
  (#324).

  Returns `{:ok, map}` with the snapshot id, path, and key manifest
  fields ready for serialisation to JSON.
  """
  @spec handle_dr_snapshot_create(map()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_dr_snapshot_create(opts \\ %{}), to: DRHandler

  @doc """
  Lists every DR snapshot in the `_system` volume's `/dr` directory,
  newest first. Used by `neonfs dr snapshot list` (#324).
  """
  @spec handle_dr_snapshot_list() :: {:ok, [map()]} | {:error, term()}
  defdelegate handle_dr_snapshot_list(), to: DRHandler

  @doc """
  Fetches a single DR snapshot's manifest by id. Used by
  `neonfs dr snapshot show <id>` (#324).
  """
  @spec handle_dr_snapshot_show(String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_dr_snapshot_show(id), to: DRHandler

  @doc """
  Applies a DR snapshot's cluster-wide metadata back into live Ra
  state. Used by `neonfs dr snapshot apply <id>` (#1005).
  """
  @spec handle_dr_snapshot_apply(String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_dr_snapshot_apply(id), to: DRHandler

  @doc """
  Exports a DR snapshot off-cluster to a filesystem path so it survives
  a bare-metal disaster. Used by `neonfs dr snapshot export` (#1367).
  """
  @spec handle_dr_snapshot_export(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_dr_snapshot_export(id, dest_dir), to: DRHandler

  @doc """
  Stages an exported DR snapshot back into the `_system` volume. Used by
  `neonfs dr snapshot import` (#1367).
  """
  @spec handle_dr_snapshot_import(String.t()) :: {:ok, map()} | {:error, term()}
  defdelegate handle_dr_snapshot_import(source_dir), to: DRHandler
end
