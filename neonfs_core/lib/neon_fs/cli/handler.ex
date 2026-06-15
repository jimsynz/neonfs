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

  import NeonFS.CLI.Handler.Common

  alias NeonFS.CLI.Handler.ACL, as: ACLHandler
  alias NeonFS.CLI.Handler.CA, as: CAHandler
  alias NeonFS.CLI.Handler.Credential, as: CredentialHandler
  alias NeonFS.CLI.Handler.DR, as: DRHandler
  alias NeonFS.CLI.Handler.Drives, as: DrivesHandler
  alias NeonFS.CLI.Handler.Escalation, as: EscalationHandler
  alias NeonFS.CLI.Handler.Jobs, as: JobsHandler
  alias NeonFS.CLI.Handler.Maintenance, as: MaintenanceHandler
  alias NeonFS.CLI.Handler.Node, as: NodeHandler
  alias NeonFS.CLI.Handler.S3, as: S3Handler
  alias NeonFS.CLI.Handler.ScrubRepair, as: ScrubRepairHandler
  alias NeonFS.CLI.Handler.Snapshots, as: SnapshotsHandler
  alias NeonFS.CLI.Handler.VolumeLifecycle, as: VolumeLifecycleHandler
  alias NeonFS.Client.HealthCheck, as: ClientHealthCheck
  alias NeonFS.Cluster.{Init, Invite, Join, State}

  alias NeonFS.Core.{
    ACLManager,
    AuditLog,
    Authorise,
    DriveManager,
    DriveRegistry,
    KeyManager,
    KeyRotation,
    RaServer,
    RaSupervisor,
    ServiceRegistry,
    Volume,
    VolumeACL,
    VolumeEncryption,
    VolumeRegistry
  }

  alias NeonFS.Core.Drive.Identity
  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.Core.Volume.Reconstruction
  alias NeonFS.Core.Volume.Reconstruction.OnDisk

  alias NeonFS.Error.{
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
  def cluster_init(cluster_name, drive_config \\ nil, opts \\ %{})
      when is_binary(cluster_name) and (is_map(drive_config) or is_nil(drive_config)) and
             is_map(opts) do
    set_cli_metadata()
    init_opts = cluster_init_opts(opts)

    cluster_name
    |> Init.init_cluster(drive_config, init_opts)
    |> format_cluster_init_result()
  end

  defp cluster_init_opts(opts) do
    case Map.get(opts, "system_replicas") do
      n when is_integer(n) and n >= 1 -> [system_replicas: n]
      _ -> []
    end
  end

  defp format_cluster_init_result({:ok, cluster_id}) do
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
        {:ok, %{cluster_id: cluster_id}}
    end
  end

  defp format_cluster_init_result({:error, :already_initialised}),
    do: {:error, Invalid.exception(message: "Cluster already initialised")}

  defp format_cluster_init_result({:error, :no_drives_available}),
    do:
      {:error,
       Invalid.exception(
         message:
           "No drives available — pass `--drive <path>` to `neonfs cluster init` " <>
             "to designate the initial drive"
       )}

  defp format_cluster_init_result({:error, {:drive_preflight_failed, reason}}) do
    detail = if is_binary(reason), do: reason, else: inspect(reason)

    {:error,
     Invalid.exception(
       message:
         "Initial drive preflight failed: #{detail}. " <>
           "The cluster was not initialised — fix the drive and re-run " <>
           "`neonfs cluster init`."
     )}
  end

  defp format_cluster_init_result({:error, {:initial_drive_failed, reason}}) do
    {:error,
     Invalid.exception(
       message:
         "Ra cluster bootstrapped but the initial drive failed to register: " <>
           "#{inspect(reason)}. The cluster will report `running` from `neonfs cluster status` " <>
           "but has no drives or system volume yet — re-run `neonfs drive add <path>` to " <>
           "finish bootstrap. (#980)"
     )}
  end

  defp format_cluster_init_result({:error, reason}), do: {:error, wrap_error(reason)}

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
  def handle_remove_node(node_name, opts \\ %{}) when is_binary(node_name) do
    set_cli_metadata()
    force = Map.get(opts, "force", false)

    with :ok <- require_cluster(),
         {:ok, target_node} <- resolve_target_node(node_name),
         :ok <- refuse_if_self(target_node),
         :ok <- refuse_if_leader(target_node),
         :ok <- refuse_if_drives_present(target_node, force),
         :ok <- ra_remove_member(target_node) do
      revoked = best_effort_revoke(node_name)

      remaining =
        case :ra.members(RaSupervisor.server_id(), 2_000) do
          {:ok, members, _leader} -> length(members)
          _ -> 0
        end

      {:ok,
       %{
         node: Atom.to_string(target_node),
         status: "removed",
         remaining_members: remaining,
         certificate_revoked: revoked
       }}
    else
      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

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
  def handle_force_reset(opts) when is_map(opts) do
    set_cli_metadata()

    keep_names = Map.get(opts, "keep", [])
    min_unreachable_s = Map.get(opts, "min_unreachable_seconds", 1800)
    accepted = Map.get(opts, "yes_i_accept_data_loss", false)

    with :ok <- require_cluster(),
         :ok <- require_data_loss_acknowledged(accepted),
         {:ok, state} <- load_cluster_state(),
         {:ok, members} <- current_ra_members(),
         {:ok, keep_nodes} <- resolve_keep_nodes(keep_names, members),
         :ok <- require_keep_reachable_and_healthy(keep_nodes),
         :ok <- require_keep_is_minority(keep_nodes, members),
         departed = members -- keep_nodes,
         :ok <- require_departed_unreachable_long_enough(departed, state, min_unreachable_s) do
      log_force_reset_attempt(state, members, keep_nodes, min_unreachable_s)

      perform_force_reset(state, members, keep_nodes, departed)
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  # The actual Ra minority-recovery mutation (#473). The safety
  # gates above gave us a green light; the survivor's local Ra
  # replica gets snapshot-extracted, the Ra server destroyed, and a
  # fresh single-node cluster bootstrapped with the extracted state
  # injected via `MetadataStateMachine.init/1`'s `:initial_state`.
  defp perform_force_reset(state, members, keep_nodes, departed) do
    case RaServer.force_reset_to_self() do
      {:ok, snapshot_path} ->
        log_force_reset_completion(
          state,
          members,
          keep_nodes,
          departed,
          snapshot_path
        )

        {:ok,
         %{
           survivors: keep_nodes,
           departed: departed,
           snapshot_path: snapshot_path
         }}

      {:error, reason} ->
        log_force_reset_failure(state, members, keep_nodes, departed, reason)

        {:error,
         Unavailable.exception(message: "Force-reset mutation failed: #{inspect(reason)}")}
    end
  end

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
  def handle_cluster_reconstruct_from_disk(opts) when is_map(opts) do
    set_cli_metadata()

    yes? = Map.get(opts, "yes", false)
    overwrite? = Map.get(opts, "overwrite_ra_state", false)
    dry_run? = Map.get(opts, "dry_run", false)

    with :ok <- require_yes_for_reconstruct(yes?, dry_run?),
         {:ok, state} <- load_cluster_state(),
         :ok <- require_empty_volume_roots_or_overwrite(overwrite?, dry_run?),
         drive_paths = configured_drive_paths(),
         result = run_reconstruction(drive_paths, state, dry_run?),
         {:ok, submitted, failed_subs} <- submit_commands(result.commands, dry_run?) do
      log_reconstruction_summary(state, result, submitted, failed_subs, dry_run?)

      {:ok,
       %{
         drives: length(result.drives),
         volumes: map_size(result.volumes),
         commands: length(result.commands),
         commands_submitted: submitted,
         commands_failed: failed_subs,
         warnings: format_warnings(result.warnings),
         dry_run: dry_run?
       }}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  defp require_yes_for_reconstruct(_yes, true = _dry_run), do: :ok
  defp require_yes_for_reconstruct(true, _dry_run), do: :ok

  defp require_yes_for_reconstruct(_, _) do
    {:error,
     Unavailable.exception(
       message:
         "Refusing to reconstruct-from-disk without --yes. " <>
           "Reconstruction overwrites the bootstrap layer's Ra state from on-disk " <>
           "volume data; re-run with the flag once you have confirmed this is the " <>
           "right move (or pass --dry-run to preview)."
     )}
  end

  defp require_empty_volume_roots_or_overwrite(true, _dry_run), do: :ok
  defp require_empty_volume_roots_or_overwrite(_, true = _dry_run), do: :ok

  defp require_empty_volume_roots_or_overwrite(_, _) do
    case RaSupervisor.local_query(&MetadataStateMachine.get_volume_roots/1) do
      {:ok, roots} when is_map(roots) and map_size(roots) == 0 ->
        :ok

      {:ok, roots} when is_map(roots) ->
        {:error,
         Unavailable.exception(
           message:
             "Refusing to reconstruct-from-disk: bootstrap layer already has " <>
               "#{map_size(roots)} volume(s) registered. Pass --overwrite-ra-state to " <>
               "force, or --dry-run to preview without submitting."
         )}

      {:error, reason} ->
        {:error,
         Unavailable.exception(message: "Cannot query bootstrap-layer state: #{inspect(reason)}")}
    end
  end

  defp configured_drive_paths do
    :neonfs_core
    |> Application.get_env(:drives, [])
    |> Enum.map(&(Map.get(&1, :path) || Map.get(&1, "path")))
    |> Enum.reject(&is_nil/1)
  end

  defp run_reconstruction(drive_paths, state, dry_run?) do
    Reconstruction.reconstruct(drive_paths,
      expected_cluster_id: state.cluster_id,
      node: Node.self(),
      dry_run?: dry_run?,
      identity_reader: &Identity.read/1,
      chunk_lister: &OnDisk.list_candidate_hashes/1,
      chunk_reader: &OnDisk.read_chunk/2
    )
  end

  defp submit_commands(_commands, true = _dry_run), do: {:ok, 0, []}

  defp submit_commands(commands, false) do
    {submitted, failed} =
      Enum.reduce(commands, {0, []}, fn command, {ok_count, failures} ->
        case RaSupervisor.command(command) do
          {:ok, _result, _leader} ->
            {ok_count + 1, failures}

          {:error, reason} ->
            {ok_count,
             [%{command: summarise_command(command), reason: inspect(reason)} | failures]}

          other ->
            {ok_count,
             [%{command: summarise_command(command), reason: inspect(other)} | failures]}
        end
      end)

    {:ok, submitted, Enum.reverse(failed)}
  end

  defp summarise_command({:register_drive, %{drive_id: id}}),
    do: "register_drive #{id}"

  defp summarise_command({:register_volume_root, %{volume_id: id}}),
    do: "register_volume_root #{id}"

  defp summarise_command(other), do: inspect(other)

  defp format_warnings(warnings) do
    Enum.map(warnings, fn {tag, message, _ctx} ->
      %{tag: tag, message: message}
    end)
  end

  defp log_reconstruction_summary(state, result, submitted, failed_subs, dry_run?) do
    Logger.info(
      "Cluster reconstruct-from-disk run: " <>
        "drives=#{length(result.drives)} volumes=#{map_size(result.volumes)} " <>
        "commands=#{length(result.commands)} submitted=#{submitted} " <>
        "failed=#{length(failed_subs)} warnings=#{length(result.warnings)}",
      cluster_id: state.cluster_id,
      dry_run: dry_run?
    )
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
      {:ok, :joining} ->
        # The join finishes asynchronously: the node restarts TLS distribution
        # (dropping this connection) and then connects + completes Ra membership.
        # The audit-log entry and quorum-ring rebuild now happen in that worker
        # once the node is connected. The CLI reconnects and validates via
        # `cluster status` (#1033).
        {:ok,
         %{
           "status" => "joining",
           "via_address" => via_address,
           "node_name" => Atom.to_string(Node.self())
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
           :ok <- check_durability_fits_cluster(name, final_opts),
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
        {:error, %NeonFS.Error.AlreadyExists{}} ->
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

        {:error, %{class: :forbidden}} ->
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
  named `new_volume_name` (#968). Identical wiring to
  `handle_volume_import/2`.
  """
  @spec handle_backup_restore(binary(), binary()) ::
          {:ok, map()} | {:error, term()}
  defdelegate handle_backup_restore(input_path, new_volume_name), to: VolumeLifecycleHandler

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

  # Private helper functions

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

  # RPC wrappers for MountManager operations. Bounded timeouts so a misbehaving
  # FUSE node can't hang the calling CLI command indefinitely (#1035) — a stuck
  # mount surfaces as a clear error rather than an unbounded wait.
  @mount_rpc_timeout 60_000
  @unmount_rpc_timeout 30_000

  defp rpc_mount(fuse_node, volume_name, mount_point, opts) do
    case :rpc.call(
           fuse_node,
           NeonFS.FUSE.MountManager,
           :mount,
           [volume_name, mount_point, opts],
           @mount_rpc_timeout
         ) do
      {:badrpc, reason} ->
        {:error, Unavailable.exception(message: "FUSE RPC failed: #{inspect(reason)}")}

      result ->
        result
    end
  end

  defp rpc_unmount(fuse_node, mount_id) do
    case :rpc.call(
           fuse_node,
           NeonFS.FUSE.MountManager,
           :unmount,
           [mount_id],
           @unmount_rpc_timeout
         ) do
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

  defp rpc_get_mount_by_volume_name(fuse_node, volume_name) do
    case :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :get_mount_by_volume_name, [volume_name]) do
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

  defp clear_nfs_export(%{nfs_export: false}), do: {:error, :not_exported}
  defp clear_nfs_export(volume), do: VolumeRegistry.update(volume.id, nfs_export: false)

  defp ensure_nfs_exported(%{nfs_export: true}), do: :ok
  defp ensure_nfs_exported(_volume), do: {:error, :not_exported}

  defp rpc_nfs_bind_info(nfs_node) do
    host =
      case :rpc.call(nfs_node, Application, :get_env, [:neonfs_nfs, :bind_address]) do
        {:badrpc, _} -> nfs_node_hostname(nfs_node)
        nil -> nfs_node_hostname(nfs_node)
        address when address in ["0.0.0.0", "::"] -> nfs_node_hostname(nfs_node)
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

  defp nfs_node_hostname(nfs_node) do
    nfs_node
    |> Atom.to_string()
    |> String.split("@")
    |> List.last()
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

  # Refuse `neonfs volume create` when the requested durability needs
  # more drives than the cluster currently has — replicas and erasure
  # shards are placed on distinct drives, which may live on the same
  # node, so the bound is the cluster-wide drive count, not the core
  # node count (#1032). A single multi-drive node can satisfy
  # `replicate:2`; a factor above the total drive count cannot be
  # placed and writes would leave the chunk under-replicated.
  # `--allow-under-replicated` plumbs through as
  # `allow_under_replicated: true` for operators who are about to
  # scale out and want the volume in place ahead of the new drives.
  defp check_durability_fits_cluster(name, opts) do
    cond do
      Keyword.get(opts, :allow_under_replicated, false) ->
        :ok

      needed = required_replicas_from_opts(opts) ->
        check_replica_count(name, needed)

      true ->
        :ok
    end
  end

  defp check_replica_count(name, needed) do
    drive_count = cluster_drive_count()

    if needed <= drive_count do
      :ok
    else
      {:error,
       Invalid.exception(
         message:
           "Volume '#{name}' needs #{needed} replicas but the cluster has only " <>
             "#{drive_count} drive(s). Add more drives, lower the replication " <>
             "factor, or pass `--allow-under-replicated` to create the volume " <>
             "anyway (chunks stay under-replicated until the cluster grows).",
         details: %{volume_name: name, required_replicas: needed, drives: drive_count}
       )}
    end
  end

  # The authoritative cluster-wide drive set lives in the Ra
  # `MetadataStateMachine` (strongly consistent). `DriveRegistry`'s ETS
  # table is an eventually-consistent local cache that under-counts
  # remote drives until its periodic peer sync catches up, which races
  # volume creation during cluster formation (#1032). Prefer Ra; fall
  # back to the local cache only when Ra is unreachable (no core node
  # to coordinate with — e.g. handler unit tests without a cluster).
  defp cluster_drive_count do
    case RaSupervisor.local_query(&MetadataStateMachine.get_drives/1) do
      {:ok, drives} when is_map(drives) -> map_size(drives)
      _ -> length(DriveRegistry.list_drives())
    end
  end

  defp required_replicas_from_opts(opts) do
    case Keyword.get(opts, :durability) do
      %{type: :replicate, factor: factor} when is_integer(factor) -> factor
      %{type: :erasure, data_chunks: d, parity_chunks: p} -> d + p
      _ -> nil
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

  # ── handle_remove_node/2 helpers ─────────────────────────────────────

  defp resolve_target_node(node_name) do
    # First try ServiceRegistry — authoritative source for nodes currently
    # in the cluster. Match on the full Erlang atom ("neonfs_core@host"),
    # the CN portion after the `@`, or the entire string.
    match =
      ServiceRegistry.list()
      |> Enum.find(fn %{node: node} ->
        s = Atom.to_string(node)
        s == node_name or host_part(s) == node_name or s == "neonfs_core@#{node_name}"
      end)

    case match do
      %{node: node} ->
        {:ok, node}

      nil ->
        # Fall back: accept a literal Erlang node atom. This covers the
        # decommission case where the service has already been shut down
        # (ServiceRegistry no longer lists it) but the Ra membership
        # entry remains.
        case safe_string_to_existing_atom(node_name) do
          {:ok, atom} ->
            {:ok, atom}

          :error ->
            {:error, NotFound.exception(message: "No node '#{node_name}' found in cluster")}
        end
    end
  end

  defp host_part(node_string) do
    case String.split(node_string, "@", parts: 2) do
      [_, host] -> host
      _ -> node_string
    end
  end

  defp safe_string_to_existing_atom(str) do
    {:ok, String.to_existing_atom(str)}
  rescue
    ArgumentError -> :error
  end

  defp refuse_if_self(target_node) do
    if target_node == node() do
      {:error,
       Unavailable.exception(
         message:
           "Cannot remove the node running this command (#{target_node}). Run remove-node from a peer."
       )}
    else
      :ok
    end
  end

  defp refuse_if_leader(target_node) do
    case :ra.members(RaSupervisor.server_id(), 2_000) do
      {:ok, _members, {_cluster_name, leader_node}} when leader_node == target_node ->
        {:error,
         Unavailable.exception(
           message:
             "Node '#{target_node}' is the current Ra leader. It must step down (or be stopped) before removal."
         )}

      _ ->
        :ok
    end
  end

  defp refuse_if_drives_present(_target_node, true), do: :ok

  defp refuse_if_drives_present(target_node, false) do
    case DriveManager.list_all_drives(node: target_node) do
      [] ->
        :ok

      drives ->
        {:error,
         Unavailable.exception(
           message:
             "Node '#{target_node}' still owns #{length(drives)} drive(s). " <>
               "Evacuate (`neonfs drive evacuate`) and remove them first, or pass --force " <>
               "to accept potential data loss for any chunk whose only replica lives on that node."
         )}
    end
  end

  defp ra_remove_member(target_node) do
    server_id = {RaSupervisor.cluster_name(), target_node}

    case :ra.remove_member(RaSupervisor.server_id(), server_id, 10_000) do
      {:ok, _, _} -> :ok
      # Target is not a Ra member (e.g. interface-only node) — treat as
      # success so the rest of the flow (cert revoke) still runs.
      {:error, :not_member} -> :ok
      {:error, :cluster_change_not_permitted} = err -> err
      {:error, reason} -> {:error, reason}
      {:timeout, _} -> {:error, :ra_remove_timeout}
    end
  end

  defp best_effort_revoke(node_name) do
    case handle_ca_revoke(node_name) do
      {:ok, _} -> true
      {:error, _} -> false
    end
  end

  # handle_force_reset/1 helpers

  defp require_data_loss_acknowledged(true), do: :ok

  defp require_data_loss_acknowledged(_) do
    {:error,
     Unavailable.exception(
       message:
         "Refusing to force-reset without --yes-i-accept-data-loss. " <>
           "Force-reset destroys Ra state on the surviving minority and can drop committed writes. " <>
           "Re-run with the flag once you have accepted the data-loss risk."
     )}
  end

  defp current_ra_members do
    case :ra.members(RaSupervisor.server_id(), 2_000) do
      {:ok, members, _leader} ->
        {:ok, Enum.map(members, fn {_cluster_name, node} -> node end)}

      _other ->
        {:error,
         Unavailable.exception(
           message:
             "Cannot read Ra membership. Force-reset requires the local Ra server to be running."
         )}
    end
  end

  defp resolve_keep_nodes([], _members) do
    {:error, Unavailable.exception(message: "--keep must name at least one surviving node.")}
  end

  defp resolve_keep_nodes(names, members) when is_list(names) do
    Enum.reduce_while(names, {:ok, []}, fn name, {:ok, acc} ->
      case find_node_in_members(name, members) do
        {:ok, node} -> {:cont, {:ok, [node | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, nodes} -> {:ok, Enum.reverse(nodes)}
      other -> other
    end
  end

  defp find_node_in_members(name, members) do
    match =
      Enum.find(members, fn node ->
        s = Atom.to_string(node)
        s == name or host_part(s) == name or s == "neonfs_core@#{name}"
      end)

    case match do
      nil ->
        {:error,
         NotFound.exception(
           message:
             "--keep node '#{name}' is not in the current Ra membership. " <>
               "Use 'neonfs cluster status' to list current members."
         )}

      node ->
        {:ok, node}
    end
  end

  defp require_keep_reachable_and_healthy(nodes) do
    Enum.reduce_while(nodes, :ok, fn node, :ok ->
      case probe_keep_health(node) do
        :healthy -> {:cont, :ok}
        reason -> {:halt, {:error, keep_unhealthy_error(node, reason)}}
      end
    end)
  end

  defp probe_keep_health(node) do
    case :erpc.call(node, ClientHealthCheck, :check, [], 5_000) do
      %{status: :healthy} -> :healthy
      %{status: status} -> {:unhealthy, status}
      other -> {:unexpected, other}
    end
  rescue
    _ -> :rpc_failed
  catch
    :exit, reason -> {:rpc_exit, reason}
  end

  defp keep_unhealthy_error(node, {:unhealthy, status}) do
    Unavailable.exception(
      message:
        "--keep node '#{node}' is reachable but its health check reports #{status}. Refusing."
    )
  end

  defp keep_unhealthy_error(node, _reason) do
    Unavailable.exception(
      message:
        "--keep node '#{node}' is unreachable. All survivors must be healthy and reachable."
    )
  end

  defp require_keep_is_minority(keep_nodes, members) do
    if length(keep_nodes) * 2 >= length(members) do
      {:error,
       Unavailable.exception(
         message:
           "--keep has #{length(keep_nodes)} of #{length(members)} members. " <>
             "That is not a minority, so Ra can elect a leader normally; " <>
             "force-reset is not appropriate."
       )}
    else
      :ok
    end
  end

  defp require_departed_unreachable_long_enough(departed, state, min_unreachable_s) do
    Enum.reduce_while(departed, :ok, fn node, :ok ->
      case check_departed_node(node, state, min_unreachable_s) do
        :ok -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  defp check_departed_node(node, state, min_unreachable_s) do
    case :net_adm.ping(node) do
      :pong ->
        {:error,
         Unavailable.exception(
           message:
             "Departed member '#{node}' is currently reachable. Refusing — " <>
               "force-reset is for permanently unreachable members, not a healing partition."
         )}

      :pang ->
        check_unreachable_duration(node, state, min_unreachable_s)
    end
  end

  defp check_unreachable_duration(node, state, min_unreachable_s) do
    case lookup_last_seen(state, node) do
      nil ->
        {:error,
         Unavailable.exception(
           message:
             "No last-seen record for '#{node}'. " <>
               "Cannot prove it has been unreachable for at least #{min_unreachable_s}s."
         )}

      datetime ->
        elapsed_s = DateTime.diff(DateTime.utc_now(), datetime, :second)

        if elapsed_s >= min_unreachable_s do
          :ok
        else
          {:error,
           Unavailable.exception(
             message:
               "'#{node}' last seen #{elapsed_s}s ago; require #{min_unreachable_s}s of unreachability. " <>
                 "Raise --min-unreachable-seconds if you are certain it is not coming back."
           )}
        end
    end
  end

  defp lookup_last_seen(state, node) do
    case Enum.find(state.known_peers, &(&1.name == node)) do
      nil -> nil
      peer -> peer.last_seen
    end
  end

  defp log_force_reset_attempt(state, members, keep_nodes, min_unreachable_s) do
    AuditLog.log_event(
      event_type: :cluster_force_reset_attempt,
      actor_uid: 0,
      resource: "cluster:#{state.cluster_id}",
      details: %{
        ra_cluster_members: Enum.map(members, &Atom.to_string/1),
        member_last_seen:
          Map.new(members, fn node ->
            last_seen = lookup_last_seen(state, node)
            {Atom.to_string(node), last_seen && DateTime.to_iso8601(last_seen)}
          end),
        keep: Enum.map(keep_nodes, &Atom.to_string/1),
        min_unreachable_seconds: min_unreachable_s,
        data_loss_acknowledged: true
      }
    )
  end

  defp log_force_reset_completion(state, members, keep_nodes, departed, snapshot_path) do
    AuditLog.log_event(
      event_type: :cluster_force_reset_completed,
      actor_uid: 0,
      resource: "cluster:#{state.cluster_id}",
      details: %{
        ra_cluster_members: Enum.map(members, &Atom.to_string/1),
        keep: Enum.map(keep_nodes, &Atom.to_string/1),
        departed: Enum.map(departed, &Atom.to_string/1),
        snapshot_path: snapshot_path
      }
    )
  end

  defp log_force_reset_failure(state, members, keep_nodes, departed, reason) do
    AuditLog.log_event(
      event_type: :cluster_force_reset_failed,
      actor_uid: 0,
      resource: "cluster:#{state.cluster_id}",
      details: %{
        ra_cluster_members: Enum.map(members, &Atom.to_string/1),
        keep: Enum.map(keep_nodes, &Atom.to_string/1),
        departed: Enum.map(departed, &Atom.to_string/1),
        reason: inspect(reason)
      }
    )
  end
end
