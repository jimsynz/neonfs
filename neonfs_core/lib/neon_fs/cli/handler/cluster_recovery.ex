defmodule NeonFS.CLI.Handler.ClusterRecovery do
  @moduledoc """
  Cluster-recovery and dangerous-operations CLI command handlers:
  node decommission, minority force-reset, and reconstruct-from-disk.

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates its `handle_remove_node/2`, `handle_force_reset/1` and
  `handle_cluster_reconstruct_from_disk/1` RPC entry points here, so the
  CLI wire contract is unchanged.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.CLI.Handler.CA, as: CAHandler
  alias NeonFS.Client.HealthCheck, as: ClientHealthCheck

  alias NeonFS.Core.{
    AuditLog,
    ClusterMode,
    CordonStopCheck,
    DriveEvacuation,
    DriveManager,
    DRSnapshotScheduler,
    MetadataStateMachine,
    NodeRegistry,
    PlacementBarrier,
    RaServer,
    RaSupervisor,
    ServiceRegistry
  }

  alias NeonFS.Core.Drive.Identity
  alias NeonFS.Core.Volume.Reconstruction
  alias NeonFS.Core.Volume.Reconstruction.OnDisk
  alias NeonFS.Error.{NotFound, Unavailable}

  require Logger

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
  Begins graceful decommission of a node: marks it `:draining` and starts
  evacuating each of its drives (#1325).

  Marking `:draining` first is the point — placement (`DriveSelector` /
  `Provisioner`, #1323) and client routing (`CostFunction`, #1324)
  immediately stop giving the node new work, so nothing new lands on it
  while its existing data drains off. Then each of the node's drives is
  handed to `DriveEvacuation` to migrate its chunks elsewhere.

  This returns as soon as evacuation is *started* (evacuation runs as a
  background job). The operator polls drive state, then runs
  `neonfs cluster remove-node` once the drives are empty — that command
  already refuses while the node still owns drives.

  ## Parameters
  - `node_name` — target node (`"neonfs_core@host2"` or `"host2"`).
  - `opts` — `"evacuate"` (boolean, default `true`): set `false` to mark
    draining without kicking off drive evacuation.

  ## Returns
  - `{:ok, %{node, status: "draining", drives: [%{drive_id, evacuation}]}}`.
  - `{:error, reason}` if the cluster isn't formed, the node can't be
    resolved, or the lifecycle write fails.
  """
  @spec handle_drain_node(String.t(), map()) :: {:ok, map()} | {:error, Exception.t()}
  def handle_drain_node(node_name, opts \\ %{}) when is_binary(node_name) do
    set_cli_metadata()
    evacuate? = Map.get(opts, "evacuate", true)

    with :ok <- require_cluster(),
         {:ok, target_node} <- resolve_target_node(node_name),
         :ok <- NodeRegistry.set_status(target_node, :draining) do
      evacuations =
        if evacuate? do
          start_drive_evacuations(target_node, DriveManager.list_all_drives(node: target_node))
        else
          []
        end

      {:ok,
       %{
         node: Atom.to_string(target_node),
         status: "draining",
         drives: evacuations
       }}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Reverses a drain: marks the node `:active` again (#1325) so placement
  and routing resume giving it new work.

  Note this does not reverse drive evacuation already in flight — chunks
  that migrated off stay off; this only re-enables the node for *new*
  work. Use it to abort a decommission before the node is removed.
  """
  @spec handle_undrain_node(String.t()) :: {:ok, map()} | {:error, Exception.t()}
  def handle_undrain_node(node_name) when is_binary(node_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, target_node} <- resolve_target_node(node_name),
         :ok <- NodeRegistry.set_status(target_node, :active) do
      {:ok, %{node: Atom.to_string(target_node), status: "active"}}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Cordons a node for planned, temporary absence (#1376): marks it
  `:maintenance` so placement (`DriveSelector` / `Provisioner`) and
  client routing (`CostFunction`) stop giving it new work — **without**
  evacuating its drives or removing it from Ra. Use this before a
  reboot / kernel upgrade / hardware swap, then `uncordon-node` (or, in
  a later slice, auto-resume) once it is back.

  Unlike `drain-node`, this never starts drive evacuation: the node is
  expected to return with its data intact.

  ## Returns
  - `{:ok, %{node, status: "maintenance"}}`.
  - `{:error, reason}` if the cluster isn't formed, the node can't be
    resolved, or the lifecycle write fails.
  """
  @spec handle_cordon_node(String.t()) :: {:ok, map()} | {:error, Exception.t()}
  def handle_cordon_node(node_name) when is_binary(node_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, target_node} <- resolve_target_node(node_name),
         :ok <- NodeRegistry.set_status(target_node, :maintenance) do
      {:ok, %{node: Atom.to_string(target_node), status: "maintenance"}}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Reverses a cordon: marks the node `:active` again (#1376) so placement
  and routing resume giving it new work.
  """
  @spec handle_uncordon_node(String.t()) :: {:ok, map()} | {:error, Exception.t()}
  def handle_uncordon_node(node_name) when is_binary(node_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, target_node} <- resolve_target_node(node_name),
         :ok <- NodeRegistry.set_status(target_node, :active) do
      {:ok, %{node: Atom.to_string(target_node), status: "active"}}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Read-only pre-shutdown safety check for stopping a cordoned node
  (#1417). Reports whether taking the node offline would break Ra
  quorum, strand a chunk (no trusted replica elsewhere), or drop a
  chunk below its volume's `min_copies`. Does not mutate anything —
  the caller (CLI) decides whether to proceed.

  ## Returns
  - `{:ok, %{node, safe: boolean, reasons: [%{kind, ...}]}}`.
  - `{:error, reason}` if the cluster isn't formed or the node can't be
    resolved.
  """
  @spec handle_cordon_stop_check(String.t()) :: {:ok, map()} | {:error, Exception.t()}
  def handle_cordon_stop_check(node_name) when is_binary(node_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, target_node} <- resolve_target_node(node_name) do
      result = CordonStopCheck.check(target_node)

      {:ok,
       %{
         node: Atom.to_string(target_node),
         safe: result.safe,
         reasons: Enum.map(result.reasons, &serialise_reason/1)
       }}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Freezes the whole cluster for a coordinated maintenance shutdown
  (#1378): sets the cluster mode `:frozen` (cutting client write ingress
  via the #1438 write-gate), drains outstanding background chunk placements
  cluster-wide so every acknowledged write reaches its `min_copies` replica
  set (#1504), then triggers a metadata DR snapshot. Reports
  ready-to-power-off — the operator then stops interface nodes, then core
  nodes.

  Setting `:frozen` first stops new write ingress, so the outstanding
  placement set only shrinks while the drain runs. The drain is bounded by
  the settle window (`settle_ms`) so a hung placement — e.g. a dead target
  drive — can't wedge the freeze; a bounded-out drain still transitions the
  cluster (Ra's own on-disk persistence is the primary safety net) and
  reports the unfinished count.

  The DR snapshot is belt-and-braces and is leader-only, so it reports
  `:skipped` when this handler runs on a follower.

  `opts` (test injection): `:cluster_mode_mod`, `:snapshot_fn`, `:settle_ms`,
  `:drain_fn`.
  """
  @spec handle_cluster_freeze(keyword()) :: {:ok, map()} | {:error, Exception.t()}
  def handle_cluster_freeze(opts \\ []) do
    set_cli_metadata()
    cluster_mode_mod = Keyword.get(opts, :cluster_mode_mod, ClusterMode)
    snapshot_fn = Keyword.get(opts, :snapshot_fn, &default_snapshot/0)
    drain_fn = Keyword.get(opts, :drain_fn, &PlacementBarrier.drain_cluster/1)
    settle_ms = Keyword.get(opts, :settle_ms, freeze_settle_ms())

    with :ok <- require_cluster(),
         :ok <- cluster_mode_mod.set_mode(:frozen, "cluster freeze") do
      drain = drain_fn.(settle_ms)

      {:ok,
       %{
         status: "frozen",
         drained: drain.drained,
         drain_timed_out: drain.timed_out,
         snapshot: safe_snapshot(snapshot_fn)
       }}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Thaws the cluster after a coordinated restart (#1378): sets the cluster
  mode `:recovering` so failure-driven repair stays suppressed and
  verification throttled (#1436) while the cluster reassembles. The
  `ClusterRecoveryMonitor` (#1437) returns the mode to `:normal` once all
  members are back and drives are trusted (or on its timeout backstop).

  `opts` (test injection): `:cluster_mode_mod`.
  """
  @spec handle_cluster_thaw(keyword()) :: {:ok, map()} | {:error, Exception.t()}
  def handle_cluster_thaw(opts \\ []) do
    set_cli_metadata()
    cluster_mode_mod = Keyword.get(opts, :cluster_mode_mod, ClusterMode)

    with :ok <- require_cluster(),
         :ok <- cluster_mode_mod.set_mode(:recovering, "cluster thaw") do
      {:ok, %{status: "recovering"}}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  # The metadata DR snapshot is best-effort belt-and-braces — Ra's own
  # on-disk persistence is the primary safety net across the power-cycle.
  # A freeze must not fail because the snapshot is unavailable, so any
  # error (the scheduler not running, a leader-only skip, an exit) degrades
  # to "unavailable"/"skipped" and the freeze still succeeds.
  defp default_snapshot do
    if Process.whereis(DRSnapshotScheduler) do
      DRSnapshotScheduler.run_now()
    else
      {:error, :not_running}
    end
  end

  defp safe_snapshot(snapshot_fn) do
    describe_snapshot(snapshot_fn.())
  rescue
    _ -> "unavailable"
  catch
    :exit, _ -> "unavailable"
  end

  defp describe_snapshot({:ok, %{snapshot: :skipped}}), do: "skipped"
  defp describe_snapshot({:ok, %{snapshot: _}}), do: "taken"
  defp describe_snapshot(_), do: "unavailable"

  defp freeze_settle_ms do
    Application.get_env(:neonfs_core, :cluster_freeze_settle_ms, 5_000)
  end

  defp serialise_reason(%{kind: :quorum, surviving: surviving, majority: majority}) do
    %{kind: "quorum", surviving: surviving, majority: majority}
  end

  defp serialise_reason(%{kind: kind, chunks: chunks, sample: sample}) do
    %{
      kind: Atom.to_string(kind),
      chunks: chunks,
      sample: Enum.map(sample, &Base.encode16(&1, case: :lower))
    }
  end

  # Hands each of the node's drives to `DriveEvacuation`, collecting a
  # per-drive start result. Already-draining drives surface their error
  # rather than aborting the whole drain.
  defp start_drive_evacuations(target_node, drives) do
    Enum.map(drives, fn %{id: drive_id} ->
      case DriveEvacuation.start_evacuation(target_node, drive_id) do
        {:ok, _job} -> %{drive_id: drive_id, evacuation: "started"}
        {:error, reason} -> %{drive_id: drive_id, evacuation: "error", reason: inspect(reason)}
      end
    end)
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

  # Private

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

  defp summarise_command({:register_volume_root, id, shard, _entry}),
    do: "register_volume_root #{id} shard #{shard}"

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

  # handle_remove_node/2 helpers

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
    case CAHandler.handle_ca_revoke(node_name) do
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
