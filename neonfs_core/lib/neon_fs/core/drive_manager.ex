defmodule NeonFS.Core.DriveManager do
  @moduledoc """
  Orchestrates runtime drive lifecycle: add, remove, and list drives.

  DriveManager coordinates between BlobStore (NIF handles), DriveRegistry (cluster-wide
  drive tracking), DriveState (power management), and cluster.json (persistent config).

  Drives can be added and removed at runtime via the CLI without restarting the node.
  All changes are persisted to cluster.json for recovery on restart.

  ## Telemetry Events

    * `[:neonfs, :drive_manager, :add]` — emitted when a drive is added
    * `[:neonfs, :drive_manager, :remove]` — emitted when a drive is removed
  """

  use GenServer
  require Logger

  alias NeonFS.Cluster.State

  alias NeonFS.Core.{
    BlobStore,
    Drive,
    DriveConfig,
    DriveRegistry,
    DriveState,
    DriveTrust,
    JobTracker,
    MetadataStateMachine,
    NodeRegistry,
    RaServer,
    RaSupervisor,
    ReplicaAudit,
    VolumeRegistry
  }

  alias NeonFS.Core.Drive.Identity
  alias NeonFS.Core.Job.Runners.Scrub
  alias NeonFS.Events.{Broadcaster, DriveAdded, DriveRemoved}

  @valid_tiers [:hot, :warm, :cold]
  @drive_state_supervisor NeonFS.Core.DriveStateSupervisor
  @bootstrap_register_attempts 5
  @system_volume_factor_cap 3
  @default_bootstrap_register_backoff_ms 200

  # A dirty drive's verification must wait for Ra to have a leader (the
  # trust write is a Ra command). Retry the boot-time recovery until then.
  @dirty_recovery_interval_ms 2_000
  @dirty_recovery_max_retries 60

  ## Client API

  @doc """
  Starts the DriveManager GenServer.

  ## Options

    * `:name` - GenServer name (default: `__MODULE__`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Adds a new drive at runtime.

  Validates the config, opens a BlobStore handle, registers in DriveRegistry,
  starts a DriveState child, and persists to cluster.json.

  ## Parameters

    * `config` - Drive configuration map with keys:
      * `:path` (required) - Absolute path to the storage directory
      * `:tier` (required) - Storage tier: `:hot`, `:warm`, or `:cold`
      * `:capacity` (optional) - Capacity string (e.g. "1T", "500G") or integer bytes (default: 0)
      * `:id` (optional) - Unique drive ID (auto-generated from path if not provided)

  ## Returns

    * `{:ok, drive_map}` - The registered drive as a serialisable map
    * `{:error, reason}` - Validation or registration failure
  """
  @spec add_drive(map()) :: {:ok, map()} | {:error, term()}
  def add_drive(config) when is_map(config) do
    # 60 s rather than the default 5 s — inner operations (BlobStore.open_store,
    # the bootstrap-layer Ra writes) already pass `:infinity`, and on real
    # hardware the first add_drive after `RaServer.init_cluster()` can land in
    # the leader-settle window, blowing the 5 s budget.
    GenServer.call(__MODULE__, {:add_drive, config}, 60_000)
  end

  @doc """
  Removes a drive at runtime.

  Refuses when the removal would drop a volume below its `min_copies`
  (`NeonFS.Core.ReplicaAudit.guard_removal/3`), then checks for data on
  the drive: if data exists and `:force` is not set, returns
  `{:error, :drive_has_data}`. Otherwise closes the BlobStore handle,
  deregisters from DriveRegistry, stops DriveState, and persists to cluster.json.

  ## Parameters

    * `drive_id` - Drive identifier to remove
    * `opts` - Optional keyword list:
      * `:force` - Skip the data check and override a below-`min_copies`
        replica finding (default: `false`). Never overrides `_system`
        being left with no surviving copy.

  ## Returns

    * `:ok` - Drive removed successfully
    * `{:error, reason}` - Failure reason
  """
  @spec remove_drive(String.t(), keyword()) :: :ok | {:error, term()}
  def remove_drive(drive_id, opts \\ []) do
    # Same 60 s budget as `add_drive/1` — the inner data-presence check can be
    # slow on large drives.
    GenServer.call(__MODULE__, {:remove_drive, drive_id, opts}, 60_000)
  end

  @doc """
  Validates a drive configuration without mutating any state.

  Runs the same shape, path-exists, and path-writable checks as
  `add_drive/1`, but with no side effects. Cluster bootstrap calls
  this *before* persisting cluster state or starting Ra so a
  read-only drive path fails fast and leaves the daemon untouched.

  Returns `:ok` on success or `{:error, reason}` with a
  human-readable string explaining the failure.
  """
  @spec preflight_drive_config(map()) :: :ok | {:error, term()}
  def preflight_drive_config(config) when is_map(config) do
    with {:ok, parsed} <- validate_drive_config(config),
         :ok <- check_path_exists(parsed.path) do
      check_path_writable(parsed.path)
    end
  end

  @doc """
  Registers every locally-managed drive in the Ra bootstrap layer.

  Drives configured at startup land in `DriveRegistry`'s ETS table
  before Ra is up, so they never make it into the bootstrap layer
  through the regular `add_drive/1` path. This helper is invoked by
  `Cluster.Init.init_cluster/1` after Ra is available, so the
  bootstrap layer reflects the local drives before the system
  volume tries to provision per-volume metadata.

  Best-effort per drive — a failed Ra command logs a warning and
  the next drive is attempted. Returns `:ok` either way.
  """
  @spec register_local_drives_in_bootstrap() :: :ok
  def register_local_drives_in_bootstrap do
    DriveRegistry.drives_for_node(Node.self())
    |> Enum.each(&register_drive_in_bootstrap_layer/1)

    scale_system_volume_to_drive_count()
  end

  @doc """
  Lists all drives across the cluster as serialisable maps.

  ## Options

    * `:node` - Filter to drives on a specific node (atom). If omitted, returns all nodes.

  ## Returns

    * List of drive info maps sorted by `{node, id}`
  """
  @spec list_all_drives(keyword()) :: [map()]
  def list_all_drives(opts \\ []) do
    GenServer.call(__MODULE__, {:list_all_drives, opts})
  end

  @doc """
  Lists all local drives as serialisable maps.
  """
  @spec list_drives() :: [map()]
  def list_drives do
    GenServer.call(__MODULE__, :list_drives)
  end

  @doc """
  Reacts to how a drive presented itself at open. A `:dirty`
  drive (one that came back from an unclean shutdown) is marked
  `:unverified` — so it stops counting toward `min_copies` and reads
  verify-on-read — and a drive-scoped scrub is queued to
  verify it at high priority; on a clean clear it returns to `:trusted`.
  `:clean` and `:fresh` drives need nothing.

  The scrub is queued whether or not the mark reported success, and that
  is the point. A mark is a Ra command, and a command whose reply is lost
  during a cold reform has still been committed — so a reported failure
  covers both "the drive is not marked" and "the drive is marked and
  nobody here knows it". Skipping the scrub in the second case leaves the
  drive `:unverified` with nothing that will ever clear it, and an
  unverified drive holds the whole cluster in `:recovering` until the
  recovery timeout. Running it resolves both: a clean drive-scoped scrub
  ends in `mark_trusted`, which clears a mark that landed and is a no-op
  for one that did not.

  Returns `{:error, reason}` when the mark did not report success, so a
  caller can tell that the drive spent its verification window still
  counting toward `min_copies`.

  The `:mark_fn` and `:scrub_fn` deps are injectable for tests; they
  default to `DriveTrust.mark_unverified/2` and a drive-scoped
  `JobTracker` scrub.
  """
  @spec recover_drive(drive_id :: String.t(), :clean | :dirty | :fresh | nil, keyword()) ::
          :ok | {:error, term()}
  def recover_drive(drive_id, open_state, opts \\ [])

  def recover_drive(drive_id, :dirty, opts) do
    mark_fn = Keyword.get(opts, :mark_fn, &DriveTrust.mark_unverified/2)
    scrub_fn = Keyword.get(opts, :scrub_fn, &default_scrub/1)

    result = mark_fn.(Node.self(), drive_id)
    scrub_fn.(drive_id)

    case result do
      :ok ->
        :telemetry.execute(
          [:neonfs, :drive_manager, :dirty_drive_recovered],
          %{},
          %{drive_id: drive_id}
        )

        :ok

      {:error, reason} ->
        Logger.warning("Could not mark dirty drive :unverified; its scrub still runs",
          drive_id: drive_id,
          reason: inspect(reason)
        )

        {:error, reason}
    end
  end

  def recover_drive(_drive_id, _open_state, _opts), do: :ok

  ## Server Callbacks

  @impl true
  def init(_opts) do
    drives = Application.get_env(:neonfs_core, :drives, [])

    command_module =
      Application.get_env(:neonfs_core, :drive_command_module, DriveCommand.Default)

    start_drive_state_children(drives, command_module)
    attach_trust_telemetry()

    {:ok, %{command_module: command_module}, {:continue, :recover_dirty_drives}}
  end

  @impl true
  def handle_cast(:maybe_auto_uncordon, state) do
    attempt_auto_uncordon()
    {:noreply, state}
  end

  @impl true
  def handle_cast({:verify_drive, drive_id}, state) do
    verify_drive(drive_id)
    {:noreply, state}
  end

  @impl true
  def handle_continue(:recover_dirty_drives, state) do
    attempt_dirty_drive_recovery(0)
    {:noreply, state}
  end

  # Runs after the `add_drive` reply so the caller isn't charged a
  # cluster-wide volume write on top of its own 60 s budget.
  @impl true
  def handle_continue(:scale_system_volume, state) do
    scale_system_volume_to_drive_count()
    {:noreply, state}
  end

  @impl true
  def handle_info({:recover_dirty_drives, attempt}, state) do
    attempt_dirty_drive_recovery(attempt)
    {:noreply, state}
  end

  @impl true
  def handle_call({:add_drive, config}, _from, state) do
    case do_add_drive(config, state.command_module) do
      {:ok, %Drive{} = drive} ->
        :telemetry.execute(
          [:neonfs, :drive_manager, :add],
          %{},
          %{drive_id: drive.id, tier: drive.tier}
        )

        safe_broadcast_drive(%DriveAdded{
          node: drive.node,
          drive_id: drive.id,
          drive: drive_to_event_map(drive)
        })

        # A drive added at runtime may itself be returning dirty —
        # Ra is already up here, so react immediately.
        recover_drive(drive.id, Map.get(BlobStore.drive_open_states(), drive.id))

        {:reply, {:ok, drive_to_map(drive)}, state, {:continue, :scale_system_volume}}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:remove_drive, drive_id, opts}, _from, state) do
    case do_remove_drive(drive_id, opts) do
      :ok ->
        :telemetry.execute(
          [:neonfs, :drive_manager, :remove],
          %{},
          %{drive_id: drive_id}
        )

        safe_broadcast_drive(%DriveRemoved{
          node: Node.self(),
          drive_id: drive_id
        })

        {:reply, :ok, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:list_all_drives, opts}, _from, state) do
    drives =
      case Keyword.get(opts, :node) do
        nil -> DriveRegistry.list_drives()
        node -> DriveRegistry.drives_for_node(node)
      end
      |> Enum.sort_by(&{&1.node, &1.id})
      |> Enum.map(&drive_to_map/1)

    {:reply, drives, state}
  end

  def handle_call(:list_drives, _from, state) do
    drives =
      DriveRegistry.drives_for_node(Node.self())
      |> Enum.map(&drive_to_map/1)

    {:reply, drives, state}
  end

  ## Private — Add drive

  defp do_add_drive(config, command_module) do
    with {:ok, parsed} <- validate_drive_config(config),
         :ok <- check_path_exists(parsed.path),
         :ok <- check_path_writable(parsed.path),
         :ok <- ensure_drive_identity(parsed),
         :ok <- check_id_unique(parsed.id),
         {:ok, _drive_id} <- BlobStore.open_store(parsed, timeout: :infinity),
         drive = parsed |> Drive.from_config(Node.self()) |> DriveConfig.detect_capacity(),
         :ok <- DriveRegistry.register_drive(drive, timeout: :infinity),
         :ok <- start_single_drive_state(parsed, command_module),
         :ok <- validate_capacity(drive) do
      register_drive_in_bootstrap_layer(drive)
      warn_on_persistence_failure(save_drives_to_cluster_state(), "add", drive.id)
      {:ok, drive}
    end
  end

  # Mirrors the ETS register_drive into the Ra-replicated bootstrap
  # layer. During multi-node cluster formation the joining
  # node's `:register_drive` command can transiently fail (`:noproc`,
  # leadership in flux, a command timeout) before Ra settles; a bounded
  # retry closes that window rather than silently dropping the drive
  # from the bootstrap layer, where `Volume.Provisioner` and the
  # create-time durability gate would never see it.
  # Still best-effort after the retries are exhausted — the ETS write
  # is the source of truth and anti-entropy reconciles — but the
  # give-up is surfaced via telemetry rather than a silent single-shot.
  # Cluster-critical data — the CA key, cluster identity, serial and CRL —
  # lives on `_system`, whose replication factor used to track core-node
  # count alone. A single-core cluster therefore kept exactly one copy of
  # it however many drives were attached, so "add a drive for resilience"
  # bought none for the data whose loss is unrecoverable. Raise the
  # factor with the drive
  # count instead, capped at 3 — past that the extra copies cost more
  # than they buy for a volume this small.
  #
  # Only `_system`: auto-raising a user's factor-1 volume would change
  # durability they chose explicitly and multiply their space usage
  # unasked. Never lowers either — `adjust_system_volume_replication/1`
  # ignores anything at or below the current factor, so losing a drive
  # leaves the target where it was for repair to satisfy.
  #
  # Best-effort: a drive is registered whether or not this succeeds. During
  # `cluster init` the drives are registered before the volume exists, so
  # that call finds `:not_found`; `Cluster.Init` calls this again once the
  # volume is there.
  @doc """
  Raises the `_system` volume's replication factor to the cluster's drive
  count, capped at #{@system_volume_factor_cap}.

  Invoked after a drive is registered, and once by `cluster init` after the
  volume exists — during init the drives are registered first, so the
  registration-time call finds no volume to raise.

  Best-effort and raise-only: it never lowers the factor, and it reports
  `:ok` whether or not the adjustment landed.
  """
  @spec scale_system_volume_to_drive_count() :: :ok
  def scale_system_volume_to_drive_count do
    with {:ok, count} when count > 1 <- cluster_drive_count() do
      apply_system_volume_factor(min(count, @system_volume_factor_cap))
    end

    :ok
  end

  defp apply_system_volume_factor(factor) do
    case VolumeRegistry.adjust_system_volume_replication(factor) do
      {:ok, volume} ->
        Logger.info("System volume replication target tracks drive count",
          factor: volume.durability.factor
        )

      {:error, reason} ->
        Logger.debug("Skipped system volume replication adjustment", reason: inspect(reason))
    end
  catch
    :exit, reason ->
      Logger.debug("Volume registry unavailable for replication adjustment",
        reason: inspect(reason)
      )
  end

  # The count must be cluster-wide and current: `DriveRegistry`'s ETS is a
  # polled cache that under-counts remote drives, and a local Ra query can
  # miss the registration we just committed through the leader. A
  # consistent query costs one round trip on an operation that happens
  # once per drive.
  defp cluster_drive_count do
    case RaSupervisor.query(&MetadataStateMachine.get_drives/1) do
      {:ok, drives} when is_map(drives) -> {:ok, map_size(drives)}
      other -> other
    end
  catch
    :exit, reason -> {:error, reason}
  end

  defp register_drive_in_bootstrap_layer(%Drive{} = drive) do
    case current_cluster_id() do
      {:ok, cluster_id} ->
        entry = %{
          drive_id: drive.id,
          node: drive.node,
          cluster_id: cluster_id,
          on_disk_format_version: 1,
          registered_at: DateTime.utc_now()
        }

        register_in_bootstrap_with_retry(drive, entry, @bootstrap_register_attempts)

      {:error, reason} ->
        Logger.warning("Skipping bootstrap-layer drive registration (no cluster id)",
          drive_id: drive.id,
          reason: inspect(reason)
        )

        :ok
    end
  end

  defp register_in_bootstrap_with_retry(drive, entry, attempts_left) do
    case RaSupervisor.command({:register_drive, entry}) do
      {:ok, _result, _leader} ->
        :ok

      _transient when attempts_left > 1 ->
        Process.sleep(bootstrap_register_backoff_ms())
        register_in_bootstrap_with_retry(drive, entry, attempts_left - 1)

      failure ->
        :telemetry.execute(
          [:neonfs, :drive_manager, :bootstrap_register_failed],
          %{attempts: @bootstrap_register_attempts},
          %{drive_id: drive.id, node: drive.node}
        )

        Logger.warning(
          "Failed to register drive in bootstrap layer after #{@bootstrap_register_attempts} attempts",
          drive_id: drive.id,
          reason: inspect(failure)
        )

        :ok
    end
  end

  defp bootstrap_register_backoff_ms do
    Application.get_env(
      :neonfs_core,
      :bootstrap_register_backoff_ms,
      @default_bootstrap_register_backoff_ms
    )
  end

  defp deregister_drive_in_bootstrap_layer(drive_id) do
    case RaSupervisor.command({:deregister_drive, {Node.self(), drive_id}}) do
      {:ok, _result, _leader} ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to deregister drive from bootstrap layer",
          drive_id: drive_id,
          reason: inspect(reason)
        )

        :ok
    end
  end

  defp ensure_drive_identity(parsed) do
    case current_cluster_id() do
      {:ok, cluster_id} ->
        case Identity.ensure(parsed.path, cluster_id, parsed.id) do
          :ok ->
            :ok

          {:error, {:foreign_cluster, expected: expected, actual: actual}} ->
            {:error,
             "Drive at #{parsed.path} belongs to a different cluster " <>
               "(found cluster_id=#{actual}, this cluster is #{expected}). " <>
               "Refusing to add."}

          {:error, {:drive_id_mismatch, expected: expected, actual: actual}} ->
            {:error,
             "Drive at #{parsed.path} was previously registered as drive_id=#{actual}; " <>
               "refusing to re-register as #{expected}. Remove and re-add with the " <>
               "original id, or use a different drive."}

          {:error, reason} ->
            {:error, "Cannot validate drive identity at #{parsed.path}: #{inspect(reason)}"}
        end

      {:error, reason} ->
        {:error, "Cannot determine local cluster_id: #{inspect(reason)}"}
    end
  end

  defp current_cluster_id do
    case State.load() do
      {:ok, %State{cluster_id: cluster_id}} -> {:ok, cluster_id}
      {:error, _} = error -> error
    end
  end

  defp validate_drive_config(config) do
    raw_path = to_string(config[:path] || config["path"] || "")
    path = Drive.normalize_path(raw_path)
    tier_raw = config[:tier] || config["tier"] || "hot"
    capacity_raw = config[:capacity] || config["capacity"] || "0"
    id = to_string(config[:id] || config["id"] || generate_drive_id(path))

    with {:ok, tier} <- parse_tier(tier_raw),
         {:ok, capacity} <- parse_capacity(capacity_raw) do
      {:ok, %{id: id, path: path, tier: tier, capacity: capacity}}
    end
  end

  defp parse_tier(tier) when tier in @valid_tiers, do: {:ok, tier}

  defp parse_tier(tier) when is_binary(tier) and tier in ["hot", "warm", "cold"],
    do: {:ok, String.to_existing_atom(tier)}

  defp parse_tier(_), do: {:error, "Invalid tier. Must be hot, warm, or cold"}

  defp parse_capacity(capacity) when is_integer(capacity) and capacity >= 0, do: {:ok, capacity}

  defp parse_capacity(capacity) when is_binary(capacity) do
    DriveConfig.parse_capacity(capacity)
  end

  defp parse_capacity(_), do: {:ok, 0}

  defp check_path_exists(path) when byte_size(path) == 0, do: {:error, "Path is required"}

  defp check_path_exists(path) do
    case File.stat(path) do
      {:ok, %{type: :directory}} -> :ok
      {:ok, _} -> {:error, "Path exists but is not a directory: #{path}"}
      {:error, :enoent} -> {:error, "Path does not exist: #{path}"}
      {:error, reason} -> {:error, "Cannot access path #{path}: #{reason}"}
    end
  end

  defp check_path_writable(path) do
    probe = Path.join(path, ".neonfs-probe-#{System.unique_integer([:positive])}")
    result = probe_write(probe)
    _ = File.rm(probe)

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        {:error,
         "Path #{path} is not writable by the daemon (#{:file.format_error(reason)}); " <>
           "try `chown neonfs:neonfs #{path}`"}
    end
  end

  defp probe_write(probe) do
    case :file.open(probe, [:write, :raw]) do
      {:ok, fd} ->
        write_result =
          case :file.write(fd, "neonfs-probe") do
            :ok -> :file.sync(fd)
            err -> err
          end

        _ = :file.close(fd)
        write_result

      {:error, _reason} = err ->
        err
    end
  end

  defp check_id_unique(drive_id) do
    case DriveRegistry.get_drive(Node.self(), drive_id) do
      {:ok, _} -> {:error, {:duplicate_drive_id, drive_id}}
      {:error, :not_found} -> :ok
    end
  end

  defp validate_capacity(drive) do
    DriveConfig.validate_drives([drive])
    :ok
  end

  defp generate_drive_id(path) do
    path
    |> Path.basename()
    |> String.replace(~r/[^a-zA-Z0-9_-]/, "_")
  end

  ## Private — Remove drive

  defp do_remove_drive(drive_id, opts) do
    force = Keyword.get(opts, :force, false)

    with {:ok, _drive} <- get_local_drive(drive_id),
         :ok <- check_drive_data(drive_id, force),
         :ok <- check_critical_replicas(drive_id, force),
         :ok <- BlobStore.close_store(drive_id, timeout: :infinity),
         :ok <- DriveRegistry.deregister_drive(drive_id, timeout: :infinity),
         :ok <- stop_drive_state(drive_id) do
      deregister_drive_in_bootstrap_layer(drive_id)
      warn_on_persistence_failure(save_drives_to_cluster_state(), "remove", drive_id)
      :ok
    end
  end

  defp get_local_drive(drive_id) do
    case DriveRegistry.get_drive(Node.self(), drive_id) do
      {:ok, drive} -> {:ok, drive}
      {:error, :not_found} -> {:error, {:unknown_drive, drive_id}}
    end
  end

  # Removal abandons whatever is on the drive, so it is guarded on what
  # would survive rather than on `drive_has_data?` alone. `:force`
  # reaches the guard rather than skipping it: it overrides a
  # below-`min_copies` finding, but not `_system` being left with no
  # surviving copy.
  #
  # Runs *after* `check_drive_data/2` so an unforced removal of a drive
  # with data still answers `:drive_has_data` — the CLI turns that into
  # "run `drive evacuate` first", which is the right advice and better
  # than a replica diagnostic. The guard's own case is the force path,
  # where the data check is skipped and abandoning the last copies is
  # exactly what the operator is about to do.
  defp check_critical_replicas(drive_id, force) do
    ReplicaAudit.guard_removal(Node.self(), drive_id, force: force, operation: "Removing")
  end

  defp check_drive_data(_drive_id, true = _force), do: :ok

  defp check_drive_data(drive_id, _force) do
    case BlobStore.drive_has_data?(drive_id, timeout: :infinity) do
      {:ok, false} -> :ok
      {:ok, true} -> {:error, :drive_has_data}
      {:error, reason} -> {:error, reason}
    end
  end

  defp stop_drive_state(drive_id) do
    via = DriveState.via_tuple(drive_id)

    case GenServer.whereis(via) do
      nil ->
        :ok

      pid ->
        DynamicSupervisor.terminate_child(@drive_state_supervisor, pid)
    end
  end

  ## Private — Cluster state persistence

  defp save_drives_to_cluster_state do
    drives =
      DriveRegistry.drives_for_node(Node.self())
      |> Enum.map(fn drive ->
        %{
          "id" => drive.id,
          "path" => drive.path,
          "tier" => Atom.to_string(drive.tier),
          "capacity" => to_string(drive.capacity_bytes)
        }
      end)

    State.update_drives(drives)
  end

  defp warn_on_persistence_failure(:ok, _operation, _drive_id), do: :ok

  defp warn_on_persistence_failure({:error, reason}, operation, drive_id) do
    Logger.warning(
      "Drive #{operation} succeeded but config will not survive restart: #{inspect(reason)}",
      drive_id: drive_id
    )
  end

  ## Private — DriveState management

  defp start_drive_state_children(drives, command_module) do
    Enum.each(drives, fn config ->
      start_single_drive_state(config, command_module)
    end)
  end

  # Marking a drive `:unverified` is a Ra command, so the boot-time
  # reaction waits until Ra has a leader, then reacts to every drive that
  # opened dirty. Retries on a fixed interval until Ra is ready or the
  # retry budget runs out.
  defp attempt_dirty_drive_recovery(attempt) do
    cond do
      RaServer.initialized?() ->
        react_to_dirty_drives()
        # If this node was cordoned and came back clean (or its drives have
        # already cleared), resume immediately.
        attempt_auto_uncordon()

      attempt < @dirty_recovery_max_retries ->
        Process.send_after(
          self(),
          {:recover_dirty_drives, attempt + 1},
          @dirty_recovery_interval_ms
        )

      true ->
        Logger.warning("Ra not ready after #{attempt} attempts; skipping dirty-drive recovery")
    end
  end

  # Auto-uncordon: a node that was cordoned for maintenance
  # resumes on its own once it's back and its drives are trusted again —
  # zero operator step on the happy path. A clean reboot's drives are
  # already `:trusted` (so it resumes immediately); a dirty one's drives
  # clear once their scoped scrub passes, and the
  # `set_drive_trust` telemetry re-drives this check as each clears.
  defp attempt_auto_uncordon do
    if auto_uncordon?(NodeRegistry.status(Node.self()), local_drive_unverified?()) do
      case NodeRegistry.set_status(Node.self(), :active) do
        :ok ->
          :telemetry.execute([:neonfs, :drive_manager, :auto_uncordoned], %{}, %{
            node: Node.self()
          })

        {:error, reason} ->
          Logger.warning("Could not auto-uncordon node", reason: inspect(reason))
      end
    end
  end

  @doc false
  # Pure decision: a `:maintenance` node with no still-`:unverified` local
  # drive is caught up and can resume. Exposed for testing.
  @spec auto_uncordon?(NodeRegistry.status() | nil, boolean()) :: boolean()
  def auto_uncordon?(:maintenance, false), do: true
  def auto_uncordon?(_status, _local_drive_unverified?), do: false

  defp local_drive_unverified? do
    Enum.any?(DriveTrust.unverified(), fn {node, _drive_id} -> node == Node.self() end)
  end

  @auto_uncordon_handler_id "neonfs-drive-manager-auto-uncordon"

  # React to a local drive's trust changing: a clear back to `:trusted`
  # re-checks whether this cordoned node can now resume, and a drive
  # arriving at `:unverified` gets a scrub, because verification is the
  # only thing that clears it again.
  #
  # Keying the scrub off the observed transition rather than off the
  # write that caused it is what makes the boot path safe. A mark is a Ra
  # command, and one whose reply is lost during a cold reform commits
  # anyway — possibly *after* the scrub that was meant to clear it has
  # already finished. Then the drive is `:unverified` with no scrub
  # coming, and an unverified drive holds the whole cluster in
  # `:recovering` until the recovery timeout.
  #
  # The captured pid makes a post-restart stale handler a harmless no-op
  # (a cast to a dead pid is dropped); init re-attaches fresh.
  defp attach_trust_telemetry do
    pid = self()
    :telemetry.detach(@auto_uncordon_handler_id)

    :telemetry.attach(
      @auto_uncordon_handler_id,
      [:neonfs, :ra, :command, :set_drive_trust],
      fn _event, _measurements, metadata, _config -> react_to_trust_change(pid, metadata) end,
      nil
    )
  end

  defp react_to_trust_change(pid, %{node: drive_node, to: :trusted}) when drive_node == node() do
    GenServer.cast(pid, :maybe_auto_uncordon)
  end

  defp react_to_trust_change(pid, %{node: drive_node, to: :unverified, drive_id: drive_id})
       when drive_node == node() do
    GenServer.cast(pid, {:verify_drive, drive_id})
  end

  defp react_to_trust_change(_pid, _metadata), do: :ok

  # One scrub per drive at a time: a drive marked `:unverified` twice in
  # quick succession needs one verification, not two.
  defp verify_drive(drive_id) when is_binary(drive_id) do
    if scrub_running?(drive_id) do
      :ok
    else
      default_scrub(drive_id)
    end
  end

  defp verify_drive(_drive_id), do: :ok

  defp scrub_running?(drive_id) do
    JobTracker.list(type: Scrub, status: :running)
    |> Enum.any?(&(&1.params[:drive_id] == drive_id))
  catch
    # A JobTracker that isn't up yet can't be running a scrub either.
    :exit, _ -> false
  end

  defp react_to_dirty_drives do
    BlobStore.drive_open_states()
    |> Enum.each(fn {drive_id, open_state} -> recover_drive(drive_id, open_state) end)
  rescue
    # The storage layer may not be ready in a degraded boot — a missed
    # reaction is recovered by the next scrub cadence, so don't crash.
    error ->
      Logger.warning("Dirty-drive recovery pass failed", reason: Exception.message(error))
  end

  defp default_scrub(drive_id) do
    case JobTracker.create(Scrub, %{drive_id: drive_id}) do
      {:ok, _job} ->
        :ok

      {:error, reason} ->
        Logger.warning("Could not queue drive scrub", drive_id: drive_id, reason: inspect(reason))
    end
  end

  defp start_single_drive_state(config, command_module) do
    drive_state_opts = drive_state_opts_from_config(config, command_module)
    drive_id = Keyword.fetch!(drive_state_opts, :drive_id)

    child_spec = %{
      id: {NeonFS.Core.DriveState, drive_id},
      start: {NeonFS.Core.DriveState, :start_link, [drive_state_opts]},
      restart: :permanent
    }

    case DynamicSupervisor.start_child(@drive_state_supervisor, child_spec) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> {:error, {:drive_state_failed, drive_id, reason}}
    end
  end

  defp drive_state_opts_from_config(config, command_module) do
    [
      drive_id: to_string(config[:id] || config["id"]),
      drive_path: to_string(config[:path] || config["path"]),
      power_management: config[:power_management] || config["power_management"] || false,
      idle_timeout: config[:idle_timeout] || config["idle_timeout"] || 1800,
      command_module: command_module
    ]
  end

  ## Private — Serialisation

  defp drive_to_map(%Drive{} = drive) do
    %{
      id: drive.id,
      node: Atom.to_string(drive.node),
      path: drive.path,
      tier: Atom.to_string(drive.tier),
      capacity_bytes: drive.capacity_bytes,
      used_bytes: drive.used_bytes,
      state: Atom.to_string(drive.state)
    }
  end

  defp drive_to_event_map(%Drive{} = drive) do
    %{
      id: drive.id,
      node: drive.node,
      path: drive.path,
      tier: drive.tier,
      capacity_bytes: drive.capacity_bytes,
      used_bytes: drive.used_bytes,
      state: drive.state,
      power_management: drive.power_management,
      idle_timeout: drive.idle_timeout
    }
  end

  defp safe_broadcast_drive(event) do
    Broadcaster.broadcast_drive_event(event)
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end
end
