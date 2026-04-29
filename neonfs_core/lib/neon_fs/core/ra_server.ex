defmodule NeonFS.Core.RaServer do
  @moduledoc """
  GenServer wrapper for starting and managing the Ra server.

  This wrapper handles the initialization of the Ra server. Ra is NOT started
  automatically - it must be explicitly started via either:
  - `init_cluster/0` - for founding a new cluster
  - `join_cluster/1` - for joining an existing cluster

  This deferred startup ensures nodes don't form independent single-node clusters
  before they have a chance to join an existing cluster.
  """

  use GenServer

  alias NeonFS.Cluster.State, as: ClusterState
  alias NeonFS.Core.MetadataStateMachine

  require Logger

  @cluster_name :neonfs_meta

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Initialize Ra as a founding single-node cluster.

  This should be called on the first node when creating a new cluster.
  Other nodes should use `join_cluster/1` instead.
  """
  @spec init_cluster() :: :ok | {:error, term()}
  def init_cluster do
    GenServer.call(__MODULE__, :init_cluster, 60_000)
  end

  @doc """
  Check if the Ra server has been initialized (either as founder or by joining).

  Returns false if the RaServer process is not running (e.g., Ra disabled).
  """
  @spec initialized?() :: boolean()
  def initialized? do
    GenServer.call(__MODULE__, :initialized?)
  catch
    :exit, {:noproc, _} -> false
  end

  @doc """
  Reset the Ra server for testing purposes.

  Stops the Ra server if running, deletes its state, and resets
  internal status to allow re-initialization. This should only be
  used in tests to achieve proper isolation between test cases.
  """
  @spec reset!() :: :ok
  def reset! do
    GenServer.call(__MODULE__, :reset!, 10_000)
  catch
    :exit, {:noproc, _} -> :ok
  end

  @doc """
  Start Ra and join an existing cluster.

  This should be called after the node has been added to the cluster via add_member
  on the leader. It will start the Ra server configured to join the existing cluster.
  """
  @spec join_cluster([atom()]) :: :ok | {:error, term()}
  def join_cluster(existing_members) when is_list(existing_members) do
    GenServer.call(__MODULE__, {:join_cluster, existing_members}, 60_000)
  end

  @doc """
  Operator-invoked quorum recovery from a surviving minority (#473).

  Snapshot-extracts the local Ra replica's state, force-deletes the
  Ra server (and its log), and rebootstraps a fresh single-node
  cluster with the extracted state injected via
  `MetadataStateMachine.init/1`'s `:initial_state` option. After this
  call returns, the survivor is a 1-node cluster with the cluster
  metadata preserved but every membership trace of the dropped
  nodes erased.

  The path is destructive: every command the dropped majority
  committed but never replicated to the survivor is lost. The
  caller (`NeonFS.CLI.Handler.handle_force_reset/1`) is responsible
  for the operator safety gates that surface the data-loss
  acknowledgement before this is invoked.

  Returns `{:ok, snapshot_path}` on success — the path of the
  on-disk backup of the extracted state, kept under
  `$NEONFS_DATA_DIR/ra/force-reset-snapshot-<timestamp>.bin` for
  paranoia (operators can inspect it post-hoc if the new cluster
  comes up wrong).
  """
  @spec force_reset_to_self() :: {:ok, Path.t()} | {:error, term()}
  def force_reset_to_self do
    GenServer.call(__MODULE__, :force_reset_to_self, 60_000)
  end

  @impl true
  def init(opts) do
    # Use handle_continue to ensure Ra application is ready, but don't start the server
    {:ok, %{opts: opts, status: :not_initialized, ra_pid: nil}, {:continue, :ensure_ra_ready}}
  end

  @impl true
  def handle_continue(:ensure_ra_ready, state) do
    # Ensure Ra application is started
    ensure_ra_started()

    # Check if the Ra system is ready (ETS tables created)
    case :ets.whereis(:ra_directory) do
      :undefined ->
        # Not ready yet — schedule a retry
        Process.send_after(self(), {:check_ra_system, 1}, 100)
        {:noreply, state}

      _tid ->
        {:noreply, state, {:continue, :ra_system_ready}}
    end
  end

  @impl true
  def handle_continue(:ra_system_ready, state) do
    Logger.info("Ra system is ready")

    # If this node was previously part of a cluster, Ra will have persisted
    # state on disk.  Auto-restart from that state so the node rejoins the
    # cluster without requiring an explicit init_cluster/join_cluster call.
    case try_auto_restart() do
      {:ok, status} ->
        Logger.info("Ra server auto-restarted from persisted state")
        {:noreply, %{state | status: status}}

      :no_persisted_state ->
        Logger.info("Ra system ready, waiting for cluster init or join")
        {:noreply, %{state | status: :waiting_for_cluster}}
    end
  end

  @impl true
  def handle_call(:initialized?, _from, state) do
    initialized = state.status in [:running, :joined]
    {:reply, initialized, state}
  end

  @impl true
  def handle_call(:reset!, _from, state) do
    # Stop and delete Ra server - always attempt regardless of internal state
    # Ra's registry may have stale data from previous test runs
    server_id = {@cluster_name, Node.self()}

    Logger.info("Resetting Ra server for testing")

    # Always attempt to stop, even if we think it's not running
    case :ra.stop_server(:default, server_id) do
      :ok -> Logger.debug("Stopped Ra server")
      {:error, reason} -> Logger.debug("Could not stop Ra server", reason: inspect(reason))
    end

    # Always attempt to delete, to clear Ra's registry
    case :ra.force_delete_server(:default, server_id) do
      :ok ->
        Logger.debug("Deleted Ra server state")

      {:error, reason} ->
        Logger.debug("Could not delete Ra server state", reason: inspect(reason))
    end

    wait_for_ra_cleanup(server_id)

    {:reply, :ok, %{state | status: :waiting_for_cluster}}
  end

  @impl true
  def handle_call(:init_cluster, _from, %{status: status} = state)
      when status in [:running, :joined] do
    {:reply, {:error, :already_initialized}, state}
  end

  @impl true
  def handle_call(:init_cluster, _from, state) do
    node_name = Node.self()
    server_id = {@cluster_name, node_name}
    sanitized_node = node_name |> to_string() |> String.replace(~r/[@\.]/, "_")

    # Ra expects machine config as tuple: {module, Mod, Args}
    machine_config = {:module, MetadataStateMachine, %{}}

    ra_config = %{
      id: server_id,
      uid: "neonfs_meta_#{sanitized_node}",
      cluster_name: @cluster_name,
      machine: machine_config,
      log_init_args: %{
        uid: "neonfs_meta_#{sanitized_node}"
      },
      initial_members: [server_id]
    }

    Logger.info("Initialising Ra cluster",
      cluster_name: inspect(@cluster_name),
      node: inspect(node_name)
    )

    Logger.debug("Ra config", config: inspect(ra_config))

    case start_or_restart_ra_server(ra_config, server_id) do
      {:ok, pid} when is_pid(pid) ->
        Logger.info("Ra server started successfully", pid: inspect(pid))
        trigger_and_wait_for_election(server_id)
        {:reply, :ok, %{state | status: :running, ra_pid: pid}}

      {:ok, :started} ->
        Logger.info("Ra server started successfully")
        trigger_and_wait_for_election(server_id)
        {:reply, :ok, %{state | status: :running}}

      {:ok, :restarted} ->
        Logger.info("Ra server restarted from persisted state")
        trigger_and_wait_for_election(server_id)
        {:reply, :ok, %{state | status: :running}}

      {:error, {:already_started, pid}} ->
        Logger.info("Ra server already running", pid: inspect(pid))
        trigger_and_wait_for_election(server_id)
        {:reply, :ok, %{state | status: :running, ra_pid: pid}}

      {:error, reason} ->
        Logger.error("Failed to start Ra server", reason: inspect(reason))
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:join_cluster, existing_members}, _from, %{status: status} = state)
      when status in [:running, :joined] do
    # Already running - this might be a restart scenario, try to reconfigure
    Logger.info("Ra already running, attempting to reconfigure for cluster join")
    do_join_cluster(existing_members, state)
  end

  @impl true
  def handle_call({:join_cluster, existing_members}, _from, state) do
    do_join_cluster(existing_members, state)
  end

  @impl true
  def handle_call(:force_reset_to_self, _from, %{status: status} = state)
      when status not in [:running, :joined] do
    {:reply,
     {:error,
      "Ra server is not in a running state (status=#{inspect(status)}); " <>
        "force-reset requires a live local replica to extract state from"}, state}
  end

  @impl true
  def handle_call(:force_reset_to_self, _from, state) do
    node_name = Node.self()
    server_id = {@cluster_name, node_name}
    sanitized_node = node_name |> to_string() |> String.replace(~r/[@\.]/, "_")

    Logger.warning("Force-reset: extracting local Ra state and rebootstrapping",
      cluster_name: inspect(@cluster_name),
      node: inspect(node_name)
    )

    with {:ok, snapshot_state} <- extract_local_ra_state(server_id),
         _ = Logger.debug("Force-reset: state extracted"),
         {:ok, snapshot_path} <- write_force_reset_snapshot(snapshot_state, sanitized_node),
         _ = Logger.debug("Force-reset: snapshot written"),
         :ok <- stop_and_delete_local_server(server_id),
         _ = Logger.debug("Force-reset: old server deleted"),
         :ok <- wait_for_ra_cleanup(server_id),
         _ = Logger.debug("Force-reset: cleanup done; starting fresh cluster"),
         {:ok, _pid_or_atom} <-
           start_force_reset_cluster(server_id, sanitized_node, snapshot_state) do
      Logger.warning(
        "Force-reset succeeded; survivor #{inspect(node_name)} is a fresh single-node cluster (snapshot=#{snapshot_path})"
      )

      {:reply, {:ok, snapshot_path}, %{state | status: :running}}
    else
      {:error, reason} ->
        Logger.error("Force-reset failed", reason: inspect(reason))
        {:reply, {:error, reason}, state}
    end
  end

  defp do_join_cluster(existing_members, state) do
    node_name = Node.self()
    server_id = {@cluster_name, node_name}
    sanitized_node = node_name |> to_string() |> String.replace(~r/[@\.]/, "_")

    Logger.info("Starting Ra server to join cluster", members: inspect(existing_members))

    # If there's an existing Ra server, stop and delete it first
    case :ra.stop_server(:default, server_id) do
      :ok ->
        Logger.info("Stopped existing Ra server")

      {:error, reason} ->
        Logger.debug("No existing Ra server to stop", reason: inspect(reason))
    end

    case :ra.force_delete_server(:default, server_id) do
      :ok ->
        Logger.info("Deleted existing Ra server state")

      {:error, reason} ->
        Logger.debug("No existing Ra server state to delete", reason: inspect(reason))
    end

    # Brief delay for Ra cleanup to complete
    receive do
    after
      100 -> :ok
    end

    # Build cluster configuration with existing members
    existing_server_ids = Enum.map(existing_members, &{@cluster_name, &1})

    ra_config = %{
      id: server_id,
      uid: "neonfs_meta_#{sanitized_node}",
      cluster_name: @cluster_name,
      machine: {:module, MetadataStateMachine, %{}},
      log_init_args: %{
        uid: "neonfs_meta_#{sanitized_node}"
      },
      # Include existing cluster members so we can sync from them
      initial_members: [server_id | existing_server_ids]
    }

    Logger.info("Starting Ra server with cluster config", config: inspect(ra_config))

    case :ra.start_server(:default, ra_config) do
      {:ok, pid} ->
        Logger.info("Ra server joined cluster successfully")
        :ra.trigger_election(server_id)
        {:reply, :ok, %{state | status: :joined, ra_pid: pid}}

      :ok ->
        Logger.info("Ra server joined cluster successfully")
        :ra.trigger_election(server_id)
        {:reply, :ok, %{state | status: :joined}}

      {:error, reason} ->
        Logger.error("Failed to start Ra server for cluster join", reason: inspect(reason))
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:check_ra_system, attempt}, state) do
    max_attempts = 50

    case :ets.whereis(:ra_directory) do
      :undefined when attempt >= max_attempts ->
        Logger.error("Ra system did not initialise after max attempts",
          max_attempts: max_attempts
        )

        {:noreply, state}

      :undefined ->
        Process.send_after(self(), {:check_ra_system, attempt + 1}, 100)
        {:noreply, state}

      _tid ->
        {:noreply, state, {:continue, :ra_system_ready}}
    end
  end

  @impl true
  def handle_info(msg, state) do
    # Handle unexpected messages (e.g., late replies from async operations)
    Logger.debug("RaServer received unexpected message", message: inspect(msg))
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    # Only try to stop if we successfully started the Ra server
    if state.status in [:running, :joined] do
      server_id = {@cluster_name, Node.self()}

      case :ra.stop_server(:default, server_id) do
        :ok ->
          Logger.info("Ra server stopped successfully")

        {:error, reason} ->
          Logger.warning("Failed to stop Ra server", reason: inspect(reason))
      end
    end

    :ok
  end

  # Ensure Ra application is started (system readiness checked via handle_continue/handle_info)
  defp ensure_ra_started do
    # Ensure Ra application is started
    case Application.ensure_all_started(:ra) do
      {:ok, started} ->
        if started != [] do
          Logger.info("Started Ra application", apps: inspect(started))
        end

      {:error, {app, reason}} ->
        Logger.error("Failed to start application", app: app, reason: inspect(reason))
    end

    # CRITICAL: Application.ensure_all_started(:ra) does NOT start the Ra system!
    # We must explicitly start the default system which creates the ETS tables
    # and registers the system in persistent_term.
    case :ra_system.start_default() do
      {:ok, _pid} ->
        Logger.info("Ra default system started")

      {:error, {:already_started, _pid}} ->
        Logger.debug("Ra default system already started")

      {:error, reason} ->
        Logger.error("Failed to start Ra default system", reason: inspect(reason))
    end
  end

  # Trigger an election and wait for it to complete
  defp trigger_and_wait_for_election(server_id, attempts \\ 0, max_attempts \\ 10) do
    Logger.info("Triggering Ra election", server_id: inspect(server_id))
    result = :ra.trigger_election(server_id, 15_000)
    Logger.debug("trigger_election result", result: inspect(result))

    # Brief delay to allow election to start, then check for leader
    # Single-node clusters elect instantly; multi-node may need retries
    receive do
    after
      50 -> :ok
    end

    # Check if we have a leader with a short timeout
    case :ra.members(server_id, 1000) do
      {:ok, members, leader} when leader != :undefined ->
        Logger.info("Ra election complete",
          leader: inspect(leader),
          members: inspect(members)
        )

        :ok

      {:ok, _members, :undefined} ->
        if attempts < max_attempts do
          Logger.debug("No leader yet, retrying election",
            attempt: attempts + 1,
            max_attempts: max_attempts
          )

          trigger_and_wait_for_election(server_id, attempts + 1, max_attempts)
        else
          Logger.warning("Failed to elect leader after max attempts", max_attempts: max_attempts)
          :timeout
        end

      {:timeout, _} ->
        if attempts < max_attempts do
          Logger.debug("Ra members query timed out, retrying",
            attempt: attempts + 1,
            max_attempts: max_attempts
          )

          trigger_and_wait_for_election(server_id, attempts + 1, max_attempts)
        else
          Logger.warning("Ra members query timed out after max attempts",
            max_attempts: max_attempts
          )

          :timeout
        end

      {:error, reason} ->
        Logger.error("Failed to query Ra members", reason: inspect(reason))
        {:error, reason}
    end
  end

  # Try to auto-restart the Ra server from persisted state.
  # Returns {:ok, :running} if successful, :no_persisted_state if no data exists.
  #
  # Does NOT create new clusters — only restarts from existing data. If there's
  # no persisted state, returns :no_persisted_state and the node must explicitly
  # call init_cluster/0 or join_cluster/1.
  #
  # Only attempts restart when a ClusterState file exists on disk, proving
  # this node was previously part of a cluster (avoids picking up stale Ra
  # data in test environments or fresh installs).
  defp try_auto_restart do
    if ClusterState.exists?() do
      do_try_auto_restart()
    else
      :no_persisted_state
    end
  end

  defp do_try_auto_restart do
    server_id = {@cluster_name, Node.self()}

    case :ra.restart_server(:default, server_id) do
      :ok ->
        {:ok, :running}

      {:error, {:already_started, _}} ->
        {:ok, :running}

      {:error, :name_not_registered} ->
        # The Ra directory DETS (names.dets) was empty — typically caused by an
        # unclean VM shutdown before DETS buffers were flushed. Check if data
        # files actually exist on disk and try to recover the directory entry.
        try_recover_lost_directory(server_id)

      {:error, _reason} ->
        :no_persisted_state
    end
  end

  # Check if Ra data files exist on disk despite the directory entry being lost.
  # If they do, re-register the UID and retry the restart.
  # Dialyzer: Ra's register_name spec says pid() but accepts :undefined at runtime
  # (used in ra_directory:init/2 when loading from DETS with undefined pids).
  @dialyzer {:nowarn_function, try_recover_lost_directory: 1}
  defp try_recover_lost_directory(server_id) do
    sanitized_node = Node.self() |> to_string() |> String.replace(~r/[@\.]/, "_")
    uid = "neonfs_meta_#{sanitized_node}"

    ra_data_dir =
      Application.get_env(:ra, :data_dir, ~c"/var/lib/neonfs/ra") |> to_string()

    server_data_dir =
      Path.join([ra_data_dir, Atom.to_string(Node.self()), uid])

    if File.dir?(server_data_dir) do
      Logger.info("Ra directory entry lost but data files exist, recovering: #{server_data_dir}")

      # ServerName must be an atom (the first element of the server_id tuple),
      # NOT the full {name, node} tuple — Ra's directory uses it with whereis/1.
      {server_name, _node} = server_id

      :ra_directory.register_name(
        :default,
        uid,
        :undefined,
        :undefined,
        server_name,
        @cluster_name
      )

      case :ra.restart_server(:default, server_id) do
        :ok ->
          {:ok, :running}

        {:error, reason} ->
          Logger.warning("Ra recovery after re-register failed", reason: inspect(reason))
          :no_persisted_state
      end
    else
      :no_persisted_state
    end
  end

  # Start a new Ra server, or restart an existing one if it has persisted state
  defp start_or_restart_ra_server(ra_config, server_id) do
    case :ra.start_server(:default, ra_config) do
      {:ok, pid} -> {:ok, pid}
      :ok -> {:ok, :started}
      {:error, {:already_started, pid}} -> {:error, {:already_started, pid}}
      {:error, :not_new} -> restart_existing_server(ra_config, server_id)
      {:error, reason} -> {:error, reason}
    end
  end

  # Handle restarting a server that has persisted state from a previous run
  defp restart_existing_server(ra_config, server_id) do
    Logger.info("Ra server has persisted state, restarting...")

    case :ra.restart_server(:default, server_id) do
      :ok ->
        {:ok, :restarted}

      {:error, :name_not_registered} ->
        # Directory entry lost (e.g. DETS not flushed before crash).
        # In the init_cluster flow the safest option is to start fresh.
        force_fresh_start(ra_config, server_id)

      {:error, :enoent} ->
        force_fresh_start(ra_config, server_id)

      {:error, reason} ->
        {:error, {:restart_failed, reason}}
    end
  end

  # Poll until Ra's directory no longer holds a pid for this server.
  # Used after force_delete_server to avoid racing the next start_server call.
  defp wait_for_ra_cleanup(server_id, attempts \\ 0) do
    {_name, node} = server_id
    sanitized = node |> to_string() |> String.replace(~r/[@\.]/, "_")
    uid = "neonfs_meta_#{sanitized}"

    case :ra_directory.pid_of(:default, uid) do
      :undefined ->
        :ok

      _pid when attempts >= 50 ->
        Logger.warning("Ra cleanup did not complete after 5s, proceeding anyway")
        :ok

      _pid ->
        Process.sleep(100)
        wait_for_ra_cleanup(server_id, attempts + 1)
    end
  end

  # Data files were deleted but registry still thinks server exists
  defp force_fresh_start(ra_config, server_id) do
    Logger.info("Restart failed (files missing), cleaning up and starting fresh...")
    :ra.force_delete_server(:default, server_id)
    wait_for_ra_cleanup(server_id)

    case :ra.start_server(:default, ra_config) do
      {:ok, pid} -> {:ok, pid}
      :ok -> {:ok, :started}
      {:error, reason} -> {:error, {:fresh_start_failed, reason}}
    end
  end

  # ─── Force-reset helpers (#473) ────────────────────────────────────

  # Snapshot-extract the local replica's state. `local_query` doesn't
  # require quorum — exactly the property we need when the cluster
  # has lost majority. The query function is a `{m,f,a}` per Ra's
  # cross-cluster query convention; the `apply_query/2` helper in
  # `RaSupervisor` deserialises it.
  defp extract_local_ra_state(server_id) do
    query_mfa = {NeonFS.Core.RaSupervisor, :apply_query, [&Function.identity/1]}

    case :ra.local_query(server_id, query_mfa, 5_000) do
      {:ok, {_idxterm, state}, _local_server} ->
        {:ok, state}

      {:error, reason} ->
        {:error, {:local_query_failed, reason}}

      {:timeout, _} ->
        {:error, {:local_query_failed, :timeout}}
    end
  end

  # Persist the extracted state to disk before destroying anything,
  # so an operator can recover by hand if the rebootstrap fails.
  defp write_force_reset_snapshot(state, sanitized_node) do
    data_dir = ra_data_dir()
    snapshot_dir = Path.join(data_dir, "force-reset-snapshots")
    File.mkdir_p!(snapshot_dir)

    timestamp = System.system_time(:second)

    snapshot_path =
      Path.join(snapshot_dir, "#{sanitized_node}-#{timestamp}.bin")

    bin = :erlang.term_to_binary(state)
    File.write!(snapshot_path, bin)

    {:ok, snapshot_path}
  end

  defp stop_and_delete_local_server(server_id) do
    _ = :ra.stop_server(:default, server_id)

    case :ra.force_delete_server(:default, server_id) do
      :ok -> :ok
      {:error, :name_not_registered} -> :ok
      {:error, reason} -> {:error, {:force_delete_failed, reason}}
    end
  end

  defp start_force_reset_cluster(server_id, sanitized_node, initial_state) do
    machine_config = {:module, MetadataStateMachine, %{initial_state: initial_state}}

    ra_config = %{
      id: server_id,
      uid: "neonfs_meta_#{sanitized_node}",
      cluster_name: @cluster_name,
      machine: machine_config,
      log_init_args: %{uid: "neonfs_meta_#{sanitized_node}"},
      initial_members: [server_id]
    }

    case :ra.start_server(:default, ra_config) do
      {:ok, pid} ->
        trigger_and_wait_for_election(server_id)
        {:ok, pid}

      :ok ->
        trigger_and_wait_for_election(server_id)
        {:ok, :started}

      {:error, reason} ->
        {:error, {:start_force_reset_cluster_failed, reason}}
    end
  end

  defp ra_data_dir do
    Application.get_env(:neonfs_core, :ra_data_dir, "/var/lib/neonfs/ra")
  end
end
