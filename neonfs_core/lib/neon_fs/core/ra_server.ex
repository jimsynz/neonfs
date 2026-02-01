defmodule NeonFS.Core.RaServer do
  @moduledoc """
  GenServer wrapper for starting and managing the Ra server.

  This wrapper handles the asynchronous initialization of the Ra server,
  ensuring it starts after Ra's system is fully ready.
  """

  use GenServer

  alias NeonFS.Core.MetadataStateMachine

  require Logger

  @cluster_name :neonfs_meta

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Reconfigure the Ra server to join an existing cluster.

  This should be called after the node has been added to the cluster via add_member
  on the leader. It will stop the current Ra server (which was running as single-node),
  delete its state, and restart it with the proper cluster configuration.
  """
  @spec join_cluster([atom()]) :: :ok | {:error, term()}
  def join_cluster(existing_members) when is_list(existing_members) do
    GenServer.call(__MODULE__, {:join_cluster, existing_members}, 60_000)
  end

  @impl true
  def init(opts) do
    # Use handle_continue to defer Ra server startup until after the GenServer is initialized
    {:ok, %{opts: opts}, {:continue, :start_ra_server}}
  end

  @impl true
  def handle_continue(:start_ra_server, state) do
    # Ensure Ra application is started and system is initialized
    ensure_ra_started()

    # Get node name
    node_name = Node.self()

    # Sanitize node name for UID
    sanitized_node = node_name |> to_string() |> String.replace(~r/[@\.]/, "_")

    # Ra server configuration
    server_id = {@cluster_name, node_name}

    # Ra expects machine config as tuple: {module, Mod, Args}
    # Args is passed to Mod.init/1 to create the initial state
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

    Logger.info("Starting Ra server: #{inspect(@cluster_name)} on #{inspect(node_name)}")
    Logger.debug("Ra config: #{inspect(ra_config)}")

    # Start the Ra server
    case start_or_restart_ra_server(ra_config, server_id) do
      {:ok, pid} when is_pid(pid) ->
        Logger.info("Ra server started successfully: #{inspect(pid)}")
        trigger_and_wait_for_election(server_id)
        {:noreply, Map.put(state, :ra_pid, pid)}

      {:ok, :restarted} ->
        Logger.info("Ra server restarted from persisted state")
        trigger_and_wait_for_election(server_id)
        {:noreply, Map.put(state, :ra_status, :restarted)}

      {:error, {:already_started, pid}} ->
        Logger.info("Ra server already running: #{inspect(pid)}")
        # Still trigger election in case it hasn't happened yet
        trigger_and_wait_for_election(server_id)
        {:noreply, Map.put(state, :ra_pid, pid)}

      {:error, reason} ->
        Logger.error("Failed to start Ra server: #{inspect(reason)}")
        {:stop, {:ra_start_failed, reason}, state}
    end
  end

  @impl true
  def handle_call({:join_cluster, existing_members}, _from, state) do
    node_name = Node.self()
    server_id = {@cluster_name, node_name}
    sanitized_node = node_name |> to_string() |> String.replace(~r/[@\.]/, "_")

    Logger.info(
      "Reconfiguring Ra server to join cluster with members: #{inspect(existing_members)}"
    )

    # Step 1: Stop the current Ra server
    case :ra.stop_server(:default, server_id) do
      :ok ->
        Logger.info("Stopped current Ra server")

      {:error, reason} ->
        Logger.debug("Could not stop Ra server: #{inspect(reason)}")
    end

    # Step 2: Force delete the server's state
    case :ra.force_delete_server(:default, server_id) do
      :ok ->
        Logger.info("Deleted Ra server state")

      {:error, reason} ->
        Logger.debug("Could not delete Ra server state: #{inspect(reason)}")
    end

    # Give Ra time to clean up
    Process.sleep(1000)

    # Step 3: Start fresh with proper cluster configuration
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

    Logger.info("Starting Ra server with cluster config: #{inspect(ra_config)}")

    case :ra.start_server(:default, ra_config) do
      {:ok, pid} ->
        Logger.info("Ra server joined cluster successfully")
        # Trigger election - we might become a follower in the existing cluster
        :ra.trigger_election(server_id)
        {:reply, :ok, Map.put(state, :ra_pid, pid)}

      :ok ->
        # Ra 2.x can return just :ok for restarted servers
        Logger.info("Ra server joined cluster successfully (restarted)")
        :ra.trigger_election(server_id)
        {:reply, :ok, Map.put(state, :ra_status, :joined)}

      {:error, reason} ->
        Logger.error("Failed to start Ra server for cluster join: #{inspect(reason)}")
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    # Only try to stop if we successfully started the Ra server
    if Map.has_key?(state, :ra_pid) do
      server_id = {@cluster_name, Node.self()}

      case :ra.stop_server(server_id) do
        :ok ->
          Logger.info("Ra server stopped successfully")

        {:error, reason} ->
          Logger.warning("Failed to stop Ra server: #{inspect(reason)}")
      end
    end

    :ok
  end

  # Ensure Ra application is started and the system is ready
  defp ensure_ra_started do
    # Ensure Ra application is started
    case Application.ensure_all_started(:ra) do
      {:ok, started} ->
        if started != [] do
          Logger.info("Started Ra application: #{inspect(started)}")
        end

      {:error, {app, reason}} ->
        Logger.error("Failed to start #{app}: #{inspect(reason)}")
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
        Logger.error("Failed to start Ra default system: #{inspect(reason)}")
    end

    # Wait for Ra system to be fully ready (ETS tables created)
    case wait_for_ra_system() do
      :ok ->
        Logger.info("Ra system is ready")

      :timeout ->
        Logger.error("Ra system failed to initialize")
    end
  end

  # Wait for Ra system to be fully initialized
  # Ra starts asynchronously and creates ETS tables that we need
  defp wait_for_ra_system(attempts \\ 0, max_attempts \\ 50) do
    if attempts >= max_attempts do
      Logger.error("Ra system did not initialize after #{max_attempts} attempts")
      :timeout
    else
      # Check if the ra_directory ETS table exists
      case :ets.whereis(:ra_directory) do
        :undefined ->
          # Not ready yet, wait a bit and try again
          Process.sleep(100)
          wait_for_ra_system(attempts + 1, max_attempts)

        _tid ->
          # Ra system is ready
          :ok
      end
    end
  end

  # Trigger an election and wait for it to complete
  defp trigger_and_wait_for_election(server_id, attempts \\ 0, max_attempts \\ 10) do
    Logger.info("Triggering Ra election for #{inspect(server_id)}")
    result = :ra.trigger_election(server_id)
    Logger.debug("trigger_election result: #{inspect(result)}")

    # Give the election some time to complete
    Process.sleep(500)

    # Check if we have a leader
    case :ra.members(server_id, 5000) do
      {:ok, members, leader} when leader != :undefined ->
        Logger.info(
          "Ra election complete, leader: #{inspect(leader)}, members: #{inspect(members)}"
        )

        :ok

      {:ok, _members, :undefined} ->
        if attempts < max_attempts do
          Logger.debug(
            "No leader yet, retrying election (attempt #{attempts + 1}/#{max_attempts})"
          )

          trigger_and_wait_for_election(server_id, attempts + 1, max_attempts)
        else
          Logger.warning("Failed to elect leader after #{max_attempts} attempts")
          :timeout
        end

      {:timeout, _} ->
        if attempts < max_attempts do
          Logger.debug(
            "Ra members query timed out, retrying (attempt #{attempts + 1}/#{max_attempts})"
          )

          trigger_and_wait_for_election(server_id, attempts + 1, max_attempts)
        else
          Logger.warning("Ra members query timed out after #{max_attempts} attempts")
          :timeout
        end

      {:error, reason} ->
        Logger.error("Failed to query Ra members: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # Start a new Ra server, or restart an existing one if it has persisted state
  defp start_or_restart_ra_server(ra_config, server_id) do
    case :ra.start_server(ra_config) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:error, {:already_started, pid}}

      {:error, :not_new} ->
        # Server has persisted state from a previous run - restart it instead
        Logger.info("Ra server has persisted state, restarting...")

        case :ra.restart_server(server_id) do
          :ok ->
            # restart_server returns :ok, not {:ok, pid}
            # Get the actual pid from the ra process
            {:ok, :restarted}

          {:error, reason} ->
            {:error, {:restart_failed, reason}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
end
