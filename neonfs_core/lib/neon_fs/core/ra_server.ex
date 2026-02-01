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

  @impl true
  def init(opts) do
    # Use handle_continue to defer Ra server startup until after the GenServer is initialized
    {:ok, %{opts: opts}, {:continue, :start_ra_server}}
  end

  @impl true
  def handle_continue(:start_ra_server, state) do
    # Get Ra data directory
    data_dir = Keyword.get(state.opts, :data_dir)

    # Configure Ra's data directory
    Application.put_env(:ra, :data_dir, data_dir)

    # Ensure Ra system is started
    # This is required before any Ra servers can be started
    case :ra.start() do
      :ok ->
        :ok

      {:error, {:already_started, _}} ->
        :ok

      {:error, reason} ->
        Logger.warning("Ra system already running or failed: #{inspect(reason)}")
    end

    # Wait for Ra system to be fully ready
    # Ra starts asynchronously, so we need to wait for the directory ETS table
    wait_for_ra_system()

    # Get node name
    node_name = Node.self()

    # Sanitize node name for UID
    sanitized_node = node_name |> to_string() |> String.replace(~r/[@\.]/, "_")

    # Ra server configuration
    server_id = {@cluster_name, node_name}

    machine_config = %{
      module: MetadataStateMachine,
      init: fn -> MetadataStateMachine.init(%{}) end
    }

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
    case :ra.start_server(ra_config) do
      {:ok, pid} ->
        Logger.info("Ra server started successfully: #{inspect(pid)}")
        {:noreply, Map.put(state, :ra_pid, pid)}

      {:error, {:already_started, pid}} ->
        Logger.info("Ra server already running: #{inspect(pid)}")
        {:noreply, Map.put(state, :ra_pid, pid)}

      {:error, reason} ->
        Logger.error("Failed to start Ra server: #{inspect(reason)}")
        {:stop, {:ra_start_failed, reason}, state}
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
end
