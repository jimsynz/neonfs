defmodule NeonFS.TestCluster do
  @moduledoc """
  Manages containerised NeonFS clusters for integration testing.

  Provides functions to:
  - Start/stop multi-node clusters in Docker/Podman containers
  - Simulate failures (kill, pause, restart nodes)
  - Execute RPC calls against cluster nodes
  - Manage cluster lifecycle for tests
  """

  require Logger

  defstruct [:id, :nodes, :network, :container_runtime, :cookie]

  @type t :: %__MODULE__{
          id: String.t(),
          nodes: [node_info()],
          network: String.t(),
          container_runtime: :docker | :podman,
          cookie: String.t()
        }

  @type node_info :: %{
          name: atom(),
          container_id: String.t(),
          beam_node: node(),
          state: :running | :stopped
        }

  @doc """
  Start a new test cluster with the specified number of nodes.

  ## Options

  - `:nodes` - Number of nodes (default: 3)
  - `:runtime` - Container runtime, `:docker` or `:podman` (default: auto-detect)
  - `:image` - NeonFS container image (default: "neonfs:latest")

  ## Example

      {:ok, cluster} = TestCluster.start(nodes: 3)
      # Run tests...
      TestCluster.stop(cluster)
  """
  @spec start(keyword()) :: {:ok, t()} | {:error, term()}
  def start(opts \\ []) do
    node_count = Keyword.get(opts, :nodes, 3)
    runtime = Keyword.get(opts, :runtime, detect_runtime())
    image = Keyword.get(opts, :image, "neonfs:latest")

    cluster_id = generate_cluster_id()
    network = "neonfs-test-#{cluster_id}"
    cookie = generate_cookie()

    Logger.info("Starting #{node_count}-node test cluster #{cluster_id}")

    with :ok <- create_network(runtime, network),
         {:ok, nodes} <- start_nodes(runtime, network, image, node_count, cookie),
         :ok <- wait_for_nodes_ready(nodes) do
      cluster = %__MODULE__{
        id: cluster_id,
        nodes: nodes,
        network: network,
        container_runtime: runtime,
        cookie: cookie
      }

      Logger.info("Test cluster #{cluster_id} ready with #{node_count} nodes")
      {:ok, cluster}
    else
      {:error, reason} = error ->
        Logger.error("Failed to start cluster: #{inspect(reason)}")
        # Cleanup on failure
        cleanup_network(runtime, network)
        error
    end
  end

  @doc """
  Stop and remove all cluster resources.
  """
  @spec stop(t()) :: :ok
  def stop(%__MODULE__{} = cluster) do
    Logger.info("Stopping test cluster #{cluster.id}")

    for node <- cluster.nodes do
      stop_container(cluster.container_runtime, node.container_id)
    end

    remove_network(cluster.container_runtime, cluster.network)
    :ok
  end

  @doc """
  Kill a node abruptly (simulates crash).
  """
  @spec kill_node(t(), atom()) :: {:ok, t()} | {:error, term()}
  def kill_node(%__MODULE__{} = cluster, node_name) do
    case find_node(cluster, node_name) do
      nil ->
        {:error, :node_not_found}

      node ->
        Logger.info("Killing node #{node_name}")
        :ok = kill_container(cluster.container_runtime, node.container_id)
        updated_nodes = update_node_state(cluster.nodes, node_name, :stopped)
        {:ok, %{cluster | nodes: updated_nodes}}
    end
  end

  @doc """
  Restart a stopped node.
  """
  @spec restart_node(t(), atom()) :: {:ok, t()} | {:error, term()}
  def restart_node(%__MODULE__{} = cluster, node_name) do
    case find_node(cluster, node_name) do
      nil ->
        {:error, :node_not_found}

      node ->
        Logger.info("Restarting node #{node_name}")
        :ok = start_container(cluster.container_runtime, node.container_id)
        :ok = wait_for_node_ready(node)
        updated_nodes = update_node_state(cluster.nodes, node_name, :running)
        {:ok, %{cluster | nodes: updated_nodes}}
    end
  end

  @doc """
  Execute an RPC call against a specific node.
  """
  @spec rpc(t(), atom(), module(), atom(), [term()]) :: term()
  def rpc(%__MODULE__{} = cluster, node_name, module, function, args) do
    node = find_node(cluster, node_name) || raise "Node #{node_name} not found"

    # Connect to the remote node and execute RPC
    :rpc.call(node.beam_node, module, function, args)
  end

  # Private implementation functions

  defp detect_runtime do
    cond do
      System.find_executable("podman") -> :podman
      System.find_executable("docker") -> :docker
      true -> raise "No container runtime found (docker or podman)"
    end
  end

  defp generate_cluster_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end

  defp generate_cookie do
    :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
  end

  defp create_network(runtime, network) do
    cmd = runtime_cmd(runtime)
    {_output, 0} = System.cmd(cmd, ["network", "create", network], stderr_to_stdout: true)
    Logger.debug("Created network: #{network}")
    :ok
  rescue
    e ->
      Logger.error("Failed to create network: #{inspect(e)}")
      {:error, :network_creation_failed}
  end

  defp remove_network(runtime, network) do
    cmd = runtime_cmd(runtime)

    case System.cmd(cmd, ["network", "rm", network], stderr_to_stdout: true) do
      {_output, 0} ->
        Logger.debug("Removed network: #{network}")
        :ok

      {output, _code} ->
        Logger.warning("Failed to remove network #{network}: #{output}")
        :ok
    end
  end

  defp cleanup_network(runtime, network) do
    remove_network(runtime, network)
  end

  defp start_nodes(runtime, network, image, count, cookie) do
    results =
      for i <- 1..count do
        node_name = :"node#{i}"
        start_node(runtime, network, image, node_name, i, cookie)
      end

    if Enum.all?(results, &match?({:ok, _}, &1)) do
      nodes = Enum.map(results, fn {:ok, node} -> node end)
      {:ok, nodes}
    else
      errors = Enum.filter(results, &match?({:error, _}, &1))
      {:error, {:node_start_failed, errors}}
    end
  end

  defp start_node(runtime, network, image, node_name, _node_num, cookie) do
    cmd = runtime_cmd(runtime)
    container_name = "neonfs-test-#{node_name}-#{System.unique_integer([:positive])}"
    beam_node = :"neonfs_core@#{node_name}"

    args = [
      "run",
      "-d",
      "--name",
      container_name,
      "--network",
      network,
      "--hostname",
      Atom.to_string(node_name),
      "-e",
      "RELEASE_COOKIE=#{cookie}",
      "-e",
      "RELEASE_NODE=#{beam_node}",
      "-e",
      "RELEASE_DISTRIBUTION=name",
      image
    ]

    case System.cmd(cmd, args, stderr_to_stdout: true) do
      {container_id, 0} ->
        container_id = String.trim(container_id)
        Logger.debug("Started container #{container_name} (#{container_id})")

        node_info = %{
          name: node_name,
          container_id: container_id,
          beam_node: beam_node,
          state: :running
        }

        {:ok, node_info}

      {output, code} ->
        Logger.error("Failed to start node #{node_name}: #{output} (exit: #{code})")
        {:error, {:container_start_failed, output}}
    end
  end

  defp wait_for_nodes_ready(nodes) do
    Logger.info("Waiting for #{length(nodes)} nodes to become ready...")

    # Wait for all nodes to be pingable
    results =
      for node <- nodes do
        wait_for_node_ready(node)
      end

    if Enum.all?(results, &(&1 == :ok)) do
      :ok
    else
      {:error, :nodes_not_ready}
    end
  end

  defp wait_for_node_ready(node, retries \\ 30) do
    if retries == 0 do
      Logger.error("Node #{node.name} failed to become ready")
      {:error, :node_not_ready}
    else
      # Try to ping the node
      case Node.ping(node.beam_node) do
        :pong ->
          Logger.debug("Node #{node.name} is ready")
          :ok

        :pang ->
          # Not ready yet, wait and retry
          Process.sleep(1_000)
          wait_for_node_ready(node, retries - 1)
      end
    end
  end

  defp stop_container(runtime, container_id) do
    cmd = runtime_cmd(runtime)

    case System.cmd(cmd, ["stop", container_id], stderr_to_stdout: true) do
      {_output, 0} ->
        Logger.debug("Stopped container #{container_id}")
        # Also remove the container
        System.cmd(cmd, ["rm", container_id], stderr_to_stdout: true)
        :ok

      {output, code} ->
        Logger.warning("Failed to stop container #{container_id}: #{output} (exit: #{code})")
        :ok
    end
  end

  defp kill_container(runtime, container_id) do
    cmd = runtime_cmd(runtime)

    case System.cmd(cmd, ["kill", container_id], stderr_to_stdout: true) do
      {_output, 0} ->
        Logger.debug("Killed container #{container_id}")
        :ok

      {output, code} ->
        Logger.warning("Failed to kill container #{container_id}: #{output} (exit: #{code})")
        :ok
    end
  end

  defp start_container(runtime, container_id) do
    cmd = runtime_cmd(runtime)

    case System.cmd(cmd, ["start", container_id], stderr_to_stdout: true) do
      {_output, 0} ->
        Logger.debug("Started container #{container_id}")
        :ok

      {output, code} ->
        Logger.error("Failed to start container #{container_id}: #{output} (exit: #{code})")
        {:error, :container_start_failed}
    end
  end

  defp runtime_cmd(:docker), do: "docker"
  defp runtime_cmd(:podman), do: "podman"

  defp find_node(cluster, node_name) do
    Enum.find(cluster.nodes, &(&1.name == node_name))
  end

  defp update_node_state(nodes, node_name, new_state) do
    Enum.map(nodes, fn node ->
      if node.name == node_name do
        %{node | state: new_state}
      else
        node
      end
    end)
  end
end
