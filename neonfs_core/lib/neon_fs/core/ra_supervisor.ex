defmodule NeonFS.Core.RaSupervisor do
  @moduledoc """
  Supervisor for Ra cluster initialization and management.

  This supervisor is responsible for starting the Ra server for cluster-wide
  metadata storage. The Ra server provides Raft-based consensus for:
  - Node membership
  - Volume definitions
  - User/group definitions
  - Segment assignments

  ## Requirements

  Ra requires a named Erlang node to function properly. To run with Ra enabled:

      # For tests
      MIX_ENV=test elixir --sname test -S mix test --only ra

      # For releases
      RELEASE_NODE=neonfs_core@localhost mix release

  For Phase 1 single-node operation, Ra is optional. Phase 2+ requires Ra
  for distributed cluster coordination.
  """

  use Supervisor

  require Logger

  @cluster_name :neonfs_meta

  @doc """
  Start the Ra supervisor.
  """
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Initialize the supervisor and start the Ra cluster.
  """
  @impl Supervisor
  def init(_opts) do
    # Get Ra data directory from config
    data_dir = ra_data_dir()

    # Ra appends the node name to create a subdirectory, so we need to create that too
    # e.g., /var/lib/neonfs/data/ra -> /var/lib/neonfs/data/ra/neonfs_core@neonfs-core-1
    node_data_dir = Path.join(data_dir, Atom.to_string(Node.self()))

    File.mkdir_p!(data_dir)
    File.mkdir_p!(node_data_dir)

    Logger.info(
      "Initializing Ra supervisor with data dir: #{data_dir} (node dir: #{node_data_dir})"
    )

    # Start RaServer GenServer which will initialize the Ra server asynchronously
    children = [
      {NeonFS.Core.RaServer, data_dir: data_dir}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Get the Ra cluster name.
  """
  def cluster_name, do: @cluster_name

  @doc """
  Get the server ID for the current node.
  """
  def server_id do
    {@cluster_name, Node.self()}
  end

  @doc """
  Execute a command on the Ra cluster.

  Commands are replicated via Raft consensus before being applied.
  """
  def command(cmd, timeout \\ 5000) do
    :ra.process_command(server_id(), cmd, timeout)
  end

  @doc """
  Read the current state from the Ra cluster.

  This performs a consistent read by querying the leader.
  """
  def query(fun, timeout \\ 5000) when is_function(fun, 1) do
    # Ra 3.0.2 requires {M, F, A} tuples for consistent_query
    query_mfa = {__MODULE__, :apply_query, [fun]}

    case :ra.consistent_query(server_id(), query_mfa, timeout) do
      {:ok, result, _leader} -> {:ok, result}
      {:error, _} = error -> error
      {:timeout, _} -> {:error, :timeout}
    end
  end

  @doc false
  def apply_query(fun, state), do: fun.(state)

  @doc """
  Get the current state (for testing/debugging).
  """
  def get_state do
    query(fn state -> state end)
  end

  # Private helpers

  defp ra_data_dir do
    Application.get_env(:neonfs_core, :ra_data_dir, "/var/lib/neonfs/ra")
  end
end
