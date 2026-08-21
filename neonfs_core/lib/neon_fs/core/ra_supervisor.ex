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

  @default_system :default
  @default_cluster_name :neonfs_meta
  @default_uid_prefix "neonfs_meta"

  @identity_key {__MODULE__, :identity}

  @typedoc """
  Which Ra cluster this node's calls address.

  Every one of `command/2`, `query/2`, `local_query/2` and `server_id/0`
  resolves through this, and none of them takes it as an argument — see
  `identity/0`.
  """
  @type identity :: %{
          system: atom(),
          cluster_name: atom(),
          data_dir: String.t(),
          uid_prefix: String.t()
        }

  @doc """
  Start the Ra supervisor.

  ## Options

  All four default to the production values, so a real node needs none of
  them:

    * `:system` — the Ra system name (default `#{inspect(@default_system)}`).
      A system owns its own supervision tree, ETS tables and directory, so
      two systems coexist in one VM without seeing each other's servers.
    * `:cluster_name` — the Ra cluster name, which is also the registered
      name of the local server (default `#{inspect(@default_cluster_name)}`).
    * `:data_dir` — where the system's logs and snapshots live (default
      `:neonfs_core, :ra_data_dir`).
    * `:uid_prefix` — the prefix of the per-node Ra UID (default
      `#{inspect(@default_uid_prefix)}`). Ra keys its log ETS tables by UID,
      so two clusters sharing one leave each other stale entries.
  """
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Initialize the supervisor and start the Ra cluster.
  """
  @impl Supervisor
  def init(opts) do
    identity = build_identity(opts)
    :persistent_term.put(@identity_key, identity)

    # Ra appends the node name to create a subdirectory, so we need to create that too
    # e.g., /var/lib/neonfs/ra -> /var/lib/neonfs/ra/neonfs_core@neonfs-core-1
    node_data_dir = Path.join(identity.data_dir, Atom.to_string(Node.self()))

    File.mkdir_p!(identity.data_dir)
    File.mkdir_p!(node_data_dir)

    Logger.info(
      "Initializing Ra supervisor with data dir: #{identity.data_dir} (node dir: #{node_data_dir})",
      ra_system: identity.system,
      cluster_name: identity.cluster_name
    )

    children = [
      {NeonFS.Core.RaServer, data_dir: identity.data_dir}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Which Ra cluster this node's calls address.

  Read from `:persistent_term`, written once by `init/1`. That is what lets
  the identity be a runtime value without threading a handle through the
  hundred-odd bare `RaSupervisor.command/2` and `query/2` callsites: a node
  hosts one Ra cluster at a time, so the identity is a property of the node
  rather than of the call.

  Falls back to the production defaults when no supervisor has started,
  because `cluster_name/0` and `server_id/0` are called on paths that do not
  require one.
  """
  @spec identity() :: identity()
  def identity, do: :persistent_term.get(@identity_key, default_identity())

  @doc "The Ra system this node's cluster lives in."
  @spec system() :: atom()
  def system, do: identity().system

  @doc """
  Get the Ra cluster name.
  """
  def cluster_name, do: identity().cluster_name

  @doc """
  This node's Ra UID.

  Ra keys its log ETS tables and its server directory by UID, so a UID
  shared between two clusters leaves each holding the other's stale log
  entries — which is why the prefix is part of the identity rather than a
  constant.
  """
  @spec uid() :: String.t()
  def uid, do: "#{identity().uid_prefix}_#{sanitised_node()}"

  @doc """
  This node's Ra data directory.

  Always a binary, whatever the configuration held. `:neonfs_core,
  :ra_data_dir` is set as a charlist in places — Ra hands the path to DETS,
  which will not take a binary — and `Path.join/2` and `String.to_charlist/1`
  both refuse one, so the normalisation belongs here rather than at each of
  the handful of callsites that would otherwise each have to remember.
  """
  @spec data_dir() :: String.t()
  def data_dir, do: identity().data_dir

  @doc """
  Get the server ID for the current node.
  """
  def server_id do
    {cluster_name(), Node.self()}
  end

  @doc """
  The node name as Ra's UIDs and directory paths spell it.

  `@` and `.` are not usable in a path segment, and the UID becomes one.
  """
  @spec sanitised_node() :: String.t()
  def sanitised_node, do: Node.self() |> to_string() |> String.replace(~r/[@\.]/, "_")

  defp build_identity(opts) do
    %{
      system: Keyword.get(opts, :system, @default_system),
      cluster_name: Keyword.get(opts, :cluster_name, @default_cluster_name),
      data_dir: (Keyword.get(opts, :data_dir) || ra_data_dir()) |> IO.chardata_to_string(),
      uid_prefix: Keyword.get(opts, :uid_prefix, @default_uid_prefix)
    }
  end

  defp default_identity do
    %{
      system: @default_system,
      cluster_name: @default_cluster_name,
      data_dir: IO.chardata_to_string(ra_data_dir()),
      uid_prefix: @default_uid_prefix
    }
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

  @doc """
  Read the current state from the local Ra replica without going through
  the leader.

  `apply/3` runs on every cluster member as commands commit, so every
  replica's in-memory state is the committed state — a local read is
  correct for orchestration-layer lookups (auth, ACL checks, KV fetches)
  and avoids the leader round-trip cost of `query/2`.

  Returns the same `{:ok, result} | {:error, reason}` shape as `query/2`
  for caller convenience.
  """
  @spec local_query((term() -> term()), timeout()) :: {:ok, term()} | {:error, term()}
  def local_query(fun, timeout \\ 5000) when is_function(fun, 1) do
    # Ra requires {M, F, A} tuples for queries that cross the cluster —
    # match the convention of `query/2` even though local reads never
    # serialise the fun.
    query_mfa = {__MODULE__, :apply_query, [fun]}

    case :ra.local_query(server_id(), query_mfa, timeout) do
      {:ok, {_idxterm, result}, _local_server} -> {:ok, result}
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
