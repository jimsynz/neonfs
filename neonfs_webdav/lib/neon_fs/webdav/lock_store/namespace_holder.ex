defmodule NeonFS.WebDAV.LockStore.NamespaceHolder do
  @moduledoc """
  Stable holder pid for `NeonFS.Core.NamespaceCoordinator` claims taken
  by this WebDAV node.

  The coordinator monitors the holder pid passed to
  `claim_subtree_for/4` and releases every claim a holder owns when its
  monitor fires. Calls from this node travel via
  `NeonFS.Client.Router.call/4` → `:erpc.call/4`, which spawns a
  short-lived process on the core node — its `self()` would die the
  moment the call returns and take every claim with it.

  This GenServer exists purely to be that long-lived pid: it owns no
  state, runs on every WebDAV node, and shares its pid across every
  LOCK request on this node. If the WebDAV node disconnects from the
  core cluster (or this process crashes), the coordinator's monitor
  fires and the cluster releases this node's WebDAV namespace claims
  in one shot — no stuck-lock cleanup required.
  """

  use GenServer

  @doc """
  Starts the holder process. The default registered name is
  `__MODULE__` so the supervisor and lookups find it without options.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns the pid of the holder, raising if the process isn't running.
  """
  @spec pid() :: pid()
  def pid, do: pid(__MODULE__)

  @doc """
  Returns the pid of a specific holder process by name.
  """
  @spec pid(GenServer.server()) :: pid()
  def pid(server) do
    case GenServer.whereis(server) do
      pid when is_pid(pid) -> pid
      _ -> raise "#{inspect(server)} is not running"
    end
  end

  @impl true
  def init(_opts), do: {:ok, %{}}
end
