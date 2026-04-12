defmodule NeonFS.Core.LockManager.GraceCoordinator do
  @moduledoc """
  Tracks lock recovery grace periods after lock master failover.

  When a core node departs the cluster, files whose lock master was on that
  node lose their in-memory lock state. The grace coordinator detects these
  topology changes and enforces a grace window during which only lock
  reclaim requests are accepted — new conflicting locks are rejected with
  `{:error, :grace_period}`.

  This follows the NFS/SMB lock recovery model: after a master change,
  clients have a fixed window to re-establish (reclaim) their locks. Once
  the grace period expires, normal locking resumes and any unreclaimed locks
  are considered released.

  ## Ring tracking

  The coordinator maintains its own snapshot of the lock ring (the
  consistent hash ring used by `LockManager.master_for/1`). When a node
  departs, the snapshot still contains the departed node, allowing us to
  determine which files were mastered by it. The ring is then updated to
  reflect the new topology.

  ## Performance

  Grace state is stored in an ETS table for lock-free concurrent reads.
  The common case (no active grace period) is a single ETS lookup returning
  an empty list.
  """

  use GenServer

  require Logger

  alias NeonFS.Core.{MetadataRing, ServiceRegistry}

  @default_grace_duration_ms 45_000
  @ets_table __MODULE__

  @type grace_entry :: %{
          departed_node: node(),
          ring: MetadataRing.t(),
          deadline: integer()
        }

  @doc """
  Returns `true` if the given file ID is currently in a grace period.

  A file is in grace if its lock master (according to the ring snapshot
  taken before the node departure) was a node that recently left the
  cluster and the grace window has not yet expired.
  """
  @spec in_grace?(binary()) :: boolean()
  def in_grace?(file_id) do
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(@ets_table, :grace_periods) do
      [{:grace_periods, []}] ->
        false

      [{:grace_periods, periods}] ->
        Enum.any?(periods, fn entry ->
          entry.deadline > now and file_mastered_by?(entry.ring, file_id, entry.departed_node)
        end)

      [] ->
        false
    end
  rescue
    ArgumentError ->
      false
  end

  @doc """
  Returns the current grace state for diagnostics.
  """
  @spec grace_status() :: %{
          active_grace_periods: non_neg_integer(),
          departed_nodes: [node()],
          grace_duration_ms: non_neg_integer()
        }
  def grace_status do
    GenServer.call(__MODULE__, :grace_status)
  end

  @doc """
  Starts the grace coordinator.

  ## Options

    * `:grace_duration_ms` — grace period duration in milliseconds
      (default: #{@default_grace_duration_ms})
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    grace_duration_ms = Keyword.get(opts, :grace_duration_ms, @default_grace_duration_ms)

    table = :ets.new(@ets_table, [:named_table, :set, :protected, read_concurrency: true])
    :ets.insert(table, {:grace_periods, []})

    :net_kernel.monitor_nodes(true, node_type: :visible)

    ring = build_lock_ring()

    {:ok,
     %{
       ring: ring,
       grace_periods: [],
       grace_duration_ms: grace_duration_ms
     }}
  end

  @impl true
  def handle_call(:grace_status, _from, state) do
    now = System.monotonic_time(:millisecond)
    active = Enum.filter(state.grace_periods, &(&1.deadline > now))

    reply = %{
      active_grace_periods: length(active),
      departed_nodes: Enum.map(active, & &1.departed_node),
      grace_duration_ms: state.grace_duration_ms
    }

    {:reply, reply, state}
  end

  @impl true
  def handle_info({:nodedown, node, _info}, state) do
    handle_node_departure(node, state)
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    handle_node_departure(node, state)
  end

  @impl true
  def handle_info({:nodeup, node, _info}, state) do
    handle_node_arrival(node, state)
  end

  @impl true
  def handle_info({:nodeup, node}, state) do
    handle_node_arrival(node, state)
  end

  @impl true
  def handle_info(:purge_expired, state) do
    now = System.monotonic_time(:millisecond)
    {expired, active} = Enum.split_with(state.grace_periods, &(&1.deadline <= now))

    for entry <- expired do
      Logger.info("Lock grace period ended",
        departed_node: entry.departed_node
      )

      :telemetry.execute(
        [:neonfs, :lock_manager, :grace, :ended],
        %{},
        %{departed_node: entry.departed_node}
      )
    end

    :ets.insert(@ets_table, {:grace_periods, active})

    if active != [] do
      schedule_purge(active)
    end

    {:noreply, %{state | grace_periods: active}}
  end

  ## Private

  defp handle_node_departure(node, state) do
    if MapSet.member?(state.ring.node_set, node) do
      now = System.monotonic_time(:millisecond)
      deadline = now + state.grace_duration_ms

      entry = %{
        departed_node: node,
        ring: state.ring,
        deadline: deadline
      }

      {new_ring, _affected} = MetadataRing.remove_node(state.ring, node)
      new_periods = [entry | state.grace_periods]

      :ets.insert(@ets_table, {:grace_periods, new_periods})

      Logger.info("Lock grace period started",
        departed_node: node,
        duration_ms: state.grace_duration_ms
      )

      :telemetry.execute(
        [:neonfs, :lock_manager, :grace, :started],
        %{duration_ms: state.grace_duration_ms},
        %{departed_node: node}
      )

      schedule_purge(new_periods)

      {:noreply, %{state | ring: new_ring, grace_periods: new_periods}}
    else
      {:noreply, state}
    end
  end

  defp handle_node_arrival(node, state) do
    if not MapSet.member?(state.ring.node_set, node) and core_node?(node) do
      {new_ring, _affected} = MetadataRing.add_node(state.ring, node)
      {:noreply, %{state | ring: new_ring}}
    else
      {:noreply, state}
    end
  end

  defp file_mastered_by?(ring, file_id, node) do
    case MetadataRing.locate(ring, file_id) do
      {_segment, [^node | _]} -> true
      _ -> false
    end
  end

  defp build_lock_ring do
    core_nodes = [Node.self() | ServiceRegistry.connected_nodes_by_type(:core)]

    MetadataRing.new(Enum.uniq(core_nodes),
      virtual_nodes_per_physical: 64,
      replicas: 1
    )
  rescue
    ArgumentError ->
      MetadataRing.new([Node.self()],
        virtual_nodes_per_physical: 64,
        replicas: 1
      )
  end

  defp core_node?(node) do
    node in ServiceRegistry.connected_nodes_by_type(:core)
  rescue
    _ -> false
  end

  defp schedule_purge(periods) do
    now = System.monotonic_time(:millisecond)

    next_deadline =
      periods
      |> Enum.map(& &1.deadline)
      |> Enum.min()

    delay = max(next_deadline - now, 100)
    Process.send_after(self(), :purge_expired, delay)
  end
end
