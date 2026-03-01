defmodule NeonFS.Core.ClockMonitor do
  @moduledoc """
  Periodically probes cluster nodes for clock alignment and quarantines nodes
  with excessive skew.

  Uses round-trip compensation to estimate skew:
  `skew = abs(remote_time - (t1 + (t2 - t1) / 2))`

  ## Skew Thresholds

  | Level      | Default  | Action                           |
  |------------|----------|----------------------------------|
  | Warning    | 200ms    | Log warning + telemetry          |
  | Critical   | 500ms    | Log error + telemetry            |
  | Quarantine | 1000ms   | Exclude from quorum writes       |

  ## Quarantine State

  Quarantine state is stored in a local ETS table (`:clock_quarantine`).
  Quarantined nodes are excluded from quorum writes but can still serve reads.
  A node is removed from quarantine when its skew drops below the quarantine
  threshold.

  ## Telemetry Events

  - `[:neonfs, :clock, :skew]` — emitted for every probed node with `%{skew_ms: N}` and `%{node: node}`
  - `[:neonfs, :clock, :quarantine]` — emitted when a node is quarantined with `%{node: node, skew_ms: N}`
  - `[:neonfs, :clock, :unquarantine]` — emitted when a node is removed from quarantine
  """

  use GenServer
  require Logger

  @ets_table :clock_quarantine
  @default_check_interval_ms 30_000
  @default_warning_threshold_ms 200
  @default_critical_threshold_ms 500
  @default_quarantine_threshold_ms 1_000

  ## Client API

  @doc """
  Starts the ClockMonitor GenServer.

  ## Options

    * `:check_interval_ms` — probe interval in milliseconds (default: #{@default_check_interval_ms})
    * `:warning_threshold_ms` — skew threshold for warnings (default: #{@default_warning_threshold_ms})
    * `:critical_threshold_ms` — skew threshold for errors (default: #{@default_critical_threshold_ms})
    * `:quarantine_threshold_ms` — skew threshold for quarantine (default: #{@default_quarantine_threshold_ms})
    * `:node_lister` — function returning active node list (default: uses ServiceRegistry)
    * `:time_fetcher` — function `(node) -> {:ok, ms} | {:error, term}` for remote time (default: `:rpc.call`)
    * `:name` — GenServer name (default: `__MODULE__`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns whether a node is currently quarantined.
  """
  @spec quarantined?(node()) :: boolean()
  def quarantined?(node) do
    case :ets.lookup(@ets_table, node) do
      [{^node, _skew_ms}] -> true
      [] -> false
    end
  end

  @doc """
  Returns the list of currently quarantined nodes.
  """
  @spec quarantined_nodes() :: [node()]
  def quarantined_nodes do
    @ets_table
    |> :ets.tab2list()
    |> Enum.map(fn {node, _skew} -> node end)
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    _table =
      :ets.new(@ets_table, [
        :named_table,
        :set,
        :public,
        read_concurrency: true
      ])

    check_interval = Keyword.get(opts, :check_interval_ms, @default_check_interval_ms)
    warning_threshold = Keyword.get(opts, :warning_threshold_ms, @default_warning_threshold_ms)
    critical_threshold = Keyword.get(opts, :critical_threshold_ms, @default_critical_threshold_ms)

    quarantine_threshold =
      Keyword.get(opts, :quarantine_threshold_ms, @default_quarantine_threshold_ms)

    node_lister = Keyword.get(opts, :node_lister, &default_node_lister/0)
    time_fetcher = Keyword.get(opts, :time_fetcher, &default_time_fetcher/1)

    state = %{
      check_interval: check_interval,
      warning_threshold: warning_threshold,
      critical_threshold: critical_threshold,
      quarantine_threshold: quarantine_threshold,
      node_lister: node_lister,
      time_fetcher: time_fetcher
    }

    if check_interval > 0 do
      Process.send_after(self(), :check_clocks, check_interval)
    end

    Logger.info("ClockMonitor started", interval_ms: check_interval)

    {:ok, state}
  end

  @impl true
  def handle_info(:check_clocks, state) do
    check_cluster_clocks(state)

    if state.check_interval > 0 do
      Process.send_after(self(), :check_clocks, state.check_interval)
    end

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :ok
  end

  ## Private Functions

  defp check_cluster_clocks(state) do
    nodes = state.node_lister.()

    Enum.each(nodes, fn node ->
      case check_node_clock(node, state) do
        {:ok, skew} ->
          maybe_unquarantine(node)

          :telemetry.execute(
            [:neonfs, :clock, :skew],
            %{skew_ms: skew},
            %{node: node}
          )

        {:warning, skew} ->
          maybe_unquarantine(node)
          Logger.warning("Clock skew detected", node: node, skew_ms: skew)

          :telemetry.execute(
            [:neonfs, :clock, :skew],
            %{skew_ms: skew},
            %{node: node}
          )

        {:critical, skew} ->
          maybe_unquarantine(node)
          Logger.error("Critical clock skew", node: node, skew_ms: skew)

          :telemetry.execute(
            [:neonfs, :clock, :skew],
            %{skew_ms: skew},
            %{node: node}
          )

        {:quarantine, skew} ->
          quarantine_node(node, skew)

          :telemetry.execute(
            [:neonfs, :clock, :skew],
            %{skew_ms: skew},
            %{node: node}
          )

        {:error, reason} ->
          Logger.warning("Failed to probe clock on node", node: node, reason: inspect(reason))
      end
    end)

    :telemetry.execute(
      [:neonfs, :clock, :check_complete],
      %{},
      %{nodes_checked: length(nodes)}
    )
  end

  defp check_node_clock(node, state) do
    t1 = System.system_time(:millisecond)

    case state.time_fetcher.(node) do
      {:ok, remote_time} ->
        t2 = System.system_time(:millisecond)
        estimated_local = t1 + div(t2 - t1, 2)
        skew = abs(remote_time - estimated_local)
        classify_skew(skew, state)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp classify_skew(skew, state) do
    cond do
      skew >= state.quarantine_threshold -> {:quarantine, skew}
      skew >= state.critical_threshold -> {:critical, skew}
      skew >= state.warning_threshold -> {:warning, skew}
      true -> {:ok, skew}
    end
  end

  defp quarantine_node(node, skew) do
    was_quarantined = quarantined?(node)
    :ets.insert(@ets_table, {node, skew})

    unless was_quarantined do
      Logger.error("Quarantining node, clock skew exceeds threshold",
        node: node,
        skew_ms: skew
      )

      :telemetry.execute(
        [:neonfs, :clock, :quarantine],
        %{skew_ms: skew},
        %{node: node}
      )
    end
  end

  defp maybe_unquarantine(node) do
    if quarantined?(node) do
      :ets.delete(@ets_table, node)
      Logger.info("Unquarantining node, clock skew within bounds", node: node)

      :telemetry.execute(
        [:neonfs, :clock, :unquarantine],
        %{},
        %{node: node}
      )
    end
  end

  defp default_node_lister do
    Node.list()
  end

  defp default_time_fetcher(node) do
    case :rpc.call(node, System, :system_time, [:millisecond]) do
      {:badrpc, reason} -> {:error, reason}
      time when is_integer(time) -> {:ok, time}
    end
  end
end
