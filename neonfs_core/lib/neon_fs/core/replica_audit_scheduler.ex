defmodule NeonFS.Core.ReplicaAuditScheduler do
  @moduledoc """
  Runs `NeonFS.Core.ReplicaAudit.audit/0` on an interval so
  under-replication is alerted on rather than discovered during an
  incident.

  `audit/0`'s telemetry only fires when something calls it, and until this
  scheduler existed the only callers were `neonfs drive replicas` and the
  pre-flight guard — both operator-initiated. A Prometheus rule built on
  `[:neonfs, :replica_audit, :under_replicated]` therefore had no series to
  watch between manual invocations, which is the opposite of what an alert
  is for.

  The scheduler emits nothing of its own about *which* volumes are
  affected; it just re-drives the audit, and the audit's existing events
  are the signal.

  ## Cost, and why the default is an hour

  Each run range-scans every volume's `chunk_index` tree and, for erasure
  volumes, their stripes. That is the whole point — it reads the
  authoritative state rather than a cache — but it is not free, so the
  default interval is deliberately conservative and a run never overlaps
  its predecessor.

  The audit runs in a task rather than in the scheduler process, so a slow
  traversal doesn't make `status/0` block behind it. A tick that arrives
  while the previous run is still going is skipped, not queued.

  ## Configuration

    * `:interval_ms` — how often to audit (default: 3_600_000 = 1 hour).
      From `:neonfs_core, :replica_audit_interval_ms`, which
      `cluster.json`'s `replica_audit.interval_ms` populates at boot.
    * `:enabled` — set `false` to schedule no ticks at all (default:
      `true`). From `:neonfs_core, :replica_audit_enabled`.
    * `:audit_mod` / `:cluster_mode_mod` / `:ra_server_mod` — injectable
      for testing.

  Lives in `NeonFS.Core.Supervisor`, so it runs on core nodes only —
  interface nodes have no Ra state to audit.

  ## Telemetry

    * `[:neonfs, :replica_audit_scheduler, :skipped]` — `%{}`,
      `%{reason: :already_running | :recovering | :ra_unavailable}`
    * `[:neonfs, :replica_audit_scheduler, :completed]` —
      `%{duration_ms, volume_count, under_replicated_count}`, `%{}`
    * `[:neonfs, :replica_audit_scheduler, :failed]` — `%{}`,
      `%{reason}`

  The per-volume findings come from `ReplicaAudit`'s own events.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{ClusterMode, RaServer, ReplicaAudit}

  # A full traversal of every volume's chunk tree. Hourly is frequent
  # enough that an alert fires well inside a repair window, and rare
  # enough that the scan is not a background load.
  @default_interval_ms 3_600_000

  ## Client API

  @doc """
  Starts the scheduler. See moduledoc for options.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Scheduler state for observability.
  """
  @spec status(GenServer.server()) :: map()
  def status(server \\ __MODULE__) do
    GenServer.call(server, :status)
  end

  ## GenServer callbacks

  @impl true
  def init(opts) do
    state = %{
      interval_ms: Keyword.get_lazy(opts, :interval_ms, &configured_interval/0),
      enabled?: Keyword.get_lazy(opts, :enabled, &configured_enabled?/0),
      audit_mod: Keyword.get(opts, :audit_mod, ReplicaAudit),
      cluster_mode_mod: Keyword.get(opts, :cluster_mode_mod, ClusterMode),
      ra_server_mod: Keyword.get(opts, :ra_server_mod, RaServer),
      task: nil,
      last_run_at: nil,
      last_result: nil
    }

    if state.enabled? do
      schedule_tick(state)
      Logger.info("ReplicaAuditScheduler started", interval_ms: state.interval_ms)
    else
      Logger.info("ReplicaAuditScheduler disabled")
    end

    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    fields = [:interval_ms, :enabled?, :last_run_at, :last_result]
    {:reply, state |> Map.take(fields) |> Map.put(:running?, state.task != nil), state}
  end

  @impl true
  def handle_info(:tick, state) do
    Logger.metadata(component: :scheduler, scheduler: :replica_audit)
    schedule_tick(state)
    {:noreply, maybe_audit(state)}
  end

  # The audit reports through `ReplicaAudit`'s own telemetry, so all this
  # needs from the result is whether it got there.
  def handle_info({:audit_done, pid, result}, %{task: {pid, ref}} = state) do
    Process.demonitor(ref, [:flush])
    report(result)

    {:noreply,
     %{state | task: nil, last_run_at: DateTime.utc_now(), last_result: outcome(result)}}
  end

  # A crashed audit must clear `task` or the scheduler skips every
  # subsequent tick as "already running" and goes quiet for good — the
  # failure mode this scheduler exists to prevent.
  def handle_info({:DOWN, ref, :process, pid, reason}, %{task: {pid, ref}} = state) do
    :telemetry.execute([:neonfs, :replica_audit_scheduler, :failed], %{}, %{reason: reason})
    Logger.warning("Replica audit crashed", reason: inspect(reason))
    {:noreply, %{state | task: nil, last_run_at: DateTime.utc_now(), last_result: :crashed}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Private

  # `spawn_monitor`, not `Task.async` — the latter links, so an audit that
  # raises takes the scheduler down with it and no `:DOWN` handler ever
  # runs. Monitoring without linking is the whole point here: the
  # scheduler's job is to survive a bad run and try again.
  defp maybe_audit(state) do
    case skip_reason(state) do
      nil ->
        scheduler = self()
        audit_mod = state.audit_mod

        spawned =
          spawn_monitor(fn -> send(scheduler, {:audit_done, self(), run_audit(audit_mod)}) end)

        %{state | task: spawned}

      reason ->
        :telemetry.execute([:neonfs, :replica_audit_scheduler, :skipped], %{}, %{reason: reason})
        state
    end
  end

  defp skip_reason(%{task: task}) when task != nil, do: :already_running

  defp skip_reason(state) do
    cond do
      not ra_available?(state) -> :ra_unavailable
      recovering?(state) -> :recovering
      true -> nil
    end
  end

  # Both of these belong to subsystems that may not be running, and
  # neither answer is worth taking the scheduler down for — an unknown
  # answer means "skip this tick", and the next one asks again.
  defp ra_available?(state) do
    state.ra_server_mod.initialized?()
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  defp recovering?(state) do
    state.cluster_mode_mod.recovering?()
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  defp run_audit(audit_mod) do
    started = System.monotonic_time(:millisecond)
    result = audit_mod.audit()
    {result, System.monotonic_time(:millisecond) - started}
  end

  defp report({{:ok, report}, duration_ms}) do
    :telemetry.execute(
      [:neonfs, :replica_audit_scheduler, :completed],
      %{
        duration_ms: duration_ms,
        volume_count: length(report.volumes),
        under_replicated_count: length(report.under_replicated)
      },
      %{}
    )
  end

  defp report({{:error, reason}, _duration_ms}) do
    :telemetry.execute([:neonfs, :replica_audit_scheduler, :failed], %{}, %{reason: reason})
    Logger.warning("Replica audit failed", reason: inspect(reason))
  end

  defp outcome({{:ok, _report}, _duration}), do: :ok
  defp outcome({{:error, reason}, _duration}), do: {:error, reason}

  defp schedule_tick(%{interval_ms: ms}), do: Process.send_after(self(), :tick, ms)

  defp configured_interval do
    Application.get_env(:neonfs_core, :replica_audit_interval_ms, @default_interval_ms)
  end

  defp configured_enabled? do
    Application.get_env(:neonfs_core, :replica_audit_enabled, true)
  end
end
