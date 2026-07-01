defmodule NeonFS.Core.ClusterRecoveryMonitor do
  @moduledoc """
  Auto-detects a cold whole-cluster quorum reform and drives the cluster
  into (and back out of) the `:recovering` mode (#1437, part of #1378).

  A power outage or a planned full power-down is not operator-initiated,
  so the `:recovering` state — which suppresses failure-driven repair and
  throttles verification (#1436) so a mass restart doesn't cause a repair
  storm — must be entered automatically.

  ## Detecting cold reform vs a single-node reboot

  The hard part is telling a *whole-cluster* cold reform (quorum was lost,
  then regained) from a *single-node* reboot into a still-healthy cluster
  (quorum never lost). The asymmetry the monitor keys off:

    * On a single-node reboot of a 3+-node cluster, the surviving majority
      keeps quorum and a *survivor* stays leader; the rebooted node rejoins
      as a follower and does **not** win leadership at boot.
    * On a cold reform, every node returns from persisted state, there is a
      leaderless window while quorum re-forms, and whichever node wins
      leadership **just booted from persisted state**.

  So a node enters `:recovering` only when it (a) auto-restarted from
  persisted state this boot (`RaServer.auto_restarted?/0` — not a fresh
  `init_cluster` founder or explicit join) **and** (b) wins Ra leadership
  during the startup detection window. Only the leader writes the mode;
  followers observe it through Ra. This biases against false positives (a
  spurious `:recovering` would suppress repair on a real failure): a
  first-time cluster formation never auto-restarted, and a single-node
  reboot rejoins as a follower.

  ## Exit

  The leader leaves `:recovering` once the cluster has reassembled — all Ra
  member nodes reconnected **and** no drives remain `:unverified`
  (`DriveTrust`) — or after a bounded timeout backstop (default 30 min,
  runtime-configurable) so a permanently-missing member or stuck drive
  can't suppress repair forever. Explicit `:recovering` set by `cluster
  thaw` (#1439) is driven to exit by the same loop.

  Every dependency is injectable for testing.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{ClusterMode, DriveTrust, RaServer, RaSupervisor}

  @default_check_interval_ms 10_000
  @default_settle_ms 5_000
  @default_recovery_timeout_ms 1_800_000
  @default_entry_window_ticks 6

  @doc """
  Starts the monitor.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    state = %{
      auto_restarted_fn: Keyword.get(opts, :auto_restarted_fn, &RaServer.auto_restarted?/0),
      leader_status_fn: Keyword.get(opts, :leader_status_fn, &local_leader_status/0),
      members_online_fn: Keyword.get(opts, :members_online_fn, &cluster_members_online?/0),
      cluster_mode_mod: Keyword.get(opts, :cluster_mode_mod, ClusterMode),
      drive_trust_mod: Keyword.get(opts, :drive_trust_mod, DriveTrust),
      now_fn: Keyword.get(opts, :now_fn, &DateTime.utc_now/0),
      check_interval_ms: Keyword.get(opts, :check_interval_ms, @default_check_interval_ms),
      recovery_timeout_ms:
        Keyword.get(
          opts,
          :recovery_timeout_ms,
          Application.get_env(
            :neonfs_core,
            :cluster_recovery_timeout_ms,
            @default_recovery_timeout_ms
          )
        ),
      entry_window_ticks: Keyword.get(opts, :entry_window_ticks, @default_entry_window_ticks),
      entry_decided: false,
      ticks: 0
    }

    Process.send_after(self(), :tick, Keyword.get(opts, :settle_ms, @default_settle_ms))
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    state =
      if state.cluster_mode_mod.recovering?() do
        maybe_exit(state)
      else
        maybe_enter(state)
      end

    Process.send_after(self(), :tick, state.check_interval_ms)
    {:noreply, state}
  end

  # Entry: only while the startup detection window is open and this node
  # returned from persisted state. Resolves once we've clearly decided.
  defp maybe_enter(%{entry_decided: true} = state), do: state

  defp maybe_enter(state) do
    if state.auto_restarted_fn.() do
      decide_from_leadership(state)
    else
      # Fresh cluster formation or an explicit join — never a cold reform.
      %{state | entry_decided: true}
    end
  end

  defp decide_from_leadership(state) do
    case state.leader_status_fn.() do
      :leader ->
        enter_recovering(state)
        %{state | entry_decided: true}

      :follower ->
        # A leader already exists elsewhere: either a single-node reboot
        # rejoining a healthy cluster, or a follower in a cold reform whose
        # leader sets the mode. Either way this node does nothing.
        %{state | entry_decided: true}

      :no_leader ->
        # Quorum not yet re-formed — keep watching until the window closes.
        ticks = state.ticks + 1
        %{state | ticks: ticks, entry_decided: ticks >= state.entry_window_ticks}
    end
  end

  defp enter_recovering(state) do
    case state.cluster_mode_mod.set_mode(:recovering, "cold quorum reform") do
      :ok ->
        :telemetry.execute([:neonfs, :cluster_recovery, :entered], %{}, %{node: node()})
        Logger.info("Cluster recovery: entered :recovering after cold quorum reform")

      {:error, reason} ->
        Logger.warning("Cluster recovery: could not enter :recovering", reason: inspect(reason))
    end
  end

  # Exit is leader-driven; a follower keeps watching in case leadership
  # moves to it. Runs for auto-detected and `cluster thaw`-set recovering.
  defp maybe_exit(state) do
    if state.leader_status_fn.() == :leader do
      cond do
        reassembled?(state) -> exit_recovering(state, "reassembly complete")
        recovery_timed_out?(state) -> exit_recovering(state, "recovery timeout")
        true -> state
      end
    else
      state
    end
  end

  defp reassembled?(state) do
    state.members_online_fn.() and state.drive_trust_mod.unverified() == []
  end

  defp recovery_timed_out?(state) do
    case state.cluster_mode_mod.entry() do
      %{updated_at: %DateTime{} = since} ->
        DateTime.diff(state.now_fn.(), since, :millisecond) > state.recovery_timeout_ms

      _ ->
        false
    end
  end

  defp exit_recovering(state, reason) do
    case state.cluster_mode_mod.set_mode(:normal, reason) do
      :ok ->
        :telemetry.execute([:neonfs, :cluster_recovery, :exited], %{}, %{reason: reason})
        Logger.info("Cluster recovery: returned to :normal", reason: reason)

      {:error, err} ->
        Logger.warning("Cluster recovery: could not return to :normal", reason: inspect(err))
    end

    state
  end

  defp local_leader_status do
    case :ra.members(RaSupervisor.server_id(), 5_000) do
      {:ok, _members, {_, leader_node}} when leader_node == node() -> :leader
      {:ok, _members, {_, _leader_node}} -> :follower
      _ -> :no_leader
    end
  catch
    :exit, _ -> :no_leader
  end

  defp cluster_members_online? do
    case :ra.members(RaSupervisor.server_id(), 1_000) do
      {:ok, members, _leader} ->
        connected = [node() | Node.list()]
        Enum.all?(members, fn {_name, member_node} -> member_node in connected end)

      _ ->
        false
    end
  catch
    :exit, _ -> false
  end
end
