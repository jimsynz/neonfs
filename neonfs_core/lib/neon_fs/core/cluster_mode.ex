defmodule NeonFS.Core.ClusterMode do
  @moduledoc """
  Read/write facade over the whole-cluster lifecycle mode (#1378) in
  `NeonFS.Core.MetadataStateMachine`.

  The cluster is `:normal` (the default), `:frozen`, or `:recovering`.
  This is distinct from per-node lifecycle (`NeonFS.Core.NodeRegistry`
  `:joining | :active | :draining | :maintenance`) and per-drive trust
  (`NeonFS.Core.DriveTrust` `:trusted | :unverified`): it is the state of
  the cluster as a whole, not of one member.

  - `:frozen` — a coordinated maintenance freeze: client ingress is cut
    and in-flight writes are left to settle (best-effort quiesce) ahead of
    a planned power-down.
  - `:recovering` — the cluster is reassembling after a freeze or an
    unplanned mass restart. Failure-driven repair is suppressed and
    verification throttled so a cold start doesn't trigger a repair storm.

  This module is the mechanism; the consumers that act on the mode (repair
  and verification suppression → #1436, the frozen write-gate → #1438) and
  the producers that set it (`cluster freeze`/`thaw` orchestration → #1439,
  mass-restart auto-detection → #1437) build on this facade. Absence of an
  entry reads as `:normal`, so the table is empty until something sets it.
  The mode lives in Ra consensus, not here.
  """

  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.Core.RaSupervisor

  @type mode :: MetadataStateMachine.cluster_mode()

  @doc """
  Sets the whole-cluster mode, with an optional human-readable `reason`.
  Replicated through Ra before returning. Idempotent.
  """
  @spec set_mode(mode(), String.t() | nil) :: :ok | {:error, term()}
  def set_mode(mode, reason \\ nil)
      when mode in [:normal, :frozen, :recovering] and (is_nil(reason) or is_binary(reason)) do
    command({:set_cluster_mode, mode, reason})
  end

  @doc """
  Returns the current cluster mode. Degrades to `:normal` (the default) if
  the state is momentarily unreadable, so a transient Ra hiccup does not
  spuriously report the cluster as frozen or recovering.
  """
  @spec mode() :: mode()
  def mode do
    case RaSupervisor.local_query(&MetadataStateMachine.get_cluster_mode/1) do
      {:ok, mode} when mode in [:normal, :frozen, :recovering] -> mode
      _ -> :normal
    end
  end

  @doc """
  Returns the full cluster-mode entry (`mode` + `updated_at` + `reason`),
  or `nil` if the mode has never been set or the state is unreadable.
  """
  @spec entry() :: MetadataStateMachine.cluster_mode_entry() | nil
  def entry do
    case RaSupervisor.local_query(&MetadataStateMachine.get_cluster_mode_entry/1) do
      {:ok, %{mode: _} = entry} -> entry
      _ -> nil
    end
  end

  @doc """
  Whether the cluster is currently `:frozen`.
  """
  @spec frozen?() :: boolean()
  def frozen?, do: mode() == :frozen

  @doc """
  Whether the cluster is currently `:recovering`.
  """
  @spec recovering?() :: boolean()
  def recovering?, do: mode() == :recovering

  defp command(cmd) do
    case RaSupervisor.command(cmd) do
      {:ok, :ok, _leader} -> :ok
      {:ok, other, _leader} -> {:error, other}
      {:error, _} = err -> err
      {:timeout, _} -> {:error, :timeout}
    end
  end
end
