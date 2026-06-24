defmodule NeonFS.Core.DriveTrust do
  @moduledoc """
  Read/write facade over the per-drive trust state (#1375) in
  `NeonFS.Core.MetadataStateMachine`.

  A replica is `:trusted` (the default) or `:unverified` — *present but
  not durable-yet*: a rebooted node mid-resync, or a crash-recovered
  drive mid-scrub. Trust is distinct from a drive's power state
  (`NeonFS.Core.Drive` `:state`) and from node lifecycle
  (`NeonFS.Core.NodeRegistry` `:joining | :active | :draining`).

  Only `:unverified` drives are stored, keyed `{node, drive_id}`, so the
  table is empty in steady state. This module is the mechanism; the
  producers that mark drives `:unverified` (node return → #1376, crash
  marker → #1380) and the consumers that act on it (durability maths,
  verify-on-read) build on this facade. The table lives in Ra consensus,
  not here.
  """

  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.Core.RaSupervisor

  @type trust :: :trusted | :unverified

  @doc """
  Marks `{node, drive_id}` `:unverified`. Replicated through Ra before
  returning. Idempotent.
  """
  @spec mark_unverified(node(), String.t()) :: :ok | {:error, term()}
  def mark_unverified(node, drive_id) when is_atom(node) and is_binary(drive_id) do
    command({:mark_drive_unverified, node, drive_id})
  end

  @doc """
  Marks `{node, drive_id}` `:trusted` (clears it from the table). Called
  when verification completes. Idempotent.
  """
  @spec mark_trusted(node(), String.t()) :: :ok | {:error, term()}
  def mark_trusted(node, drive_id) when is_atom(node) and is_binary(drive_id) do
    command({:mark_drive_trusted, node, drive_id})
  end

  @doc """
  Marks **all** of `node`'s currently-registered drives `:unverified` in
  one Ra command — the "node implies its drives" path for a node
  returning from a planned reboot (#1376). Each drive clears back to
  `:trusted` independently as it verifies.
  """
  @spec mark_node_unverified(node()) :: :ok | {:error, term()}
  def mark_node_unverified(node) when is_atom(node) do
    command({:mark_node_drives_unverified, node})
  end

  @doc """
  Returns the trust state of `{node, drive_id}`. Degrades to `:trusted`
  (the default) if the table is momentarily unreadable, so a transient Ra
  hiccup does not spuriously drop replicas out of durability counts.
  """
  @spec state(node(), String.t()) :: trust()
  def state(node, drive_id) do
    case RaSupervisor.local_query(&MetadataStateMachine.drive_trust(&1, node, drive_id)) do
      {:ok, trust} when trust in [:trusted, :unverified] -> trust
      _ -> :trusted
    end
  end

  @doc """
  Whether `{node, drive_id}` is currently `:unverified`.
  """
  @spec unverified?(node(), String.t()) :: boolean()
  def unverified?(node, drive_id), do: state(node, drive_id) == :unverified

  @doc """
  The `{node, drive_id}` keys of all currently-`:unverified` drives.
  Empty list if the table is unreadable.
  """
  @spec unverified() :: [{node(), String.t()}]
  def unverified do
    case RaSupervisor.local_query(&MetadataStateMachine.unverified_drives/1) do
      {:ok, keys} when is_list(keys) -> keys
      _ -> []
    end
  end

  defp command(cmd) do
    case RaSupervisor.command(cmd) do
      {:ok, :ok, _leader} -> :ok
      {:ok, other, _leader} -> {:error, other}
      {:error, _} = err -> err
      {:timeout, _} -> {:error, :timeout}
    end
  end
end
