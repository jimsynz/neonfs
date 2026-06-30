defmodule NeonFS.Core.NodeRegistry do
  @moduledoc """
  Read/write facade over the first-class node lifecycle table (#1323) in
  `NeonFS.Core.MetadataStateMachine`.

  A node's *lifecycle* status — `:joining | :active | :draining |
  :maintenance` — is distinct from its reachability (net_kernel / Ra
  membership) and from its per-service presence in the service registry.
  It is the cluster's intent for the node: an `:active` node takes new
  work; a `:draining` node is being decommissioned and a `:maintenance`
  node is cordoned for planned, temporary absence (#1376) — both stop
  placement and routing consumers giving them *new* work (replica
  placement today; metadata-shard ownership once #1306 lands) while
  their existing data stays put. The difference is what happens next:
  draining evacuates and is removed; maintenance is left untouched to
  return.

  Nodes upsert to `:active` automatically when they first register a
  service; `:draining` (decommission) and `:maintenance` (cordon) are
  set explicitly via `set_status/2`. This module is a pure facade — the
  table lives in Ra consensus, not here.
  """

  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.Core.RaSupervisor

  @type status :: MetadataStateMachine.node_status()

  @doc """
  Transitions `node` to `status`, creating its entry if absent.
  Replicated through Ra before returning.
  """
  @spec set_status(node(), status()) :: :ok | {:error, term()}
  def set_status(node, status)
      when is_atom(node) and status in [:joining, :active, :draining, :maintenance] do
    case RaSupervisor.command({:set_node_status, node, status}) do
      {:ok, :ok, _leader} -> :ok
      {:ok, other, _leader} -> {:error, other}
      {:error, _} = err -> err
      {:timeout, _} -> {:error, :timeout}
    end
  end

  @doc """
  Returns `node`'s lifecycle status, or `nil` if it has never registered.
  """
  @spec status(node()) :: status() | nil
  def status(node) do
    case RaSupervisor.local_query(&MetadataStateMachine.get_node(&1, node)) do
      {:ok, %{status: status}} -> status
      _ -> nil
    end
  end

  @doc """
  Returns `node`'s full lifecycle entry (`status` + `updated_at`), or
  `nil` if it has never registered. `updated_at` is when the status was
  last set — e.g. roughly when a node was cordoned.
  """
  @spec entry(node()) :: MetadataStateMachine.node_entry() | nil
  def entry(node) do
    case RaSupervisor.local_query(&MetadataStateMachine.get_node(&1, node)) do
      {:ok, %{status: _} = entry} -> entry
      _ -> nil
    end
  end

  @doc """
  Whether `node` is currently draining.
  """
  @spec draining?(node()) :: boolean()
  def draining?(node), do: status(node) == :draining

  @doc """
  Whether `node` is currently cordoned for maintenance (#1376).
  """
  @spec maintenance?(node()) :: boolean()
  def maintenance?(node), do: status(node) == :maintenance

  @doc """
  The full node lifecycle table — `node => node_entry`.
  """
  @spec list() :: %{optional(node()) => MetadataStateMachine.node_entry()}
  def list do
    case RaSupervisor.local_query(&MetadataStateMachine.get_nodes/1) do
      {:ok, nodes} when is_map(nodes) -> nodes
      _ -> %{}
    end
  end

  @doc """
  The set of nodes currently in the `:draining` state. The decommission
  flow keys off this specifically; for new-work exclusion use
  `excluded_nodes/0`. Returns an empty set if the table is unreadable.
  """
  @spec draining_nodes() :: MapSet.t(node())
  def draining_nodes do
    case RaSupervisor.local_query(&MetadataStateMachine.draining_nodes/1) do
      {:ok, %MapSet{} = set} -> set
      _ -> MapSet.new()
    end
  end

  @doc """
  The set of nodes currently in the `:maintenance` state (#1376).
  Returns an empty set if the table is unreadable.
  """
  @spec maintenance_nodes() :: MapSet.t(node())
  def maintenance_nodes do
    case RaSupervisor.local_query(&MetadataStateMachine.maintenance_nodes/1) do
      {:ok, %MapSet{} = set} -> set
      _ -> MapSet.new()
    end
  end

  @doc """
  The set of nodes excluded from *new* work — `:draining` ∪
  `:maintenance`. Placement and routing consumers use this. Returns an
  empty set if the table is unreadable, so a transient Ra hiccup
  degrades to "place anywhere" rather than failing the write.
  """
  @spec excluded_nodes() :: MapSet.t(node())
  def excluded_nodes do
    case RaSupervisor.local_query(&MetadataStateMachine.excluded_nodes/1) do
      {:ok, %MapSet{} = set} -> set
      _ -> MapSet.new()
    end
  end
end
