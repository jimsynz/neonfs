defmodule NeonFS.Core.NodeRegistry do
  @moduledoc """
  Read/write facade over the first-class node lifecycle table (#1323) in
  `NeonFS.Core.MetadataStateMachine`.

  A node's *lifecycle* status — `:joining | :active | :draining` — is
  distinct from its reachability (net_kernel / Ra membership) and from
  its per-service presence in the service registry. It is the cluster's
  intent for the node: an `:active` node takes new work; a `:draining`
  node is being decommissioned, so placement and routing consumers stop
  giving it *new* work (replica placement today; metadata-shard
  ownership once #1306 lands) while its existing data stays put for
  graceful evacuation.

  Nodes upsert to `:active` automatically when they first register a
  service; `:draining` is set explicitly via `set_status/2` (the
  decommission flow). This module is a pure facade — the table lives in
  Ra consensus, not here.
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
      when is_atom(node) and status in [:joining, :active, :draining] do
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
  Whether `node` is currently draining.
  """
  @spec draining?(node()) :: boolean()
  def draining?(node), do: status(node) == :draining

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
  The set of nodes currently in the `:draining` state — what placement
  consumers exclude. Returns an empty set if the table is unreadable, so
  a transient Ra hiccup degrades to "place anywhere" rather than failing
  the write.
  """
  @spec draining_nodes() :: MapSet.t(node())
  def draining_nodes do
    case RaSupervisor.local_query(&MetadataStateMachine.draining_nodes/1) do
      {:ok, %MapSet{} = set} -> set
      _ -> MapSet.new()
    end
  end
end
