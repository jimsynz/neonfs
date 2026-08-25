defmodule NeonFS.Client.ServiceInfo do
  @moduledoc """
  Information about a registered service in the NeonFS cluster.

  Each node that joins the cluster registers one or more service instances with
  their service type and optional metadata describing capabilities.

  `dist_port` is a field rather than a `metadata` entry because every node has
  one — it is a property of the node, not a capability of the interface — and
  a metadata entry would make every consumer write a `Map.get` with a
  fallback. `NeonFS.Client.PeerPorts` is what turns these into something
  `NeonFS.Epmd` can resolve, which is how one interface node dials another.
  """

  alias NeonFS.Client.{Join, ServiceType}

  @type status :: :online | :offline | :draining | :maintenance

  @type t :: %__MODULE__{
          node: node(),
          type: ServiceType.t(),
          registered_at: DateTime.t(),
          metadata: map(),
          status: status(),
          dist_port: non_neg_integer()
        }

  @enforce_keys [:node, :type]
  defstruct [:node, :type, :registered_at, metadata: %{}, status: :online, dist_port: 0]

  @doc """
  Creates a new ServiceInfo struct.

  ## Examples

      iex> ServiceInfo.new(:neonfs_core@host1, :core)
      %ServiceInfo{node: :neonfs_core@host1, type: :core, ...}
  """
  @spec new(node(), ServiceType.t(), keyword()) :: t()
  def new(node, type, opts \\ []) do
    %__MODULE__{
      node: node,
      type: type,
      registered_at: Keyword.get(opts, :registered_at, DateTime.utc_now()),
      metadata: Keyword.get(opts, :metadata, %{}),
      status: Keyword.get(opts, :status, :online),
      dist_port: Keyword.get(opts, :dist_port, 0)
    }
  end

  @doc """
  Creates a ServiceInfo describing *this* node.

  Five places construct a registration and four of them describe the local
  node; each would otherwise have to remember to fill `dist_port`, and the
  one that forgot would produce a node that registers, discovers and routes
  perfectly well while being undialable by its siblings — visible only when
  something tries to reach it directly. Filling it here is what stops that
  being a per-callsite decision.

  The fifth is `NeonFS.Cluster.Join`, which registers on behalf of a node
  that is joining; that one carries the joiner's own port and must pass it.
  """
  @spec for_self(ServiceType.t(), keyword()) :: t()
  def for_self(type, opts \\ []) do
    new(Node.self(), type, Keyword.put_new(opts, :dist_port, Join.local_dist_port()))
  end

  @doc """
  Returns the registry key for a service instance.
  """
  @spec key(t()) :: {node(), ServiceType.t()}
  def key(%__MODULE__{node: node, type: type}), do: {node, type}

  @doc """
  Returns the registry key for a node/type pair.
  """
  @spec key(node(), ServiceType.t()) :: {node(), ServiceType.t()}
  def key(node, type), do: {node, type}

  @doc """
  Converts a ServiceInfo to a plain map for Ra storage.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = info) do
    %{
      node: info.node,
      type: info.type,
      registered_at: info.registered_at,
      metadata: info.metadata,
      status: info.status,
      dist_port: info.dist_port
    }
  end

  @doc """
  Reconstructs a ServiceInfo from a plain map or struct (from Ra storage or RPC).
  """
  @spec from_map(map()) :: t()
  def from_map(%__MODULE__{} = info), do: info

  def from_map(map) do
    %__MODULE__{
      node: map.node,
      type: map.type,
      registered_at: Map.get(map, :registered_at) || DateTime.utc_now(),
      metadata: Map.get(map, :metadata) || %{},
      status: Map.get(map, :status) || :online,
      dist_port: Map.get(map, :dist_port) || 0
    }
  end
end
