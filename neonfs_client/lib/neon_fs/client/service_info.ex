defmodule NeonFS.Client.ServiceInfo do
  @moduledoc """
  Information about a registered service in the NeonFS cluster.

  Each node that joins the cluster registers itself with its service type
  and optional metadata describing its capabilities.
  """

  alias NeonFS.Client.ServiceType

  @type status :: :online | :offline | :draining

  @type t :: %__MODULE__{
          node: node(),
          type: ServiceType.t(),
          registered_at: DateTime.t(),
          metadata: map(),
          status: status()
        }

  @enforce_keys [:node, :type]
  defstruct [:node, :type, :registered_at, metadata: %{}, status: :online]

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
      status: Keyword.get(opts, :status, :online)
    }
  end

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
      status: info.status
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
      status: Map.get(map, :status) || :online
    }
  end
end
