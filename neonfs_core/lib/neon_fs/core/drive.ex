defmodule NeonFS.Core.Drive do
  @moduledoc """
  Represents a physical storage drive in the NeonFS cluster.

  Each drive has a unique ID, belongs to a node, is assigned to a storage tier,
  and tracks capacity and usage. Drives can be in `:active` or `:standby` state
  for power management purposes.
  """

  @type state :: :active | :standby

  @type t :: %__MODULE__{
          id: String.t(),
          node: node(),
          path: String.t(),
          tier: atom(),
          capacity_bytes: non_neg_integer(),
          used_bytes: non_neg_integer(),
          state: state(),
          power_management: boolean(),
          idle_timeout: non_neg_integer()
        }

  @enforce_keys [:id, :node, :path, :tier]
  defstruct [
    :id,
    :node,
    :path,
    :tier,
    capacity_bytes: 0,
    used_bytes: 0,
    state: :active,
    power_management: false,
    idle_timeout: 1800
  ]

  @doc """
  Creates a Drive from a drive config map and node.

  Drive config maps come from application environment and have shape:
  `%{id: string, path: string, tier: atom, capacity: integer}`
  """
  @spec from_config(map(), node()) :: t()
  def from_config(config, node) do
    %__MODULE__{
      id: to_string(config[:id] || config["id"]),
      node: node,
      path: to_string(config[:path] || config["path"]),
      tier: normalize_tier(config[:tier] || config["tier"]),
      capacity_bytes: config[:capacity] || config["capacity"] || config[:capacity_bytes] || 0,
      used_bytes: 0,
      state: :active,
      power_management: config[:power_management] || false,
      idle_timeout: config[:idle_timeout] || 1800
    }
  end

  @doc """
  Returns the usage ratio for this drive (0.0 to 1.0).

  Returns 0.0 if capacity is zero (unlimited).
  """
  @spec usage_ratio(t()) :: float()
  def usage_ratio(%__MODULE__{capacity_bytes: 0}), do: 0.0

  def usage_ratio(%__MODULE__{used_bytes: used, capacity_bytes: capacity}) do
    used / capacity
  end

  defp normalize_tier(tier) when is_atom(tier), do: tier
  defp normalize_tier(tier) when is_binary(tier), do: String.to_existing_atom(tier)
end
