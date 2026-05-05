defmodule NeonFS.Core.Drive do
  @moduledoc """
  Represents a physical storage drive in the NeonFS cluster.

  Each drive has a unique ID, belongs to a node, is assigned to a storage tier,
  and tracks capacity and usage. Drives can be in `:active`, `:standby`, or
  `:draining` state. The `:draining` state indicates an evacuation is in progress
  and no new writes should be directed to this drive.
  """

  @type state :: :active | :standby | :draining

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
      path: normalize_path(to_string(config[:path] || config["path"])),
      tier: normalize_tier(config[:tier] || config["tier"]),
      capacity_bytes: config[:capacity] || config["capacity"] || config[:capacity_bytes] || 0,
      used_bytes: 0,
      state: :active,
      power_management: config[:power_management] || false,
      idle_timeout: config[:idle_timeout] || 1800
    }
  end

  @doc """
  Normalises a drive path: collapses consecutive slashes, strips a
  trailing slash (except `/`), expands `.`/`..` segments, and resolves
  `~`. The empty string passes through unchanged so callers can run
  their own `"Path is required"` validation rather than have
  `Path.expand/1` substitute the daemon's CWD.

  Existing on-disk paths in `cluster.json` self-heal on next daemon
  start because every config goes through this call. No migration
  needed.
  """
  @spec normalize_path(String.t()) :: String.t()
  def normalize_path(""), do: ""
  def normalize_path(path) when is_binary(path), do: Path.expand(path)

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
