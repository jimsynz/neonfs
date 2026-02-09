defmodule NeonFS.Core.Topology do
  @moduledoc """
  Queries tier availability across the cluster.

  Provides functions to discover which storage tiers are available locally
  and cluster-wide, based on the drives registered in the DriveRegistry.
  """

  alias NeonFS.Core.DriveRegistry

  @doc """
  Returns tiers available on the local node (active drives only).
  """
  @spec available_tiers() :: [atom()]
  def available_tiers do
    DriveRegistry.drives_for_node(Node.self())
    |> Enum.filter(&(&1.state == :active))
    |> Enum.map(& &1.tier)
    |> Enum.uniq()
    |> Enum.sort()
  end

  @doc """
  Returns all tiers available across the cluster (active drives only).
  """
  @spec cluster_tiers() :: [atom()]
  def cluster_tiers do
    DriveRegistry.list_drives()
    |> Enum.filter(&(&1.state == :active))
    |> Enum.map(& &1.tier)
    |> Enum.uniq()
    |> Enum.sort()
  end

  @doc """
  Validates that a tier is available on the local node.

  ## Returns

    * `:ok` - Tier is available locally
    * `{:error, :tier_unavailable}` - No active drives in this tier
  """
  @spec validate_tier_available(atom()) :: :ok | {:error, :tier_unavailable}
  def validate_tier_available(tier) do
    if tier in available_tiers() do
      :ok
    else
      {:error, :tier_unavailable}
    end
  end
end
