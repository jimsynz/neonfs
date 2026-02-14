defmodule NeonFS.Core.ChunkMeta do
  @moduledoc """
  Chunk metadata structure tracking everything about a chunk across the cluster.

  For Phase 1, this is stored in-memory via ETS. In Phase 2, it will be backed
  by Ra consensus for cluster-wide consistency.
  """

  @type commit_state :: :uncommitted | :committed
  @type compression :: :none | :zstd
  @type write_id :: String.t()
  @type location :: %{
          node: atom(),
          drive_id: String.t(),
          tier: :hot | :warm | :cold
        }

  @type t :: %__MODULE__{
          hash: binary(),
          original_size: non_neg_integer(),
          stored_size: non_neg_integer(),
          compression: compression(),
          crypto: NeonFS.Core.ChunkCrypto.t() | nil,
          locations: [location()],
          target_replicas: pos_integer(),
          commit_state: commit_state(),
          active_write_refs: MapSet.t(write_id()),
          stripe_id: String.t() | nil,
          stripe_index: non_neg_integer() | nil,
          created_at: DateTime.t(),
          last_verified: DateTime.t() | nil
        }

  defstruct [
    :hash,
    :original_size,
    :stored_size,
    :compression,
    :crypto,
    :locations,
    :target_replicas,
    :commit_state,
    :active_write_refs,
    :stripe_id,
    :stripe_index,
    :created_at,
    :last_verified
  ]

  @doc """
  Creates a new ChunkMeta struct with default values.
  """
  @spec new(binary(), non_neg_integer(), non_neg_integer(), compression()) :: %__MODULE__{}
  def new(hash, original_size, stored_size, compression \\ :none) do
    %__MODULE__{
      hash: hash,
      original_size: original_size,
      stored_size: stored_size,
      compression: compression,
      locations: [],
      target_replicas: 1,
      commit_state: :uncommitted,
      active_write_refs: MapSet.new(),
      stripe_id: nil,
      stripe_index: nil,
      created_at: DateTime.utc_now(),
      last_verified: nil
    }
  end

  @doc """
  Adds a location to the chunk metadata.
  """
  @spec add_location(t(), location()) :: t()
  def add_location(%__MODULE__{} = meta, location) do
    %{meta | locations: [location | meta.locations]}
  end

  @doc """
  Removes a location from the chunk metadata.
  """
  @spec remove_location(t(), location()) :: t()
  def remove_location(%__MODULE__{} = meta, location) do
    %{meta | locations: Enum.reject(meta.locations, &(&1 == location))}
  end

  @doc """
  Adds a write ID to the active write refs.
  """
  @spec add_write_ref(t(), write_id()) :: t()
  def add_write_ref(%__MODULE__{} = meta, write_id) do
    %{meta | active_write_refs: MapSet.put(meta.active_write_refs, write_id)}
  end

  @doc """
  Removes a write ID from the active write refs.
  """
  @spec remove_write_ref(t(), write_id()) :: t()
  def remove_write_ref(%__MODULE__{} = meta, write_id) do
    %{meta | active_write_refs: MapSet.delete(meta.active_write_refs, write_id)}
  end

  @doc """
  Commits the chunk, transitioning from :uncommitted to :committed.
  Only allows transition if there are no active write refs.
  """
  @spec commit(t()) :: {:ok, t()} | {:error, :has_active_writes}
  def commit(%__MODULE__{commit_state: :committed} = meta), do: {:ok, meta}

  def commit(%__MODULE__{commit_state: :uncommitted, active_write_refs: refs} = meta) do
    if MapSet.size(refs) == 0 do
      {:ok, %{meta | commit_state: :committed}}
    else
      {:error, :has_active_writes}
    end
  end
end
