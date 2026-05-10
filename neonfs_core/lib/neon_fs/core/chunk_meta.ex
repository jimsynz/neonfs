defmodule NeonFS.Core.ChunkMeta do
  @moduledoc """
  Chunk metadata structure tracking everything about a chunk across the cluster.

  Per the design call on #874, chunks carry the **set** of volumes that
  pin them (`volume_ids`) rather than a single owning volume — this
  restores cross-volume content-addressed dedup while still letting
  per-volume background runners look up a representative `volume_id`
  from any chunk hash.
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
          volume_ids: MapSet.t(binary()),
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

  defstruct volume_ids: MapSet.new(),
            hash: nil,
            original_size: nil,
            stored_size: nil,
            compression: nil,
            crypto: nil,
            locations: nil,
            target_replicas: nil,
            commit_state: nil,
            active_write_refs: nil,
            stripe_id: nil,
            stripe_index: nil,
            created_at: nil,
            last_verified: nil

  @doc """
  Creates a new ChunkMeta struct with default values.

  Accepts a single `volume_id` for compatibility with existing call
  sites; it's wrapped into a single-element `MapSet` as the chunk's
  initial `volume_ids` set. Subsequent volumes that pin the chunk
  go through `add_volume/2`.
  """
  @spec new(binary(), binary(), non_neg_integer(), non_neg_integer(), compression()) ::
          %__MODULE__{}
  def new(volume_id, hash, original_size, stored_size, compression \\ :none)
      when is_binary(volume_id) do
    %__MODULE__{
      volume_ids: MapSet.new([volume_id]),
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
  Adds `volume_id` to the chunk's `volume_ids` set.

  Idempotent — re-adding an already-present id is a no-op.
  """
  @spec add_volume(t(), binary()) :: t()
  def add_volume(%__MODULE__{} = meta, volume_id) when is_binary(volume_id) do
    %{meta | volume_ids: MapSet.put(meta.volume_ids, volume_id)}
  end

  @doc """
  Removes `volume_id` from the chunk's `volume_ids` set.

  Note: a chunk with an empty `volume_ids` set is unreferenced from
  the metadata side and should be GC'd by the chunk-level garbage
  collector. Callers that detach the last volume are expected to
  handle that lifecycle.
  """
  @spec remove_volume(t(), binary()) :: t()
  def remove_volume(%__MODULE__{} = meta, volume_id) when is_binary(volume_id) do
    %{meta | volume_ids: MapSet.delete(meta.volume_ids, volume_id)}
  end

  @doc """
  Returns a representative `volume_id` from the chunk's `volume_ids`
  set, or `nil` if the set is empty.

  Use this from structural callers that need to address one of the
  volumes the chunk is pinned by — for example, looking up the
  chunk in a per-volume index. Callers that need to operate on every
  pinning volume should iterate `volume_ids` directly.
  """
  @spec any_volume_id(t()) :: binary() | nil
  def any_volume_id(%__MODULE__{volume_ids: ids}) do
    case MapSet.to_list(ids) do
      [] -> nil
      [first | _] -> first
    end
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
