defmodule NeonFS.Core.Snapshot do
  @moduledoc """
  Volume snapshots (#960 / epic #959).

  A snapshot is a frozen pointer to a volume's root chunk hash at a
  point in time. Snapshots don't carry chunk data — they share storage
  with the live volume via the content-addressed graph. Deletion is
  just a Ra write that removes the pin; reclamation is the GC
  scheduler's job (separate sub-issue).
  """

  alias NeonFS.Core.{MetadataStateMachine, RaSupervisor, VolumeRegistry}

  @enforce_keys [:id, :volume_id, :root_chunk_hash, :created_at]
  defstruct [:id, :volume_id, :root_chunk_hash, :created_at, :name]

  @type t :: %__MODULE__{
          id: binary(),
          volume_id: binary(),
          name: String.t() | nil,
          root_chunk_hash: binary(),
          created_at: DateTime.t()
        }

  @doc """
  Creates a snapshot of `volume_id` at its current root chunk.

  Options:

    * `:name` — optional human-readable label.

  Returns `{:error, :volume_not_found}` if `volume_id` isn't registered
  in the bootstrap volume_roots map.
  """
  @spec create(binary(), keyword()) :: {:ok, t()} | {:error, term()}
  def create(volume_id, opts \\ []) when is_binary(volume_id) do
    name = Keyword.get(opts, :name)

    with {:ok, root_chunk_hash} <- fetch_volume_root_hash(volume_id) do
      entry = %{
        id: UUIDv7.generate(),
        volume_id: volume_id,
        name: name,
        root_chunk_hash: root_chunk_hash,
        created_at: DateTime.utc_now()
      }

      case RaSupervisor.command({:put_snapshot, entry}) do
        {:ok, :ok, _leader} -> {:ok, to_struct(entry)}
        {:error, _} = err -> err
        {:timeout, _} -> {:error, :timeout}
      end
    end
  end

  @doc """
  Looks up a snapshot by id.
  """
  @spec get(binary(), binary()) :: {:ok, t()} | {:error, :not_found | term()}
  def get(volume_id, snapshot_id) when is_binary(volume_id) and is_binary(snapshot_id) do
    case RaSupervisor.local_query(&MetadataStateMachine.get_snapshot(&1, volume_id, snapshot_id)) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, entry} -> {:ok, to_struct(entry)}
      {:error, _} = err -> err
    end
  end

  @doc """
  Lists every snapshot for `volume_id`, newest first.
  """
  @spec list(binary()) :: {:ok, [t()]} | {:error, term()}
  def list(volume_id) when is_binary(volume_id) do
    case RaSupervisor.local_query(&MetadataStateMachine.list_snapshots(&1, volume_id)) do
      {:ok, entries} -> {:ok, Enum.map(entries, &to_struct/1)}
      {:error, _} = err -> err
    end
  end

  @doc """
  Removes the snapshot's pin. Idempotent — deleting a missing snapshot
  is a no-op. Does not reclaim any chunks the snapshot was holding
  alive; that's the GC scheduler's responsibility (separate sub-issue).
  """
  @spec delete(binary(), binary()) :: :ok | {:error, term()}
  def delete(volume_id, snapshot_id) when is_binary(volume_id) and is_binary(snapshot_id) do
    case RaSupervisor.command({:delete_snapshot, volume_id, snapshot_id}) do
      {:ok, :ok, _leader} -> :ok
      {:error, _} = err -> err
      {:timeout, _} -> {:error, :timeout}
    end
  end

  @doc """
  Promotes a snapshot to a brand-new top-level volume that shares
  storage with the source (#964).

  Creates `new_volume_name` whose `volume_root` points at the same
  `root_chunk_hash` as `snapshot_id`. Chunks and per-volume tree pages
  are content-addressed, so no bytes are copied — both volumes pin the
  same chunk graph, and per-volume GC keeps them alive as long as
  *either* volume's root reaches them.

  By default the new volume inherits the source volume's storage
  policy (durability, write_ack, tiering, caching, compression,
  verification, encryption). Pass `:volume_opts` to override individual
  fields — anything accepted by `NeonFS.Core.Volume.new/2`.

  ## Errors

    * `{:error, :volume_not_found}` — source volume isn't registered.
    * `{:error, :not_found}` — snapshot id doesn't match a snapshot on
      the source volume.
    * `{:error, :source_volume_root_unknown}` — source volume has no
      bootstrap-layer `volume_root` entry yet (snapshot taken before
      first metadata write).
    * Any error from `VolumeRegistry.create/2` (e.g. name collision,
      invalid policy) or Ra unavailable.
  """
  @spec promote(binary(), binary(), String.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def promote(source_volume_id, snapshot_id, new_volume_name, opts \\ [])
      when is_binary(source_volume_id) and is_binary(snapshot_id) and
             is_binary(new_volume_name) and is_list(opts) do
    with {:ok, source_volume} <- fetch_source_volume(source_volume_id),
         {:ok, snapshot} <- get(source_volume_id, snapshot_id),
         {:ok, source_root} <- fetch_source_volume_root(source_volume_id),
         volume_opts <- merge_volume_opts(source_volume, opts),
         {:ok, new_volume} <-
           VolumeRegistry.create(
             new_volume_name,
             Keyword.put(volume_opts, :skip_provisioning?, true)
           ),
         :ok <- register_promoted_root(new_volume, snapshot.root_chunk_hash, source_root) do
      {:ok, new_volume}
    end
  end

  ## Private

  defp fetch_volume_root_hash(volume_id) do
    case RaSupervisor.query(&MetadataStateMachine.get_volume_root(&1, volume_id)) do
      {:ok, nil} -> {:error, :volume_not_found}
      {:ok, %{root_chunk_hash: hash}} -> {:ok, hash}
      {:error, _} = err -> err
    end
  end

  defp fetch_source_volume(source_volume_id) do
    case VolumeRegistry.get(source_volume_id) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, :volume_not_found}
    end
  end

  defp fetch_source_volume_root(source_volume_id) do
    case RaSupervisor.query(&MetadataStateMachine.get_volume_root(&1, source_volume_id)) do
      {:ok, nil} -> {:error, :source_volume_root_unknown}
      {:ok, %{} = entry} -> {:ok, entry}
      {:error, _} = err -> err
    end
  end

  # Inherit the source volume's storage policy, then layer caller
  # overrides on top. Anything not listed here resets to the new
  # volume's defaults (id, name, sizes, timestamps).
  defp merge_volume_opts(source_volume, opts) do
    overrides = Keyword.get(opts, :volume_opts, [])

    inherited = [
      owner: source_volume.owner,
      atime_mode: source_volume.atime_mode,
      durability: source_volume.durability,
      write_ack: source_volume.write_ack,
      tiering: source_volume.tiering,
      caching: source_volume.caching,
      io_weight: source_volume.io_weight,
      compression: source_volume.compression,
      verification: source_volume.verification,
      encryption: source_volume.encryption,
      metadata_consistency: source_volume.metadata_consistency
    ]

    Keyword.merge(inherited, overrides)
  end

  defp register_promoted_root(new_volume, root_chunk_hash, source_root) do
    entry = %{
      volume_id: new_volume.id,
      root_chunk_hash: root_chunk_hash,
      drive_locations: source_root.drive_locations,
      durability_cache: build_durability_cache(new_volume),
      updated_at: DateTime.utc_now()
    }

    case RaSupervisor.command({:register_volume_root, entry}) do
      {:ok, :ok, _leader} -> :ok
      {:error, _} = err -> err
      {:timeout, _} -> {:error, :timeout}
    end
  end

  defp build_durability_cache(%{durability: %{type: :replicate} = d}) do
    %{type: :replicate, factor: d.factor, min_copies: Map.get(d, :min_copies, d.factor)}
  end

  defp build_durability_cache(%{durability: %{type: :erasure} = d}) do
    %{
      type: :erasure,
      data_chunks: d.data_chunks,
      parity_chunks: d.parity_chunks
    }
  end

  defp to_struct(entry) do
    %__MODULE__{
      id: entry.id,
      volume_id: entry.volume_id,
      name: Map.get(entry, :name),
      root_chunk_hash: entry.root_chunk_hash,
      created_at: entry.created_at
    }
  end
end
