defmodule NeonFS.Core.Snapshot do
  @moduledoc """
  Volume snapshots (#960 / epic #959).

  A snapshot is a frozen pointer to a volume's root chunk hash at a
  point in time. Snapshots don't carry chunk data — they share storage
  with the live volume via the content-addressed graph. Deletion is
  just a Ra write that removes the pin; reclamation is the GC
  scheduler's job (separate sub-issue).
  """

  alias NeonFS.Core.{MetadataStateMachine, RaSupervisor}

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

  ## Private

  defp fetch_volume_root_hash(volume_id) do
    case RaSupervisor.query(&MetadataStateMachine.get_volume_root(&1, volume_id)) do
      {:ok, nil} -> {:error, :volume_not_found}
      {:ok, %{root_chunk_hash: hash}} -> {:ok, hash}
      {:error, _} = err -> err
    end
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
