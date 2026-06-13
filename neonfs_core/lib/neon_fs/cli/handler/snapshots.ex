defmodule NeonFS.CLI.Handler.Snapshots do
  @moduledoc """
  CLI command handlers for volume snapshots: create, list, show and
  delete, addressing snapshots by ULID or by (unique) human-readable
  name.

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates its `handle_volume_snapshot_*` RPC entry points here. The
  `resolve_snapshot/3` lookup is public because the volume
  promote/restore/export commands (still in `Handler`) resolve a
  snapshot ref the same way.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.Snapshot
  alias NeonFS.Error.{Invalid, NotFound}

  @doc """
  Creates a snapshot of the named volume (optional `"name"` label).
  """
  @spec handle_volume_snapshot_create(binary(), map()) :: {:ok, map()} | {:error, term()}
  def handle_volume_snapshot_create(volume_name, opts \\ %{}) when is_binary(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- fetch_volume(volume_name),
         {:ok, snapshot} <- Snapshot.create(volume.id, snapshot_create_opts(opts)) do
      {:ok, snapshot_to_map(snapshot, volume_name)}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Lists every snapshot for the named volume, newest first.
  """
  @spec handle_volume_snapshot_list(binary()) :: {:ok, [map()]} | {:error, term()}
  def handle_volume_snapshot_list(volume_name) when is_binary(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- fetch_volume(volume_name),
         {:ok, snapshots} <- Snapshot.list(volume.id) do
      {:ok, Enum.map(snapshots, &snapshot_to_map(&1, volume_name))}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Shows a single snapshot, addressed by ULID or by unique name.
  """
  @spec handle_volume_snapshot_show(binary(), binary()) :: {:ok, map()} | {:error, term()}
  def handle_volume_snapshot_show(volume_name, snapshot_ref)
      when is_binary(volume_name) and is_binary(snapshot_ref) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- fetch_volume(volume_name),
         {:ok, snapshot} <- resolve_snapshot(volume.id, snapshot_ref, volume_name) do
      {:ok, snapshot_to_map(snapshot, volume_name)}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Deletes a snapshot's pin (by ULID or unique name). Idempotent for a
  missing ULID; chunk reclamation is the GC scheduler's job.
  """
  @spec handle_volume_snapshot_delete(binary(), binary()) :: :ok | {:error, term()}
  def handle_volume_snapshot_delete(volume_name, snapshot_ref)
      when is_binary(volume_name) and is_binary(snapshot_ref) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- fetch_volume(volume_name),
         {:ok, snapshot} <- resolve_snapshot(volume.id, snapshot_ref, volume_name),
         :ok <- Snapshot.delete(volume.id, snapshot.id) do
      :ok
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Looks up `ref` first as a snapshot ULID, then — if that misses — as a
  human-readable name (refusing ambiguous names). Public because the
  volume promote/restore/export commands resolve refs identically.
  """
  @spec resolve_snapshot(binary(), binary(), binary()) :: {:ok, Snapshot.t()} | {:error, term()}
  def resolve_snapshot(volume_id, ref, volume_name) do
    case Snapshot.get(volume_id, ref) do
      {:ok, snapshot} ->
        {:ok, snapshot}

      {:error, :not_found} ->
        resolve_snapshot_by_name(volume_id, ref, volume_name)

      {:error, _} = err ->
        err
    end
  end

  # Private

  defp snapshot_create_opts(%{"name" => name}) when is_binary(name) and name != "",
    do: [name: name]

  defp snapshot_create_opts(_), do: []

  defp resolve_snapshot_by_name(volume_id, name, volume_name) do
    case Snapshot.list(volume_id) do
      {:ok, snapshots} ->
        case Enum.filter(snapshots, &(&1.name == name)) do
          [snapshot] ->
            {:ok, snapshot}

          [] ->
            {:error, NotFound.exception(message: "snapshot #{name} not found on #{volume_name}")}

          [_ | _] ->
            {:error,
             Invalid.exception(
               message:
                 "snapshot name #{inspect(name)} is ambiguous on #{volume_name} — " <>
                   "address by ULID instead"
             )}
        end

      {:error, _} = err ->
        err
    end
  end

  defp snapshot_to_map(%Snapshot{} = snap, volume_name) do
    %{
      id: snap.id,
      volume_id: snap.volume_id,
      volume_name: volume_name,
      name: snap.name,
      root_chunk_hash_hex: Base.encode16(snap.root_chunk_hash, case: :lower),
      created_at: DateTime.to_iso8601(snap.created_at)
    }
  end
end
