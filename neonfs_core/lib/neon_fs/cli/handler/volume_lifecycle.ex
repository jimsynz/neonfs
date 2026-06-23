defmodule NeonFS.CLI.Handler.VolumeLifecycle do
  @moduledoc """
  CLI command handlers for whole-volume lifecycle operations built on
  snapshots and archives: promoting a snapshot to a new volume,
  restoring a volume to a snapshot, TAR export/import, and the backup
  create/describe/restore commands.

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates the matching RPC entry points here. Snapshot refs are
  resolved via the (public) `NeonFS.CLI.Handler.Snapshots.resolve_snapshot/3`.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.CLI.Handler.Snapshots, as: SnapshotsHandler
  alias NeonFS.Core.{Backup, Snapshot, VolumeExport, VolumeImport}

  @doc """
  Promotes a snapshot of `source_volume_name` into a new volume.
  """
  @spec handle_volume_promote(binary(), binary(), binary(), map()) ::
          {:ok, map()} | {:error, term()}
  def handle_volume_promote(source_volume_name, snapshot_ref, new_volume_name, _opts \\ %{})
      when is_binary(source_volume_name) and is_binary(snapshot_ref) and
             is_binary(new_volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, source_volume} <- fetch_volume(source_volume_name),
         {:ok, snapshot} <-
           SnapshotsHandler.resolve_snapshot(source_volume.id, snapshot_ref, source_volume_name),
         {:ok, promoted} <- Snapshot.promote(source_volume.id, snapshot.id, new_volume_name) do
      {:ok,
       %{
         volume_id: promoted.id,
         volume_name: promoted.name,
         source_volume_id: source_volume.id,
         source_volume_name: source_volume_name,
         snapshot_id: snapshot.id,
         root_chunk_hash_hex: render_root_hashes(snapshot.root_chunk_hashes)
       }}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Rolls a volume's live root back to a snapshot (#963).
  """
  @spec handle_volume_restore(binary(), binary(), map()) :: {:ok, map()} | {:error, term()}
  def handle_volume_restore(volume_name, snapshot_ref, opts \\ %{})
      when is_binary(volume_name) and is_binary(snapshot_ref) and is_map(opts) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- fetch_volume(volume_name),
         {:ok, snapshot} <-
           SnapshotsHandler.resolve_snapshot(volume.id, snapshot_ref, volume_name),
         {:ok, result} <- Snapshot.restore(volume.id, snapshot.id, restore_opts(opts)) do
      {:ok, restore_result_to_map(result)}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Exports a volume's live root (or a snapshot) as a TAR archive on the
  daemon's filesystem (#965).
  """
  @spec handle_volume_export(binary(), binary(), map()) :: {:ok, map()} | {:error, term()}
  def handle_volume_export(volume_name, output_path, opts \\ %{})
      when is_binary(volume_name) and is_binary(output_path) and is_map(opts) do
    set_cli_metadata()
    output_path = normalize_local_url(output_path)

    with :ok <- require_cluster(),
         {:ok, volume} <- fetch_volume(volume_name),
         export_opts <- export_opts_from_map(volume, opts),
         {:ok, summary} <- VolumeExport.export(volume_name, output_path, export_opts) do
      {:ok,
       %{
         path: summary.path,
         file_count: summary.file_count,
         byte_count: summary.byte_count
       }}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Imports a previously-exported tarball into a new volume (#966).
  """
  @spec handle_volume_import(binary(), binary()) :: {:ok, map()} | {:error, term()}
  def handle_volume_import(input_path, new_volume_name)
      when is_binary(input_path) and is_binary(new_volume_name) do
    set_cli_metadata()
    input_path = normalize_local_url(input_path)

    with :ok <- require_cluster(),
         {:ok, summary} <- VolumeImport.import_archive(input_path, new_volume_name) do
      {:ok,
       %{
         path: summary.path,
         volume_id: summary.volume_id,
         volume_name: summary.volume_name,
         file_count: summary.file_count,
         byte_count: summary.byte_count,
         source_volume_name: summary.source_volume_name
       }}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Snapshots `volume_name`, exports it to `output_path`, then drops the
  snapshot (#968). On export failure the snapshot is left in place.
  """
  @spec handle_backup_create(binary(), binary(), map()) :: {:ok, map()} | {:error, term()}
  def handle_backup_create(volume_name, output_path, opts \\ %{})
      when is_binary(volume_name) and is_binary(output_path) and is_map(opts) do
    set_cli_metadata()
    output_path = normalize_local_url(output_path)

    with :ok <- require_cluster(),
         {:ok, summary} <- Backup.create(volume_name, output_path, backup_create_opts(opts)) do
      {:ok, summary}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Reads a backup's manifest without unpacking the body (#968).
  """
  @spec handle_backup_describe(binary()) :: {:ok, map()} | {:error, term()}
  def handle_backup_describe(input_path) when is_binary(input_path) do
    set_cli_metadata()
    input_path = normalize_local_url(input_path)

    with :ok <- require_cluster(),
         {:ok, manifest} <- Backup.describe(input_path) do
      {:ok, manifest}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Restores a backup tarball into a brand-new volume (#968). Accepts a
  `:passphrase` (#1004) to decrypt an encrypted archive.
  """
  @spec handle_backup_restore(binary(), binary(), map()) :: {:ok, map()} | {:error, term()}
  def handle_backup_restore(input_path, new_volume_name, opts \\ %{})
      when is_binary(input_path) and is_binary(new_volume_name) and is_map(opts) do
    set_cli_metadata()
    input_path = normalize_local_url(input_path)

    with :ok <- require_cluster(),
         {:ok, summary} <- Backup.restore(input_path, new_volume_name, backup_restore_opts(opts)) do
      {:ok, summary}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Restore a chain of backup archives — a full followed by its
  incrementals, oldest-first — into a new volume (#1003). Each path is
  resolved as a local destination URL; replayed in order onto the new
  volume.
  """
  @spec handle_backup_restore_chain([binary()], binary(), map()) ::
          {:ok, map()} | {:error, term()}
  def handle_backup_restore_chain(archive_paths, new_volume_name, opts \\ %{})
      when is_list(archive_paths) and is_binary(new_volume_name) and is_map(opts) do
    set_cli_metadata()
    paths = Enum.map(archive_paths, &normalize_local_url/1)

    with :ok <- require_cluster(),
         {:ok, summary} <- Backup.restore_chain(paths, new_volume_name, backup_restore_opts(opts)) do
      {:ok, summary}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  # Private

  defp restore_opts(opts) do
    opts
    |> Enum.flat_map(fn
      {"safe", true} -> [safe: true]
      {:safe, true} -> [safe: true]
      {"force", true} -> [force: true]
      {:force, true} -> [force: true]
      _ -> []
    end)
  end

  defp restore_result_to_map(%{
         previous_root: previous_root,
         new_root: new_root,
         pre_restore_snapshot: pre_restore_snapshot
       }) do
    %{
      previous_root_hex: render_root_hashes(previous_root),
      new_root_hex: render_root_hashes(new_root),
      pre_restore_snapshot_id:
        case pre_restore_snapshot do
          nil -> nil
          %Snapshot{id: id} -> id
        end
    }
  end

  # Render per-shard roots (#1307) as one hex string: a single shard is
  # the bare hash (pre-sharding output), multiple are `shard:hex` joined.
  defp render_root_hashes(root_chunk_hashes) do
    case Map.to_list(root_chunk_hashes) do
      [{_shard, hash}] ->
        Base.encode16(hash, case: :lower)

      pairs ->
        pairs
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.map_join(",", fn {shard, hash} ->
          "#{shard}:#{Base.encode16(hash, case: :lower)}"
        end)
    end
  end

  defp export_opts_from_map(volume, opts) do
    snapshot_opts =
      case Map.get(opts, "snapshot_id") || Map.get(opts, :snapshot_id) do
        nil ->
          []

        ref when is_binary(ref) ->
          case SnapshotsHandler.resolve_snapshot(volume.id, ref, volume.name) do
            {:ok, snapshot} -> [snapshot_id: snapshot.id]
            # Bubble the error up as the export call's outer with-step.
            _ -> [snapshot_id: ref]
          end
      end

    snapshot_opts
    |> add_bool_opt(opts, "include_acls", :include_acls)
    |> add_bool_opt(opts, "include_system_xattrs", :include_system_xattrs)
  end

  defp add_bool_opt(opts_list, opts_map, str_key, atom_key) do
    case Map.get(opts_map, str_key) || Map.get(opts_map, atom_key) do
      true -> [{atom_key, true} | opts_list]
      _ -> opts_list
    end
  end

  defp backup_create_opts(opts) do
    Enum.flat_map(opts, fn
      {"name", name} when is_binary(name) -> [name: name]
      {:name, name} when is_binary(name) -> [name: name]
      {"incremental_from", p} when is_binary(p) -> [incremental_from: normalize_local_url(p)]
      {:incremental_from, p} when is_binary(p) -> [incremental_from: normalize_local_url(p)]
      {"passphrase", p} when is_binary(p) -> [passphrase: p]
      {:passphrase, p} when is_binary(p) -> [passphrase: p]
      _ -> []
    end)
  end

  defp backup_restore_opts(opts) do
    Enum.flat_map(opts, fn
      {"passphrase", p} when is_binary(p) -> [passphrase: p]
      {:passphrase, p} when is_binary(p) -> [passphrase: p]
      {"into_existing", true} -> [into_existing: true]
      {:into_existing, true} -> [into_existing: true]
      _ -> []
    end)
  end

  # Accept `file:///abs/path` URLs as a synonym for plain absolute
  # paths. `s3://` and other remote schemes aren't supported yet.
  defp normalize_local_url("file://" <> rest), do: rest
  defp normalize_local_url(path), do: path
end
