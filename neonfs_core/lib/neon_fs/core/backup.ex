defmodule NeonFS.Core.Backup do
  @moduledoc """
  Thin orchestration layer over `Snapshot.create` + `VolumeExport` +
  `Snapshot.delete` for the operator-facing "back up this volume"
  verb (#968, part of #248).

  A backup is logically: \"freeze the volume at a moment, write its
  contents out somewhere durable, then release the freeze.\" The
  freeze step is a `Snapshot.create` (O(1), shares storage with the
  live volume), the write step is a `VolumeExport` of the snapshot's
  tree (#992's `--snapshot` slice), and the release is
  `Snapshot.delete` (the export's bytes are now durable at the
  destination — the in-cluster pin isn't needed any more).

  ## Failure behaviour

  If the export fails between the snapshot-create and the
  snapshot-delete, the snapshot is *left in place* (per #968's
  acceptance criteria) so the operator can retry the export without
  paying for another snapshot.

  ## V1 scope

  Local-path destinations only. S3 / `file://` URL handling lands
  alongside the corresponding follow-ups on #992. Cross-cluster
  restore and incremental backups remain tracked in #248.
  """

  alias NeonFS.Core.Snapshot
  alias NeonFS.Core.VolumeExport
  alias NeonFS.Core.VolumeImport
  alias NeonFS.Core.VolumeRegistry

  require Logger

  @type create_summary :: %{
          path: String.t(),
          volume: String.t(),
          snapshot_id: binary(),
          file_count: non_neg_integer(),
          byte_count: non_neg_integer()
        }

  @doc """
  Snapshot `volume_name`, export the snapshot's tree to
  `output_path`, then delete the snapshot.

  Returns `{:ok, summary}` on success. On export failure the
  freshly-created snapshot is *not* deleted so the operator can
  retry without re-snapshotting.
  """
  @spec create(binary(), Path.t(), keyword()) ::
          {:ok, create_summary()} | {:error, term()}
  def create(volume_name, output_path, opts \\ [])
      when is_binary(volume_name) and is_binary(output_path) and is_list(opts) do
    snapshot_opts = Keyword.take(opts, [:name])

    with {:ok, volume} <- fetch_volume(volume_name),
         {:ok, snap} <- Snapshot.create(volume.id, snapshot_opts) do
      finish_create(volume, snap, volume_name, output_path)
    end
  end

  defp finish_create(volume, snap, volume_name, output_path) do
    case VolumeExport.export(volume_name, output_path, snapshot_id: snap.id) do
      {:ok, summary} ->
        _ = Snapshot.delete(volume.id, snap.id)

        {:ok,
         %{
           path: summary.path,
           volume: volume_name,
           snapshot_id: snap.id,
           file_count: summary.file_count,
           byte_count: summary.byte_count
         }}

      {:error, _} = err ->
        # Leave the snapshot in place per #968's "retry without
        # re-snapshotting" semantics.
        Logger.warning(
          "backup export failed; leaving snapshot #{snap.id} for #{volume_name} " <>
            "in place — retry export against #{output_path} without re-snapshotting"
        )

        err
    end
  end

  @doc """
  Restore a backup tarball at `input_path` into a brand-new volume
  named `new_volume_name`. Identical to `VolumeImport.import_archive/3`
  — exposed under the backup namespace because that's where
  operators look.
  """
  @spec restore(Path.t(), binary(), keyword()) ::
          {:ok, VolumeImport.import_summary()} | {:error, term()}
  def restore(input_path, new_volume_name, opts \\ [])
      when is_binary(input_path) and is_binary(new_volume_name) do
    VolumeImport.import_archive(input_path, new_volume_name, opts)
  end

  @doc """
  Read the manifest of the backup at `input_path` without unpacking
  the body. Used by `backup list` to show what's available at a
  destination.

  Pulls only `manifest.json` out of the tarball via `:erl_tar` with
  the `:memory` + `files:` filter, so RAM cost is bounded by the
  manifest size regardless of how large the archive is.
  """
  @spec describe(Path.t()) :: {:ok, map()} | {:error, term()}
  def describe(input_path) when is_binary(input_path) do
    if File.regular?(input_path) do
      do_describe(input_path)
    else
      {:error, :input_missing}
    end
  end

  defp do_describe(input_path) do
    # `:erl_tar.extract/2` returns `{:ok, [{Name, Bin}]}` only when
    # the `:memory` option is given as a bare atom — the keyword
    # shape (`memory: true`) silently extracts to disk and returns
    # `:ok` with no manifest data.
    case :erl_tar.extract(String.to_charlist(input_path), [
           :memory,
           {:files, [~c"manifest.json"]}
         ]) do
      {:ok, [{_name, body}]} ->
        case Jason.decode(body) do
          {:ok, manifest} -> {:ok, manifest}
          {:error, reason} -> {:error, {:manifest_invalid, reason}}
        end

      {:ok, []} ->
        {:error, {:manifest_missing, input_path}}

      {:error, reason} ->
        {:error, {:tar_open_failed, reason}}
    end
  end

  ## Private

  defp fetch_volume(name) do
    case VolumeRegistry.get_by_name(name) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, :volume_not_found}
    end
  end
end
