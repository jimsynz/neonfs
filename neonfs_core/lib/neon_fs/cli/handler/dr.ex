defmodule NeonFS.CLI.Handler.DR do
  @moduledoc """
  CLI command handlers for disaster-recovery snapshots: create, list,
  and show the cluster-state DR snapshots in the `_system` volume's
  `/dr` directory (#324), plus the full-cluster `dr restore`
  orchestration (#1005).

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates its `handle_dr_snapshot_*` and `handle_dr_restore` RPC entry
  points here.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.{AuditLog, Backup, DRSnapshot, VolumeRegistry}
  alias NeonFS.Error.NotFound

  @doc """
  Triggers an immediate DR snapshot (#324).
  """
  @spec handle_dr_snapshot_create(map()) :: {:ok, map()} | {:error, term()}
  def handle_dr_snapshot_create(_opts \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, snapshot} <- DRSnapshot.create([]) do
      AuditLog.log_event(
        event_type: :dr_snapshot_created,
        actor_uid: 0,
        resource: snapshot.path,
        details: %{
          state_version: snapshot.manifest.state_version,
          files: length(snapshot.manifest.files)
        }
      )

      {:ok, dr_snapshot_to_serialisable(snapshot)}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Lists every DR snapshot, newest first (#324).
  """
  @spec handle_dr_snapshot_list() :: {:ok, [map()]} | {:error, term()}
  def handle_dr_snapshot_list do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, snapshots} <- DRSnapshot.list() do
      {:ok, Enum.map(snapshots, &dr_snapshot_to_serialisable/1)}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Fetches a single DR snapshot's manifest by id (#324).
  """
  @spec handle_dr_snapshot_show(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_dr_snapshot_show(id) when is_binary(id) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, snapshot} <- DRSnapshot.get(id) do
      {:ok, dr_snapshot_to_serialisable(snapshot)}
    else
      {:error, :not_found} ->
        {:error, NotFound.exception(message: "DR snapshot '#{id}' not found")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Apply a DR snapshot's cluster-wide metadata back into live Ra state
  (#1005) — the restore-primitive slice of full-cluster `dr restore`.
  """
  @spec handle_dr_snapshot_apply(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_dr_snapshot_apply(id) when is_binary(id) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, %{restored: counts, generation: generation}} <- DRSnapshot.restore(id) do
      AuditLog.log_event(
        event_type: :dr_snapshot_applied,
        actor_uid: 0,
        resource: id,
        details: %{restored: counts, generation: generation}
      )

      {:ok,
       %{
         id: id,
         restored: counts,
         total: counts |> Map.values() |> Enum.sum(),
         generation: generation
       }}
    else
      {:error, :not_found} ->
        {:error, NotFound.exception(message: "DR snapshot '#{id}' not found")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Export a DR snapshot off-cluster to `dest_dir` on the daemon's
  filesystem (#1367) so it survives a bare-metal disaster.
  """
  @spec handle_dr_snapshot_export(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def handle_dr_snapshot_export(id, dest_dir) when is_binary(id) and is_binary(dest_dir) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, summary} <- DRSnapshot.export(id, dest_dir) do
      AuditLog.log_event(
        event_type: :dr_snapshot_exported,
        actor_uid: 0,
        resource: id,
        details: %{dest: summary.dest, file_count: summary.file_count}
      )

      {:ok, summary}
    else
      {:error, :not_found} ->
        {:error, NotFound.exception(message: "DR snapshot '#{id}' not found")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Stage an exported DR snapshot from `source_dir` back into the
  `_system` volume (#1367) so `handle_dr_snapshot_apply/1` can consume
  it on a freshly-bootstrapped cluster.
  """
  @spec handle_dr_snapshot_import(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_dr_snapshot_import(source_dir) when is_binary(source_dir) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, summary} <- DRSnapshot.import_snapshot(source_dir) do
      AuditLog.log_event(
        event_type: :dr_snapshot_imported,
        actor_uid: 0,
        resource: summary.id,
        details: %{source: source_dir, file_count: summary.file_count}
      )

      {:ok, summary}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  @doc """
  Full-cluster restore (#1005): stage the DR snapshot exported at
  `source_dir` back into the freshly-bootstrapped `_system` volume, apply
  its cluster-wide metadata (recreating every volume's shell and the
  cluster CA / credentials / keys), then restore each volume's content
  from its backup archive into that shell.

  This drives steps 2–3 of the bare-metal runbook; the operator does
  step 1 (`neonfs cluster init` on a fresh node) and step 4
  (`neonfs cluster join` for the remaining nodes) around it.

  `opts`:
    * `"catalogue"` — path to a JSON object `{"<volume>": "<archive>"}`
      pinning where each volume's backup archive lives. Relative archive
      paths resolve against `source_dir`; the catalogue is authoritative,
      so volumes it omits are left as empty shells. Without a catalogue,
      each volume's archive is expected at
      `<source_dir>/volumes/<volume>.backup`.
    * `"passphrase"` — passphrase for encrypted backup archives (#1004).

  Best-effort per volume: a volume whose archive is missing or fails to
  restore is reported in the result rather than aborting the whole
  restore, so one pass recovers everything recoverable.
  """
  @spec handle_dr_restore(String.t(), map()) :: {:ok, map()} | {:error, term()}
  def handle_dr_restore(source_dir, opts \\ %{}) when is_binary(source_dir) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, catalogue} <- load_catalogue(Map.get(opts, "catalogue"), source_dir),
         {:ok, %{id: snapshot_id}} <- DRSnapshot.import_snapshot(source_dir),
         {:ok, %{restored: counts, generation: generation}} <- DRSnapshot.restore(snapshot_id) do
      volume_results = restore_volumes(source_dir, catalogue, opts)

      AuditLog.log_event(
        event_type: :dr_restored,
        actor_uid: 0,
        resource: snapshot_id,
        details: %{generation: generation, volumes: length(volume_results)}
      )

      {:ok,
       %{
         snapshot_id: snapshot_id,
         generation: generation,
         restored: counts,
         volumes: volume_results,
         volumes_restored: Enum.count(volume_results, &(&1.status == "restored")),
         volumes_failed: Enum.count(volume_results, &(&1.status == "failed"))
       }}
    else
      {:error, reason} -> {:error, wrap_error(reason)}
    end
  end

  # Private

  defp restore_volumes(source_dir, catalogue, opts) do
    restore_opts = backup_restore_opts(opts)

    VolumeRegistry.list()
    |> Enum.map(fn volume ->
      case archive_for(volume.name, catalogue, source_dir) do
        {:ok, archive} -> restore_one_volume(volume.name, archive, restore_opts)
        :none -> %{name: volume.name, status: "skipped", reason: "no backup archive"}
      end
    end)
  end

  defp restore_one_volume(name, archive, restore_opts) do
    case Backup.restore(archive, name, [{:into_existing, true} | restore_opts]) do
      {:ok, summary} ->
        %{
          name: name,
          status: "restored",
          archive: archive,
          files: summary.file_count,
          bytes: summary.byte_count
        }

      {:error, reason} ->
        %{name: name, status: "failed", archive: archive, reason: inspect(reason)}
    end
  end

  defp archive_for(name, catalogue, _source_dir) when is_map(catalogue) do
    case Map.fetch(catalogue, name) do
      {:ok, archive} -> {:ok, archive}
      :error -> :none
    end
  end

  defp archive_for(name, nil, source_dir) do
    archive = Path.join([source_dir, "volumes", "#{name}.backup"])
    if File.exists?(archive), do: {:ok, archive}, else: :none
  end

  defp load_catalogue(nil, _source_dir), do: {:ok, nil}

  defp load_catalogue(path, source_dir) when is_binary(path) do
    with {:ok, raw} <- File.read(path),
         {:ok, map} when is_map(map) <- Jason.decode(raw) do
      {:ok, Map.new(map, fn {name, archive} -> {name, resolve_archive(archive, source_dir)} end)}
    else
      {:ok, _non_object} ->
        {:error, {:invalid_catalogue, "catalogue must be a JSON object of volume => archive"}}

      {:error, %Jason.DecodeError{}} ->
        {:error, {:invalid_catalogue, "catalogue is not valid JSON"}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve_archive(archive, source_dir) do
    if Path.type(archive) == :absolute, do: archive, else: Path.join(source_dir, archive)
  end

  defp backup_restore_opts(opts) do
    case Map.get(opts, "passphrase") do
      passphrase when is_binary(passphrase) and passphrase != "" -> [passphrase: passphrase]
      _ -> []
    end
  end

  defp dr_snapshot_to_serialisable(%{id: id, path: path, manifest: manifest}) do
    %{
      id: id,
      path: path,
      version: manifest.version,
      created_at: manifest.created_at,
      state_version: manifest.state_version,
      file_count: length(manifest.files || []),
      total_bytes: total_manifest_bytes(manifest.files || []),
      files: Enum.map(manifest.files || [], &dr_snapshot_file_to_serialisable/1)
    }
  end

  defp dr_snapshot_to_serialisable(%{path: path, manifest: manifest}) do
    # `DRSnapshot.create/1` returns `{:ok, %{path, manifest}}` without an
    # id key; derive the id from the directory name so the CLI surface
    # is uniform across create / list / show.
    id = Path.basename(path)
    dr_snapshot_to_serialisable(%{id: id, path: path, manifest: manifest})
  end

  defp dr_snapshot_file_to_serialisable(file) do
    %{
      path: Map.get(file, :path),
      bytes: Map.get(file, :bytes),
      sha256: Map.get(file, :sha256),
      kind: file |> Map.get(:kind) |> to_string()
    }
  end

  defp total_manifest_bytes(files) do
    Enum.reduce(files, 0, fn f, acc -> acc + (Map.get(f, :bytes) || 0) end)
  end
end
