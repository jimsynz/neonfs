defmodule NeonFS.CLI.Handler.DR do
  @moduledoc """
  CLI command handlers for disaster-recovery snapshots: create, list,
  and show the cluster-state DR snapshots in the `_system` volume's
  `/dr` directory (#324).

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates its `handle_dr_snapshot_*` RPC entry points here.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.{AuditLog, DRSnapshot}
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

  # Private

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
