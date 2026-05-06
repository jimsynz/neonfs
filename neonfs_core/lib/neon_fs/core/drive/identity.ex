defmodule NeonFS.Core.Drive.Identity do
  @moduledoc """
  On-disk self-identity for a drive.

  Each drive carries a small JSON file at its root (`.neonfs-drive.json`)
  recording which cluster owns it, when it was created, and which
  on-disk-format version it was provisioned at. The cluster reads
  this file at boot and at `drive add` time:

  - At `drive add`: refuse if the file exists with a foreign `cluster_id`
    (operator pointed `drive add` at a path that belongs to a different
    cluster — almost always a mistake).
  - At boot: refuse to mount a drive whose `cluster_id` doesn't match
    the local cluster. Same rationale.

  This file is also part of the **reconstruction invariant** for the
  per-volume-metadata epic (#750): walking every drive's identity file
  is what lets the cluster rebuild its bootstrap-layer Ra state from
  on-disk truth alone.

  ## Schema

  JSON map with this shape:

      {
        "schema_version": 1,
        "drive_id": "disk1",
        "cluster_id": "clust_...",
        "created_at": "2026-05-06T05:00:00Z",
        "created_by_neonfs_version": "0.2.7",
        "on_disk_format_version": 1
      }

  `schema_version` covers the schema of *this file*. `on_disk_format_version`
  covers the data layout of the rest of the drive. Both are `1` today.
  """

  @schema_version 1
  @file_name ".neonfs-drive.json"

  @typedoc """
  Reasons `read/1` and `ensure/3` can fail.
  """
  @type read_error ::
          :enoent
          | {:invalid_json, term()}
          | {:unsupported_schema_version, integer()}
          | {:malformed, String.t()}
          | {:io_error, File.posix()}

  @type ensure_error ::
          read_error()
          | {:foreign_cluster, expected: String.t(), actual: String.t()}
          | {:drive_id_mismatch, expected: String.t(), actual: String.t()}

  @type t :: %__MODULE__{
          schema_version: pos_integer(),
          drive_id: String.t(),
          cluster_id: String.t(),
          created_at: DateTime.t(),
          created_by_neonfs_version: String.t(),
          on_disk_format_version: pos_integer()
        }

  @enforce_keys [
    :drive_id,
    :cluster_id,
    :created_at,
    :created_by_neonfs_version,
    :on_disk_format_version
  ]
  defstruct [
    :drive_id,
    :cluster_id,
    :created_at,
    :created_by_neonfs_version,
    :on_disk_format_version,
    schema_version: @schema_version
  ]

  @doc """
  Returns the absolute path to the identity file for the drive at
  `drive_path`.
  """
  @spec path(String.t()) :: String.t()
  def path(drive_path), do: Path.join(drive_path, @file_name)

  @doc """
  Builds a fresh identity for a drive being added to `cluster_id`.

  `created_by_neonfs_version` is read from the running `:neonfs_core`
  application's `:vsn`; `on_disk_format_version` is hardcoded to the
  current format the daemon writes (1 today).
  """
  @spec new(String.t(), String.t()) :: t()
  def new(drive_id, cluster_id) do
    %__MODULE__{
      drive_id: drive_id,
      cluster_id: cluster_id,
      created_at: DateTime.utc_now(),
      created_by_neonfs_version: current_neonfs_version(),
      on_disk_format_version: 1
    }
  end

  @doc """
  Atomically writes the identity file to `<drive_path>/.neonfs-drive.json`.

  Writes to a sibling temp file first, then renames — the rename is
  atomic on POSIX filesystems, so a partial write can never leave a
  half-formed identity file in place.
  """
  @spec write(String.t(), t()) :: :ok | {:error, File.posix()}
  def write(drive_path, %__MODULE__{} = identity) do
    final = path(drive_path)
    tmp = final <> ".tmp"

    json = Jason.encode!(to_map(identity), pretty: true)

    with :ok <- File.write(tmp, json),
         :ok <- File.rename(tmp, final) do
      :ok
    else
      {:error, reason} ->
        _ = File.rm(tmp)
        {:error, reason}
    end
  end

  @doc """
  Reads and parses the identity file from `<drive_path>/.neonfs-drive.json`.
  """
  @spec read(String.t()) :: {:ok, t()} | {:error, read_error()}
  def read(drive_path) do
    file = path(drive_path)

    with {:ok, raw} <- read_file(file),
         {:ok, parsed} <- decode_json(raw) do
      from_map(parsed)
    end
  end

  @doc """
  Ensures the drive at `drive_path` carries an identity for `cluster_id`
  and `drive_id`.

  - If no identity file exists, writes a fresh one. Used both at first
    `drive add` of a brand-new drive and at flag-day for dev clusters
    whose existing drives predate this file.
  - If an identity file exists and matches, returns `:ok`.
  - If it exists with a foreign `cluster_id` or a different `drive_id`,
    returns `{:error, ...}` and writes nothing — the caller (the
    `drive add` flow) refuses the add.
  """
  @spec ensure(String.t(), String.t(), String.t()) :: :ok | {:error, ensure_error()}
  def ensure(drive_path, cluster_id, drive_id) do
    case read(drive_path) do
      {:ok, %__MODULE__{cluster_id: ^cluster_id, drive_id: ^drive_id}} ->
        :ok

      {:ok, %__MODULE__{cluster_id: other_cluster}} when other_cluster != cluster_id ->
        {:error, {:foreign_cluster, expected: cluster_id, actual: other_cluster}}

      {:ok, %__MODULE__{drive_id: other_drive_id}} ->
        {:error, {:drive_id_mismatch, expected: drive_id, actual: other_drive_id}}

      {:error, :enoent} ->
        write(drive_path, new(drive_id, cluster_id))

      {:error, _reason} = error ->
        error
    end
  end

  ## Private

  defp read_file(file) do
    case File.read(file) do
      {:ok, _} = ok -> ok
      {:error, :enoent} -> {:error, :enoent}
      {:error, reason} -> {:error, {:io_error, reason}}
    end
  end

  defp decode_json(raw) do
    case Jason.decode(raw) do
      {:ok, parsed} -> {:ok, parsed}
      {:error, reason} -> {:error, {:invalid_json, reason}}
    end
  end

  defp from_map(%{"schema_version" => version}) when version != @schema_version do
    {:error, {:unsupported_schema_version, version}}
  end

  defp from_map(
         %{
           "schema_version" => @schema_version,
           "drive_id" => drive_id,
           "cluster_id" => cluster_id,
           "created_at" => created_at,
           "created_by_neonfs_version" => created_by,
           "on_disk_format_version" => format_version
         } = _map
       )
       when is_binary(drive_id) and is_binary(cluster_id) and is_binary(created_at) and
              is_binary(created_by) and is_integer(format_version) do
    case DateTime.from_iso8601(created_at) do
      {:ok, dt, _offset} ->
        {:ok,
         %__MODULE__{
           drive_id: drive_id,
           cluster_id: cluster_id,
           created_at: dt,
           created_by_neonfs_version: created_by,
           on_disk_format_version: format_version
         }}

      {:error, _} ->
        {:error, {:malformed, "created_at is not a valid ISO 8601 timestamp"}}
    end
  end

  defp from_map(_) do
    {:error, {:malformed, "missing or wrong-type required fields"}}
  end

  defp to_map(%__MODULE__{} = identity) do
    %{
      "schema_version" => identity.schema_version,
      "drive_id" => identity.drive_id,
      "cluster_id" => identity.cluster_id,
      "created_at" => DateTime.to_iso8601(identity.created_at),
      "created_by_neonfs_version" => identity.created_by_neonfs_version,
      "on_disk_format_version" => identity.on_disk_format_version
    }
  end

  defp current_neonfs_version do
    case Application.spec(:neonfs_core, :vsn) do
      nil -> "unknown"
      vsn -> List.to_string(vsn)
    end
  end
end
