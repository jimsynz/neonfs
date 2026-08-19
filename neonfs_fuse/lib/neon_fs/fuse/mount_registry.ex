defmodule NeonFS.FUSE.MountRegistry do
  @moduledoc """
  On-disk record of the mounts this host is meant to be serving.

  `NeonFS.FUSE.MountManager` holds its mount table in memory, so a daemon
  restart used to lose every mount it held: the mountpoint stays in the kernel
  answering `ENOTCONN`, and nothing tries again. This file is what survives the
  process, so a restart can reconcile against it.

  ## Intent, not observation

  An entry means "this host is supposed to serve this volume at this path". It
  is written when a mount succeeds and removed when someone unmounts, and by
  nothing else — a crash, a `SIGKILL` or an orderly shutdown all leave the
  record intact, because none of them change what the host is supposed to be
  serving. That is what makes recovery possible at all: an entry the shutdown
  path erased is an entry the next boot cannot bring back.

  So the file and `MountManager.list_mounts/0` answer different questions and
  can legitimately disagree. `list_mounts/0` is what is mounted now.

  ## Location

  `<meta dir>/fuse_mounts.json`, alongside `cluster.json` — mounts belong to a
  host rather than to a cluster, so this is local state and deliberately not in
  the metadata layer. The `:mount_registry_path` application env overrides the
  whole path; tests set it.

  Writes go through a temp file, `datasync` and rename, matching
  `NeonFS.Cluster.State`: a half-written record read at boot would be
  indistinguishable from an operator having unmounted something.
  """

  alias NeonFS.Cluster.State
  alias NeonFS.FUSE.MountInfo

  @filename "fuse_mounts.json"

  @typedoc """
  A recorded mount: the volume, where it belongs, and the options needed to
  put it back.
  """
  @type entry :: %{
          id: String.t(),
          volume_name: String.t(),
          mount_point: String.t(),
          opts: keyword(),
          mounted_at: DateTime.t()
        }

  # Only options that change what the remounted filesystem *is* are kept.
  # Persisting the whole keyword list would carry a caller's incidental
  # extras into a remount made months later on a different daemon version.
  @persisted_opts [:allow_other, :allow_root, :atime_mode, :auto_unmount, :gids, :ro, :uid]

  @doc """
  The path of the registry file.
  """
  @spec path() :: String.t()
  def path do
    case Application.fetch_env(:neonfs_fuse, :mount_registry_path) do
      {:ok, configured} -> configured
      :error -> Path.join(State.meta_dir(), @filename)
    end
  end

  @doc """
  Read the recorded mounts.

  A missing file is an empty registry — the common case on a host that has
  never mounted anything. An unreadable or malformed one is reported so the
  caller can complain rather than silently forget every mount.
  """
  @spec load() :: {:ok, [entry()]} | {:error, term()}
  def load do
    case File.read(path()) do
      {:ok, contents} -> decode(contents)
      {:error, :enoent} -> {:ok, []}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Replace the recorded mounts with `entries`.
  """
  @spec save([entry()]) :: :ok | {:error, term()}
  def save(entries) do
    file = path()
    temp = file <> ".tmp"

    with :ok <- File.mkdir_p(Path.dirname(file)),
         :ok <- File.write(temp, encode(entries)),
         :ok <- datasync(temp) do
      File.rename(temp, file)
    end
  end

  @doc """
  Build an entry from a live mount and the options it was mounted with.
  """
  @spec entry(MountInfo.t()) :: entry()
  def entry(%MountInfo{} = mount_info) do
    %{
      id: mount_info.id,
      volume_name: mount_info.volume_name,
      mount_point: mount_info.mount_point,
      opts: Keyword.take(mount_info.opts, @persisted_opts),
      mounted_at: mount_info.started_at
    }
  end

  defp encode(entries) do
    entries
    |> Enum.map(fn entry ->
      %{
        "id" => entry.id,
        "volume_name" => entry.volume_name,
        "mount_point" => entry.mount_point,
        "opts" => Map.new(entry.opts, fn {key, value} -> {to_string(key), value} end),
        "mounted_at" => DateTime.to_iso8601(entry.mounted_at)
      }
    end)
    |> then(&%{"mounts" => &1})
    |> :json.encode()
    |> IO.iodata_to_binary()
  end

  defp decode(contents) do
    case :json.decode(contents) do
      %{"mounts" => mounts} when is_list(mounts) -> {:ok, Enum.map(mounts, &decode_entry/1)}
      _other -> {:error, :invalid_registry}
    end
  rescue
    _ -> {:error, :invalid_json}
  end

  defp decode_entry(mount) do
    %{
      id: mount["id"],
      volume_name: mount["volume_name"],
      mount_point: mount["mount_point"],
      opts: decode_opts(mount["opts"]),
      mounted_at: decode_timestamp(mount["mounted_at"])
    }
  end

  # Keys are matched against the known list rather than turned into atoms:
  # a registry file is host state an operator can edit, and
  # `String.to_atom/1` on its contents would leak the atom table.
  defp decode_opts(opts) when is_map(opts) do
    known = Map.new(@persisted_opts, fn key -> {to_string(key), key} end)

    for {key, value} <- opts, Map.has_key?(known, key) do
      {Map.fetch!(known, key), decode_opt_value(Map.fetch!(known, key), value)}
    end
  end

  defp decode_opts(_), do: []

  # `:atime_mode` reaches `NeonFS.FUSE.Session` as an atom; every other
  # persisted option is a boolean or a number and round-trips as itself.
  defp decode_opt_value(:atime_mode, value) when is_binary(value) do
    case value do
      "relatime" -> :relatime
      "strictatime" -> :strictatime
      _ -> :noatime
    end
  end

  defp decode_opt_value(_key, value), do: value

  defp decode_timestamp(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, timestamp, _offset} -> timestamp
      {:error, _reason} -> DateTime.utc_now()
    end
  end

  defp decode_timestamp(_), do: DateTime.utc_now()

  defp datasync(file) do
    case File.open(file, [:read, :write]) do
      {:ok, device} ->
        result = :file.datasync(device)
        :ok = File.close(device)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end
end
