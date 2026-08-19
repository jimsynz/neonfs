defmodule NeonFS.FUSE.MountRecovery do
  @moduledoc """
  Decides what a restarting daemon should do about each mountpoint it has a
  record for.

  `NeonFS.FUSE.MountRegistry` says what this host is meant to be serving;
  the kernel says what is actually at those paths. This module compares the
  two, one path at a time, and `NeonFS.FUSE.MountManager` acts on the answer.

  ## Why the kernel has to be asked

  A FUSE mountpoint whose server process died stays in the mount table. It is
  not gone and it is not working: every syscall against it returns `ENOTCONN`
  until something unmounts it. Remounting over the top fails, and so does
  assuming the path is free. So recovery reaps first, then remounts.

  The `ENOTCONN` is also what makes attribution unnecessary. A foreign mount
  someone else is serving answers `stat` normally, so it can never be mistaken
  for one of ours to reap — the classification below never has to decide who
  mounted a *working* filesystem, only that it is not ours to disturb.
  """

  require Logger

  alias Wick.Fusermount

  @typedoc """
  What is at a recorded mountpoint.

    * `:stale` — a mount whose server is gone (`ENOTCONN`). Ours to reap and
      remount; nothing else leaves a mountpoint in this state.
    * `:serving` — something answers there. Not ours to touch, whether it is a
      mount of ours that outlived us or an unrelated filesystem.
    * `:vacant` — the directory is there and nothing is mounted on it. Remount.
    * `:missing` — the path does not exist, or is not a directory. Nothing to
      mount onto.
  """
  @type classification :: :stale | :serving | :vacant | :missing

  @doc """
  Classify a recorded mountpoint against the running kernel.
  """
  @spec classify(String.t()) :: classification()
  def classify(mount_point) do
    case File.stat(mount_point) do
      {:error, :enotconn} -> :stale
      {:error, _reason} -> :missing
      {:ok, %File.Stat{type: :directory} = stat} -> classify_directory(mount_point, stat)
      {:ok, _stat} -> :missing
    end
  end

  @doc """
  Unmount a stale mountpoint so it can be mounted again.

  Lazy, because a stale mount can still have references held by processes that
  are themselves stuck on it; a plain unmount would fail with `EBUSY` and leave
  the path unusable. The detach happens immediately and the mount is cleaned up
  once the last reference goes.
  """
  @spec reap(String.t()) :: :ok | {:error, term()}
  def reap(mount_point) do
    case Fusermount.unmount(mount_point, lazy: true) do
      :ok ->
        :ok

      {:error, reason} = error ->
        Logger.warning("Could not reap stale mountpoint",
          mount_point: mount_point,
          reason: inspect(reason)
        )

        error
    end
  end

  # A directory that stats cleanly may still have a filesystem on it. The
  # discriminator is the device id: a mountpoint's differs from its parent's,
  # because they are on different filesystems. That is how `mountpoint(1)`
  # decides, and unlike parsing `/proc/mounts` it does not have to reason about
  # bind mounts, escaped path characters, or paths that appear more than once.
  defp classify_directory(mount_point, %File.Stat{major_device: device}) do
    case File.stat(Path.dirname(mount_point)) do
      {:ok, %File.Stat{major_device: ^device}} -> :vacant
      {:ok, _different_filesystem} -> :serving
      {:error, _reason} -> :vacant
    end
  end
end
