defmodule NeonFS.Block.Ublk.Capability do
  @moduledoc """
  Whether this node can serve a device over ublk, and if not, which half is
  missing.

  Three checks, because each fails without the others: a kernel with no
  `ublk_drv`, a control device this process may not open, or a release
  assembled without its native helper. They are reported apart so that
  forcing ublk on a host that cannot do it says *which* thing to fix — a
  single "unavailable" sends an operator to `modprobe` when the problem is a
  permission or a missing binary.

  ## The control device has to be *openable*, not merely present

  `/dev/ublk-control` is `crw------- root root` on a stock Debian, and the
  daemon does not run as root — so a probe that only asked whether the path
  exists reported ublk available on a node where every attach then failed
  deep in the helper with `EACCES`. The rig found exactly that. Opening it is
  the same syscall the helper makes, so the probe now fails where the helper
  would, with a reason that says so.

  ## A positive answer is cached; a negative one is not

  Caching is not about cost — the probe is two `File.exists?` calls. It is
  about a *positive* answer being stable: once a node can serve ublk, every
  attachment on it agrees, because neither a loaded module nor a shipped
  binary goes away under a serving target.

  A negative answer is different, and caching it was a bug the rig found. A
  node that boots before `modprobe ublk_drv` cached "driver absent" and then
  refused ublk forever — `block frontends` reported the driver missing while
  `/dev/ublk-control` sat there, and the only fix was restarting the node.
  Re-probing a negative costs two stat calls and can only flip when the world
  actually changed, so nothing is destabilised by it: an attachment that now
  succeeds is not a disagreement with one that failed before the module
  existed.

  `refresh/0` remains, for a test that needs to invalidate a positive.
  """

  @key {__MODULE__, :ublk}

  @type reason ::
          {:ublk_driver_absent, Path.t()}
          | {:ublk_control_inaccessible, Path.t(), atom()}
          | {:ublk_helper_absent, Path.t()}

  @doc """
  Whether ublk is usable here.

  A cached `:ok` is returned as-is; anything else re-probes, so a module
  loaded after this node started is picked up without a restart.
  """
  @spec check() :: :ok | {:error, reason()}
  def check do
    case :persistent_term.get(@key, :unprobed) do
      :ok -> :ok
      _unprobed_or_negative -> refresh()
    end
  end

  @doc "Re-probes and replaces the cached answer."
  @spec refresh() :: :ok | {:error, reason()}
  def refresh do
    answer = probe()
    :persistent_term.put(@key, answer)
    answer
  end

  @doc """
  Probes without touching the cache.

  The driver is checked first: a host without it cannot use a helper however
  well built, so naming the helper there would be the less useful of two true
  statements.
  """
  @spec probe() :: :ok | {:error, reason()}
  def probe do
    with :ok <- driver(), do: helper()
  end

  @doc "The ublk control device this node looks for."
  @spec control_path() :: Path.t()
  def control_path do
    Application.get_env(:neonfs_block, :ublk_control_path, "/dev/ublk-control")
  end

  @doc """
  Where the helper binary is.

  Built into `priv/` by this package's build, and overridable so a release or
  the rig can put it elsewhere. Resolved through `:code.priv_dir/1` rather
  than assumed, because that is the only thing that knows where an
  application's files landed.
  """
  @spec helper_path() :: Path.t()
  def helper_path do
    case Application.get_env(:neonfs_block, :ublk_helper) do
      path when is_binary(path) -> path
      _unset -> Path.join([:code.priv_dir(:neonfs_block), "native", "neonfs_ublk"])
    end
  end

  defp driver do
    path = control_path()

    if File.exists?(path),
      do: openable(path),
      else: {:error, {:ublk_driver_absent, path}}
  end

  # Read-write, because that is how the helper opens it: a read-only check
  # would pass where the helper fails. The handle is closed immediately —
  # this asks the kernel a question rather than taking the device.
  defp openable(path) do
    case File.open(path, [:raw, :read, :write]) do
      {:ok, handle} ->
        File.close(handle)
        :ok

      {:error, reason} ->
        {:error, {:ublk_control_inaccessible, path, reason}}
    end
  end

  # Executable, not merely present: a release that copied the file without its
  # mode is a failure this would otherwise report at spawn, as a port that
  # died for no stated reason.
  defp helper do
    path = helper_path()

    case File.stat(path) do
      {:ok, %File.Stat{mode: mode}} -> executable(path, Bitwise.band(mode, 0o111))
      {:error, _reason} -> {:error, {:ublk_helper_absent, path}}
    end
  end

  defp executable(_path, bits) when bits != 0, do: :ok
  defp executable(path, _none), do: {:error, {:ublk_helper_absent, path}}
end
