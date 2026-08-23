defmodule NeonFS.Block.Ublk.Capability do
  @moduledoc """
  Whether this node can serve a device over ublk, and if not, which half is
  missing.

  Two checks, because either can fail without the other: a kernel with the
  driver loaded and no helper shipped, or a helper present on a host whose
  kernel has no `ublk_drv`. They are reported apart so that forcing ublk on a
  host that cannot do it says *which* thing to fix — a single "unavailable"
  sends an operator to `modprobe` when the problem is a release assembled
  without its native binary.

  ## Cached per node

  The probe is two `File.exists?` calls and caching it is not about cost. It
  is about the answer being *stable*: every attachment on a node agrees about
  what that node can do, and the frontend a device gets does not depend on
  which second it was attached in. `refresh/0` exists for an operator who has
  just loaded the module, and for tests.
  """

  @key {__MODULE__, :ublk}

  @type reason :: {:ublk_driver_absent, Path.t()} | {:ublk_helper_absent, Path.t()}

  @doc """
  Whether ublk is usable here, from the cache, probing once on first ask.
  """
  @spec check() :: :ok | {:error, reason()}
  def check do
    case :persistent_term.get(@key, :unprobed) do
      :unprobed -> refresh()
      cached -> cached
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
    if File.exists?(path), do: :ok, else: {:error, {:ublk_driver_absent, path}}
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
