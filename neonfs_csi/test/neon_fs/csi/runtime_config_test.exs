defmodule NeonFS.CSI.RuntimeConfigTest do
  @moduledoc """
  `config/runtime.exs` is the only thing that lets a *release* choose its
  mode, and the mode decides which socket the plugin opens. Nothing read
  `NEONFS_CSI_MODE` before, so every containerised pod ran as a controller —
  including the node DaemonSet, whose `csi-node-driver-registrar` then dialled
  a socket nothing had created.

  Read through `Config.Reader` rather than by booting a release: the file is
  the unit under test.
  """
  use ExUnit.Case, async: false

  @runtime_config "config/runtime.exs"

  setup do
    on_exit(fn -> System.delete_env("NEONFS_CSI_MODE") end)
    :ok
  end

  test "an unset NEONFS_CSI_MODE leaves the mode alone" do
    System.delete_env("NEONFS_CSI_MODE")
    assert Config.Reader.read!(@runtime_config) == []
  end

  test "NEONFS_CSI_MODE selects the mode the pod runs as" do
    System.put_env("NEONFS_CSI_MODE", "node")
    assert Config.Reader.read!(@runtime_config) == [neonfs_csi: [mode: :node]]

    System.put_env("NEONFS_CSI_MODE", "controller")
    assert Config.Reader.read!(@runtime_config) == [neonfs_csi: [mode: :controller]]
  end

  # A typo'd mode would otherwise be indistinguishable from an unset one, and
  # would surface as a node plugin quietly serving the controller's socket.
  test "an unrecognised NEONFS_CSI_MODE refuses to boot" do
    System.put_env("NEONFS_CSI_MODE", "Node")

    assert_raise RuntimeError, ~r/NEONFS_CSI_MODE must be/, fn ->
      Config.Reader.read!(@runtime_config)
    end
  end
end
