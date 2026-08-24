defmodule NeonFS.Block.FrontendSelectionTest do
  @moduledoc """
  Which frontend this node offers, and what it says when it cannot offer the
  one it was told to.

  The distinction under test is between a *fallback* and a *refusal*. `:auto`
  falls back, because a host without ublk should still serve a device.
  Forcing does not, because a benchmark that silently measured NBD and
  reported it as ublk would be worse than one that did not run.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Block.Ublk.Capability

  setup do
    on_exit(fn ->
      for key <- [:frontend, :ublk_control_path, :ublk_helper] do
        Application.delete_env(:neonfs_block, key)
      end

      Capability.refresh()
    end)

    :ok
  end

  describe "what this node offers" do
    test "NBD always, and ublk when both halves are there" do
      with_ublk()

      assert NeonFS.Block.frontends() == [:nbd, :ublk]
    end

    test "NBD alone when the driver is missing" do
      without_driver()

      assert NeonFS.Block.frontends() == [:nbd]
    end

    test "NBD alone when the helper is missing" do
      without_helper()

      assert NeonFS.Block.frontends() == [:nbd]
    end
  end

  describe "the two checks are reported apart" do
    # An operator told only "ublk unavailable" reaches for `modprobe`, which
    # is the wrong fix half the time.
    test "a missing driver names the control device" do
      without_driver()

      assert {:error, {:ublk_driver_absent, "/dev/does-not-exist"}} = Capability.check()
    end

    test "a missing helper names the binary" do
      without_helper()

      assert {:error, {:ublk_helper_absent, path}} = Capability.check()
      assert String.ends_with?(path, "not-a-helper")
    end

    # A file that is there but not executable fails at spawn as a port that
    # died for no stated reason, which is the least informative failure of
    # the three.
    test "a helper without its executable bit is a missing helper" do
      path = Path.join(System.tmp_dir!(), "unexecutable-#{System.unique_integer([:positive])}")
      File.write!(path, "")
      File.chmod!(path, 0o644)
      on_exit(fn -> File.rm(path) end)

      with_driver()
      Application.put_env(:neonfs_block, :ublk_helper, path)
      Capability.refresh()

      assert {:error, {:ublk_helper_absent, ^path}} = Capability.check()
    end

    # The rig found this: `/dev/ublk-control` is `crw------- root root` and the
    # daemon is not root, so an existence check reported ublk available on a
    # node where every attach then failed with EACCES deep inside the helper.
    #
    # A directory rather than a mode-000 file, because the property under test
    # is "exists but will not open" and CI runs as root — which opens a
    # mode-000 file happily and would make this assert nothing there. `EISDIR`
    # is a type check rather than a permission one, so it holds for every uid.
    # The EACCES *message* is tested where it is built, in the CLI handler.
    test "a path that exists but will not open is not availability" do
      unopenable = Path.join(System.tmp_dir!(), "ublk-dir-#{System.unique_integer([:positive])}")
      File.mkdir_p!(unopenable)
      on_exit(fn -> File.rmdir(unopenable) end)

      Application.put_env(:neonfs_block, :ublk_control_path, unopenable)
      Application.put_env(:neonfs_block, :ublk_helper, System.find_executable("sh"))
      Capability.refresh()

      assert {:error, {:ublk_control_inaccessible, ^unopenable, :eisdir}} = Capability.check()
      assert NeonFS.Block.frontends() == [:nbd]
    end

    test "the driver is named first, since a helper cannot help without it" do
      without_driver()
      Application.put_env(:neonfs_block, :ublk_helper, "/also/missing")
      Capability.refresh()

      assert {:error, {:ublk_driver_absent, _path}} = Capability.check()
    end
  end

  describe "selection" do
    test "auto prefers ublk where it works" do
      with_ublk()

      assert {:ok, :ublk} = NeonFS.Block.select(:auto)
    end

    test "auto falls back to NBD where it does not" do
      without_driver()

      assert {:ok, :nbd} = NeonFS.Block.select(:auto)
    end

    test "forcing NBD is honoured on a host that could do ublk" do
      with_ublk()

      assert {:ok, :nbd} = NeonFS.Block.select(:nbd)
    end

    test "forcing ublk where it works is honoured" do
      with_ublk()

      assert {:ok, :ublk} = NeonFS.Block.select(:ublk)
    end

    test "forcing ublk where it does not carries the check that failed" do
      without_helper()

      assert {:error, {:frontend_forced_unavailable, :ublk, {:ublk_helper_absent, _path}}} =
               NeonFS.Block.select(:ublk)
    end

    test "the preference comes from config when none is given" do
      with_ublk()
      Application.put_env(:neonfs_block, :frontend, :nbd)

      assert {:ok, :nbd} = NeonFS.Block.select()
    end

    test "auto is the default preference" do
      assert NeonFS.Block.preference() == :auto
    end

    test "a preference nothing implements is an error, not a fallback" do
      assert {:error, {:unknown_frontend, :virtio}} = NeonFS.Block.select(:virtio)
    end
  end

  # A positive is cached so that two attachments in the same second cannot
  # disagree; a negative is not, so a node picks up a late `modprobe`.
  describe "caching" do
    test "a positive answer survives the world changing under it" do
      with_ublk()
      assert :ok = Capability.check()

      Application.put_env(:neonfs_block, :ublk_control_path, "/dev/does-not-exist")
      assert :ok = Capability.check()

      assert {:error, {:ublk_driver_absent, _path}} = Capability.refresh()
    end

    # The rig found this: a node that booted before `modprobe ublk_drv` cached
    # "driver absent" and then refused ublk forever, reporting a missing
    # driver while the control device sat there. Restarting the node was the
    # only fix.
    test "a negative answer is re-probed, so a late modprobe is picked up" do
      without_driver()
      assert {:error, {:ublk_driver_absent, _path}} = Capability.check()

      # The operator loads the module. Nothing tells this node.
      with_driver()

      assert :ok = Capability.check()
    end

    test "a negative for a missing helper is re-probed too" do
      without_helper()
      assert {:error, {:ublk_helper_absent, _path}} = Capability.check()

      Application.put_env(:neonfs_block, :ublk_helper, System.find_executable("sh"))

      assert :ok = Capability.check()
    end
  end

  defp with_ublk do
    with_driver()
    Application.put_env(:neonfs_block, :ublk_helper, System.find_executable("sh"))
    Capability.refresh()
  end

  defp with_driver do
    path = Path.join(System.tmp_dir!(), "ublk-control-#{System.unique_integer([:positive])}")
    File.write!(path, "")
    on_exit(fn -> File.rm(path) end)
    Application.put_env(:neonfs_block, :ublk_control_path, path)
  end

  defp without_driver do
    Application.put_env(:neonfs_block, :ublk_control_path, "/dev/does-not-exist")
    Application.put_env(:neonfs_block, :ublk_helper, System.find_executable("sh"))
    Capability.refresh()
  end

  defp without_helper do
    with_driver()
    Application.put_env(:neonfs_block, :ublk_helper, "/nonexistent/not-a-helper")
    Capability.refresh()
  end
end
