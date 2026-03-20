defmodule NeonFS.FUSE.HealthCheckTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.HealthCheck
  alias NeonFS.FUSE.HealthCheck, as: FUSEHealthCheck

  setup do
    on_exit(fn -> HealthCheck.reset() end)
  end

  describe "register_checks/0" do
    test "registers 3 FUSE checks" do
      FUSEHealthCheck.register_checks()

      report = HealthCheck.check(timeout_ms: 100)
      check_names = report.checks |> Map.keys() |> Enum.sort()

      assert :fuse_inode_table in check_names
      assert :fuse_mounts in check_names
      assert :fuse_registrar in check_names
      assert length(check_names) == 3
    end
  end
end
