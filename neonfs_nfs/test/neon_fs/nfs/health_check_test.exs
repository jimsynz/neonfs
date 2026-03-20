defmodule NeonFS.NFS.HealthCheckTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.HealthCheck
  alias NeonFS.NFS.HealthCheck, as: NFSHealthCheck

  setup do
    on_exit(fn -> HealthCheck.reset() end)
  end

  describe "register_checks/0" do
    test "registers 3 NFS checks" do
      NFSHealthCheck.register_checks()

      report = HealthCheck.check(timeout_ms: 100)
      check_names = report.checks |> Map.keys() |> Enum.sort()

      assert :nfs_exports in check_names
      assert :nfs_metadata_cache in check_names
      assert :nfs_registrar in check_names
      assert length(check_names) == 3
    end
  end
end
