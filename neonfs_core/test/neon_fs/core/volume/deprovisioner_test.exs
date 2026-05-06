defmodule NeonFS.Core.Volume.DeprovisionerTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Volume.Deprovisioner

  describe "deprovision/2" do
    test "submits :unregister_volume_root and returns {:ok, :unregistered}" do
      table = :ets.new(:dt_calls, [:public, :duplicate_bag])

      registrar = fn command ->
        :ets.insert(table, {:cmd, command})
        {:ok, :registered}
      end

      assert {:ok, :unregistered} =
               Deprovisioner.deprovision("vol-123", bootstrap_registrar: registrar)

      assert [{:cmd, {:unregister_volume_root, "vol-123"}}] = :ets.lookup(table, :cmd)
    end

    test "accepts plain :ok from the registrar" do
      registrar = fn _ -> :ok end

      assert {:ok, :unregistered} =
               Deprovisioner.deprovision("vol-1", bootstrap_registrar: registrar)
    end

    test "is idempotent — re-issuing for an already-unregistered volume is fine" do
      registrar = fn _ -> {:ok, :no_op} end

      assert {:ok, :unregistered} =
               Deprovisioner.deprovision("vol-gone", bootstrap_registrar: registrar)

      assert {:ok, :unregistered} =
               Deprovisioner.deprovision("vol-gone", bootstrap_registrar: registrar)
    end

    test "surfaces bootstrap_unregister_failed on registrar error" do
      registrar = fn _ -> {:error, :ra_timeout} end

      assert {:error, {:bootstrap_unregister_failed, :ra_timeout}} =
               Deprovisioner.deprovision("vol-1", bootstrap_registrar: registrar)
    end
  end
end
