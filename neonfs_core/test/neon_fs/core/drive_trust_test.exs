defmodule NeonFS.Core.DriveTrustTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{DriveTrust, RaServer, RaSupervisor}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    start_ra()
    :ok = RaServer.init_cluster()
    on_exit(fn -> cleanup_test_dirs() end)
    :ok
  end

  test "an unmarked drive defaults to :trusted" do
    assert DriveTrust.state(node(), "drv-1") == :trusted
    refute DriveTrust.unverified?(node(), "drv-1")
    assert DriveTrust.unverified() == []
  end

  test "mark_unverified then mark_trusted round-trips through Ra" do
    assert :ok = DriveTrust.mark_unverified(node(), "drv-1")
    assert DriveTrust.state(node(), "drv-1") == :unverified
    assert {node(), "drv-1"} in DriveTrust.unverified()

    assert :ok = DriveTrust.mark_trusted(node(), "drv-1")
    assert DriveTrust.state(node(), "drv-1") == :trusted
    assert DriveTrust.unverified() == []
  end

  test "mark_node_unverified marks all of a node's registered drives" do
    for id <- ["drv-1", "drv-2"] do
      {:ok, :ok, _leader} = RaSupervisor.command({:register_drive, drive_entry(id)})
    end

    assert :ok = DriveTrust.mark_node_unverified(node())
    assert DriveTrust.state(node(), "drv-1") == :unverified
    assert DriveTrust.state(node(), "drv-2") == :unverified
  end

  defp drive_entry(id) do
    %{
      drive_id: id,
      node: node(),
      cluster_id: "test",
      on_disk_format_version: 1,
      registered_at: DateTime.utc_now()
    }
  end
end
