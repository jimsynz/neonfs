defmodule NeonFS.NFS.ExportSupervisorTest do
  use ExUnit.Case, async: false

  alias NeonFS.NFS.{ExportSupervisor, InodeTable}

  setup do
    start_supervised!(InodeTable)
    start_supervised!(ExportSupervisor)
    :ok
  end

  test "starts a handler under supervision" do
    assert {:ok, pid} = ExportSupervisor.start_handler(test_notify: self())
    assert Process.alive?(pid)
  end

  test "stops a handler" do
    {:ok, pid} = ExportSupervisor.start_handler(test_notify: self())
    assert :ok = ExportSupervisor.stop_handler(pid)
    refute Process.alive?(pid)
  end

  test "handler receives NFS operations" do
    {:ok, pid} = ExportSupervisor.start_handler(test_notify: self())

    send(pid, {:nfs_op, 1, {"getattr", %{"inode" => 1, "volume_id" => <<0::128>>}}})
    assert_receive {:nfs_op_complete, 1, {:ok, %{"type" => "attrs"}}}, 1_000
  end
end
