defmodule NeonFS.NFS.HandlerTest do
  use ExUnit.Case, async: false

  alias NeonFS.NFS.{Handler, InodeTable}

  setup do
    start_supervised!(InodeTable)
    {:ok, handler} = start_supervised({Handler, test_notify: self()})
    {:ok, handler: handler}
  end

  describe "getattr on virtual root" do
    test "returns directory attributes for root inode", %{handler: handler} do
      send(handler, {:nfs_op, 1, {"getattr", %{"inode" => 1, "volume_id" => <<0::128>>}}})
      assert_receive {:nfs_op_complete, 1, {:ok, attrs}}, 1_000

      assert attrs["type"] == "attrs"
      assert attrs["file_id"] == 1
      assert attrs["kind"] == "directory"
      assert attrs["mode"] == 0o755
    end
  end

  describe "lookup on virtual root" do
    test "returns error when core is unavailable", %{handler: handler} do
      # lookup_volume calls core_call which returns error when no core is connected
      send(
        handler,
        {:nfs_op, 2,
         {"lookup",
          %{"parent_inode" => 1, "parent_volume_id" => <<0::128>>, "name" => "nonexistent"}}}
      )

      # errno 5 = EIO (core call failed)
      assert_receive {:nfs_op_complete, 2, {:error, 5}}, 1_000
    end
  end

  describe "operations with unregistered volume" do
    test "getattr returns stale error for unknown volume_id", %{handler: handler} do
      unknown_vol = :crypto.hash(:md5, "unknown_volume")

      send(
        handler,
        {:nfs_op, 3, {"getattr", %{"inode" => 2, "volume_id" => unknown_vol}}}
      )

      # errno 70 = ESTALE
      assert_receive {:nfs_op_complete, 3, {:error, 70}}, 1_000
    end
  end

  describe "unknown operations" do
    test "returns ENOSYS for unknown op", %{handler: handler} do
      send(handler, {:nfs_op, 4, {"nonexistent_op", %{}}})

      # errno 38 = ENOSYS
      assert_receive {:nfs_op_complete, 4, {:error, 38}}, 1_000
    end
  end

  describe "telemetry" do
    test "emits request telemetry", %{handler: handler} do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :nfs, :request, :stop]
        ])

      send(handler, {:nfs_op, 5, {"getattr", %{"inode" => 1, "volume_id" => <<0::128>>}}})
      assert_receive {:nfs_op_complete, 5, _}, 1_000

      assert_receive {[:neonfs, :nfs, :request, :stop], ^ref, %{duration: _},
                      %{operation: "getattr", result: :ok}},
                     1_000
    end
  end
end
