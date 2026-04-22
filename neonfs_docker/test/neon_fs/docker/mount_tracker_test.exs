defmodule NeonFS.Docker.MountTrackerTest do
  use ExUnit.Case, async: true

  alias NeonFS.Docker.MountTracker

  defp start_tracker(opts) do
    name = :"mount_tracker_#{System.unique_integer([:positive])}"
    {:ok, pid} = MountTracker.start_link(Keyword.put(opts, :name, name))
    Process.unlink(pid)

    on_exit(fn ->
      try do
        if Process.alive?(pid), do: GenServer.stop(pid, :shutdown, 1_000)
      catch
        :exit, _ -> :ok
      end
    end)

    %{pid: pid, name: name}
  end

  defp recording_mount_fn(parent) do
    fn volume_name ->
      send(parent, {:mount_called, volume_name})
      {:ok, {{:mount_id, volume_name}, "/tmp/" <> volume_name}}
    end
  end

  defp recording_unmount_fn(parent) do
    fn mount_id ->
      send(parent, {:unmount_called, mount_id})
      :ok
    end
  end

  describe "mount/2" do
    test "invokes mount_fn on first reference and stores the record" do
      parent = self()

      %{name: tracker} =
        start_tracker(mount_fn: recording_mount_fn(parent), unmount_fn: fn _ -> :ok end)

      assert {:ok, "/tmp/vol-a"} = MountTracker.mount(tracker, "vol-a")
      assert_received {:mount_called, "vol-a"}
      assert MountTracker.mountpoint_for(tracker, "vol-a") == "/tmp/vol-a"
    end

    test "reuses the existing mount on second reference (ref-count bump)" do
      parent = self()

      %{name: tracker} =
        start_tracker(mount_fn: recording_mount_fn(parent), unmount_fn: fn _ -> :ok end)

      assert {:ok, "/tmp/vol-a"} = MountTracker.mount(tracker, "vol-a")
      assert {:ok, "/tmp/vol-a"} = MountTracker.mount(tracker, "vol-a")

      assert_received {:mount_called, "vol-a"}
      refute_received {:mount_called, "vol-a"}

      assert [{"vol-a", %{ref_count: 2}}] = MountTracker.list(tracker)
    end

    test "propagates mount_fn errors without changing state" do
      mount_fn = fn _name -> {:error, :boom} end
      %{name: tracker} = start_tracker(mount_fn: mount_fn, unmount_fn: fn _ -> :ok end)

      assert {:error, :boom} = MountTracker.mount(tracker, "vol-a")
      assert MountTracker.mountpoint_for(tracker, "vol-a") == ""
      assert MountTracker.list(tracker) == []
    end
  end

  describe "unmount/2" do
    test "decrements the count without unmounting while references remain" do
      parent = self()

      %{name: tracker} =
        start_tracker(
          mount_fn: recording_mount_fn(parent),
          unmount_fn: recording_unmount_fn(parent)
        )

      {:ok, _} = MountTracker.mount(tracker, "vol-a")
      {:ok, _} = MountTracker.mount(tracker, "vol-a")

      assert :ok = MountTracker.unmount(tracker, "vol-a")
      refute_received {:unmount_called, _}

      assert [{"vol-a", %{ref_count: 1}}] = MountTracker.list(tracker)
      assert MountTracker.mountpoint_for(tracker, "vol-a") == "/tmp/vol-a"
    end

    test "invokes unmount_fn and drops the record when the last reference releases" do
      parent = self()

      %{name: tracker} =
        start_tracker(
          mount_fn: recording_mount_fn(parent),
          unmount_fn: recording_unmount_fn(parent)
        )

      {:ok, _} = MountTracker.mount(tracker, "vol-a")
      assert :ok = MountTracker.unmount(tracker, "vol-a")

      assert_received {:unmount_called, {:mount_id, "vol-a"}}
      assert MountTracker.list(tracker) == []
      assert MountTracker.mountpoint_for(tracker, "vol-a") == ""
    end

    test "is idempotent for volumes that were never mounted" do
      %{name: tracker} =
        start_tracker(
          mount_fn: fn _ -> {:error, :not_called} end,
          unmount_fn: fn _ -> {:error, :not_called} end
        )

      assert :ok = MountTracker.unmount(tracker, "never-mounted")
    end

    test "keeps the record intact when unmount_fn fails" do
      parent = self()

      unmount_fn = fn mount_id ->
        send(parent, {:unmount_called, mount_id})
        {:error, :device_busy}
      end

      %{name: tracker} =
        start_tracker(mount_fn: recording_mount_fn(parent), unmount_fn: unmount_fn)

      {:ok, _} = MountTracker.mount(tracker, "vol-a")
      assert {:error, :device_busy} = MountTracker.unmount(tracker, "vol-a")

      assert_received {:unmount_called, {:mount_id, "vol-a"}}
      assert [{"vol-a", %{ref_count: 1}}] = MountTracker.list(tracker)
    end
  end

  describe "mountpoint_for/2" do
    test "returns empty string for volumes that aren't mounted" do
      %{name: tracker} =
        start_tracker(mount_fn: fn _ -> {:error, :not_called} end, unmount_fn: fn _ -> :ok end)

      assert MountTracker.mountpoint_for(tracker, "nothing") == ""
    end
  end

  describe "terminate/2" do
    test "unmounts every tracked volume on shutdown" do
      parent = self()

      %{pid: pid, name: tracker} =
        start_tracker(
          mount_fn: recording_mount_fn(parent),
          unmount_fn: recording_unmount_fn(parent)
        )

      {:ok, _} = MountTracker.mount(tracker, "vol-a")
      {:ok, _} = MountTracker.mount(tracker, "vol-b")

      assert :ok = GenServer.stop(pid, :shutdown)

      assert_received {:unmount_called, {:mount_id, "vol-a"}}
      assert_received {:unmount_called, {:mount_id, "vol-b"}}
    end
  end
end
