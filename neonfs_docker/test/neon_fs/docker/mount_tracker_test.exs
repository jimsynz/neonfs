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

  describe ":max_mounts cap" do
    test "no cap configured — every new mount succeeds" do
      parent = self()

      %{name: tracker} =
        start_tracker(mount_fn: recording_mount_fn(parent), unmount_fn: fn _ -> :ok end)

      assert {:ok, _} = MountTracker.mount(tracker, "vol-a")
      assert {:ok, _} = MountTracker.mount(tracker, "vol-b")
      assert {:ok, _} = MountTracker.mount(tracker, "vol-c")

      assert MountTracker.capacity(tracker) == %{used: 3, max: nil}
    end

    test "refuses new mounts past the cap with :mount_pool_full" do
      parent = self()

      %{name: tracker} =
        start_tracker(
          mount_fn: recording_mount_fn(parent),
          unmount_fn: fn _ -> :ok end,
          max_mounts: 2
        )

      assert {:ok, _} = MountTracker.mount(tracker, "vol-a")
      assert {:ok, _} = MountTracker.mount(tracker, "vol-b")
      assert {:error, :mount_pool_full} = MountTracker.mount(tracker, "vol-c")

      # The refusal didn't allocate — the cap reflects the live count.
      assert MountTracker.capacity(tracker) == %{used: 2, max: 2}
    end

    test "ref-count bumps on an already-mounted volume don't count against the cap" do
      parent = self()

      %{name: tracker} =
        start_tracker(
          mount_fn: recording_mount_fn(parent),
          unmount_fn: fn _ -> :ok end,
          max_mounts: 1
        )

      assert {:ok, _} = MountTracker.mount(tracker, "vol-a")
      assert {:ok, _} = MountTracker.mount(tracker, "vol-a")
      assert {:ok, _} = MountTracker.mount(tracker, "vol-a")

      # One unique volume, three references — under the cap of 1.
      assert MountTracker.capacity(tracker) == %{used: 1, max: 1}
      assert [{"vol-a", %{ref_count: 3}}] = MountTracker.list(tracker)
    end

    test "unmounting frees a slot for a different volume" do
      parent = self()

      %{name: tracker} =
        start_tracker(
          mount_fn: recording_mount_fn(parent),
          unmount_fn: fn _ -> :ok end,
          max_mounts: 1
        )

      assert {:ok, _} = MountTracker.mount(tracker, "vol-a")
      assert {:error, :mount_pool_full} = MountTracker.mount(tracker, "vol-b")

      assert :ok = MountTracker.unmount(tracker, "vol-a")
      assert {:ok, _} = MountTracker.mount(tracker, "vol-b")
    end

    test "capacity/1 reports {used, max} both with and without a cap" do
      parent = self()

      %{name: capped} =
        start_tracker(
          mount_fn: recording_mount_fn(parent),
          unmount_fn: fn _ -> :ok end,
          max_mounts: 4
        )

      %{name: uncapped} =
        start_tracker(mount_fn: recording_mount_fn(parent), unmount_fn: fn _ -> :ok end)

      assert MountTracker.capacity(capped) == %{used: 0, max: 4}
      assert MountTracker.capacity(uncapped) == %{used: 0, max: nil}

      {:ok, _} = MountTracker.mount(capped, "vol-a")
      assert MountTracker.capacity(capped) == %{used: 1, max: 4}
    end
  end
end
