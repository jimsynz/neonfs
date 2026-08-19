defmodule NeonFS.FUSE.MountRecoveryTest do
  @moduledoc """
  What a restarting daemon does about the mounts it had.

  The kernel side is faked: a real FUSE mount cannot be made from the BEAM
  (see `test/test_helper.exs`), so `Wick.Fusermount` and
  `NeonFS.FUSE.Session` are stubbed and the assertions are about the
  decisions — what gets remounted, what gets left alone, and what the
  registry says afterwards.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.FUSE.{MountManager, MountRecovery, MountRegistry}

  @moduletag :tmp_dir

  describe "classify/1" do
    test "a path that does not exist has nothing to mount onto", %{tmp_dir: tmp_dir} do
      assert MountRecovery.classify(Path.join(tmp_dir, "gone")) == :missing
    end

    test "a regular file is not a mount point", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "a-file")
      File.write!(path, "")

      assert MountRecovery.classify(path) == :missing
    end

    test "an empty directory is ready to be mounted onto", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "empty")
      File.mkdir_p!(path)

      assert MountRecovery.classify(path) == :vacant
    end

    # `/proc` is a filesystem mounted on a directory of the root filesystem,
    # so its device id differs from its parent's — the same signal a stale
    # NeonFS mount point would give, minus the `ENOTCONN`. Nothing here may
    # ever be reaped, which is the point of the assertion.
    test "a directory with a filesystem on it is somebody's to serve" do
      assert MountRecovery.classify("/proc") == :serving
    end
  end

  describe "restart recovery" do
    setup :set_mimic_global

    setup %{tmp_dir: tmp_dir} do
      registry = Path.join(tmp_dir, "fuse_mounts.json")
      Application.put_env(:neonfs_fuse, :mount_registry_path, registry)
      Application.put_env(:neonfs_fuse, :mount_recovery_backoff_ms, 10)

      on_exit(fn ->
        Application.put_env(
          :neonfs_fuse,
          :mount_registry_path,
          Path.join(System.tmp_dir!(), "neonfs_fuse_test_mounts.json")
        )

        Application.put_env(:neonfs_fuse, :mount_recovery_attempts, 1)
        Application.put_env(:neonfs_fuse, :mount_recovery_backoff_ms, 10)
      end)

      {:ok, registry: registry}
    end

    test "a recorded mount is put back", %{tmp_dir: tmp_dir} do
      mount_point = Path.join(tmp_dir, "vol-a")
      File.mkdir_p!(mount_point)
      stub_mountable_volume()
      stub_kernel_mount()
      record(mount_point, "vol-a", ro: true)

      start_supervised!(MountManager)

      assert [mount] = MountManager.list_mounts()
      assert mount.volume_name == "vol-a"
      assert mount.mount_point == mount_point
      assert mount.opts[:ro] == true
    end

    # The mount whose server died is still in the kernel mount table, and
    # mounting over it fails. Reaping first is what makes the remount possible,
    # so the reap has to happen even though the path looks broken.
    test "a stale mount point is reaped before it is remounted", %{tmp_dir: tmp_dir} do
      mount_point = Path.join(tmp_dir, "vol-a")
      File.mkdir_p!(mount_point)
      test = self()

      stub_mountable_volume()
      stub_kernel_mount()
      stub(Wick.Fusermount, :unmount, fn path, opts ->
        send(test, {:reaped, path, opts})
        :ok
      end)

      stub(MountRecovery, :classify, fn ^mount_point -> :stale end)
      record(mount_point, "vol-a")

      start_supervised!(MountManager)

      assert_receive {:reaped, ^mount_point, opts}
      assert opts[:lazy] == true
      assert [%{mount_point: ^mount_point}] = MountManager.list_mounts()
    end

    test "a mount point something else is serving is left alone", %{registry: registry} do
      stub_mountable_volume()

      stub(Wick.Fusermount, :mount, fn path, _opts ->
        flunk("recovery mounted over a filesystem it does not own: #{path}")
      end)

      record("/proc", "vol-a")

      start_supervised!(MountManager)

      assert MountManager.list_mounts() == []
      assert {:ok, []} = load(registry)
    end

    # Retrying forever would be worse than giving up: `unmount/1` only knows
    # about live mounts, so a record that never recovers is one no operator can
    # clear.
    test "a record that cannot be remounted is dropped once the attempts run out", %{
      tmp_dir: tmp_dir,
      registry: registry
    } do
      mount_point = Path.join(tmp_dir, "vol-a")
      File.mkdir_p!(mount_point)
      Application.put_env(:neonfs_fuse, :mount_recovery_attempts, 2)

      stub(NeonFS.Client, :core_call, fn NeonFS.Core.VolumeRegistry, :get_by_name, _ ->
        {:error, :all_nodes_unreachable}
      end)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :fuse, :mount_recovery, :abandoned]
        ])

      record(mount_point, "vol-a")

      start_supervised!(MountManager)

      assert_receive {[:neonfs, :fuse, :mount_recovery, :abandoned], ^ref, %{count: 1}, _}, 2_000
      assert MountManager.list_mounts() == []
      assert {:ok, []} = load(registry)
    end

    test "a mount point that has gone away is dropped", %{tmp_dir: tmp_dir, registry: registry} do
      record(Path.join(tmp_dir, "never-created"), "vol-a")

      start_supervised!(MountManager)

      assert MountManager.list_mounts() == []
      assert {:ok, []} = load(registry)
    end
  end

  describe "what clears a record" do
    setup :set_mimic_global

    setup %{tmp_dir: tmp_dir} do
      registry = Path.join(tmp_dir, "fuse_mounts.json")
      Application.put_env(:neonfs_fuse, :mount_registry_path, registry)

      on_exit(fn ->
        Application.put_env(
          :neonfs_fuse,
          :mount_registry_path,
          Path.join(System.tmp_dir!(), "neonfs_fuse_test_mounts.json")
        )
      end)

      mount_point = Path.join(tmp_dir, "vol-a")
      File.mkdir_p!(mount_point)
      stub_mountable_volume()
      stub_kernel_mount()

      start_supervised!(MountManager)
      {:ok, mount_id} = MountManager.mount("vol-a", mount_point)

      {:ok, mount_id: mount_id, mount_point: mount_point, registry: registry}
    end

    test "mounting records it", %{mount_point: mount_point, registry: registry} do
      assert {:ok, [%{volume_name: "vol-a", mount_point: ^mount_point}]} = load(registry)
    end

    test "unmounting clears it", %{mount_id: mount_id, registry: registry} do
      assert :ok = MountManager.unmount(mount_id)
      assert {:ok, []} = load(registry)
    end

    # The whole point of the record. If shutting down cleared it, the DaemonSet
    # rollout this exists for would still lose every mount — the pod stops
    # tidily, and the next one has nothing to recover.
    test "shutting down does not clear it", %{mount_point: mount_point, registry: registry} do
      assert :ok = MountManager.detach_all()

      assert MountManager.list_mounts() == []
      assert {:ok, [%{mount_point: ^mount_point}]} = load(registry)
    end
  end

  defp record(mount_point, volume_name, opts \\ []) do
    :ok =
      MountRegistry.save([
        %{
          id: "mount_" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower),
          volume_name: volume_name,
          mount_point: mount_point,
          opts: opts,
          mounted_at: DateTime.utc_now()
        }
      ])
  end

  defp load(registry) do
    Application.put_env(:neonfs_fuse, :mount_registry_path, registry)
    MountRegistry.load()
  end

  defp stub_mountable_volume do
    stub(NeonFS.Client, :core_call, fn
      NeonFS.Core.VolumeRegistry, :get_by_name, [name] ->
        {:ok, %{id: "vol-#{name}", name: name, type: :filesystem}}

      NeonFS.Core.Authorise, :check, _args ->
        :ok
    end)
  end

  defp stub_kernel_mount do
    stub(Wick.Fusermount, :mount, fn _path, _opts -> {:ok, make_ref()} end)
    stub(Wick.Fusermount, :unmount, fn _path -> :ok end)
    stub(Wick.Fusermount, :unmount, fn _path, _opts -> :ok end)
    stub(NeonFS.FUSE.MountSupervisor, :start_cache, fn _opts -> {:ok, spawn_idle()} end)
    stub(NeonFS.FUSE.MountSupervisor, :stop_cache, fn _pid -> :ok end)
    stub(NeonFS.FUSE.MetadataCache, :table, fn _pid, _opts -> :ets.new(:cache, [:set, :public]) end)
    stub(NeonFS.FUSE.Session, :start_link, fn _opts -> {:ok, spawn_idle()} end)
  end

  # An OTP process, not a bare `spawn`: `MountManager` stops the session with
  # `GenServer.stop/3`, which waits out its full timeout against anything that
  # does not answer a system message.
  defp spawn_idle do
    {:ok, pid} = Agent.start(fn -> nil end)
    pid
  end
end
