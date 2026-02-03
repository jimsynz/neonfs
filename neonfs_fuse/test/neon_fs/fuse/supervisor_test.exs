defmodule NeonFS.FUSE.SupervisorTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.VolumeRegistry
  alias NeonFS.FUSE.{InodeTable, MountManager, MountSupervisor, Supervisor}

  # Start the full FUSE supervisor for this test module since it tests the supervisor itself
  setup do
    # Start the full supervisor (which starts InodeTable, MountSupervisor, MountManager)
    start_supervised!(Supervisor)

    :ok
  end

  describe "Supervisor" do
    test "starts without errors" do
      # Supervisor is started by application, verify it's running
      assert Process.whereis(Supervisor) != nil
    end

    test "starts all required children" do
      children = :supervisor.which_children(Supervisor)
      child_modules = Enum.map(children, fn {id, _pid, _type, _modules} -> id end)

      # Verify all required children are started
      assert InodeTable in child_modules
      assert MountSupervisor in child_modules
      assert MountManager in child_modules
    end

    test "children are started in correct order" do
      children = :supervisor.which_children(Supervisor)

      # Extract child order (reverse because which_children returns in reverse)
      child_order =
        children
        |> Enum.reverse()
        |> Enum.map(fn {id, _pid, _type, _modules} -> id end)

      # InodeTable should start before MountManager
      inode_index = Enum.find_index(child_order, &(&1 == InodeTable))
      manager_index = Enum.find_index(child_order, &(&1 == MountManager))

      assert inode_index < manager_index,
             "InodeTable should start before MountManager"

      # MountSupervisor should start before MountManager
      supervisor_index = Enum.find_index(child_order, &(&1 == MountSupervisor))

      assert supervisor_index < manager_index,
             "MountSupervisor should start before MountManager"
    end

    test "uses one_for_one restart strategy" do
      # Test by killing a child and verifying others remain running
      inode_pid = Process.whereis(InodeTable)
      manager_pid = Process.whereis(MountManager)

      assert inode_pid != nil
      assert manager_pid != nil

      # Kill InodeTable
      Process.exit(inode_pid, :kill)
      :timer.sleep(100)

      # MountManager should still be running (one_for_one strategy)
      assert Process.whereis(MountManager) == manager_pid

      # InodeTable should have restarted
      new_inode_pid = Process.whereis(InodeTable)
      assert new_inode_pid != nil
      assert new_inode_pid != inode_pid
    end
  end

  describe "MountSupervisor" do
    test "starts without errors" do
      assert Process.whereis(MountSupervisor) != nil
    end

    test "can start handlers dynamically" do
      # Create a test volume first
      {:ok, vol_id} = create_test_volume()

      handler_opts = [volume: vol_id, mount_id: "test_mount_1"]

      {:ok, handler_pid} = MountSupervisor.start_handler(handler_opts)

      assert Process.alive?(handler_pid)

      # Cleanup
      MountSupervisor.stop_handler(handler_pid)
    end

    test "can stop handlers" do
      # Create a test volume first
      {:ok, vol_id} = create_test_volume()

      handler_opts = [volume: vol_id, mount_id: "test_mount_2"]
      {:ok, handler_pid} = MountSupervisor.start_handler(handler_opts)

      assert Process.alive?(handler_pid)

      :ok = MountSupervisor.stop_handler(handler_pid)

      # Wait a bit for the process to terminate
      :timer.sleep(50)

      refute Process.alive?(handler_pid)
    end

    test "returns error when stopping non-existent handler" do
      fake_pid = spawn(fn -> :ok end)
      :timer.sleep(10)

      assert {:error, :not_found} = MountSupervisor.stop_handler(fake_pid)
    end

    test "isolates handler failures" do
      # Create a test volume
      {:ok, vol_id} = create_test_volume()

      # Start two handlers
      {:ok, handler1} = MountSupervisor.start_handler(volume: vol_id, mount_id: "mount1")
      {:ok, handler2} = MountSupervisor.start_handler(volume: vol_id, mount_id: "mount2")

      assert Process.alive?(handler1)
      assert Process.alive?(handler2)

      # Kill handler1
      Process.exit(handler1, :kill)
      :timer.sleep(50)

      # Handler2 should still be alive (isolation)
      assert Process.alive?(handler2)

      # Cleanup
      MountSupervisor.stop_handler(handler2)
    end
  end

  # NOTE: Mount integration tests that require a core node have been moved to
  # neonfs_integration/test/integration/mount_manager_test.exs

  describe "Application graceful shutdown" do
    test "stop callback unmounts all filesystems" do
      # This test verifies the stop/1 callback exists and is callable
      # We can't actually test full shutdown without stopping the app

      # Verify the callback exists
      assert function_exported?(NeonFS.FUSE.Application, :stop, 1)

      # Call stop directly (won't actually stop application in test)
      assert :ok = NeonFS.FUSE.Application.stop(:normal)
    end
  end

  describe "neonfs_core dependency" do
    test "neonfs_core application is available" do
      # Verify neonfs_core is loaded
      assert Application.spec(:neonfs_core) != nil
    end

    test "can access VolumeRegistry from neonfs_core" do
      # Verify we can interact with core components
      assert Process.whereis(VolumeRegistry) != nil
    end
  end

  # Helper functions

  defp create_test_volume(name \\ "test_volume_#{:rand.uniform(10000)}") do
    with {:registry, pid} when is_pid(pid) <- {:registry, Process.whereis(VolumeRegistry)},
         {:get, {:error, :not_found}} <- {:get, VolumeRegistry.get_by_name(name)},
         {:create, {:ok, volume}} <-
           {:create,
            VolumeRegistry.create(name,
              durability: %{type: :replicate, factor: 1, min_copies: 1},
              compression: %{algorithm: :zstd, level: 3, min_size: 4096},
              verification: %{on_read: :never}
            )} do
      {:ok, volume.id}
    else
      {:registry, nil} -> {:error, :volume_registry_not_started}
      {:get, {:ok, volume}} -> {:ok, volume.id}
      {:create, {:error, reason}} -> {:error, reason}
    end
  end
end
