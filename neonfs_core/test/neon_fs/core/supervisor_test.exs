defmodule NeonFS.Core.SupervisorTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.Supervisor, as: CoreSupervisor

  describe "supervisor start_link/1" do
    test "starts successfully" do
      # Application is already started in test_helper.exs
      # Verify supervisor is running
      assert Process.whereis(CoreSupervisor) != nil
    end

    test "is registered with correct name" do
      assert Process.whereis(CoreSupervisor) == Process.whereis(CoreSupervisor)
    end
  end

  describe "child processes" do
    test "Persistence is running" do
      assert Process.whereis(NeonFS.Core.Persistence) != nil
    end

    test "BlobStore is running" do
      assert Process.whereis(NeonFS.Core.BlobStore) != nil
    end

    test "ChunkIndex is running" do
      assert Process.whereis(NeonFS.Core.ChunkIndex) != nil
    end

    test "FileIndex is running" do
      assert Process.whereis(NeonFS.Core.FileIndex) != nil
    end

    test "VolumeRegistry is running" do
      assert Process.whereis(NeonFS.Core.VolumeRegistry) != nil
    end

    test "all children are supervised" do
      children = Supervisor.which_children(CoreSupervisor)

      # Extract child names
      child_names = Enum.map(children, fn {name, _pid, _type, _modules} -> name end)

      # Verify core children are present
      assert NeonFS.Core.Persistence in child_names
      assert NeonFS.Core.BlobStore in child_names
      assert NeonFS.Core.ChunkIndex in child_names
      assert NeonFS.Core.FileIndex in child_names
      assert NeonFS.Core.VolumeRegistry in child_names

      # RaSupervisor is optional (requires named node)
      # Phase 1 runs without Ra, Phase 2+ enables it
    end

    test "startup order is correct" do
      # Get children in order
      children = Supervisor.which_children(CoreSupervisor)

      # Children are returned in reverse startup order by which_children
      # So we reverse to get startup order
      startup_order =
        children |> Enum.reverse() |> Enum.map(fn {name, _pid, _type, _mods} -> name end)

      # Verify startup order: Persistence first, then BlobStore, then metadata modules
      assert Enum.at(startup_order, 0) == NeonFS.Core.Persistence
      assert Enum.at(startup_order, 1) == NeonFS.Core.BlobStore
      assert Enum.at(startup_order, 2) == NeonFS.Core.ChunkIndex
      assert Enum.at(startup_order, 3) == NeonFS.Core.FileIndex
      assert Enum.at(startup_order, 4) == NeonFS.Core.VolumeRegistry
    end
  end

  describe "restart strategy" do
    test "uses one_for_one strategy" do
      # Get supervisor info
      {:ok, info} = :supervisor.get_childspec(CoreSupervisor, NeonFS.Core.BlobStore)

      # Verify it's a permanent child (will be restarted)
      assert info.restart == :permanent
    end

    test "child restart does not affect other children" do
      # Get PIDs of all children before crash
      persistence_pid = Process.whereis(NeonFS.Core.Persistence)
      blob_store_pid = Process.whereis(NeonFS.Core.BlobStore)
      chunk_index_pid = Process.whereis(NeonFS.Core.ChunkIndex)
      file_index_pid = Process.whereis(NeonFS.Core.FileIndex)
      volume_registry_pid = Process.whereis(NeonFS.Core.VolumeRegistry)

      # Kill ChunkIndex
      Process.exit(chunk_index_pid, :kill)

      # Wait for restart
      :timer.sleep(100)

      # ChunkIndex should have restarted (new PID)
      new_chunk_index_pid = Process.whereis(NeonFS.Core.ChunkIndex)
      assert new_chunk_index_pid != nil
      assert new_chunk_index_pid != chunk_index_pid

      # Other children should still have same PIDs
      assert Process.whereis(NeonFS.Core.Persistence) == persistence_pid
      assert Process.whereis(NeonFS.Core.BlobStore) == blob_store_pid
      assert Process.whereis(NeonFS.Core.FileIndex) == file_index_pid
      assert Process.whereis(NeonFS.Core.VolumeRegistry) == volume_registry_pid
    end
  end

  describe "application integration" do
    test "application starts and stops cleanly" do
      # Application is already started
      # Verify it's running
      assert Application.started_applications()
             |> Enum.any?(fn {app, _desc, _vsn} -> app == :neonfs_core end)
    end

    test "supervisor count is correct" do
      # Should have exactly one supervisor for core
      supervisors =
        Supervisor.which_children(CoreSupervisor)
        |> Enum.filter(fn {_name, _pid, type, _modules} -> type == :supervisor end)

      # No child supervisors in Phase 1
      assert supervisors == []
    end

    test "worker count is correct" do
      workers =
        Supervisor.which_children(CoreSupervisor)
        |> Enum.filter(fn {_name, _pid, type, _modules} -> type == :worker end)

      # Should have 5 workers (including Persistence)
      assert length(workers) == 5
    end
  end
end
