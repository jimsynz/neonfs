defmodule NeonFS.Core.LockManagerTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.LockManager

  setup do
    start_supervised!({Registry, keys: :unique, name: NeonFS.Core.LockManager.Registry})

    start_supervised!(
      {DynamicSupervisor, name: NeonFS.Core.LockManager.Supervisor, strategy: :one_for_one}
    )

    # ServiceRegistry ETS tables may not exist in unit tests — LockManager.master_for/1
    # falls back to local node when ServiceRegistry is unavailable
    :ok
  end

  describe "lock/5 and unlock/3" do
    test "acquires and releases a lock" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.lock(file_id, :client_a, {0, 100}, :exclusive)
      assert :ok = LockManager.unlock(file_id, :client_a, {0, 100})
    end

    test "shared locks are compatible" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.lock(file_id, :client_a, {0, 100}, :shared)
      assert :ok = LockManager.lock(file_id, :client_b, {50, 100}, :shared)
    end

    test "exclusive locks conflict with shared locks" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.lock(file_id, :client_a, {0, 100}, :shared)

      task =
        Task.async(fn ->
          LockManager.lock(file_id, :client_b, {50, 100}, :exclusive, timeout: 2_000)
        end)

      Process.sleep(50)
      assert :ok = LockManager.unlock(file_id, :client_a, {0, 100})
      assert :ok = Task.await(task)
    end
  end

  describe "unlock_all/2" do
    test "releases all state for a client" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.lock(file_id, :client_a, {0, 100}, :exclusive)
      assert :ok = LockManager.lock(file_id, :client_a, {200, 100}, :shared)
      assert :ok = LockManager.unlock_all(file_id, :client_a)

      # Should be able to lock the same ranges now
      assert :ok = LockManager.lock(file_id, :client_b, {0, 100}, :exclusive)
    end
  end

  describe "open/5 and close/2" do
    test "compatible share modes work" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.open(file_id, :client_a, :read, :none)
      assert :ok = LockManager.open(file_id, :client_b, :read, :none)
    end

    test "conflicting share modes are rejected" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.open(file_id, :client_a, :write, :read_write)

      assert {:error, :share_violation} =
               LockManager.open(file_id, :client_b, :read, :none)
    end

    test "close releases the share mode" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.open(file_id, :client_a, :write, :read_write)
      assert :ok = LockManager.close(file_id, :client_a)

      # FileLock may have stopped since it was empty, so a new one will start
      assert :ok = LockManager.open(file_id, :client_b, :read, :none)
    end
  end

  describe "grant_lease/4 and break_lease/2" do
    test "grants and breaks a lease" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.grant_lease(file_id, :client_a, :read_write)
      assert :ok = LockManager.break_lease(file_id, :client_a)
    end

    test "lease conflicts with other clients" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.grant_lease(file_id, :client_a, :read_write)
      assert {:error, :conflict} = LockManager.grant_lease(file_id, :client_b, :read)
    end
  end

  describe "renew/3" do
    test "renews TTL for a client" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.lock(file_id, :client_a, {0, 100}, :exclusive, ttl: 200)
      assert :ok = LockManager.renew(file_id, :client_a, ttl: 60_000)
    end
  end

  describe "status/1" do
    test "returns status for a locked file" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.lock(file_id, :client_a, {0, 100}, :exclusive)
      assert {:ok, status} = LockManager.status(file_id)
      assert status.lock_count == 1
    end

    test "returns not_found for unlocked file" do
      assert {:error, :not_found} = LockManager.status("nonexistent-file")
    end
  end

  describe "check_write/3" do
    test "permits write when no locks exist" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.check_write(file_id, :client_a, {0, 100})
    end

    test "permits write when only advisory locks exist" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.lock(file_id, :client_a, {0, 100}, :exclusive, mode: :advisory)
      assert :ok = LockManager.check_write(file_id, :client_b, {0, 100})
    end

    test "blocks write when mandatory lock held by another client" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.lock(file_id, :client_a, {0, 100}, :exclusive, mode: :mandatory)
      assert {:error, :lock_conflict} = LockManager.check_write(file_id, :client_b, {50, 50})
    end

    test "permits write when mandatory lock held by the same client" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.lock(file_id, :client_a, {0, 100}, :exclusive, mode: :mandatory)
      assert :ok = LockManager.check_write(file_id, :client_a, {0, 100})
    end

    test "permits write to non-overlapping range" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.lock(file_id, :client_a, {0, 100}, :exclusive, mode: :mandatory)
      assert :ok = LockManager.check_write(file_id, :client_b, {100, 50})
    end

    test "blocks write when shared mandatory lock held by another client" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.lock(file_id, :client_a, {0, 100}, :shared, mode: :mandatory)
      assert {:error, :lock_conflict} = LockManager.check_write(file_id, :client_b, {50, 50})
    end
  end

  describe "check_write/3 share mode enforcement" do
    test "blocks write when another client has deny_write open" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.open(file_id, :client_a, :read, :write)
      assert {:error, :share_denied} = LockManager.check_write(file_id, :client_b, {0, 100})
    end

    test "blocks write when another client has deny_read_write open" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.open(file_id, :client_a, :read, :read_write)
      assert {:error, :share_denied} = LockManager.check_write(file_id, :client_b, {0, 100})
    end

    test "permits write by the client that holds the deny_write open" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.open(file_id, :client_a, :read_write, :write)
      assert :ok = LockManager.check_write(file_id, :client_a, {0, 100})
    end

    test "permits write when deny is only :read" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.open(file_id, :client_a, :write, :read)
      assert :ok = LockManager.check_write(file_id, :client_b, {0, 100})
    end

    test "permits write when deny is :none" do
      file_id = "file-#{System.unique_integer([:positive])}"
      assert :ok = LockManager.open(file_id, :client_a, :read, :none)
      assert :ok = LockManager.check_write(file_id, :client_b, {0, 100})
    end
  end

  describe "master_for/1" do
    test "returns a node" do
      master = LockManager.master_for("some-file-id")
      assert is_atom(master)
    end

    test "is deterministic for the same file_id" do
      assert LockManager.master_for("file-abc") == LockManager.master_for("file-abc")
    end

    test "routes to local node in single-node cluster" do
      assert LockManager.master_for("any-file") == Node.self()
    end
  end
end
