defmodule NeonFS.Core.LockManager.TestLockTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.LockManager.FileLock

  setup do
    start_supervised!({Registry, keys: :unique, name: NeonFS.Core.LockManager.Registry})

    {:ok, pid} = start_supervised({FileLock, file_id: "test-lock-#{System.unique_integer()}"})

    %{pid: pid}
  end

  describe "test_lock/4" do
    test "returns :ok when no locks are held", %{pid: pid} do
      assert :ok = FileLock.test_lock(pid, :client_a, {0, 100}, :exclusive)
    end

    test "returns :ok when compatible shared lock is held", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :shared, ttl: 60_000)
      assert :ok = FileLock.test_lock(pid, :client_b, {0, 100}, :shared)
    end

    test "returns conflict when exclusive lock is held on overlapping range", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :exclusive, ttl: 60_000)

      assert {:error, :conflict, holder} =
               FileLock.test_lock(pid, :client_b, {50, 50}, :shared)

      assert holder.type == :exclusive
      assert holder.range == {0, 100}
    end

    test "returns conflict when shared lock is held and testing exclusive", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :shared, ttl: 60_000)

      assert {:error, :conflict, holder} =
               FileLock.test_lock(pid, :client_b, {0, 100}, :exclusive)

      assert holder.type == :shared
    end

    test "returns :ok when testing against own locks", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :exclusive, ttl: 60_000)
      assert :ok = FileLock.test_lock(pid, :client_a, {0, 100}, :exclusive)
    end

    test "returns :ok for non-overlapping ranges", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :exclusive, ttl: 60_000)
      assert :ok = FileLock.test_lock(pid, :client_b, {100, 100}, :exclusive)
    end

    test "extracts svid from tuple client_ref", %{pid: pid} do
      assert :ok =
               FileLock.lock(pid, {"host1", 42}, {0, 100}, :exclusive, ttl: 60_000)

      assert {:error, :conflict, holder} =
               FileLock.test_lock(pid, {"host2", 99}, {0, 100}, :exclusive)

      assert holder.svid == 42
    end

    test "does not acquire the lock", %{pid: pid} do
      assert :ok = FileLock.test_lock(pid, :client_a, {0, 100}, :exclusive)
      status = FileLock.status(pid)
      assert status.lock_count == 0
    end
  end
end
