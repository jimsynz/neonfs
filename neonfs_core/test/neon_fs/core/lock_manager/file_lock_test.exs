defmodule NeonFS.Core.LockManager.FileLockTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.LockManager.FileLock

  setup do
    start_supervised!({Registry, keys: :unique, name: NeonFS.Core.LockManager.Registry})

    {:ok, pid} = start_supervised({FileLock, file_id: "test-file-#{System.unique_integer()}"})

    %{pid: pid}
  end

  describe "byte-range locks" do
    test "grants a shared lock on an unlocked range", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :shared, ttl: 60_000)
    end

    test "grants multiple shared locks on overlapping ranges", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :shared, ttl: 60_000)
      assert :ok = FileLock.lock(pid, :client_b, {50, 100}, :shared, ttl: 60_000)
    end

    test "grants an exclusive lock on an unlocked range", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :exclusive, ttl: 60_000)
    end

    test "blocks exclusive lock when shared lock is held on overlapping range", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :shared, ttl: 60_000)

      task =
        Task.async(fn ->
          FileLock.lock(pid, :client_b, {50, 100}, :exclusive, ttl: 60_000, timeout: 2_000)
        end)

      Process.sleep(50)
      FileLock.unlock(pid, :client_a, {0, 100})
      assert :ok = Task.await(task)
    end

    test "blocks shared lock when exclusive lock is held on overlapping range", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :exclusive, ttl: 60_000)

      task =
        Task.async(fn ->
          FileLock.lock(pid, :client_b, {50, 100}, :shared, ttl: 60_000, timeout: 2_000)
        end)

      Process.sleep(50)
      FileLock.unlock(pid, :client_a, {0, 100})
      assert :ok = Task.await(task)
    end

    test "grants locks on non-overlapping ranges", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :exclusive, ttl: 60_000)
      assert :ok = FileLock.lock(pid, :client_b, {100, 100}, :exclusive, ttl: 60_000)
    end

    test "same client can hold multiple locks", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :exclusive, ttl: 60_000)
      assert :ok = FileLock.lock(pid, :client_a, {200, 100}, :exclusive, ttl: 60_000)
    end

    test "unlock releases a specific lock", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :exclusive, ttl: 60_000)
      assert :ok = FileLock.lock(pid, :client_a, {200, 100}, :exclusive, ttl: 60_000)
      assert :ok = FileLock.unlock(pid, :client_a, {0, 100})

      status = FileLock.status(pid)
      assert status.lock_count == 1
    end

    test "unlock_all releases all client locks", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :exclusive, ttl: 60_000)
      assert :ok = FileLock.lock(pid, :client_a, {200, 100}, :shared, ttl: 60_000)
      assert :ok = FileLock.lock(pid, :client_b, {500, 100}, :exclusive, ttl: 60_000)

      assert :ok = FileLock.unlock_all(pid, :client_a)

      status = FileLock.status(pid)
      assert status.lock_count == 1
    end
  end

  describe "share modes" do
    test "opens with compatible share modes", %{pid: pid} do
      assert :ok = FileLock.open(pid, :client_a, :read, :none, ttl: 60_000)
      assert :ok = FileLock.open(pid, :client_b, :read, :none, ttl: 60_000)
    end

    test "rejects conflicting share modes", %{pid: pid} do
      assert :ok = FileLock.open(pid, :client_a, :write, :read, ttl: 60_000)
      assert {:error, :share_violation} = FileLock.open(pid, :client_b, :read, :none, ttl: 60_000)
    end

    test "deny_write prevents write opens", %{pid: pid} do
      assert :ok = FileLock.open(pid, :client_a, :read, :write, ttl: 60_000)

      assert {:error, :share_violation} =
               FileLock.open(pid, :client_b, :write, :none, ttl: 60_000)
    end

    test "deny_read_write prevents all opens", %{pid: pid} do
      assert :ok = FileLock.open(pid, :client_a, :read, :read_write, ttl: 60_000)

      assert {:error, :share_violation} =
               FileLock.open(pid, :client_b, :read, :none, ttl: 60_000)
    end

    test "close allows subsequent opens", %{pid: pid} do
      assert :ok = FileLock.open(pid, :client_a, :write, :read_write, ttl: 60_000)
      assert {:error, :share_violation} = FileLock.open(pid, :client_b, :read, :none, ttl: 60_000)

      assert :ok = FileLock.close(pid, :client_a)

      # Need a new FileLock since close may have stopped it
      {:ok, pid2} = start_supervised({FileLock, file_id: "test-file-close"}, id: :close_test)
      assert :ok = FileLock.open(pid2, :client_b, :read, :none, ttl: 60_000)
    end
  end

  describe "leases" do
    test "grants a lease to a single client", %{pid: pid} do
      assert :ok = FileLock.grant_lease(pid, :client_a, :read_write, ttl: 60_000)
    end

    test "rejects lease when another client holds one", %{pid: pid} do
      assert :ok = FileLock.grant_lease(pid, :client_a, :read_write, ttl: 60_000)
      assert {:error, :conflict} = FileLock.grant_lease(pid, :client_b, :read, ttl: 60_000)
    end

    test "same client can renew their lease type", %{pid: pid} do
      assert :ok = FileLock.grant_lease(pid, :client_a, :read, ttl: 60_000)
      assert :ok = FileLock.grant_lease(pid, :client_a, :read_write, ttl: 60_000)
    end

    test "break_lease removes the lease", %{pid: pid} do
      assert :ok = FileLock.grant_lease(pid, :client_a, :read_write, ttl: 60_000)
      assert :ok = FileLock.break_lease(pid, :client_a)

      # Process may have stopped, so start a fresh one
      {:ok, pid2} = start_supervised({FileLock, file_id: "test-file-break"}, id: :break_test)
      assert :ok = FileLock.grant_lease(pid2, :client_b, :read, ttl: 60_000)
    end

    test "break_lease invokes the callback", %{pid: pid} do
      test_pid = self()
      callback = fn -> send(test_pid, :lease_broken) end

      assert :ok =
               FileLock.grant_lease(pid, :client_a, :read_write,
                 ttl: 60_000,
                 break_callback: callback
               )

      assert :ok = FileLock.break_lease(pid, :client_a)
      assert_receive :lease_broken, 1_000
    end

    test "break_lease returns error for unknown client", %{pid: pid} do
      assert {:error, :not_found} = FileLock.break_lease(pid, :unknown_client)
    end
  end

  describe "TTL expiry" do
    test "expired locks are purged", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :exclusive, ttl: 1)

      Process.sleep(50)

      # Trigger TTL check by attempting a new lock
      assert :ok = FileLock.lock(pid, :client_b, {0, 100}, :exclusive, ttl: 60_000)
    end

    test "renew extends TTL", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :exclusive, ttl: 200)
      Process.sleep(100)
      assert :ok = FileLock.renew(pid, :client_a, ttl: 60_000)

      status = FileLock.status(pid)
      assert status.lock_count == 1
    end

    test "renew returns error for unknown client", %{pid: pid} do
      assert {:error, :not_found} = FileLock.renew(pid, :unknown_client)
    end
  end

  describe "status" do
    test "returns lock status", %{pid: pid} do
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :shared, ttl: 60_000)
      assert :ok = FileLock.open(pid, :client_b, :read, :none, ttl: 60_000)
      assert :ok = FileLock.grant_lease(pid, :client_c, :read, ttl: 60_000)

      status = FileLock.status(pid)
      assert status.lock_count == 1
      assert status.open_count == 1
      assert status.lease_count == 1
      assert status.wait_queue_length == 0
    end
  end

  describe "process lifecycle" do
    test "process stops when all state is released" do
      {:ok, pid} =
        start_supervised({FileLock, file_id: "lifecycle-test"}, id: :lifecycle_test)

      ref = Process.monitor(pid)
      assert :ok = FileLock.lock(pid, :client_a, {0, 100}, :exclusive, ttl: 60_000)
      assert :ok = FileLock.unlock(pid, :client_a, {0, 100})

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000
    end
  end
end
