defmodule NeonFS.NFS.WriteThrottleTest do
  use ExUnit.Case, async: false

  alias NeonFS.NFS.WriteThrottle

  setup do
    start_supervised!(WriteThrottle)
    :ok
  end

  describe "acquire/release" do
    test "grants permits when under limits" do
      assert {:ok, permit} = WriteThrottle.acquire(1024)
      assert WriteThrottle.in_flight_count() == 1
      assert WriteThrottle.in_flight_bytes() == 1024

      assert :ok = WriteThrottle.release(permit)
      assert WriteThrottle.in_flight_count() == 0
      assert WriteThrottle.in_flight_bytes() == 0
    end

    test "blocks and times out when write count limit is reached" do
      original = Application.get_env(:neonfs_nfs, :max_in_flight_writes)
      Application.put_env(:neonfs_nfs, :max_in_flight_writes, 1)
      Application.put_env(:neonfs_nfs, :write_acquire_timeout, 50)

      on_exit(fn ->
        if original,
          do: Application.put_env(:neonfs_nfs, :max_in_flight_writes, original),
          else: Application.delete_env(:neonfs_nfs, :max_in_flight_writes)

        Application.delete_env(:neonfs_nfs, :write_acquire_timeout)
      end)

      {:ok, _permit} = WriteThrottle.acquire(100)
      assert {:error, :overloaded} = WriteThrottle.acquire(100)
    end

    test "blocks and times out when byte limit is reached" do
      original = Application.get_env(:neonfs_nfs, :max_in_flight_bytes)
      Application.put_env(:neonfs_nfs, :max_in_flight_bytes, 500)
      Application.put_env(:neonfs_nfs, :write_acquire_timeout, 50)

      on_exit(fn ->
        if original,
          do: Application.put_env(:neonfs_nfs, :max_in_flight_bytes, original),
          else: Application.delete_env(:neonfs_nfs, :max_in_flight_bytes)

        Application.delete_env(:neonfs_nfs, :write_acquire_timeout)
      end)

      {:ok, _permit} = WriteThrottle.acquire(400)
      assert {:error, :overloaded} = WriteThrottle.acquire(200)
    end

    test "unblocks waiters when permits are released" do
      Application.put_env(:neonfs_nfs, :max_in_flight_writes, 1)
      Application.put_env(:neonfs_nfs, :write_acquire_timeout, 2_000)

      on_exit(fn ->
        Application.delete_env(:neonfs_nfs, :max_in_flight_writes)
        Application.delete_env(:neonfs_nfs, :write_acquire_timeout)
      end)

      {:ok, permit} = WriteThrottle.acquire(100)

      waiter =
        Task.async(fn ->
          WriteThrottle.acquire(100)
        end)

      Process.sleep(20)
      WriteThrottle.release(permit)

      assert {:ok, _permit2} = Task.await(waiter, 1_000)
    end
  end

  describe "with_permit/2" do
    test "acquires and releases around the function" do
      result =
        WriteThrottle.with_permit(4096, fn ->
          assert WriteThrottle.in_flight_count() == 1
          assert WriteThrottle.in_flight_bytes() == 4096
          :result
        end)

      assert result == :result
      assert WriteThrottle.in_flight_count() == 0
      assert WriteThrottle.in_flight_bytes() == 0
    end

    test "releases permit even when function raises" do
      assert_raise RuntimeError, fn ->
        WriteThrottle.with_permit(1024, fn ->
          raise "boom"
        end)
      end

      assert WriteThrottle.in_flight_count() == 0
      assert WriteThrottle.in_flight_bytes() == 0
    end

    test "returns {:error, :overloaded} when permits unavailable" do
      Application.put_env(:neonfs_nfs, :max_in_flight_writes, 1)
      Application.put_env(:neonfs_nfs, :write_acquire_timeout, 50)

      on_exit(fn ->
        Application.delete_env(:neonfs_nfs, :max_in_flight_writes)
        Application.delete_env(:neonfs_nfs, :write_acquire_timeout)
      end)

      {:ok, _permit} = WriteThrottle.acquire(100)

      result =
        WriteThrottle.with_permit(100, fn ->
          flunk("should not execute")
        end)

      assert result == {:error, :overloaded}
    end
  end
end
