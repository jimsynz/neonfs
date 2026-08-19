defmodule NeonFS.Client.HostLockTest do
  @moduledoc """
  Serialisation between unrelated processes on one host.

  The lock's whole purpose is to hold across OS processes that share nothing but
  a directory, so these tests contend from separate BEAM processes against a real
  file rather than asserting on internals.
  """

  use ExUnit.Case, async: true

  alias NeonFS.Client.HostLock

  @moduletag :tmp_dir

  test "the body runs and the lock is gone afterwards", %{tmp_dir: dir} do
    assert HostLock.with_lock("job", fn -> :did_the_work end, dir: dir) == :did_the_work
    refute File.exists?(Path.join(dir, ".job.lock"))
  end

  test "the lock is released even when the body raises", %{tmp_dir: dir} do
    assert_raise RuntimeError, fn ->
      HostLock.with_lock("job", fn -> raise "boom" end, dir: dir)
    end

    refute File.exists?(Path.join(dir, ".job.lock"))
    assert HostLock.with_lock("job", fn -> :ok end, dir: dir) == :ok
  end

  test "a missing directory is created rather than failing", %{tmp_dir: dir} do
    nested = Path.join([dir, "does", "not", "exist"])

    assert HostLock.with_lock("job", fn -> :ok end, dir: nested) == :ok
  end

  # The property the redemption depends on: two arrivals, one execution.
  test "only one of two contenders runs the body", %{tmp_dir: dir} do
    test_pid = self()

    holder =
      spawn(fn ->
        HostLock.with_lock(
          "job",
          fn ->
            send(test_pid, :holding)
            # Held until the test says so, so the second caller genuinely
            # contends rather than arriving after the fact.
            receive do
              :finish -> :ok
            end
          end,
          dir: dir
        )

        send(test_pid, :holder_done)
      end)

    assert_receive :holding, 2_000

    contender =
      spawn(fn ->
        result = HostLock.with_lock("job", fn -> :ran end, dir: dir, wait_ms: 5_000)
        send(test_pid, {:contender, result})
      end)

    # The contender must not have run while the lock was held.
    refute_receive {:contender, _}, 300

    send(holder, :finish)
    assert_receive :holder_done, 2_000
    assert_receive {:contender, :ran}, 5_000

    refute Process.alive?(contender)
  end

  test "a contender that waits past its deadline reports the timeout", %{tmp_dir: dir} do
    test_pid = self()

    holder =
      spawn(fn ->
        HostLock.with_lock(
          "job",
          fn ->
            send(test_pid, :holding)

            receive do
              :finish -> :ok
            end
          end,
          dir: dir
        )
      end)

    assert_receive :holding, 2_000

    assert HostLock.with_lock("job", fn -> :ran end, dir: dir, wait_ms: 200) ==
             {:error, {:lock_timeout, ".job.lock"}}

    send(holder, :finish)
  end

  test "a waiting contender is told it is waiting, once", %{tmp_dir: dir} do
    test_pid = self()

    holder =
      spawn(fn ->
        HostLock.with_lock(
          "job",
          fn ->
            send(test_pid, :holding)

            receive do
              :finish -> :ok
            end
          end,
          dir: dir
        )
      end)

    assert_receive :holding, 2_000

    HostLock.with_lock("job", fn -> :ran end,
      dir: dir,
      wait_ms: 300,
      on_wait: fn -> send(test_pid, :waited) end
    )

    assert_receive :waited, 1_000
    refute_receive :waited, 200

    send(holder, :finish)
  end

  # The failure mode this exists to prevent: an init container killed
  # mid-operation leaves its lock file behind, and a lock nothing can clear
  # wedges every pod that later lands on the host. The state is produced by
  # actually killing a holder, not by writing a file and calling it stale.
  describe "a lock left behind by a killed holder" do
    test "does not wedge the host", %{tmp_dir: dir} do
      test_pid = self()

      holder =
        spawn(fn ->
          HostLock.with_lock(
            "job",
            fn ->
              send(test_pid, :holding)
              Process.sleep(:infinity)
            end,
            dir: dir
          )
        end)

      assert_receive :holding, 2_000

      ref = Process.monitor(holder)
      Process.exit(holder, :kill)
      assert_receive {:DOWN, ^ref, :process, ^holder, :killed}, 2_000

      lock = Path.join(dir, ".job.lock")
      assert File.exists?(lock), "a killed holder should leave its lock behind"

      # `stale_after_ms: 0` is the same judgement the default makes two minutes
      # later, without the test waiting for it.
      assert HostLock.with_lock("job", fn -> :ran end, dir: dir, stale_after_ms: 0) == :ran
      refute File.exists?(lock)
    end

    test "is respected until it is actually stale", %{tmp_dir: dir} do
      test_pid = self()

      holder =
        spawn(fn ->
          HostLock.with_lock(
            "job",
            fn ->
              send(test_pid, :holding)
              Process.sleep(:infinity)
            end,
            dir: dir
          )
        end)

      assert_receive :holding, 2_000
      ref = Process.monitor(holder)
      Process.exit(holder, :kill)
      assert_receive {:DOWN, ^ref, :process, ^holder, :killed}, 2_000

      # A generous staleness window means the abandoned lock is still honoured,
      # so the caller waits it out and reports a timeout rather than breaking a
      # lock that might still be held by someone doing work.
      assert HostLock.with_lock("job", fn -> :ran end,
               dir: dir,
               stale_after_ms: 600_000,
               wait_ms: 200
             ) == {:error, {:lock_timeout, ".job.lock"}}

      assert File.exists?(Path.join(dir, ".job.lock"))
    end
  end
end
