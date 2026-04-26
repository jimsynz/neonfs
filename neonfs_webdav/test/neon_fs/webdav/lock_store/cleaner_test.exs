defmodule NeonFS.WebDAV.LockStore.CleanerTest do
  use ExUnit.Case, async: true

  alias NeonFS.WebDAV.LockStore.Cleaner

  @table :cleaner_test_table

  setup do
    table = :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])

    on_exit(fn ->
      if :ets.whereis(@table) != :undefined, do: :ets.delete(@table)
    end)

    {:ok, table: table}
  end

  defp insert_lock(table, token, opts \\ []) do
    expires_at = Keyword.get(opts, :expires_at, System.system_time(:second) + 300)
    lock_null = Keyword.get(opts, :lock_null, false)
    namespace_claim_id = Keyword.get(opts, :namespace_claim_id)

    lock_info = %{
      token: token,
      path: ["vol", "test.txt"],
      scope: :exclusive,
      type: :write,
      owner: "test-user",
      timeout: 300,
      expires_at: expires_at,
      file_id: "file-#{token}",
      lock_null: lock_null,
      namespace_claim_id: namespace_claim_id
    }

    :ets.insert(table, {token, lock_info})
  end

  describe "periodic sweep" do
    test "removes expired entries from ETS" do
      insert_lock(@table, "expired-1", expires_at: System.system_time(:second) - 60)
      insert_lock(@table, "expired-2", expires_at: System.system_time(:second) - 10)
      insert_lock(@table, "active-1", expires_at: System.system_time(:second) + 300)

      assert :ets.info(@table, :size) == 3

      {:ok, pid} =
        Cleaner.start_link(
          name: :cleaner_sweep_test,
          table: @table,
          interval_ms: 50
        )

      send(pid, :sweep)
      :sys.get_state(pid)

      assert :ets.info(@table, :size) == 1
      assert :ets.lookup(@table, "active-1") != []
      assert :ets.lookup(@table, "expired-1") == []
      assert :ets.lookup(@table, "expired-2") == []
    end

    test "preserves all entries when none are expired" do
      insert_lock(@table, "active-1")
      insert_lock(@table, "active-2")

      {:ok, pid} =
        Cleaner.start_link(
          name: :cleaner_preserve_test,
          table: @table,
          interval_ms: 50
        )

      send(pid, :sweep)
      :sys.get_state(pid)

      assert :ets.info(@table, :size) == 2
    end

    test "handles empty table" do
      {:ok, pid} =
        Cleaner.start_link(
          name: :cleaner_empty_test,
          table: @table,
          interval_ms: 50
        )

      send(pid, :sweep)
      :sys.get_state(pid)

      assert :ets.info(@table, :size) == 0
    end

    test "removes expired lock-null entries" do
      insert_lock(@table, "lock-null-expired",
        expires_at: System.system_time(:second) - 30,
        lock_null: true
      )

      insert_lock(@table, "lock-null-active",
        expires_at: System.system_time(:second) + 300,
        lock_null: true
      )

      {:ok, pid} =
        Cleaner.start_link(
          name: :cleaner_lock_null_test,
          table: @table,
          interval_ms: 50
        )

      send(pid, :sweep)
      :sys.get_state(pid)

      assert :ets.info(@table, :size) == 1
      assert :ets.lookup(@table, "lock-null-active") != []
    end

    test "releases namespace coordinator claims on expired entries" do
      test_pid = self()

      Application.put_env(:neonfs_webdav, :namespace_coordinator_call_fn, fn function, args ->
        send(test_pid, {:namespace_coordinator, function, args})
        :ok
      end)

      on_exit(fn ->
        Application.delete_env(:neonfs_webdav, :namespace_coordinator_call_fn)
      end)

      insert_lock(@table, "expired-with-claim",
        expires_at: System.system_time(:second) - 30,
        namespace_claim_id: "ns-claim-expired"
      )

      insert_lock(@table, "active-with-claim",
        expires_at: System.system_time(:second) + 300,
        namespace_claim_id: "ns-claim-active"
      )

      insert_lock(@table, "expired-no-claim", expires_at: System.system_time(:second) - 30)

      {:ok, pid} =
        Cleaner.start_link(
          name: :cleaner_claim_release_test,
          table: @table,
          interval_ms: 50
        )

      send(pid, :sweep)
      :sys.get_state(pid)

      assert_received {:namespace_coordinator, :release, ["ns-claim-expired"]}
      refute_received {:namespace_coordinator, :release, ["ns-claim-active"]}

      assert :ets.lookup(@table, "expired-with-claim") == []
      assert :ets.lookup(@table, "expired-no-claim") == []
      assert :ets.lookup(@table, "active-with-claim") != []
    end
  end

  describe "telemetry" do
    test "emits cleanup event with expired count" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :webdav, :lock_store, :cleanup]
        ])

      insert_lock(@table, "expired-1", expires_at: System.system_time(:second) - 60)
      insert_lock(@table, "expired-2", expires_at: System.system_time(:second) - 10)
      insert_lock(@table, "active-1")

      {:ok, pid} =
        Cleaner.start_link(
          name: :cleaner_telemetry_test,
          table: @table,
          interval_ms: 50
        )

      send(pid, :sweep)
      :sys.get_state(pid)

      assert_receive {[:neonfs, :webdav, :lock_store, :cleanup], ^ref, %{expired_count: 2}, %{}}
    end

    test "emits cleanup event with zero count when nothing expired" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :webdav, :lock_store, :cleanup]
        ])

      insert_lock(@table, "active-1")

      {:ok, pid} =
        Cleaner.start_link(
          name: :cleaner_telemetry_zero_test,
          table: @table,
          interval_ms: 50
        )

      send(pid, :sweep)
      :sys.get_state(pid)

      assert_receive {[:neonfs, :webdav, :lock_store, :cleanup], ^ref, %{expired_count: 0}, %{}}
    end
  end

  describe "scheduling" do
    test "automatically sweeps on the configured interval" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :webdav, :lock_store, :cleanup]
        ])

      insert_lock(@table, "expired", expires_at: System.system_time(:second) - 10)

      {:ok, _pid} =
        Cleaner.start_link(
          name: :cleaner_schedule_test,
          table: @table,
          interval_ms: 50
        )

      assert_receive {[:neonfs, :webdav, :lock_store, :cleanup], ^ref, %{expired_count: 1}, %{}},
                     1_000
    end
  end
end
