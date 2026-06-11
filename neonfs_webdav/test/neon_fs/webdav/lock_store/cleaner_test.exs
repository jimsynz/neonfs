defmodule NeonFS.WebDAV.LockStore.CleanerTest do
  use ExUnit.Case, async: false

  alias NeonFS.WebDAV.LockStore.Cleaner
  alias NeonFS.WebDAV.Test.FakeKV

  @key_prefix "webdav_lock:"

  setup do
    FakeKV.stub!()

    on_exit(fn ->
      Application.delete_env(:neonfs_webdav, :kv_call_fn)
    end)

    :ok
  end

  defp insert_lock(token, opts \\ []) do
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

    FakeKV.call(:put, [@key_prefix <> token, lock_info])
  end

  defp stored_tokens do
    [@key_prefix]
    |> then(&FakeKV.call(:list_prefix, &1))
    |> Enum.map(fn {@key_prefix <> token, _info} -> token end)
    |> Enum.sort()
  end

  defp start_cleaner!(name) do
    {:ok, pid} = Cleaner.start_link(name: name, interval_ms: 50)
    pid
  end

  defp sweep!(pid) do
    send(pid, :sweep)
    :sys.get_state(pid)
  end

  describe "periodic sweep" do
    test "removes expired entries from the KV index" do
      insert_lock("expired-1", expires_at: System.system_time(:second) - 60)
      insert_lock("expired-2", expires_at: System.system_time(:second) - 10)
      insert_lock("active-1", expires_at: System.system_time(:second) + 300)

      :cleaner_sweep_test |> start_cleaner!() |> sweep!()

      assert stored_tokens() == ["active-1"]
    end

    test "preserves all entries when none are expired" do
      insert_lock("active-1")
      insert_lock("active-2")

      :cleaner_preserve_test |> start_cleaner!() |> sweep!()

      assert stored_tokens() == ["active-1", "active-2"]
    end

    test "handles empty index" do
      :cleaner_empty_test |> start_cleaner!() |> sweep!()

      assert stored_tokens() == []
    end

    test "removes expired lock-null entries" do
      insert_lock("lock-null-expired",
        expires_at: System.system_time(:second) - 30,
        lock_null: true
      )

      insert_lock("lock-null-active",
        expires_at: System.system_time(:second) + 300,
        lock_null: true
      )

      :cleaner_lock_null_test |> start_cleaner!() |> sweep!()

      assert stored_tokens() == ["lock-null-active"]
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

      insert_lock("expired-with-claim",
        expires_at: System.system_time(:second) - 30,
        namespace_claim_id: "ns-claim-expired"
      )

      insert_lock("active-with-claim",
        expires_at: System.system_time(:second) + 300,
        namespace_claim_id: "ns-claim-active"
      )

      insert_lock("expired-no-claim", expires_at: System.system_time(:second) - 30)

      :cleaner_claim_release_test |> start_cleaner!() |> sweep!()

      assert_received {:namespace_coordinator, :release, ["ns-claim-expired"]}
      refute_received {:namespace_coordinator, :release, ["ns-claim-active"]}

      assert stored_tokens() == ["active-with-claim"]
    end
  end

  describe "telemetry" do
    test "emits cleanup event with expired count" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :webdav, :lock_store, :cleanup]
        ])

      insert_lock("expired-1", expires_at: System.system_time(:second) - 60)
      insert_lock("expired-2", expires_at: System.system_time(:second) - 10)
      insert_lock("active-1")

      :cleaner_telemetry_test |> start_cleaner!() |> sweep!()

      assert_receive {[:neonfs, :webdav, :lock_store, :cleanup], ^ref, %{expired_count: 2}, %{}}
    end

    test "emits cleanup event with zero count when nothing expired" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :webdav, :lock_store, :cleanup]
        ])

      insert_lock("active-1")

      :cleaner_telemetry_zero_test |> start_cleaner!() |> sweep!()

      assert_receive {[:neonfs, :webdav, :lock_store, :cleanup], ^ref, %{expired_count: 0}, %{}}
    end
  end

  describe "scheduling" do
    test "automatically sweeps on the configured interval" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :webdav, :lock_store, :cleanup]
        ])

      insert_lock("expired", expires_at: System.system_time(:second) - 10)

      start_cleaner!(:cleaner_schedule_test)

      assert_receive {[:neonfs, :webdav, :lock_store, :cleanup], ^ref, %{expired_count: 1}, %{}},
                     1_000
    end
  end
end
