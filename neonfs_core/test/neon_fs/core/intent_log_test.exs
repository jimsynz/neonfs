defmodule NeonFS.Core.IntentLogTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{Intent, IntentLog, RaServer}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    start_ra()
    :ok = RaServer.init_cluster()
    on_exit(fn -> cleanup_test_dirs() end)

    # Generate unique prefix per test to avoid conflict key collisions
    prefix = System.unique_integer([:positive])
    %{prefix: prefix}
  end

  defp make_intent(overrides \\ []) do
    attrs =
      Keyword.merge(
        [
          id: "intent-#{System.unique_integer([:positive])}",
          operation: :file_write,
          conflict_key: {:file, "file-#{System.unique_integer([:positive])}"}
        ],
        overrides
      )

    Intent.new(attrs)
  end

  defp make_expired_intent(overrides) do
    past = DateTime.add(DateTime.utc_now(), -600, :second)
    make_intent(Keyword.merge([started_at: past, ttl_seconds: 1], overrides))
  end

  describe "try_acquire/1" do
    test "acquires intent when no conflict" do
      intent = make_intent()
      assert {:ok, intent_id} = IntentLog.try_acquire(intent)
      assert intent_id == intent.id
    end

    test "acquired intent is visible via get/1" do
      intent = make_intent()
      {:ok, _id} = IntentLog.try_acquire(intent)

      assert {:ok, stored} = IntentLog.get(intent.id)
      assert stored.id == intent.id
      assert stored.operation == :file_write
      assert stored.state == :pending
    end

    test "acquired intent appears in list_active/0" do
      intent = make_intent()
      {:ok, _id} = IntentLog.try_acquire(intent)

      assert {:ok, active} = IntentLog.list_active()
      assert Enum.any?(active, &(&1.id == intent.id))
    end

    test "returns conflict when active intent exists with same conflict key", %{prefix: p} do
      key = {:file, "conflict-#{p}"}
      first = make_intent(id: "first-#{p}", conflict_key: key)
      second = make_intent(id: "second-#{p}", conflict_key: key)

      assert {:ok, _} = IntentLog.try_acquire(first)
      assert {:error, :conflict, existing} = IntentLog.try_acquire(second)
      assert existing.id == "first-#{p}"
    end

    test "allows intents with different conflict keys", %{prefix: p} do
      first = make_intent(id: "first-#{p}", conflict_key: {:file, "a-#{p}"})
      second = make_intent(id: "second-#{p}", conflict_key: {:file, "b-#{p}"})

      assert {:ok, _} = IntentLog.try_acquire(first)
      assert {:ok, _} = IntentLog.try_acquire(second)

      assert {:ok, first_stored} = IntentLog.get("first-#{p}")
      assert first_stored.state == :pending
      assert {:ok, second_stored} = IntentLog.get("second-#{p}")
      assert second_stored.state == :pending
    end

    test "different conflict key types do not interfere", %{prefix: p} do
      intents = [
        make_intent(id: "file-#{p}", conflict_key: {:file, "f-#{p}"}),
        make_intent(id: "create-#{p}", conflict_key: {:create, "v1", "/", "test-#{p}.txt"}),
        make_intent(id: "dir-#{p}", conflict_key: {:dir, "v1", "/#{p}"}),
        make_intent(id: "chunk-#{p}", conflict_key: {:chunk_migration, "hash-#{p}"}),
        make_intent(id: "rotation-#{p}", conflict_key: {:volume_key_rotation, "v-#{p}"})
      ]

      for intent <- intents do
        assert {:ok, _} = IntentLog.try_acquire(intent)
      end

      # All five are independently acquirable and visible
      for intent <- intents do
        assert {:ok, stored} = IntentLog.get(intent.id)
        assert stored.state == :pending
      end
    end

    test "expired intent takeover succeeds", %{prefix: p} do
      key = {:file, "takeover-#{p}"}
      expired = make_expired_intent(id: "old-#{p}", conflict_key: key)
      new_intent = make_intent(id: "new-#{p}", conflict_key: key)

      assert {:ok, _} = IntentLog.try_acquire(expired)
      assert {:ok, _} = IntentLog.try_acquire(new_intent)

      assert {:ok, old_stored} = IntentLog.get("old-#{p}")
      assert old_stored.state == :expired

      assert {:ok, new_stored} = IntentLog.get("new-#{p}")
      assert new_stored.state == :pending
    end
  end

  describe "complete/1" do
    test "marks intent as completed and releases conflict key", %{prefix: p} do
      key = {:file, "complete-#{p}"}
      intent = make_intent(id: "complete-#{p}", conflict_key: key)
      {:ok, _id} = IntentLog.try_acquire(intent)

      assert :ok = IntentLog.complete("complete-#{p}")

      assert {:ok, stored} = IntentLog.get("complete-#{p}")
      assert stored.state == :completed
      assert stored.completed_at != nil

      # Conflict key is released — another intent with same key can acquire
      new_intent = make_intent(id: "after-complete-#{p}", conflict_key: key)
      assert {:ok, _} = IntentLog.try_acquire(new_intent)
    end

    test "completed intent no longer appears in list_active/0", %{prefix: p} do
      intent = make_intent(id: "done-#{p}")
      {:ok, _id} = IntentLog.try_acquire(intent)
      :ok = IntentLog.complete("done-#{p}")

      assert {:ok, active} = IntentLog.list_active()
      refute Enum.any?(active, &(&1.id == "done-#{p}"))
    end

    test "returns error for non-existent intent" do
      assert {:error, :not_found} = IntentLog.complete("nonexistent")
    end
  end

  describe "fail/2" do
    test "marks intent as failed and releases conflict key", %{prefix: p} do
      key = {:file, "fail-#{p}"}
      intent = make_intent(id: "fail-#{p}", conflict_key: key)
      {:ok, _id} = IntentLog.try_acquire(intent)

      assert :ok = IntentLog.fail("fail-#{p}", :timeout)

      assert {:ok, stored} = IntentLog.get("fail-#{p}")
      assert stored.state == :failed
      assert stored.error == :timeout
      assert stored.completed_at != nil

      # Conflict key is released
      new_intent = make_intent(id: "after-fail-#{p}", conflict_key: key)
      assert {:ok, _} = IntentLog.try_acquire(new_intent)
    end

    test "failed intent no longer appears in list_active/0", %{prefix: p} do
      intent = make_intent(id: "fail-active-#{p}")
      {:ok, _id} = IntentLog.try_acquire(intent)
      :ok = IntentLog.fail("fail-active-#{p}", :error)

      assert {:ok, active} = IntentLog.list_active()
      refute Enum.any?(active, &(&1.id == "fail-active-#{p}"))
    end

    test "returns error for non-existent intent" do
      assert {:error, :not_found} = IntentLog.fail("nonexistent", :reason)
    end
  end

  describe "extend/2" do
    test "extends the TTL of a pending intent", %{prefix: p} do
      intent = make_intent(id: "extend-#{p}", ttl_seconds: 300)
      {:ok, _id} = IntentLog.try_acquire(intent)

      {:ok, before_extend} = IntentLog.get("extend-#{p}")
      original_expires = before_extend.expires_at

      assert :ok = IntentLog.extend("extend-#{p}", 600)

      {:ok, after_extend} = IntentLog.get("extend-#{p}")
      assert DateTime.compare(after_extend.expires_at, original_expires) == :gt
      diff = DateTime.diff(after_extend.expires_at, original_expires, :second)
      assert diff == 600
    end

    test "uses default extension of 300 seconds", %{prefix: p} do
      intent = make_intent(id: "ext-default-#{p}", ttl_seconds: 100)
      {:ok, _id} = IntentLog.try_acquire(intent)

      {:ok, before_extend} = IntentLog.get("ext-default-#{p}")
      original_expires = before_extend.expires_at

      assert :ok = IntentLog.extend("ext-default-#{p}")

      {:ok, after_extend} = IntentLog.get("ext-default-#{p}")
      diff = DateTime.diff(after_extend.expires_at, original_expires, :second)
      assert diff == 300
    end

    test "returns error for non-existent intent" do
      assert {:error, :not_found} = IntentLog.extend("nonexistent", 300)
    end

    test "returns error for non-pending intent", %{prefix: p} do
      intent = make_intent(id: "ext-done-#{p}")
      {:ok, _id} = IntentLog.try_acquire(intent)
      :ok = IntentLog.complete("ext-done-#{p}")

      assert {:error, :not_pending} = IntentLog.extend("ext-done-#{p}", 300)
    end
  end

  describe "get/1" do
    test "returns intent by ID", %{prefix: p} do
      intent = make_intent(id: "get-#{p}")
      {:ok, _id} = IntentLog.try_acquire(intent)

      assert {:ok, stored} = IntentLog.get("get-#{p}")
      assert stored.id == "get-#{p}"
      assert stored.operation == :file_write
    end

    test "returns not_found for missing intent" do
      assert {:error, :not_found} = IntentLog.get("missing")
    end
  end

  describe "list_active/0" do
    test "returns only active intents (excludes completed and failed)", %{prefix: p} do
      active = make_intent(id: "la-active-#{p}", conflict_key: {:file, "la-a-#{p}"})
      to_complete = make_intent(id: "la-done-#{p}", conflict_key: {:file, "la-b-#{p}"})
      to_fail = make_intent(id: "la-failed-#{p}", conflict_key: {:file, "la-c-#{p}"})

      {:ok, _} = IntentLog.try_acquire(active)
      {:ok, _} = IntentLog.try_acquire(to_complete)
      {:ok, _} = IntentLog.try_acquire(to_fail)
      :ok = IntentLog.complete("la-done-#{p}")
      :ok = IntentLog.fail("la-failed-#{p}", :error)

      assert {:ok, active_list} = IntentLog.list_active()
      test_ids = MapSet.new(["la-active-#{p}", "la-done-#{p}", "la-failed-#{p}"])

      relevant =
        Enum.filter(active_list, fn intent -> MapSet.member?(test_ids, intent.id) end)

      assert length(relevant) == 1
      assert hd(relevant).id == "la-active-#{p}"
    end
  end

  describe "list_expired/0" do
    test "returns intents past their TTL", %{prefix: p} do
      expired = make_expired_intent(id: "le-exp-#{p}", conflict_key: {:file, "le-a-#{p}"})
      active = make_intent(id: "le-active-#{p}", conflict_key: {:file, "le-b-#{p}"})

      {:ok, _} = IntentLog.try_acquire(expired)
      {:ok, _} = IntentLog.try_acquire(active)

      assert {:ok, expired_list} = IntentLog.list_expired()
      assert Enum.any?(expired_list, &(&1.id == "le-exp-#{p}"))
      refute Enum.any?(expired_list, &(&1.id == "le-active-#{p}"))
    end

    test "returns empty list when no expired intents", %{prefix: p} do
      intent = make_intent(id: "le-none-#{p}")
      {:ok, _} = IntentLog.try_acquire(intent)

      assert {:ok, expired_list} = IntentLog.list_expired()
      refute Enum.any?(expired_list, &(&1.id == "le-none-#{p}"))
    end
  end

  describe "cleanup_expired_intents/0" do
    test "marks expired pending intents as expired", %{prefix: p} do
      expired1 = make_expired_intent(id: "ce-exp1-#{p}", conflict_key: {:file, "ce-a-#{p}"})
      expired2 = make_expired_intent(id: "ce-exp2-#{p}", conflict_key: {:file, "ce-b-#{p}"})
      active = make_intent(id: "ce-active-#{p}", conflict_key: {:file, "ce-c-#{p}"})

      {:ok, _} = IntentLog.try_acquire(expired1)
      {:ok, _} = IntentLog.try_acquire(expired2)
      {:ok, _} = IntentLog.try_acquire(active)

      assert {:ok, count} = IntentLog.cleanup_expired_intents()
      # At least our 2 expired intents are cleaned (possibly more from other tests)
      assert count >= 2

      assert {:ok, stored_exp1} = IntentLog.get("ce-exp1-#{p}")
      assert stored_exp1.state == :expired

      assert {:ok, stored_exp2} = IntentLog.get("ce-exp2-#{p}")
      assert stored_exp2.state == :expired

      assert {:ok, stored_active} = IntentLog.get("ce-active-#{p}")
      assert stored_active.state == :pending
    end

    test "returns zero when no intents are expired", %{prefix: p} do
      # Clean up any leftover expired intents from earlier tests first
      IntentLog.cleanup_expired_intents()

      intent = make_intent(id: "ce-none-#{p}")
      {:ok, _} = IntentLog.try_acquire(intent)

      assert {:ok, 0} = IntentLog.cleanup_expired_intents()
    end

    test "skips already-completed intents", %{prefix: p} do
      expired = make_expired_intent(id: "ce-done-#{p}", conflict_key: {:file, "ce-d-#{p}"})
      {:ok, _} = IntentLog.try_acquire(expired)
      :ok = IntentLog.complete("ce-done-#{p}")

      # Clean up other expired intents first so count is deterministic
      IntentLog.cleanup_expired_intents()

      assert {:ok, 0} = IntentLog.cleanup_expired_intents()

      assert {:ok, stored} = IntentLog.get("ce-done-#{p}")
      assert stored.state == :completed
    end

    test "conflict keys released after cleanup", %{prefix: p} do
      key = {:file, "ce-release-#{p}"}
      expired = make_expired_intent(id: "ce-old-#{p}", conflict_key: key)
      {:ok, _} = IntentLog.try_acquire(expired)

      {:ok, _count} = IntentLog.cleanup_expired_intents()

      # Conflict key is released — new intent can acquire it
      new_intent = make_intent(id: "ce-new-#{p}", conflict_key: key)
      assert {:ok, _} = IntentLog.try_acquire(new_intent)
    end
  end

  describe "full lifecycle" do
    test "acquire → extend → complete", %{prefix: p} do
      intent = make_intent(id: "lc-full-#{p}", ttl_seconds: 60)
      assert {:ok, _} = IntentLog.try_acquire(intent)

      assert :ok = IntentLog.extend("lc-full-#{p}", 120)

      {:ok, stored} = IntentLog.get("lc-full-#{p}")
      assert stored.state == :pending

      assert :ok = IntentLog.complete("lc-full-#{p}")

      {:ok, final} = IntentLog.get("lc-full-#{p}")
      assert final.state == :completed
    end

    test "acquire → fail releases conflict key for new writer", %{prefix: p} do
      key = {:file, "lc-retry-#{p}"}
      intent = make_intent(id: "lc-w1-#{p}", conflict_key: key)
      assert {:ok, _} = IntentLog.try_acquire(intent)
      assert :ok = IntentLog.fail("lc-w1-#{p}", :crashed)

      retry = make_intent(id: "lc-w2-#{p}", conflict_key: key)
      assert {:ok, _} = IntentLog.try_acquire(retry)
    end
  end
end
