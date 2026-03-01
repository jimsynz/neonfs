defmodule NeonFS.Core.AuditLogTest do
  use NeonFS.TestCase, async: false
  use ExUnit.Case, async: false

  alias NeonFS.Core.{AuditEvent, AuditLog}

  setup do
    start_audit_log(max_events: 100, max_age_days: 90, prune_interval_ms: 0)
    :ok
  end

  describe "log/1" do
    test "stores an event" do
      event = AuditEvent.new(event_type: :volume_created, actor_uid: 1000, resource: "vol-1")
      AuditLog.log(event)
      AuditLog.flush()

      assert AuditLog.count() == 1
    end

    test "stores multiple events" do
      for i <- 1..5 do
        AuditLog.log(
          AuditEvent.new(event_type: :volume_created, actor_uid: i, resource: "vol-#{i}")
        )
      end

      AuditLog.flush()
      assert AuditLog.count() == 5
    end

    test "is non-blocking (uses cast)" do
      # Verify log returns :ok immediately
      event = AuditEvent.new(event_type: :volume_created, actor_uid: 0)
      assert :ok = AuditLog.log(event)
    end

    test "does not raise if AuditLog process is not running" do
      # Stop the audit log
      GenServer.stop(AuditLog)

      # Should not raise
      event = AuditEvent.new(event_type: :volume_created, actor_uid: 0)
      assert :ok = AuditLog.log(event)
    end
  end

  describe "log_event/1" do
    test "creates and logs an event from keyword attributes" do
      AuditLog.log_event(event_type: :volume_deleted, actor_uid: 42, resource: "vol-x")
      AuditLog.flush()

      [event] = AuditLog.recent(1)
      assert event.event_type == :volume_deleted
      assert event.actor_uid == 42
      assert event.resource == "vol-x"
    end
  end

  describe "recent/1" do
    test "returns empty list when no events" do
      assert AuditLog.recent() == []
    end

    test "returns events in chronological order" do
      for i <- 1..5 do
        AuditLog.log(
          AuditEvent.new(event_type: :admin_action, actor_uid: i, resource: "res-#{i}")
        )
      end

      AuditLog.flush()

      events = AuditLog.recent(10)
      assert length(events) == 5
      uids = Enum.map(events, & &1.actor_uid)
      assert uids == [1, 2, 3, 4, 5]
    end

    test "respects limit" do
      for i <- 1..10 do
        AuditLog.log(AuditEvent.new(event_type: :admin_action, actor_uid: i))
      end

      AuditLog.flush()

      events = AuditLog.recent(3)
      assert length(events) == 3
    end
  end

  describe "query/1" do
    setup do
      events = [
        AuditEvent.new(
          event_type: :volume_created,
          actor_uid: 1000,
          resource: "vol-alpha",
          outcome: :success
        ),
        AuditEvent.new(
          event_type: :volume_deleted,
          actor_uid: 1001,
          resource: "vol-beta",
          outcome: :success
        ),
        AuditEvent.new(
          event_type: :authorisation_denied,
          actor_uid: 2000,
          resource: "vol-alpha",
          outcome: :denied
        ),
        AuditEvent.new(
          event_type: :volume_acl_changed,
          actor_uid: 1000,
          resource: "vol-alpha",
          outcome: :success
        ),
        AuditEvent.new(
          event_type: :key_rotated,
          actor_uid: 0,
          resource: "vol-gamma",
          outcome: :success
        )
      ]

      for event <- events do
        AuditLog.log(event)
      end

      AuditLog.flush()
      :ok
    end

    test "returns all events without filters" do
      events = AuditLog.query()
      assert length(events) == 5
    end

    test "filters by event_type atom" do
      events = AuditLog.query(event_type: :volume_created)
      assert length(events) == 1
      assert hd(events).event_type == :volume_created
    end

    test "filters by event_type list" do
      events = AuditLog.query(event_type: [:volume_created, :volume_deleted])
      assert length(events) == 2
      types = Enum.map(events, & &1.event_type)
      assert :volume_created in types
      assert :volume_deleted in types
    end

    test "filters by actor_uid" do
      events = AuditLog.query(actor_uid: 1000)
      assert length(events) == 2
      assert Enum.all?(events, &(&1.actor_uid == 1000))
    end

    test "filters by resource prefix" do
      events = AuditLog.query(resource: "vol-alpha")
      assert length(events) == 3
    end

    test "filters by time range" do
      now = DateTime.utc_now()
      past = DateTime.add(now, -3600, :second)
      future = DateTime.add(now, 3600, :second)

      events = AuditLog.query(since: past, until: future)
      assert length(events) == 5

      # All events should be in the future from a very old datetime
      ancient = DateTime.add(now, -86_400 * 365, :second)
      events = AuditLog.query(since: ancient)
      assert length(events) == 5
    end

    test "respects limit" do
      events = AuditLog.query(limit: 2)
      assert length(events) == 2
    end

    test "combines multiple filters" do
      events = AuditLog.query(event_type: :authorisation_denied, actor_uid: 2000)
      assert length(events) == 1
      assert hd(events).event_type == :authorisation_denied
      assert hd(events).actor_uid == 2000
    end
  end

  describe "bounded storage" do
    test "prunes oldest events when max_events exceeded" do
      # Reconfigure with a small limit
      GenServer.stop(AuditLog)
      cleanup_ets_table(:audit_log)
      start_audit_log(max_events: 5, prune_interval_ms: 0)

      for i <- 1..10 do
        AuditLog.log(AuditEvent.new(event_type: :admin_action, actor_uid: i, resource: "r-#{i}"))
      end

      AuditLog.flush()

      assert AuditLog.count() <= 5
      # Most recent events should be retained
      events = AuditLog.recent(10)
      uids = Enum.map(events, & &1.actor_uid)
      # The newest events should be present
      assert 10 in uids
    end
  end

  describe "retention pruning" do
    test "removes expired events" do
      # Insert events with old timestamps
      old_ts = DateTime.add(DateTime.utc_now(), -100, :day)

      old_event =
        AuditEvent.new(event_type: :admin_action, actor_uid: 1)
        |> Map.put(:timestamp, old_ts)

      AuditLog.log(old_event)

      # Insert a recent event
      AuditLog.log(AuditEvent.new(event_type: :volume_created, actor_uid: 2))
      AuditLog.flush()

      assert AuditLog.count() == 2

      # Trigger prune by sending the message directly
      send(Process.whereis(AuditLog), :prune)
      :sys.get_state(AuditLog)

      assert AuditLog.count() == 1
      [remaining] = AuditLog.recent(10)
      assert remaining.actor_uid == 2
    end
  end

  describe "count/0" do
    test "returns 0 when empty" do
      assert AuditLog.count() == 0
    end

    test "returns correct count" do
      for _ <- 1..3 do
        AuditLog.log(AuditEvent.new(event_type: :admin_action, actor_uid: 0))
      end

      AuditLog.flush()
      assert AuditLog.count() == 3
    end
  end

  describe "telemetry" do
    test "emits [:neonfs, :audit, :logged] on event storage" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :audit, :logged]
        ])

      AuditLog.log(AuditEvent.new(event_type: :volume_created, actor_uid: 0))
      AuditLog.flush()

      assert_received {[:neonfs, :audit, :logged], ^ref, %{count: 1},
                       %{event_type: :volume_created}}
    end
  end

  defp cleanup_ets_table(table) do
    case :ets.whereis(table) do
      :undefined -> :ok
      ref -> :ets.delete(ref)
    end
  end
end
