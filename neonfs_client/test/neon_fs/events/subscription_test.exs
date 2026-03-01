defmodule NeonFS.Events.SubscriptionTest do
  use ExUnit.Case, async: false

  alias NeonFS.Events
  alias NeonFS.Events.{Envelope, FileCreated, Relay, VolumeDeleted}

  @pg_scope :neonfs_events
  @volume_id "vol-sub-test"

  setup do
    start_supervised!(%{id: :pg, start: {:pg, :start_link, [@pg_scope]}})
    start_supervised!({Registry, keys: :duplicate, name: NeonFS.Events.Registry})
    start_supervised!(Relay)

    :ok
  end

  describe "subscribe/1" do
    test "registers the calling process in the local Registry" do
      Events.subscribe(@volume_id)

      entries = Registry.lookup(NeonFS.Events.Registry, {:volume, @volume_id})
      assert {self(), []} in entries
    end

    test "tells Relay to join pg group" do
      Events.subscribe(@volume_id)

      relay_pid = Process.whereis(Relay)
      assert relay_pid in :pg.get_members(@pg_scope, {:volume, @volume_id})
    end

    test "subscriber receives events after subscribing" do
      Events.subscribe(@volume_id)

      envelope = %Envelope{
        event: %FileCreated{volume_id: @volume_id, file_id: "f1", path: "/test.txt"},
        source_node: node(),
        sequence: 1,
        hlc_timestamp: {1_000_000, 0, node()}
      }

      relay_pid = Process.whereis(Relay)
      send(relay_pid, {:neonfs_event, envelope})

      assert_receive {:neonfs_event, ^envelope}, 1_000
    end
  end

  describe "unsubscribe/1" do
    test "removes the registration from Registry" do
      Events.subscribe(@volume_id)
      Events.unsubscribe(@volume_id)

      entries = Registry.lookup(NeonFS.Events.Registry, {:volume, @volume_id})
      assert entries == []
    end

    test "tells Relay to leave pg group when last subscriber" do
      Events.subscribe(@volume_id)
      Events.unsubscribe(@volume_id)

      assert :pg.get_members(@pg_scope, {:volume, @volume_id}) == []
    end

    test "subscriber no longer receives events after unsubscribing" do
      Events.subscribe(@volume_id)
      Events.unsubscribe(@volume_id)

      envelope = %Envelope{
        event: %FileCreated{volume_id: @volume_id, file_id: "f1", path: "/test.txt"},
        source_node: node(),
        sequence: 1,
        hlc_timestamp: {1_000_000, 0, node()}
      }

      relay_pid = Process.whereis(Relay)
      send(relay_pid, {:neonfs_event, envelope})

      refute_receive {:neonfs_event, _}, 200
    end
  end

  describe "subscribe_volumes/0" do
    test "registers the calling process under {:volumes}" do
      Events.subscribe_volumes()

      entries = Registry.lookup(NeonFS.Events.Registry, {:volumes})
      assert {self(), []} in entries
    end

    test "subscriber receives volume events" do
      Events.subscribe_volumes()

      envelope = %Envelope{
        event: %VolumeDeleted{volume_id: @volume_id},
        source_node: node(),
        sequence: 1,
        hlc_timestamp: {1_000_000, 0, node()}
      }

      relay_pid = Process.whereis(Relay)
      send(relay_pid, {:neonfs_event, envelope})

      assert_receive {:neonfs_event, ^envelope}, 1_000
    end
  end

  describe "unsubscribe_volumes/0" do
    test "removes the registration from Registry" do
      Events.subscribe_volumes()
      Events.unsubscribe_volumes()

      entries = Registry.lookup(NeonFS.Events.Registry, {:volumes})
      assert entries == []
    end
  end

  describe "multiple subscribers" do
    test "multiple processes subscribing to the same volume each receive events" do
      Events.subscribe(@volume_id)

      parent = self()

      child =
        spawn(fn ->
          Events.subscribe(@volume_id)
          send(parent, :child_ready)

          receive do
            {:neonfs_event, envelope} -> send(parent, {:child_got, envelope})
          end
        end)

      assert_receive :child_ready, 1_000

      envelope = %Envelope{
        event: %FileCreated{volume_id: @volume_id, file_id: "f1", path: "/test.txt"},
        source_node: node(),
        sequence: 1,
        hlc_timestamp: {1_000_000, 0, node()}
      }

      relay_pid = Process.whereis(Relay)
      send(relay_pid, {:neonfs_event, envelope})

      # Parent should receive it
      assert_receive {:neonfs_event, ^envelope}, 1_000

      # Child should receive it too
      assert_receive {:child_got, ^envelope}, 1_000

      # Relay should only be in pg group once
      members = :pg.get_members(@pg_scope, {:volume, @volume_id})
      assert Enum.count(members, &(&1 == relay_pid)) == 1

      # Clean up
      Process.exit(child, :normal)
    end
  end

  describe "automatic cleanup on process exit" do
    test "Registry auto-unregisters when subscribing process exits" do
      tref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :events, :relay, :subscriber_down]
        ])

      parent = self()

      {pid, mref} =
        spawn_monitor(fn ->
          Events.subscribe(@volume_id)
          send(parent, :child_ready)

          receive do
            :stop -> :ok
          end
        end)

      assert_receive :child_ready, 1_000

      entries = Registry.lookup(NeonFS.Events.Registry, {:volume, @volume_id})
      assert Enum.any?(entries, fn {p, _} -> p == pid end)

      send(pid, :stop)
      assert_receive {:DOWN, ^mref, :process, ^pid, :normal}, 1_000

      assert_receive {[:neonfs, :events, :relay, :subscriber_down], ^tref, %{},
                      %{pid: ^pid, volumes_left: _}},
                     1_000

      entries = Registry.lookup(NeonFS.Events.Registry, {:volume, @volume_id})
      refute Enum.any?(entries, fn {p, _} -> p == pid end)
    end
  end
end
