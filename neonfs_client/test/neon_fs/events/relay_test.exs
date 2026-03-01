defmodule NeonFS.Events.RelayTest do
  use ExUnit.Case, async: false

  alias NeonFS.Events.{Envelope, FileCreated, Relay, VolumeCreated}

  @pg_scope :neonfs_events
  @volume_id "vol-relay-test"

  setup do
    start_supervised!(%{id: :pg, start: {:pg, :start_link, [@pg_scope]}})
    start_supervised!({Registry, keys: :duplicate, name: NeonFS.Events.Registry})
    start_supervised!(Relay)

    :ok
  end

  describe "init" do
    test "joins the {:volumes} pg group on startup" do
      members = :pg.get_members(@pg_scope, {:volumes})
      relay_pid = Process.whereis(Relay)

      assert relay_pid in members
    end
  end

  describe "ensure_volume_group/1" do
    test "joins pg group on first subscriber" do
      assert :pg.get_members(@pg_scope, {:volume, @volume_id}) == []

      Relay.ensure_volume_group(@volume_id)

      relay_pid = Process.whereis(Relay)
      assert relay_pid in :pg.get_members(@pg_scope, {:volume, @volume_id})
    end

    test "does not join pg group again on subsequent subscribers" do
      Relay.ensure_volume_group(@volume_id)
      Relay.ensure_volume_group(@volume_id)

      relay_pid = Process.whereis(Relay)
      members = :pg.get_members(@pg_scope, {:volume, @volume_id})

      # The relay should appear exactly once
      assert Enum.count(members, &(&1 == relay_pid)) == 1
    end
  end

  describe "maybe_leave_volume_group/1" do
    test "leaves pg group when last subscriber unsubscribes" do
      Relay.ensure_volume_group(@volume_id)
      Relay.maybe_leave_volume_group(@volume_id)

      assert :pg.get_members(@pg_scope, {:volume, @volume_id}) == []
    end

    test "does not leave pg group when other subscribers remain" do
      Relay.ensure_volume_group(@volume_id)
      Relay.ensure_volume_group(@volume_id)
      Relay.maybe_leave_volume_group(@volume_id)

      relay_pid = Process.whereis(Relay)
      assert relay_pid in :pg.get_members(@pg_scope, {:volume, @volume_id})
    end
  end

  describe "event dispatch" do
    test "dispatches file events to {:volume, volume_id} subscribers" do
      Registry.register(NeonFS.Events.Registry, {:volume, @volume_id}, [])

      envelope = %Envelope{
        event: %FileCreated{volume_id: @volume_id, file_id: "f1", path: "/a.txt"},
        source_node: node(),
        sequence: 1,
        hlc_timestamp: {1_000_000, 0, node()}
      }

      relay_pid = Process.whereis(Relay)
      send(relay_pid, {:neonfs_event, envelope})

      assert_receive {:neonfs_event, ^envelope}, 1_000
    end

    test "dispatches volume events to {:volumes} subscribers" do
      Registry.register(NeonFS.Events.Registry, {:volumes}, [])

      envelope = %Envelope{
        event: %VolumeCreated{volume_id: @volume_id},
        source_node: node(),
        sequence: 1,
        hlc_timestamp: {1_000_000, 0, node()}
      }

      relay_pid = Process.whereis(Relay)
      send(relay_pid, {:neonfs_event, envelope})

      assert_receive {:neonfs_event, ^envelope}, 1_000
    end

    test "does not dispatch file events to {:volumes} subscribers" do
      Registry.register(NeonFS.Events.Registry, {:volumes}, [])

      envelope = %Envelope{
        event: %FileCreated{volume_id: @volume_id, file_id: "f1", path: "/a.txt"},
        source_node: node(),
        sequence: 1,
        hlc_timestamp: {1_000_000, 0, node()}
      }

      relay_pid = Process.whereis(Relay)
      send(relay_pid, {:neonfs_event, envelope})

      refute_receive {:neonfs_event, _}, 200
    end

    test "dispatches to multiple subscribers for the same volume" do
      # Register the test process under two different aliases
      Registry.register(NeonFS.Events.Registry, {:volume, @volume_id}, :sub1)
      Registry.register(NeonFS.Events.Registry, {:volume, @volume_id}, :sub2)

      envelope = %Envelope{
        event: %FileCreated{volume_id: @volume_id, file_id: "f1", path: "/a.txt"},
        source_node: node(),
        sequence: 1,
        hlc_timestamp: {1_000_000, 0, node()}
      }

      relay_pid = Process.whereis(Relay)
      send(relay_pid, {:neonfs_event, envelope})

      # Should receive the event twice (once per registration)
      assert_receive {:neonfs_event, ^envelope}, 1_000
      assert_receive {:neonfs_event, ^envelope}, 1_000
    end
  end

  describe "subscriber process exit" do
    test "cleans up volume ref count when subscriber exits" do
      tref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :events, :relay, :subscriber_down]
        ])

      parent = self()

      # Spawn a process that subscribes and then exits
      {pid, mref} =
        spawn_monitor(fn ->
          Registry.register(NeonFS.Events.Registry, {:volume, @volume_id}, [])
          Relay.ensure_volume_group(@volume_id)
          send(parent, :child_ready)

          receive do
            :stop -> :ok
          end
        end)

      assert_receive :child_ready, 1_000

      relay_pid = Process.whereis(Relay)
      assert relay_pid in :pg.get_members(@pg_scope, {:volume, @volume_id})

      # Kill the subscriber
      send(pid, :stop)
      assert_receive {:DOWN, ^mref, :process, ^pid, :normal}, 1_000

      # Wait for Relay to process the DOWN message via telemetry
      assert_receive {[:neonfs, :events, :relay, :subscriber_down], ^tref, %{},
                      %{pid: ^pid, volumes_left: _}},
                     1_000

      assert :pg.get_members(@pg_scope, {:volume, @volume_id}) == []
    end
  end
end
