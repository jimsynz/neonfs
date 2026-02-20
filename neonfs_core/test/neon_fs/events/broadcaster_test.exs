defmodule NeonFS.Events.BroadcasterTest do
  use ExUnit.Case, async: false

  alias NeonFS.Events.{
    Broadcaster,
    Envelope,
    FileCreated,
    VolumeCreated,
    VolumeDeleted,
    VolumeUpdated
  }

  @pg_scope :neonfs_events
  @volume_id "vol-broadcaster-test"

  setup do
    start_supervised!(%{id: :pg, start: {:pg, :start_link, [@pg_scope]}})

    # Clean up any persistent_term counters from previous tests
    cleanup_counter(@volume_id)
    cleanup_counter("vol-broadcaster-a")
    cleanup_counter("vol-broadcaster-b")

    # Clean up process dictionary HLC state
    Process.delete({Broadcaster, :hlc})

    :ok
  end

  describe "broadcast/2" do
    test "sends event to :pg group members for the volume" do
      :pg.join(@pg_scope, {:volume, @volume_id}, self())

      event = %FileCreated{volume_id: @volume_id, file_id: "f1", path: "/test.txt"}
      assert :ok = Broadcaster.broadcast(@volume_id, event)

      assert_receive {:neonfs_event, %Envelope{} = envelope}, 1_000
      assert envelope.event == event
      assert envelope.source_node == node()
      assert envelope.sequence == 1
      assert is_tuple(envelope.hlc_timestamp)
      assert tuple_size(envelope.hlc_timestamp) == 3
    end

    test "sends volume events to both volume and volumes groups" do
      :pg.join(@pg_scope, {:volume, @volume_id}, self())
      :pg.join(@pg_scope, {:volumes}, self())

      event = %VolumeCreated{volume_id: @volume_id}
      assert :ok = Broadcaster.broadcast(@volume_id, event)

      # Should receive from the volume group and the volumes group (2 messages)
      assert_receive {:neonfs_event, %Envelope{event: %VolumeCreated{}}}, 1_000
      assert_receive {:neonfs_event, %Envelope{event: %VolumeCreated{}}}, 1_000
    end

    test "sends VolumeUpdated to volumes group" do
      :pg.join(@pg_scope, {:volumes}, self())

      event = %VolumeUpdated{volume_id: @volume_id}
      assert :ok = Broadcaster.broadcast(@volume_id, event)

      assert_receive {:neonfs_event, %Envelope{event: %VolumeUpdated{}}}, 1_000
    end

    test "sends VolumeDeleted to volumes group" do
      :pg.join(@pg_scope, {:volumes}, self())

      event = %VolumeDeleted{volume_id: @volume_id}
      assert :ok = Broadcaster.broadcast(@volume_id, event)

      assert_receive {:neonfs_event, %Envelope{event: %VolumeDeleted{}}}, 1_000
    end

    test "does not send non-volume events to volumes group" do
      :pg.join(@pg_scope, {:volumes}, self())

      event = %FileCreated{volume_id: @volume_id, file_id: "f1", path: "/test.txt"}
      assert :ok = Broadcaster.broadcast(@volume_id, event)

      refute_receive {:neonfs_event, _}, 200
    end

    test "returns :ok with no subscribers" do
      event = %FileCreated{volume_id: @volume_id, file_id: "f1", path: "/test.txt"}
      assert :ok = Broadcaster.broadcast(@volume_id, event)
    end

    test "envelope contains correct source_node" do
      :pg.join(@pg_scope, {:volume, @volume_id}, self())

      event = %FileCreated{volume_id: @volume_id, file_id: "f1", path: "/a.txt"}
      Broadcaster.broadcast(@volume_id, event)

      assert_receive {:neonfs_event, %Envelope{source_node: source_node}}, 1_000
      assert source_node == node()
    end

    test "envelope contains valid HLC timestamp" do
      :pg.join(@pg_scope, {:volume, @volume_id}, self())

      event = %FileCreated{volume_id: @volume_id, file_id: "f1", path: "/a.txt"}
      Broadcaster.broadcast(@volume_id, event)

      assert_receive {:neonfs_event, %Envelope{hlc_timestamp: {wall_ms, counter, node_id}}},
                     1_000

      assert is_integer(wall_ms) and wall_ms > 0
      assert is_integer(counter) and counter >= 0
      assert node_id == node()
    end
  end

  describe "sequence counters" do
    test "sequence numbers are monotonically increasing for the same volume" do
      :pg.join(@pg_scope, {:volume, @volume_id}, self())

      for i <- 1..5 do
        event = %FileCreated{volume_id: @volume_id, file_id: "f#{i}", path: "/#{i}.txt"}
        Broadcaster.broadcast(@volume_id, event)
      end

      sequences =
        for _ <- 1..5 do
          assert_receive {:neonfs_event, %Envelope{sequence: seq}}, 1_000
          seq
        end

      assert sequences == [1, 2, 3, 4, 5]
    end

    test "sequence counters are independent per volume" do
      volume_a = "vol-broadcaster-a"
      volume_b = "vol-broadcaster-b"

      :pg.join(@pg_scope, {:volume, volume_a}, self())
      :pg.join(@pg_scope, {:volume, volume_b}, self())

      # Broadcast 3 events to volume A
      for i <- 1..3 do
        Broadcaster.broadcast(volume_a, %FileCreated{
          volume_id: volume_a,
          file_id: "f#{i}",
          path: "/a#{i}.txt"
        })
      end

      # Broadcast 2 events to volume B
      for i <- 1..2 do
        Broadcaster.broadcast(volume_b, %FileCreated{
          volume_id: volume_b,
          file_id: "f#{i}",
          path: "/b#{i}.txt"
        })
      end

      # Collect all events
      events =
        for _ <- 1..5 do
          assert_receive {:neonfs_event, %Envelope{} = env}, 1_000
          env
        end

      vol_a_seqs =
        events
        |> Enum.filter(&(&1.event.volume_id == volume_a))
        |> Enum.map(& &1.sequence)

      vol_b_seqs =
        events
        |> Enum.filter(&(&1.event.volume_id == volume_b))
        |> Enum.map(& &1.sequence)

      assert Enum.sort(vol_a_seqs) == [1, 2, 3]
      assert Enum.sort(vol_b_seqs) == [1, 2]
    end

    test "first event gets sequence 1" do
      :pg.join(@pg_scope, {:volume, @volume_id}, self())

      event = %FileCreated{volume_id: @volume_id, file_id: "f1", path: "/test.txt"}
      Broadcaster.broadcast(@volume_id, event)

      assert_receive {:neonfs_event, %Envelope{sequence: 1}}, 1_000
    end
  end

  describe "get_or_create_counter/1" do
    test "is idempotent — calling twice returns the same counter" do
      counter1 = Broadcaster.get_or_create_counter(@volume_id)
      counter2 = Broadcaster.get_or_create_counter(@volume_id)

      assert counter1 == counter2
    end

    test "different volumes get different counters" do
      counter_a = Broadcaster.get_or_create_counter("vol-broadcaster-a")
      counter_b = Broadcaster.get_or_create_counter("vol-broadcaster-b")

      refute counter_a == counter_b
    end
  end

  describe "next_sequence/1" do
    test "returns incrementing values" do
      assert Broadcaster.next_sequence(@volume_id) == 1
      assert Broadcaster.next_sequence(@volume_id) == 2
      assert Broadcaster.next_sequence(@volume_id) == 3
    end
  end

  defp cleanup_counter(volume_id) do
    key = {Broadcaster, :counter, volume_id}
    :persistent_term.erase(key)
  rescue
    ArgumentError -> :ok
  end
end
