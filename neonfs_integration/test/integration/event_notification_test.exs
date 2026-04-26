defmodule NeonFS.Integration.EventNotificationTest do
  @moduledoc """
  Phase 10 integration tests for event notification.

  Tests that the event notification system works correctly across a multi-node
  cluster: event emission on metadata writes, cross-node delivery via `:pg`,
  local dispatch via Registry, subscription management, sequence ordering,
  gap detection, and partition recovery.

  Uses `cluster: :shared` to spin up a single 3-node cluster for the entire
  module rather than per-test, saving ~19s of cluster setup per test.
  """
  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Events.Envelope
  alias NeonFS.Integration.EventCollector

  @moduletag timeout: 300_000
  @moduletag :integration
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "event-test")
    %{}
  end

  setup %{cluster: cluster} do
    # Each test gets a unique volume to avoid cross-test interference
    suffix = System.unique_integer([:positive])
    volume_name = "evt-vol-#{suffix}"

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        volume_name,
        %{}
      ])

    %{volume_id: volume.id, volume_name: volume_name}
  end

  # ─── Basic event delivery ──────────────────────────────────────────

  describe "cross-node event delivery" do
    test "file creation on node1 emits FileCreated received by subscriber on node2",
         %{cluster: cluster, volume_id: vid, volume_name: vname} do
      {:ok, collector} =
        PeerCluster.rpc(cluster, :node2, EventCollector, :start, [vid])

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          vname,
          "/hello.txt",
          "hello"
        ])

      assert_eventually timeout: 10_000 do
        PeerCluster.rpc(cluster, :node2, EventCollector, :count, [collector]) > 0
      end

      events = PeerCluster.rpc(cluster, :node2, EventCollector, :events, [collector])

      file_created =
        Enum.find(events, &match?(%Envelope{event: %NeonFS.Events.FileCreated{}}, &1))

      assert file_created != nil
      assert file_created.event.volume_id == vid
      assert file_created.event.path == "/hello.txt"
    end

    test "file deletion emits FileDeleted received by subscriber",
         %{cluster: cluster, volume_id: vid, volume_name: vname} do
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          vname,
          "/to-delete.txt",
          "data"
        ])

      {:ok, collector} =
        PeerCluster.rpc(cluster, :node2, EventCollector, :start, [vid])

      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :delete_file, [
          vname,
          "/to-delete.txt"
        ])

      assert_eventually timeout: 10_000 do
        events = PeerCluster.rpc(cluster, :node2, EventCollector, :events, [collector])
        Enum.any?(events, &match?(%Envelope{event: %NeonFS.Events.FileDeleted{}}, &1))
      end

      events = PeerCluster.rpc(cluster, :node2, EventCollector, :events, [collector])
      deleted = Enum.find(events, &match?(%Envelope{event: %NeonFS.Events.FileDeleted{}}, &1))
      assert deleted.event.path == "/to-delete.txt"
    end

    test "directory creation emits DirCreated received by subscriber",
         %{cluster: cluster, volume_id: vid} do
      {:ok, collector} =
        PeerCluster.rpc(cluster, :node2, EventCollector, :start, [vid])

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :mkdir, [vid, "/mydir"])

      assert_eventually timeout: 10_000 do
        events = PeerCluster.rpc(cluster, :node2, EventCollector, :events, [collector])
        Enum.any?(events, &match?(%Envelope{event: %NeonFS.Events.DirCreated{}}, &1))
      end
    end

    test "ACL change emits VolumeAclChanged event",
         %{cluster: cluster, volume_id: vid} do
      {:ok, collector} =
        PeerCluster.rpc(cluster, :node2, EventCollector, :start, [vid])

      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ACLManager, :grant, [
          vid,
          {:uid, 1000},
          [:read]
        ])

      assert_eventually timeout: 10_000 do
        events = PeerCluster.rpc(cluster, :node2, EventCollector, :events, [collector])
        Enum.any?(events, &match?(%Envelope{event: %NeonFS.Events.VolumeAclChanged{}}, &1))
      end
    end
  end

  # ─── Volume lifecycle events ───────────────────────────────────────

  describe "volume lifecycle events" do
    test "volume creation emits VolumeCreated received by volume subscribers",
         %{cluster: cluster} do
      {:ok, collector} =
        PeerCluster.rpc(cluster, :node2, EventCollector, :start_volumes, [])

      {:ok, _vol} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "lifecycle-vol-#{System.unique_integer([:positive])}",
          %{}
        ])

      assert_eventually timeout: 10_000 do
        events = PeerCluster.rpc(cluster, :node2, EventCollector, :events, [collector])
        Enum.any?(events, &match?(%Envelope{event: %NeonFS.Events.VolumeCreated{}}, &1))
      end

      events = PeerCluster.rpc(cluster, :node2, EventCollector, :events, [collector])
      created = Enum.find(events, &match?(%Envelope{event: %NeonFS.Events.VolumeCreated{}}, &1))
      assert created.event.volume_id != nil
    end
  end

  # ─── Envelope fields and ordering ──────────────────────────────────

  describe "envelope metadata" do
    test "events contain correct envelope fields",
         %{cluster: cluster, volume_id: vid, volume_name: vname} do
      {:ok, collector} =
        PeerCluster.rpc(cluster, :node2, EventCollector, :start, [vid])

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          vname,
          "/envelope-test.txt",
          "data"
        ])

      assert_eventually timeout: 10_000 do
        PeerCluster.rpc(cluster, :node2, EventCollector, :count, [collector]) > 0
      end

      events = PeerCluster.rpc(cluster, :node2, EventCollector, :events, [collector])
      envelope = List.first(events)

      # Verify envelope fields
      assert is_atom(envelope.source_node)
      assert is_integer(envelope.sequence)
      assert envelope.sequence > 0
      assert is_tuple(envelope.hlc_timestamp)
      # HLC timestamp is {wall_ms, logical, node}
      {wall_ms, logical, hlc_node} = envelope.hlc_timestamp
      assert is_integer(wall_ms)
      assert is_integer(logical)
      assert is_atom(hlc_node)
    end

    test "sequence numbers are monotonically increasing from the same source",
         %{cluster: cluster, volume_id: vid, volume_name: vname} do
      {:ok, collector} =
        PeerCluster.rpc(cluster, :node2, EventCollector, :start, [vid])

      for i <- 1..5 do
        {:ok, _} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
            vname,
            "/seq-#{i}.txt",
            "data-#{i}"
          ])
      end

      assert_eventually timeout: 15_000 do
        PeerCluster.rpc(cluster, :node2, EventCollector, :count, [collector]) >= 5
      end

      events = PeerCluster.rpc(cluster, :node2, EventCollector, :events, [collector])

      node1_info = PeerCluster.get_node!(cluster, :node1)

      node1_events =
        events
        |> Enum.filter(&(&1.source_node == node1_info.node))
        |> Enum.map(& &1.sequence)

      # Sequences should be strictly increasing
      assert node1_events == Enum.sort(node1_events)
      assert Enum.uniq(node1_events) == node1_events
    end
  end

  # ─── Fan-out and isolation ─────────────────────────────────────────

  describe "subscription fan-out and isolation" do
    test "multiple subscribers on the same node all receive the event",
         %{cluster: cluster, volume_id: vid, volume_name: vname} do
      collectors =
        for _i <- 1..3 do
          {:ok, pid} =
            PeerCluster.rpc(cluster, :node2, EventCollector, :start, [vid])

          pid
        end

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          vname,
          "/fanout.txt",
          "data"
        ])

      for collector <- collectors do
        assert_eventually timeout: 10_000 do
          PeerCluster.rpc(cluster, :node2, EventCollector, :count, [collector]) > 0
        end
      end
    end

    test "subscribing to volume X does not receive events for volume Y",
         %{cluster: cluster, volume_id: vid_x, volume_name: vname_x} do
      # Create a second volume
      other_name = "other-vol-#{System.unique_integer([:positive])}"

      {:ok, vol_y} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          other_name,
          %{}
        ])

      {:ok, collector_x} =
        PeerCluster.rpc(cluster, :node2, EventCollector, :start, [vid_x])

      # Write to volume Y on node1
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          other_name,
          "/other.txt",
          "data"
        ])

      # Also write to volume X so we know events are flowing
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          vname_x,
          "/marker.txt",
          "data"
        ])

      assert_eventually timeout: 10_000 do
        events = PeerCluster.rpc(cluster, :node2, EventCollector, :events, [collector_x])
        Enum.any?(events, fn env -> env.event.volume_id == vid_x end)
      end

      events = PeerCluster.rpc(cluster, :node2, EventCollector, :events, [collector_x])

      assert Enum.all?(events, fn env -> env.event.volume_id != vol_y.id end),
             "Collector for volume X should not receive events from volume Y"
    end

    test "subscriber on node2 receives events for writes on node1 and node3",
         %{cluster: cluster, volume_id: vid, volume_name: vname} do
      {:ok, collector} =
        PeerCluster.rpc(cluster, :node2, EventCollector, :start, [vid])

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          vname,
          "/from-node1.txt",
          "data1"
        ])

      {:ok, _} =
        PeerCluster.rpc(cluster, :node3, NeonFS.TestHelpers, :write_file_from_binary, [
          vname,
          "/from-node3.txt",
          "data3"
        ])

      assert_eventually timeout: 15_000 do
        events = PeerCluster.rpc(cluster, :node2, EventCollector, :events, [collector])
        node1_info = PeerCluster.get_node!(cluster, :node1)
        node3_info = PeerCluster.get_node!(cluster, :node3)

        has_node1 = Enum.any?(events, &(&1.source_node == node1_info.node))
        has_node3 = Enum.any?(events, &(&1.source_node == node3_info.node))
        has_node1 and has_node3
      end
    end
  end

  # ─── Unsubscribe ───────────────────────────────────────────────────

  describe "unsubscribe" do
    test "unsubscribe stops event delivery",
         %{cluster: cluster, volume_id: vid, volume_name: vname} do
      {:ok, collector} =
        PeerCluster.rpc(cluster, :node2, EventCollector, :start, [vid])

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          vname,
          "/before-unsub.txt",
          "data"
        ])

      assert_eventually timeout: 10_000 do
        PeerCluster.rpc(cluster, :node2, EventCollector, :count, [collector]) > 0
      end

      # Stop the collector (triggers process exit → automatic unsubscribe)
      PeerCluster.rpc(cluster, :node2, GenServer, :stop, [collector])

      # Wait for the Relay to detect the DOWN and leave :pg group
      assert_eventually timeout: 5_000 do
        members =
          PeerCluster.rpc(cluster, :node2, :pg, :get_members, [
            :neonfs_events,
            {:volume, vid}
          ])

        node2_relay =
          PeerCluster.rpc(cluster, :node2, Process, :whereis, [NeonFS.Events.Relay])

        node2_relay not in members
      end

      # Start a new collector to check no events leak
      {:ok, new_collector} =
        PeerCluster.rpc(cluster, :node2, EventCollector, :start, [vid])

      # Clear any events that might have been in flight
      PeerCluster.rpc(cluster, :node2, EventCollector, :clear, [new_collector])

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          vname,
          "/after-unsub.txt",
          "data"
        ])

      assert_eventually timeout: 10_000 do
        PeerCluster.rpc(cluster, :node2, EventCollector, :count, [new_collector]) > 0
      end

      # The original collector is stopped, so it can't receive anything
      # This test mainly verifies the system doesn't crash when a subscriber exits
      assert true
    end
  end

  # ─── Partition recovery ────────────────────────────────────────────

  describe "partition recovery" do
    test "subscriber caches are invalidated when a core node reconnects",
         %{cluster: cluster, volume_id: vid, volume_name: vname} do
      {:ok, collector} =
        PeerCluster.rpc(cluster, :node2, EventCollector, :start, [vid])

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          vname,
          "/pre-partition.txt",
          "data"
        ])

      assert_eventually timeout: 10_000 do
        PeerCluster.rpc(cluster, :node2, EventCollector, :count, [collector]) > 0
      end

      # Disconnect node1 from node2 (simulate network partition)
      node1_info = PeerCluster.get_node!(cluster, :node1)

      true =
        PeerCluster.rpc(cluster, :node2, :net_kernel, :disconnect, [node1_info.node])

      assert_eventually timeout: 10_000 do
        nodes = PeerCluster.rpc(cluster, :node2, Node, :list, [])
        node1_info.node not in nodes
      end

      # Reconnect node1 to node2
      true =
        PeerCluster.rpc(cluster, :node2, Node, :connect, [node1_info.node])

      assert_eventually timeout: 10_000 do
        nodes = PeerCluster.rpc(cluster, :node2, Node, :list, [])
        node1_info.node in nodes
      end

      # Wait for PartitionRecovery debounce (200ms in tests) + margin
      assert_eventually timeout: 10_000 do
        PeerCluster.rpc(cluster, :node2, EventCollector, :invalidated?, [collector])
      end

      # Restore full mesh and quorum rings for subsequent tests
      PeerCluster.connect_nodes(cluster)
      wait_for_full_mesh(cluster)
      rebuild_quorum_rings(cluster)
    end
  end

  # FUSE metadata cache invalidation moved to
  # `neonfs_fuse/test/integration/metadata_cache_invalidation_test.exs`
  # alongside the other FUSE integration tests (#600 / #582).
end
