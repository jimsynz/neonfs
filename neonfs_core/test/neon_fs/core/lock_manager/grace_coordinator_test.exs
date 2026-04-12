defmodule NeonFS.Core.LockManager.GraceCoordinatorTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.LockManager.GraceCoordinator
  alias NeonFS.Core.MetadataRing

  @grace_duration_ms 200

  setup do
    # GraceCoordinator creates a named ETS table, so we need to ensure
    # only one instance runs at a time.
    pid =
      start_supervised!({GraceCoordinator, grace_duration_ms: @grace_duration_ms})

    %{pid: pid}
  end

  describe "in_grace?/1" do
    test "returns false when no grace period is active" do
      refute GraceCoordinator.in_grace?("some-file-id")
    end

    test "returns true for files mastered by a departed node", %{pid: pid} do
      # Build a ring with a fake node to simulate departure
      fake_node = :departed@host
      ring_with_fake = build_ring([Node.self(), fake_node])

      # Find a file_id that is mastered by the fake node
      file_id = find_file_mastered_by(ring_with_fake, fake_node)

      # Simulate the internal state as if the fake node was in the ring
      # We do this by sending a raw :nodedown (but the node needs to be
      # in the coordinator's ring). Instead, inject state directly.
      inject_grace_period(pid, fake_node, ring_with_fake)

      assert GraceCoordinator.in_grace?(file_id)
    end

    test "returns false for files NOT mastered by the departed node", %{pid: pid} do
      fake_node = :departed@host
      ring_with_fake = build_ring([Node.self(), fake_node])

      # Find a file mastered by the LOCAL node
      file_id = find_file_mastered_by(ring_with_fake, Node.self())

      inject_grace_period(pid, fake_node, ring_with_fake)

      refute GraceCoordinator.in_grace?(file_id)
    end

    test "returns false after grace period expires", %{pid: pid} do
      fake_node = :departed@host
      ring_with_fake = build_ring([Node.self(), fake_node])
      file_id = find_file_mastered_by(ring_with_fake, fake_node)

      inject_grace_period(pid, fake_node, ring_with_fake)

      assert GraceCoordinator.in_grace?(file_id)

      # Wait for grace period to expire and be purged
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :lock_manager, :grace, :ended]
        ])

      assert_receive {[:neonfs, :lock_manager, :grace, :ended], ^ref, %{},
                      %{departed_node: ^fake_node}},
                     @grace_duration_ms + 500

      refute GraceCoordinator.in_grace?(file_id)
    end
  end

  describe "grace_status/0" do
    test "reports no active grace periods initially" do
      status = GraceCoordinator.grace_status()
      assert status.active_grace_periods == 0
      assert status.departed_nodes == []
      assert status.grace_duration_ms == @grace_duration_ms
    end

    test "reports active grace periods after node departure", %{pid: pid} do
      fake_node = :departed@host
      ring_with_fake = build_ring([Node.self(), fake_node])

      inject_grace_period(pid, fake_node, ring_with_fake)

      status = GraceCoordinator.grace_status()
      assert status.active_grace_periods == 1
      assert status.departed_nodes == [fake_node]
    end
  end

  describe "telemetry" do
    test "emits :started event on grace period creation", %{pid: pid} do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :lock_manager, :grace, :started]
        ])

      fake_node = :departed@host
      ring_with_fake = build_ring([Node.self(), fake_node])

      inject_grace_period(pid, fake_node, ring_with_fake)

      assert_receive {[:neonfs, :lock_manager, :grace, :started], ^ref,
                      %{duration_ms: @grace_duration_ms}, %{departed_node: ^fake_node}},
                     1_000
    end

    test "emits :ended event when grace period expires", %{pid: pid} do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :lock_manager, :grace, :ended]
        ])

      fake_node = :departed@host
      ring_with_fake = build_ring([Node.self(), fake_node])

      inject_grace_period(pid, fake_node, ring_with_fake)

      assert_receive {[:neonfs, :lock_manager, :grace, :ended], ^ref, %{},
                      %{departed_node: ^fake_node}},
                     @grace_duration_ms + 500
    end
  end

  describe "multiple node departures" do
    test "tracks grace periods for multiple departed nodes", %{pid: pid} do
      node_a = :departed_a@host
      node_b = :departed_b@host
      ring = build_ring([Node.self(), node_a, node_b])

      file_a = find_file_mastered_by(ring, node_a)
      file_b = find_file_mastered_by(ring, node_b)

      inject_grace_period(pid, node_a, ring)

      # After node_a departs, build the ring without it for node_b's departure
      ring_without_a = build_ring([Node.self(), node_b])
      inject_grace_period(pid, node_b, ring_without_a)

      assert GraceCoordinator.in_grace?(file_a)
      assert GraceCoordinator.in_grace?(file_b)

      status = GraceCoordinator.grace_status()
      assert status.active_grace_periods == 2
    end
  end

  ## Helpers

  defp build_ring(nodes) do
    MetadataRing.new(nodes, virtual_nodes_per_physical: 64, replicas: 1)
  end

  defp find_file_mastered_by(ring, target_node) do
    # Try file IDs until we find one mastered by the target node
    Enum.find_value(0..999, fn i ->
      file_id = "file-#{i}"

      case MetadataRing.locate(ring, file_id) do
        {_segment, [^target_node | _]} -> file_id
        _ -> nil
      end
    end) || raise "could not find file mastered by #{target_node}"
  end

  defp inject_grace_period(pid, departed_node, ring) do
    # Simulate a grace period by sending the coordinator a message that
    # triggers the same internal logic as a :nodedown event.
    # We do this by updating the coordinator's ring to include the departed
    # node, then sending :nodedown.
    :sys.replace_state(pid, fn state ->
      %{state | ring: ring}
    end)

    send(pid, {:nodedown, departed_node})
    :sys.get_state(pid)
  end
end
