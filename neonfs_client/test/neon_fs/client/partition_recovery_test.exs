defmodule NeonFS.Client.PartitionRecoveryTest do
  use ExUnit.Case, async: true

  alias NeonFS.Client.PartitionRecovery

  @moduletag :capture_log

  setup do
    # Start the event infrastructure required by PartitionRecovery
    pg_id = :"pg_pr_#{System.unique_integer([:positive])}"
    registry_name = :"events_registry_pr_#{System.unique_integer([:positive])}"
    server_name = :"partition_recovery_#{System.unique_integer([:positive])}"

    start_supervised!(%{
      id: pg_id,
      start: {:pg, :start_link, [:neonfs_events]}
    })

    start_supervised!({Registry, keys: :duplicate, name: NeonFS.Events.Registry})

    %{server_name: server_name, registry_name: registry_name}
  end

  defp start_recovery(ctx, opts) do
    base_opts = [name: ctx.server_name] ++ opts

    start_supervised!({PartitionRecovery, base_opts})
  end

  defp core_predicate(core_nodes) do
    fn node -> node in core_nodes end
  end

  describe "init/1" do
    test "starts monitoring nodes", ctx do
      pid = start_recovery(ctx, core_node?: core_predicate([]))
      assert is_pid(pid)
    end
  end

  describe "nodedown" do
    test "removes core node from known set", ctx do
      core = :core@test
      pid = start_recovery(ctx, core_node?: core_predicate([core]), debounce_ms: 60_000)

      # First, make the node known via nodeup
      send(pid, {:nodeup, core})
      # Allow GenServer to process
      _ = :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert MapSet.member?(state.known_core_nodes, core)

      # Now send nodedown
      send(pid, {:nodedown, core})
      _ = :sys.get_state(pid)

      state = :sys.get_state(pid)
      refute MapSet.member?(state.known_core_nodes, core)
    end

    test "non-core nodes are ignored", ctx do
      core = :core@test
      non_core = :fuse@test

      pid =
        start_recovery(ctx, core_node?: core_predicate([core]), debounce_ms: 60_000)

      # nodeup core, then nodedown non-core
      send(pid, {:nodeup, core})
      _ = :sys.get_state(pid)
      send(pid, {:nodedown, non_core})
      _ = :sys.get_state(pid)

      state = :sys.get_state(pid)
      # Core node should still be in known set
      assert MapSet.member?(state.known_core_nodes, core)
    end
  end

  describe "nodeup" do
    test "schedules debounced invalidation for core node", ctx do
      core = :core@test

      pid =
        start_recovery(ctx, core_node?: core_predicate([core]), debounce_ms: 60_000)

      send(pid, {:nodeup, core})
      _ = :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert MapSet.member?(state.known_core_nodes, core)
      assert MapSet.member?(state.pending_invalidations, core)
    end

    test "non-core nodes do not trigger invalidation", ctx do
      non_core = :fuse@test

      pid =
        start_recovery(ctx, core_node?: core_predicate([]), debounce_ms: 60_000)

      send(pid, {:nodeup, non_core})
      _ = :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert MapSet.size(state.pending_invalidations) == 0
      assert MapSet.size(state.known_core_nodes) == 0
    end

    test "already-known core node does not trigger invalidation", ctx do
      core = :core@test

      pid =
        start_recovery(ctx, core_node?: core_predicate([core]), debounce_ms: 60_000)

      # First nodeup
      send(pid, {:nodeup, core})
      _ = :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert MapSet.member?(state.pending_invalidations, core)

      # Clear pending by simulating invalidation
      send(pid, {:do_invalidate, core})
      _ = :sys.get_state(pid)

      # Second nodeup while still known — should not add to pending
      send(pid, {:nodeup, core})
      _ = :sys.get_state(pid)

      state = :sys.get_state(pid)
      assert MapSet.member?(state.known_core_nodes, core)
      refute MapSet.member?(state.pending_invalidations, core)
    end
  end

  describe "do_invalidate" do
    test "sends invalidation to registered subscribers", ctx do
      core = :core@test

      pid =
        start_recovery(ctx, core_node?: core_predicate([core]), debounce_ms: 60_000)

      # Register ourselves as a subscriber
      volume_id = "vol-#{System.unique_integer([:positive])}"
      Registry.register(NeonFS.Events.Registry, {:volume, volume_id}, [])

      # Simulate nodeup + manual invalidation trigger
      send(pid, {:nodeup, core})
      _ = :sys.get_state(pid)
      send(pid, {:do_invalidate, core})
      _ = :sys.get_state(pid)

      assert_received :neonfs_invalidate_all

      state = :sys.get_state(pid)
      refute MapSet.member?(state.pending_invalidations, core)
    end

    test "skips invalidation when node went down before debounce expired", ctx do
      core = :core@test

      pid =
        start_recovery(ctx, core_node?: core_predicate([core]), debounce_ms: 60_000)

      # Register subscriber
      volume_id = "vol-#{System.unique_integer([:positive])}"
      Registry.register(NeonFS.Events.Registry, {:volume, volume_id}, [])

      # nodeup then nodedown before debounce
      send(pid, {:nodeup, core})
      _ = :sys.get_state(pid)
      send(pid, {:nodedown, core})
      _ = :sys.get_state(pid)

      # Debounce fires — should be a no-op
      send(pid, {:do_invalidate, core})
      _ = :sys.get_state(pid)

      refute_received :neonfs_invalidate_all
    end

    test "only sends one invalidation per subscriber even with multiple volumes", ctx do
      core = :core@test

      pid =
        start_recovery(ctx, core_node?: core_predicate([core]), debounce_ms: 60_000)

      # Register under multiple volume keys
      Registry.register(NeonFS.Events.Registry, {:volume, "vol-a"}, [])
      Registry.register(NeonFS.Events.Registry, {:volume, "vol-b"}, [])
      Registry.register(NeonFS.Events.Registry, {:volumes}, [])

      send(pid, {:nodeup, core})
      _ = :sys.get_state(pid)
      send(pid, {:do_invalidate, core})
      _ = :sys.get_state(pid)

      # Should receive exactly one invalidation despite three registrations
      assert_received :neonfs_invalidate_all
      refute_received :neonfs_invalidate_all
    end
  end

  describe "rapid flapping" do
    test "nodedown + nodeup within debounce — only one invalidation", ctx do
      core = :core@test

      pid =
        start_recovery(ctx, core_node?: core_predicate([core]), debounce_ms: 60_000)

      volume_id = "vol-#{System.unique_integer([:positive])}"
      Registry.register(NeonFS.Events.Registry, {:volume, volume_id}, [])

      # nodeup -> pending
      send(pid, {:nodeup, core})
      _ = :sys.get_state(pid)

      # nodedown -> clears from known + pending
      send(pid, {:nodedown, core})
      _ = :sys.get_state(pid)

      # First debounce fires — should be skipped (node is down)
      send(pid, {:do_invalidate, core})
      _ = :sys.get_state(pid)
      refute_received :neonfs_invalidate_all

      # nodeup again -> new pending
      send(pid, {:nodeup, core})
      _ = :sys.get_state(pid)

      # Second debounce fires — should invalidate
      send(pid, {:do_invalidate, core})
      _ = :sys.get_state(pid)
      assert_received :neonfs_invalidate_all
    end
  end

  describe "debounce configuration" do
    test "uses configured debounce_ms", ctx do
      core = :core@test

      # Use a very short debounce
      pid =
        start_recovery(ctx, core_node?: core_predicate([core]), debounce_ms: 50)

      volume_id = "vol-#{System.unique_integer([:positive])}"
      Registry.register(NeonFS.Events.Registry, {:volume, volume_id}, [])

      send(pid, {:nodeup, core})
      _ = :sys.get_state(pid)

      # Wait for the debounce timer to fire
      Process.sleep(100)

      assert_received :neonfs_invalidate_all
    end
  end
end
