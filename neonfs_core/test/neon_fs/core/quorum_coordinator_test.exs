defmodule NeonFS.Core.QuorumCoordinatorTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.MetadataRing
  alias NeonFS.Core.QuorumCoordinator

  # Use small virtual node count for predictable test routing
  @vnodes_per_physical 4

  setup do
    ring =
      MetadataRing.new([:node_a, :node_b, :node_c],
        virtual_nodes_per_physical: @vnodes_per_physical,
        replicas: 3
      )

    base_opts = [
      ring: ring,
      quarantine_checker: fn _node -> false end,
      local_node: :node_a,
      timeout: 1_000
    ]

    %{ring: ring, base_opts: base_opts}
  end

  describe "quorum_write/3" do
    test "succeeds when all replicas respond", ctx do
      write_fn = fn _node, _segment_id, _key, _value -> :ok end

      assert {:ok, :written} =
               QuorumCoordinator.quorum_write(
                 "chunk:abc123",
                 %{data: "test"},
                 ctx.base_opts ++ [write_fn: write_fn]
               )
    end

    test "succeeds when one replica fails but quorum is met", ctx do
      write_fn = fn
        :node_c, _segment_id, _key, _value -> {:error, :timeout}
        _node, _segment_id, _key, _value -> :ok
      end

      assert {:ok, :written} =
               QuorumCoordinator.quorum_write(
                 "chunk:abc123",
                 %{data: "test"},
                 ctx.base_opts ++ [write_fn: write_fn]
               )
    end

    test "fails when too many replicas fail", ctx do
      write_fn = fn
        :node_a, _segment_id, _key, _value -> :ok
        _node, _segment_id, _key, _value -> {:error, :timeout}
      end

      assert {:error, :quorum_unavailable} =
               QuorumCoordinator.quorum_write(
                 "chunk:abc123",
                 %{data: "test"},
                 ctx.base_opts ++ [write_fn: write_fn]
               )
    end

    test "rejects writes from quarantined nodes", ctx do
      write_fn = fn _node, _segment_id, _key, _value -> :ok end
      quarantine_checker = fn _node -> true end

      assert {:error, :node_quarantined} =
               QuorumCoordinator.quorum_write(
                 "chunk:abc123",
                 %{data: "test"},
                 Keyword.merge(ctx.base_opts,
                   write_fn: write_fn,
                   quarantine_checker: quarantine_checker
                 )
               )
    end

    test "emits telemetry event on write", ctx do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :quorum, :write]
        ])

      write_fn = fn _node, _segment_id, _key, _value -> :ok end

      QuorumCoordinator.quorum_write(
        "chunk:abc123",
        %{data: "test"},
        ctx.base_opts ++ [write_fn: write_fn]
      )

      assert_receive {[:neonfs, :quorum, :write], ^ref, %{latency_ms: _},
                      %{segment_id: _, quorum_size: _}}
    end
  end

  describe "quorum_read/2" do
    test "returns latest value with consistent responses", ctx do
      timestamp = {1_000_000, 0, :node_a}

      read_fn = fn _node, _segment_id, _key ->
        {:ok, %{"name" => "test.txt"}, timestamp}
      end

      assert {:ok, %{"name" => "test.txt"}} =
               QuorumCoordinator.quorum_read(
                 "file:test.txt",
                 ctx.base_opts ++ [read_fn: read_fn]
               )
    end

    test "returns latest value when replicas have different timestamps", ctx do
      old_ts = {1_000_000, 0, :node_a}
      new_ts = {1_000_001, 0, :node_b}

      read_fn = fn
        :node_a, _segment_id, _key -> {:ok, %{"version" => 1}, old_ts}
        :node_b, _segment_id, _key -> {:ok, %{"version" => 2}, new_ts}
        :node_c, _segment_id, _key -> {:ok, %{"version" => 2}, new_ts}
      end

      # Stale node_a will trigger read repair, so we need a mock
      read_repair_fn = fn _work_fn, _work_opts -> {:ok, "mock_id"} end

      assert {:ok, %{"version" => 2}} =
               QuorumCoordinator.quorum_read(
                 "file:test.txt",
                 ctx.base_opts ++ [read_fn: read_fn, read_repair_fn: read_repair_fn]
               )
    end

    test "detects stale replicas and triggers read repair", ctx do
      old_ts = {1_000_000, 0, :node_a}
      new_ts = {1_000_001, 0, :node_b}

      read_fn = fn
        :node_a, _segment_id, _key -> {:ok, %{"version" => 1}, old_ts}
        :node_b, _segment_id, _key -> {:ok, %{"version" => 2}, new_ts}
        :node_c, _segment_id, _key -> {:ok, %{"version" => 2}, new_ts}
      end

      test_pid = self()

      read_repair_fn = fn work_fn, work_opts ->
        send(test_pid, {:read_repair, work_fn, work_opts})
        {:ok, "mock_work_id"}
      end

      stale_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :quorum, :stale_detected]
        ])

      QuorumCoordinator.quorum_read(
        "file:test.txt",
        ctx.base_opts ++ [read_fn: read_fn, read_repair_fn: read_repair_fn]
      )

      assert_receive {:read_repair, _work_fn, [priority: :high, label: "read_repair"]}

      assert_receive {[:neonfs, :quorum, :stale_detected], ^stale_ref, %{stale_count: 1},
                      %{segment_id: _, key: "file:test.txt"}}
    end

    test "returns not_found when majority report not_found", ctx do
      read_fn = fn _node, _segment_id, _key ->
        {:error, :not_found}
      end

      assert {:error, :not_found} =
               QuorumCoordinator.quorum_read(
                 "file:missing.txt",
                 ctx.base_opts ++ [read_fn: read_fn]
               )
    end

    test "returns not_found when quorum of replicas report not_found with some errors", ctx do
      read_fn = fn
        :node_a, _segment_id, _key -> {:error, :not_found}
        :node_b, _segment_id, _key -> {:error, :not_found}
        :node_c, _segment_id, _key -> {:error, :connection_refused}
      end

      assert {:error, :not_found} =
               QuorumCoordinator.quorum_read(
                 "file:missing.txt",
                 ctx.base_opts ++ [read_fn: read_fn]
               )
    end

    test "degraded read returns {:ok, value, :possibly_stale} when local replica exists", ctx do
      timestamp = {1_000_000, 0, :node_a}

      read_fn = fn
        :node_a, _segment_id, _key -> {:ok, %{"data" => "local"}, timestamp}
        _node, _segment_id, _key -> {:error, :connection_refused}
      end

      assert {:ok, %{"data" => "local"}, :possibly_stale} =
               QuorumCoordinator.quorum_read(
                 "file:test.txt",
                 ctx.base_opts ++ [read_fn: read_fn, degraded_reads: true]
               )
    end

    test "degraded read returns :quorum_unavailable when no local replica", ctx do
      read_fn = fn _node, _segment_id, _key ->
        {:error, :connection_refused}
      end

      assert {:error, :quorum_unavailable} =
               QuorumCoordinator.quorum_read(
                 "file:test.txt",
                 ctx.base_opts ++ [read_fn: read_fn, degraded_reads: true]
               )
    end

    test "degraded read disabled returns :quorum_unavailable even with local replica", ctx do
      timestamp = {1_000_000, 0, :node_a}

      read_fn = fn
        :node_a, _segment_id, _key -> {:ok, %{"data" => "local"}, timestamp}
        _node, _segment_id, _key -> {:error, :connection_refused}
      end

      assert {:error, :quorum_unavailable} =
               QuorumCoordinator.quorum_read(
                 "file:test.txt",
                 ctx.base_opts ++ [read_fn: read_fn, degraded_reads: false]
               )
    end

    test "emits telemetry event on read", ctx do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :quorum, :read]
        ])

      read_fn = fn _node, _segment_id, _key ->
        {:ok, %{"data" => "test"}, {1_000_000, 0, :node_a}}
      end

      QuorumCoordinator.quorum_read(
        "file:test.txt",
        ctx.base_opts ++ [read_fn: read_fn]
      )

      assert_receive {[:neonfs, :quorum, :read], ^ref, %{latency_ms: _},
                      %{segment_id: _, quorum_size: _}}
    end
  end

  describe "quorum_delete/2" do
    test "succeeds as tombstone write via quorum", ctx do
      delete_fn = fn _node, _segment_id, _key -> :ok end

      assert {:ok, :written} =
               QuorumCoordinator.quorum_delete(
                 "file:old.txt",
                 ctx.base_opts ++ [delete_fn: delete_fn]
               )
    end

    test "fails when quorum unavailable", ctx do
      delete_fn = fn
        :node_a, _segment_id, _key -> :ok
        _node, _segment_id, _key -> {:error, :timeout}
      end

      assert {:error, :quorum_unavailable} =
               QuorumCoordinator.quorum_delete(
                 "file:old.txt",
                 ctx.base_opts ++ [delete_fn: delete_fn]
               )
    end

    test "rejects deletes from quarantined nodes", ctx do
      delete_fn = fn _node, _segment_id, _key -> :ok end
      quarantine_checker = fn _node -> true end

      assert {:error, :node_quarantined} =
               QuorumCoordinator.quorum_delete(
                 "file:old.txt",
                 Keyword.merge(ctx.base_opts,
                   delete_fn: delete_fn,
                   quarantine_checker: quarantine_checker
                 )
               )
    end
  end

  describe "small cluster graceful fallback" do
    test "adjusts quorum for single-node cluster", ctx do
      ring =
        MetadataRing.new([:node_a],
          virtual_nodes_per_physical: @vnodes_per_physical,
          replicas: 1
        )

      write_fn = fn :node_a, _segment_id, _key, _value -> :ok end

      # With 1 node, effective N=1, W=min(2,1)=1, so single ack suffices
      assert {:ok, :written} =
               QuorumCoordinator.quorum_write(
                 "chunk:abc",
                 %{data: "test"},
                 Keyword.merge(ctx.base_opts, ring: ring, write_fn: write_fn)
               )
    end

    test "adjusts quorum for two-node cluster", ctx do
      ring =
        MetadataRing.new([:node_a, :node_b],
          virtual_nodes_per_physical: @vnodes_per_physical,
          replicas: 2
        )

      write_fn = fn _node, _segment_id, _key, _value -> :ok end

      assert {:ok, :written} =
               QuorumCoordinator.quorum_write(
                 "chunk:abc",
                 %{data: "test"},
                 Keyword.merge(ctx.base_opts, ring: ring, write_fn: write_fn)
               )
    end

    test "single-node read works with adjusted quorum", ctx do
      ring =
        MetadataRing.new([:node_a],
          virtual_nodes_per_physical: @vnodes_per_physical,
          replicas: 1
        )

      timestamp = {1_000_000, 0, :node_a}

      read_fn = fn :node_a, _segment_id, _key ->
        {:ok, %{"data" => "solo"}, timestamp}
      end

      assert {:ok, %{"data" => "solo"}} =
               QuorumCoordinator.quorum_read(
                 "file:test.txt",
                 Keyword.merge(ctx.base_opts, ring: ring, read_fn: read_fn)
               )
    end
  end

  describe "read repair" do
    test "does not trigger read repair when all replicas are consistent", ctx do
      timestamp = {1_000_000, 0, :node_a}

      read_fn = fn _node, _segment_id, _key ->
        {:ok, %{"data" => "consistent"}, timestamp}
      end

      test_pid = self()

      read_repair_fn = fn _work_fn, _work_opts ->
        send(test_pid, :unexpected_read_repair)
        {:ok, "mock_id"}
      end

      QuorumCoordinator.quorum_read(
        "file:test.txt",
        ctx.base_opts ++ [read_fn: read_fn, read_repair_fn: read_repair_fn]
      )

      refute_receive :unexpected_read_repair, 100
    end

    test "triggers repair for each stale replica", ctx do
      old_ts = {999_000, 0, :node_a}
      new_ts = {1_000_000, 0, :node_b}

      read_fn = fn
        :node_a, _segment_id, _key -> {:ok, %{"v" => 1}, old_ts}
        :node_b, _segment_id, _key -> {:ok, %{"v" => 2}, new_ts}
        :node_c, _segment_id, _key -> {:ok, %{"v" => 1}, old_ts}
      end

      test_pid = self()

      read_repair_fn = fn _work_fn, work_opts ->
        send(test_pid, {:repair, work_opts})
        {:ok, "mock_id"}
      end

      QuorumCoordinator.quorum_read(
        "file:test.txt",
        ctx.base_opts ++ [read_fn: read_fn, read_repair_fn: read_repair_fn]
      )

      # Should trigger repair for 2 stale replicas (node_a and node_c)
      assert_receive {:repair, [priority: :high, label: "read_repair"]}
      assert_receive {:repair, [priority: :high, label: "read_repair"]}
      refute_receive {:repair, _}, 100
    end

    test "no read repair when R=1 (only one response)", ctx do
      ring =
        MetadataRing.new([:node_a],
          virtual_nodes_per_physical: @vnodes_per_physical,
          replicas: 1
        )

      timestamp = {1_000_000, 0, :node_a}

      read_fn = fn :node_a, _segment_id, _key ->
        {:ok, %{"data" => "sole"}, timestamp}
      end

      test_pid = self()

      read_repair_fn = fn _work_fn, _work_opts ->
        send(test_pid, :unexpected_read_repair)
        {:ok, "mock_id"}
      end

      QuorumCoordinator.quorum_read(
        "file:test.txt",
        Keyword.merge(ctx.base_opts,
          ring: ring,
          read_fn: read_fn,
          read_repair_fn: read_repair_fn
        )
      )

      refute_receive :unexpected_read_repair, 100
    end
  end

  describe "quarantine" do
    test "quarantine check uses injected checker", ctx do
      # Only quarantine node_a (our local node)
      quarantine_checker = fn
        :node_a -> true
        _other -> false
      end

      write_fn = fn _node, _segment_id, _key, _value -> :ok end

      assert {:error, :node_quarantined} =
               QuorumCoordinator.quorum_write(
                 "chunk:abc",
                 %{data: "test"},
                 Keyword.merge(ctx.base_opts,
                   write_fn: write_fn,
                   quarantine_checker: quarantine_checker
                 )
               )
    end

    test "non-quarantined node can write", ctx do
      write_fn = fn _node, _segment_id, _key, _value -> :ok end

      assert {:ok, :written} =
               QuorumCoordinator.quorum_write(
                 "chunk:abc",
                 %{data: "test"},
                 ctx.base_opts ++ [write_fn: write_fn]
               )
    end
  end
end
