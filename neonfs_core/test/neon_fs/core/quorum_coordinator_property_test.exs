defmodule NeonFS.Core.QuorumCoordinatorPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NeonFS.Core.HLC
  alias NeonFS.Core.MetadataRing
  alias NeonFS.Core.QuorumCoordinator

  @vnodes_per_physical 4

  # Generators

  # Constructs valid quorum configs directly (no filter needed).
  # Given n, pick r in [1, n] then w in [n - r + 1, n] so r + w > n always holds.
  defp quorum_config do
    gen all(
          n <- StreamData.integer(1..7),
          r <- StreamData.integer(1..7),
          w_offset <- StreamData.non_negative_integer()
        ) do
      r = min(r, n)
      w_min = n - r + 1
      w = w_min + rem(w_offset, n - w_min + 1)
      {n, r, w}
    end
  end

  # Constructs a list of exactly `fail_count` indices to fail out of `n` nodes.
  # Uses a subset selection approach instead of filter.
  defp failure_indices(n, max_failures) do
    gen all(count <- StreamData.integer(0..max(max_failures, 0))) do
      if count == 0 or n == 0 do
        MapSet.new()
      else
        0..(n - 1)
        |> Enum.to_list()
        |> Enum.shuffle()
        |> Enum.take(count)
        |> MapSet.new()
      end
    end
  end

  defp node_list(n) do
    Enum.map(1..n, fn i -> :"node_#{i}@host" end)
  end

  defp build_ring_and_opts(nodes, n) do
    ring =
      MetadataRing.new(nodes,
        virtual_nodes_per_physical: @vnodes_per_physical,
        replicas: n
      )

    base_opts = [
      ring: ring,
      quarantine_checker: fn _node -> false end,
      local_node: List.first(nodes),
      timeout: 2_000
    ]

    {ring, base_opts}
  end

  defp failed_node_set(nodes, failed_indices) do
    nodes
    |> Enum.with_index()
    |> Enum.filter(fn {_node, idx} -> MapSet.member?(failed_indices, idx) end)
    |> Enum.map(fn {node, _idx} -> node end)
    |> MapSet.new()
  end

  describe "read-after-write consistency" do
    property "reads always see the latest write when r + w > n" do
      check all(
              {n, r, w} <- quorum_config(),
              key <- StreamData.string(:alphanumeric, min_length: 1, max_length: 20),
              value <- StreamData.integer(1..1000)
            ) do
        nodes = node_list(n)
        {_ring, base_opts} = build_ring_and_opts(nodes, n)

        # In-memory store per node
        store = :ets.new(:test_store, [:set, :public])

        timestamp = {System.system_time(:millisecond), 0, List.first(nodes)}

        write_fn = fn node, _segment_id, k, v ->
          :ets.insert(store, {{node, k}, {v, timestamp}})
          :ok
        end

        read_fn = fn node, _segment_id, k ->
          case :ets.lookup(store, {node, k}) do
            [{{^node, ^k}, {v, ts}}] -> {:ok, v, ts}
            [] -> {:error, :not_found}
          end
        end

        read_repair_fn = fn _work_fn, _opts -> {:ok, "mock"} end

        opts =
          Keyword.merge(base_opts,
            write_fn: write_fn,
            read_fn: read_fn,
            read_repair_fn: read_repair_fn,
            write_quorum: w,
            read_quorum: r,
            replicas: n
          )

        # Write the value
        case QuorumCoordinator.quorum_write("key:#{key}", value, opts) do
          {:ok, :written} ->
            # Read should see the written value
            case QuorumCoordinator.quorum_read("key:#{key}", opts) do
              {:ok, ^value} ->
                :ok

              {:ok, ^value, :possibly_stale} ->
                :ok

              other ->
                flunk("Expected to read #{value}, got: #{inspect(other)}")
            end

          {:error, :quorum_unavailable} ->
            # Write didn't meet quorum — read not guaranteed
            :ok
        end

        :ets.delete(store)
      end
    end
  end

  describe "concurrent write determinism" do
    property "concurrent writes with different HLC timestamps produce a deterministic winner" do
      check all(
              n <- StreamData.integer(3..5),
              wall_a <- StreamData.integer(1_000..5_000),
              wall_b <- StreamData.integer(1_000..5_000),
              counter_a <- StreamData.integer(0..100),
              counter_b <- StreamData.integer(0..100)
            ) do
        ts_a = {wall_a, counter_a, :writer_a@host}
        ts_b = {wall_b, counter_b, :writer_b@host}

        # Skip equal timestamps — no deterministic winner needed
        if HLC.compare(ts_a, ts_b) != :eq do
          expected_winner =
            case HLC.compare(ts_a, ts_b) do
              :gt -> :value_a
              :lt -> :value_b
            end

          nodes = node_list(n)
          {_ring, base_opts} = build_ring_and_opts(nodes, n)

          # Simulate: some nodes have value_a (ts_a), others have value_b (ts_b)
          # A quorum read should pick the one with the higher timestamp
          store = :ets.new(:test_store, [:set, :public])

          # Write value_a to all nodes first
          Enum.each(nodes, fn node ->
            :ets.insert(store, {{node, "key"}, {:value_a, ts_a}})
          end)

          # Overwrite half the nodes with value_b (simulating a concurrent write
          # that reached some but not all replicas)
          nodes
          |> Enum.take(div(n, 2) + 1)
          |> Enum.each(fn node ->
            :ets.insert(store, {{node, "key"}, {:value_b, ts_b}})
          end)

          read_fn = fn node, _segment_id, _key ->
            case :ets.lookup(store, {node, "key"}) do
              [{{^node, "key"}, {v, ts}}] -> {:ok, v, ts}
              [] -> {:error, :not_found}
            end
          end

          read_repair_fn = fn _work_fn, _opts -> {:ok, "mock"} end

          opts =
            Keyword.merge(base_opts,
              read_fn: read_fn,
              read_repair_fn: read_repair_fn,
              replicas: n,
              read_quorum: div(n, 2) + 1,
              write_quorum: div(n, 2) + 1
            )

          case QuorumCoordinator.quorum_read("key", opts) do
            {:ok, winner} ->
              assert winner == expected_winner,
                     "Expected #{inspect(expected_winner)} (ts: #{inspect(if expected_winner == :value_a, do: ts_a, else: ts_b)}), " <>
                       "got #{inspect(winner)}"

            {:ok, winner, :possibly_stale} ->
              # Degraded reads may return any available value
              assert winner in [:value_a, :value_b]

            {:error, _} ->
              :ok
          end

          :ets.delete(store)
        end
      end
    end
  end

  describe "partial failure tolerance — writes" do
    property "writes succeed when at most n - w nodes fail" do
      check all(
              {n, _r, w} <- quorum_config(),
              failed_indices <- failure_indices(n, n - w)
            ) do
        nodes = node_list(n)
        {_ring, base_opts} = build_ring_and_opts(nodes, n)

        failed_nodes = failed_node_set(nodes, failed_indices)

        write_fn = fn node, _segment_id, _key, _value ->
          if MapSet.member?(failed_nodes, node) do
            {:error, :timeout}
          else
            :ok
          end
        end

        opts =
          Keyword.merge(base_opts,
            write_fn: write_fn,
            replicas: n,
            write_quorum: w
          )

        result = QuorumCoordinator.quorum_write("key:test", "value", opts)

        assert {:ok, :written} = result,
               "Write should succeed with #{MapSet.size(failed_nodes)} failures " <>
                 "(n=#{n}, w=#{w})"
      end
    end
  end

  describe "partial failure tolerance — reads" do
    property "reads succeed when at most n - r nodes fail (and data exists)" do
      check all(
              {n, r, w} <- quorum_config(),
              failed_indices <- failure_indices(n, n - r)
            ) do
        nodes = node_list(n)
        {_ring, base_opts} = build_ring_and_opts(nodes, n)

        failed_nodes = failed_node_set(nodes, failed_indices)
        timestamp = {1_000_000, 0, List.first(nodes)}

        read_fn = fn node, _segment_id, _key ->
          if MapSet.member?(failed_nodes, node) do
            {:error, :timeout}
          else
            {:ok, "the_value", timestamp}
          end
        end

        read_repair_fn = fn _work_fn, _opts -> {:ok, "mock"} end

        opts =
          Keyword.merge(base_opts,
            read_fn: read_fn,
            read_repair_fn: read_repair_fn,
            replicas: n,
            read_quorum: r,
            write_quorum: w,
            degraded_reads: false
          )

        result = QuorumCoordinator.quorum_read("key:test", opts)

        assert {:ok, "the_value"} = result,
               "Read should succeed with #{MapSet.size(failed_nodes)} failures " <>
                 "(n=#{n}, r=#{r})"
      end
    end
  end
end
