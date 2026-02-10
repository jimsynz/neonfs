defmodule NeonFS.Core.MetadataRingTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NeonFS.Core.MetadataRing

  @nodes [:core@node1, :core@node2, :core@node3]

  describe "new/2" do
    test "builds ring from node list with default options" do
      ring = MetadataRing.new(@nodes)

      assert MetadataRing.segment_count(ring) == 3 * 64
      assert length(MetadataRing.nodes(ring)) == 3
    end

    test "builds empty ring from empty list" do
      ring = MetadataRing.new([])

      assert MetadataRing.segment_count(ring) == 0
      assert MetadataRing.nodes(ring) == []
      assert MetadataRing.segments(ring) == []
    end

    test "accepts custom virtual_nodes_per_physical" do
      ring = MetadataRing.new(@nodes, virtual_nodes_per_physical: 10)

      assert MetadataRing.segment_count(ring) == 3 * 10
    end

    test "accepts custom replicas" do
      ring = MetadataRing.new(@nodes, replicas: 2)
      {_seg, replicas} = MetadataRing.locate(ring, "key")

      assert length(replicas) == 2
    end
  end

  describe "locate/2" do
    test "returns valid segment and replica set for 3-node ring" do
      ring = MetadataRing.new(@nodes)
      {segment_id, replica_set} = MetadataRing.locate(ring, "some_key")

      assert byte_size(segment_id) == 32
      assert length(replica_set) == 3
      assert Enum.all?(replica_set, &(&1 in @nodes))
      assert replica_set == Enum.uniq(replica_set)
    end

    test "returns single node for single-node ring" do
      ring = MetadataRing.new([:core@node1])
      {segment_id, replicas} = MetadataRing.locate(ring, "any_key")

      assert byte_size(segment_id) == 32
      assert replicas == [:core@node1]
    end

    test "returns empty for empty ring" do
      ring = MetadataRing.new([])

      assert MetadataRing.locate(ring, "key") == {<<>>, []}
    end

    test "caps replicas at cluster size" do
      ring = MetadataRing.new([:core@node1, :core@node2], replicas: 5)
      {_seg, replicas} = MetadataRing.locate(ring, "key")

      assert length(replicas) == 2
    end

    test "replica set contains only distinct nodes" do
      ring = MetadataRing.new(@nodes)

      for i <- 1..100 do
        {_seg, replicas} = MetadataRing.locate(ring, "key_#{i}")
        assert replicas == Enum.uniq(replicas)
      end
    end
  end

  describe "determinism" do
    test "same inputs produce same ring" do
      ring1 = MetadataRing.new(@nodes)
      ring2 = MetadataRing.new(@nodes)

      assert ring1.sorted_ring == ring2.sorted_ring
    end

    test "same key always maps to same segment and replicas" do
      ring = MetadataRing.new(@nodes)

      result1 = MetadataRing.locate(ring, "test_key")
      result2 = MetadataRing.locate(ring, "test_key")

      assert result1 == result2
    end

    test "building ring twice produces identical segments" do
      ring1 = MetadataRing.new(@nodes)
      ring2 = MetadataRing.new(@nodes)

      assert MetadataRing.segments(ring1) == MetadataRing.segments(ring2)
    end
  end

  describe "add_node/2" do
    test "adds node and returns affected segments" do
      # Use 10 nodes so the affected ratio is clearly bounded
      nodes = for i <- 1..10, do: :"core@node#{i}"
      ring = MetadataRing.new(nodes)
      {new_ring, affected} = MetadataRing.add_node(ring, :core@node11)

      assert :core@node11 in MetadataRing.nodes(new_ring)
      assert MetadataRing.segment_count(new_ring) == 11 * 64

      # With replicas=3, approximately replicas/N segments are affected
      # For 11 nodes: ~3/11 ≈ 27%, allow generous bounds
      total = MetadataRing.segment_count(new_ring)
      ratio = length(affected) / total
      assert ratio > 0.05, "expected at least 5% affected, got #{Float.round(ratio * 100, 1)}%"
      assert ratio < 0.55, "expected less than 55% affected, got #{Float.round(ratio * 100, 1)}%"
    end

    test "no-op when node already exists" do
      ring = MetadataRing.new(@nodes)
      {same_ring, affected} = MetadataRing.add_node(ring, :core@node1)

      assert same_ring == ring
      assert affected == []
    end

    test "affected segments are valid segment IDs" do
      ring = MetadataRing.new(@nodes)
      {new_ring, affected} = MetadataRing.add_node(ring, :core@node4)

      all_segment_ids = new_ring.sorted_ring |> Enum.map(&elem(&1, 0))

      Enum.each(affected, fn seg_id ->
        assert seg_id in all_segment_ids
      end)
    end
  end

  describe "remove_node/2" do
    test "removes node and returns affected segments" do
      nodes = @nodes ++ [:core@node4, :core@node5]
      ring = MetadataRing.new(nodes)
      {new_ring, affected} = MetadataRing.remove_node(ring, :core@node5)

      refute :core@node5 in MetadataRing.nodes(new_ring)
      assert MetadataRing.segment_count(new_ring) == 4 * 64
      assert affected != []
    end

    test "no-op when node not in ring" do
      ring = MetadataRing.new(@nodes)
      {same_ring, affected} = MetadataRing.remove_node(ring, :core@node999)

      assert same_ring == ring
      assert affected == []
    end

    test "affected segments are valid segment IDs in new ring" do
      nodes = @nodes ++ [:core@node4]
      ring = MetadataRing.new(nodes)
      {new_ring, affected} = MetadataRing.remove_node(ring, :core@node4)

      all_segment_ids = new_ring.sorted_ring |> Enum.map(&elem(&1, 0))

      Enum.each(affected, fn seg_id ->
        assert seg_id in all_segment_ids
      end)
    end
  end

  describe "segments/1" do
    test "returns all segments with replica sets" do
      ring = MetadataRing.new(@nodes)
      segments = MetadataRing.segments(ring)

      assert length(segments) == 3 * 64

      Enum.each(segments, fn {segment_id, replica_set} ->
        assert byte_size(segment_id) == 32
        assert length(replica_set) == 3
        assert replica_set == Enum.uniq(replica_set)
        assert Enum.all?(replica_set, &(&1 in @nodes))
      end)
    end
  end

  describe "nodes/1" do
    test "returns sorted list of nodes" do
      ring = MetadataRing.new(@nodes)
      nodes = MetadataRing.nodes(ring)

      assert length(nodes) == 3
      assert nodes == Enum.sort(nodes)
    end
  end

  describe "distribution evenness" do
    test "keys are distributed evenly across nodes" do
      nodes = for i <- 1..5, do: :"core@node#{i}"
      ring = MetadataRing.new(nodes)

      # Hash 10,000 random keys and count per-node distribution
      counts =
        for i <- 1..10_000, reduce: %{} do
          acc ->
            {_seg, [primary | _]} = MetadataRing.locate(ring, "key_#{i}")
            Map.update(acc, primary, 1, &(&1 + 1))
        end

      values = Map.values(counts)
      mean = Enum.sum(values) / length(values)

      # No node gets more than 2x its fair share
      assert Enum.all?(values, &(&1 < mean * 2)),
             "some node got more than 2x fair share: #{inspect(counts)}"

      # Standard deviation < 15% of mean
      variance =
        Enum.sum(Enum.map(values, fn v -> (v - mean) * (v - mean) end)) / length(values)

      stddev = :math.sqrt(variance)

      assert stddev < mean * 0.15,
             "stddev #{Float.round(stddev, 1)} exceeds 15% of mean #{Float.round(mean, 1)}"
    end

    test "distribution remains even with 10 nodes" do
      nodes = for i <- 1..10, do: :"core@node#{i}"
      ring = MetadataRing.new(nodes)

      counts =
        for i <- 1..10_000, reduce: %{} do
          acc ->
            {_seg, [primary | _]} = MetadataRing.locate(ring, "key_#{i}")
            Map.update(acc, primary, 1, &(&1 + 1))
        end

      values = Map.values(counts)
      mean = Enum.sum(values) / length(values)

      # No node gets more than 2x its fair share
      assert Enum.all?(values, &(&1 < mean * 2))
    end
  end

  describe "various node counts" do
    test "works with 1 node" do
      ring = MetadataRing.new([:core@node1])
      {segment_id, replicas} = MetadataRing.locate(ring, "key")

      assert byte_size(segment_id) == 32
      assert replicas == [:core@node1]
      assert MetadataRing.segment_count(ring) == 64
    end

    test "works with 3 nodes" do
      ring = MetadataRing.new(@nodes)
      {_seg, replicas} = MetadataRing.locate(ring, "key")

      assert length(replicas) == 3
    end

    test "works with 5 nodes" do
      nodes = for i <- 1..5, do: :"core@node#{i}"
      ring = MetadataRing.new(nodes)
      {_seg, replicas} = MetadataRing.locate(ring, "key")

      assert length(replicas) == 3
      assert replicas == Enum.uniq(replicas)
    end

    test "works with 10 nodes" do
      nodes = for i <- 1..10, do: :"core@node#{i}"
      ring = MetadataRing.new(nodes)
      {_seg, replicas} = MetadataRing.locate(ring, "key")

      assert length(replicas) == 3
      assert replicas == Enum.uniq(replicas)
    end
  end

  describe "property tests" do
    property "locate always returns valid segment with correct replica count" do
      nodes = for i <- 1..5, do: :"core@node#{i}"
      ring = MetadataRing.new(nodes)

      check all(key <- binary(min_length: 1, max_length: 100)) do
        {segment_id, replica_set} = MetadataRing.locate(ring, key)

        assert byte_size(segment_id) == 32
        assert length(replica_set) == 3
        assert replica_set == Enum.uniq(replica_set)
        assert Enum.all?(replica_set, &(&1 in nodes))
      end
    end

    property "locate is deterministic for any key" do
      ring = MetadataRing.new(@nodes)

      check all(key <- binary(min_length: 1, max_length: 200)) do
        result1 = MetadataRing.locate(ring, key)
        result2 = MetadataRing.locate(ring, key)
        assert result1 == result2
      end
    end
  end
end
