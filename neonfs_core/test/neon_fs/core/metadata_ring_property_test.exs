defmodule NeonFS.Core.MetadataRingPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NeonFS.Core.MetadataRing

  # Generators

  # Construct unique node lists directly from a count to avoid
  # uniq_list_of shrinking issues with small integer domains.
  defp node_set(min_count \\ 2, max_count \\ 10) do
    StreamData.integer(min_count..max_count)
    |> StreamData.map(fn count ->
      for i <- 1..count, do: :"core@node#{i}"
    end)
  end

  defp arbitrary_key do
    StreamData.binary(min_length: 1, max_length: 200)
  end

  # Properties

  describe "determinism" do
    property "get_segment (locate) is deterministic — same key always maps to same segment" do
      check all(
              nodes <- node_set(),
              key <- arbitrary_key(),
              max_runs: 200
            ) do
        ring = MetadataRing.new(nodes)
        {seg1, _replicas1} = MetadataRing.locate(ring, key)
        {seg2, _replicas2} = MetadataRing.locate(ring, key)

        assert seg1 == seg2
      end
    end
  end

  describe "minimal disruption" do
    property "adding a node moves at most 2/n of keys" do
      check all(
              node_count <- StreamData.integer(3..10),
              max_runs: 100
            ) do
        nodes = for i <- 1..node_count, do: :"core@node#{i}"
        new_node = :"core@node#{node_count + 1}"

        ring = MetadataRing.new(nodes)
        {new_ring, _affected} = MetadataRing.add_node(ring, new_node)

        sample_size = 1000
        n = node_count + 1

        moved =
          Enum.count(1..sample_size, fn i ->
            key = "sample_key_#{i}"
            {_seg1, replicas1} = MetadataRing.locate(ring, key)
            {_seg2, replicas2} = MetadataRing.locate(new_ring, key)
            List.first(replicas1) != List.first(replicas2)
          end)

        max_allowed = ceil(sample_size * 2 / n)

        assert moved <= max_allowed,
               "#{moved}/#{sample_size} keys moved (expected <= #{max_allowed} for n=#{n})"
      end
    end

    property "removing a node moves at most 2/n of keys" do
      check all(
              node_count <- StreamData.integer(3..10),
              remove_idx <- StreamData.integer(1..10),
              max_runs: 100
            ) do
        nodes = for i <- 1..node_count, do: :"core@node#{i}"
        idx = rem(remove_idx - 1, node_count) + 1
        node_to_remove = :"core@node#{idx}"

        ring = MetadataRing.new(nodes)
        {new_ring, _affected} = MetadataRing.remove_node(ring, node_to_remove)

        sample_size = 1000
        n = node_count

        moved =
          Enum.count(1..sample_size, fn i ->
            key = "sample_key_#{i}"
            {_seg1, replicas1} = MetadataRing.locate(ring, key)
            {_seg2, replicas2} = MetadataRing.locate(new_ring, key)
            List.first(replicas1) != List.first(replicas2)
          end)

        max_allowed = ceil(sample_size * 2 / n)

        assert moved <= max_allowed,
               "#{moved}/#{sample_size} keys moved (expected <= #{max_allowed} for n=#{n})"
      end
    end
  end

  describe "no orphan segments" do
    property "all segments have at least one assigned node" do
      check all(
              nodes <- node_set(1, 10),
              max_runs: 100
            ) do
        ring = MetadataRing.new(nodes)
        segments = MetadataRing.segments(ring)

        assert length(segments) == 256

        Enum.each(segments, fn {segment_id, replica_set} ->
          assert byte_size(segment_id) == 32
          assert replica_set != [], "segment #{inspect(segment_id)} has no assigned nodes"
        end)
      end
    end
  end

  describe "segment stability" do
    property "segment ID is stable across different cluster sizes for the same key" do
      check all(
              key <- arbitrary_key(),
              extra_count <- StreamData.integer(1..5),
              max_runs: 200
            ) do
        base_nodes = [:core@base1, :core@base2, :core@base3]
        extra_nodes = for i <- 1..extra_count, do: :"core@extra#{i}"

        small_ring = MetadataRing.new(base_nodes)
        large_ring = MetadataRing.new(base_nodes ++ extra_nodes)

        {seg_small, _} = MetadataRing.locate(small_ring, key)
        {seg_large, _} = MetadataRing.locate(large_ring, key)

        assert seg_small == seg_large,
               "segment ID changed for key across ring sizes"
      end
    end
  end
end
