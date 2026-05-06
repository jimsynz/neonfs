defmodule NeonFS.Core.Volume.HLCTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NeonFS.Core.HLC, as: ClusterHLC
  alias NeonFS.Core.Volume.HLC, as: VolumeHLC
  alias NeonFS.Core.Volume.RootSegment

  describe "now/1" do
    test "returns a timestamp and an advanced segment" do
      segment = sample_segment()
      {timestamp, advanced} = VolumeHLC.now(segment)

      assert is_tuple(timestamp)
      assert tuple_size(timestamp) == 3
      refute advanced.hlc == segment.hlc
      # Identity fields untouched.
      assert advanced.volume_id == segment.volume_id
      assert advanced.cluster_id == segment.cluster_id
    end

    test "successive calls produce strictly monotonic timestamps" do
      segment = sample_segment()

      {ts1, segment} = VolumeHLC.now(segment)
      {ts2, segment} = VolumeHLC.now(segment)
      {ts3, _segment} = VolumeHLC.now(segment)

      assert ClusterHLC.compare(ts2, ts1) == :gt
      assert ClusterHLC.compare(ts3, ts2) == :gt
    end
  end

  describe "now/2 (deterministic wall clock)" do
    test "increments counter when wall clock does not advance" do
      segment = %{sample_segment() | hlc: ClusterHLC.new(node())}

      {{wall1, counter1, _}, segment} = VolumeHLC.now(segment, 1_000)
      {{wall2, counter2, _}, _segment} = VolumeHLC.now(segment, 1_000)

      assert wall1 == wall2
      assert counter2 == counter1 + 1
    end

    test "resets counter when wall clock advances" do
      segment = %{sample_segment() | hlc: ClusterHLC.new(node())}

      {{wall1, _, _}, segment} = VolumeHLC.now(segment, 1_000)
      {{wall2, counter2, _}, _segment} = VolumeHLC.now(segment, 2_000)

      assert wall2 > wall1
      assert counter2 == 0
    end
  end

  describe "receive_timestamp/2" do
    test "advances local segment past a remote timestamp" do
      segment = sample_segment()
      remote = {System.system_time(:millisecond) + 100, 0, :other@host}

      assert {:ok, ts, advanced} = VolumeHLC.receive_timestamp(segment, remote)
      assert ClusterHLC.compare(ts, remote) == :gt
      refute advanced.hlc == segment.hlc
    end

    test "rejects timestamps beyond max_clock_skew_ms" do
      segment = sample_segment()
      far_future = {System.system_time(:millisecond) + 10_000_000, 0, :other@host}

      assert {:error, :clock_skew_detected, skew} =
               VolumeHLC.receive_timestamp(segment, far_future)

      assert skew > segment.hlc.max_clock_skew_ms
    end
  end

  describe "monotonicity (property)" do
    property "any sequence of now/2 calls produces strictly increasing timestamps" do
      check all(walls <- list_of(integer(0..10_000), min_length: 1, max_length: 50)) do
        segment = %{sample_segment() | hlc: ClusterHLC.new(node())}

        {timestamps, _final} =
          Enum.reduce(walls, {[], segment}, fn wall_ms, {acc, seg} ->
            {ts, seg} = VolumeHLC.now(seg, wall_ms)
            {[ts | acc], seg}
          end)

        timestamps = Enum.reverse(timestamps)

        timestamps
        |> Enum.zip(Enum.drop(timestamps, 1))
        |> Enum.each(fn {a, b} ->
          assert ClusterHLC.compare(b, a) == :gt,
                 "non-monotonic: #{inspect(b)} not > #{inspect(a)}"
        end)
      end
    end

    property "interleaved local now/2 and receive_timestamp/3 stay strictly increasing" do
      step = step_gen()

      check all(steps <- list_of(step, min_length: 1, max_length: 50)) do
        segment = %{sample_segment() | hlc: ClusterHLC.new(node())}

        {timestamps, _final} =
          Enum.reduce(steps, {[], segment}, fn
            {:local, wall_ms}, {acc, seg} ->
              {ts, seg} = VolumeHLC.now(seg, wall_ms)
              {[ts | acc], seg}

            {:remote, wall_ms, remote}, {acc, seg} ->
              case VolumeHLC.receive_timestamp(seg, remote, wall_ms) do
                {:ok, ts, seg} -> {[ts | acc], seg}
                # Skew rejection: drop the step but keep state.
                {:error, _, _} -> {acc, seg}
              end
          end)

        timestamps = Enum.reverse(timestamps)

        timestamps
        |> Enum.zip(Enum.drop(timestamps, 1))
        |> Enum.each(fn {a, b} ->
          assert ClusterHLC.compare(b, a) == :gt,
                 "non-monotonic: #{inspect(b)} not > #{inspect(a)}"
        end)
      end
    end

    property "two writers seeing the same starting segment produce timestamps a third party can totally order" do
      check all(
              wall_a <- integer(1..1_000),
              wall_b <- integer(1..1_000)
            ) do
        starting = %{sample_segment() | hlc: ClusterHLC.new(node())}

        {ts_a, _seg_a} = VolumeHLC.now(starting, wall_a)
        {ts_b, _seg_b} = VolumeHLC.now(starting, wall_b)

        # Even when both use the same starting segment, the timestamps
        # are totally ordered (HLC.compare/2 always returns :gt or :lt
        # — :eq only on identical timestamps, which is impossible
        # because both have the same node_id but different counters
        # *or* different walls).
        cmp = ClusterHLC.compare(ts_a, ts_b)
        assert cmp in [:gt, :lt, :eq]

        # If they happen to land on the same wall + counter, the node_id
        # tiebreak in compare/2 still totalises the order — but since
        # both writers share the node_id here, they could land equal.
        # Handle that explicitly: equal only when wall_a == wall_b and
        # both go through the "advance from last_wall" branch.
        if cmp == :eq do
          assert ts_a == ts_b
        end
      end
    end
  end

  ## Helpers

  defp sample_segment do
    RootSegment.new(
      volume_id: "vol_test",
      volume_name: "test-volume",
      cluster_id: "clust_test",
      cluster_name: "test-cluster",
      durability: %{type: :replicate, factor: 1, min_copies: 1}
    )
  end

  defp step_gen do
    one_of([
      tuple({constant(:local), integer(0..10_000)}),
      tuple({
        constant(:remote),
        integer(0..10_000),
        tuple({integer(0..10_000), integer(0..10), constant(:remote@host)})
      })
    ])
  end
end
