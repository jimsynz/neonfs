defmodule NeonFS.Core.HLCTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.HLC

  describe "new/1" do
    test "creates initial state with node_id" do
      state = HLC.new(:node_a@host)

      assert %HLC{node_id: :node_a@host, last_wall: 0, last_counter: 0} = state
      assert state.max_clock_skew_ms == 1_000
    end

    test "accepts custom max_clock_skew_ms" do
      state = HLC.new(:node_a@host, max_clock_skew_ms: 500)

      assert state.max_clock_skew_ms == 500
    end
  end

  describe "now/2" do
    test "generates timestamp with current wall time" do
      state = HLC.new(:node_a@host)

      {timestamp, new_state} = HLC.now(state, 1000)

      assert {1000, 0, :node_a@host} = timestamp
      assert new_state.last_wall == 1000
      assert new_state.last_counter == 0
    end

    test "increments counter when wall time hasn't advanced" do
      state = HLC.new(:node_a@host)

      {_ts1, state} = HLC.now(state, 1000)
      {ts2, state} = HLC.now(state, 1000)
      {ts3, _state} = HLC.now(state, 1000)

      assert {1000, 1, :node_a@host} = ts2
      assert {1000, 2, :node_a@host} = ts3
    end

    test "resets counter when wall time advances" do
      state = HLC.new(:node_a@host)

      {_ts1, state} = HLC.now(state, 1000)
      {_ts2, state} = HLC.now(state, 1000)
      {ts3, _state} = HLC.now(state, 1001)

      assert {1001, 0, :node_a@host} = ts3
    end

    test "uses last_wall when wall clock goes backward" do
      state = HLC.new(:node_a@host)

      {_ts1, state} = HLC.now(state, 1000)
      # Wall clock goes backward to 999
      {ts2, state} = HLC.now(state, 999)

      # Should use last_wall (1000) and increment counter
      assert {1000, 1, :node_a@host} = ts2
      assert state.last_wall == 1000
    end

    test "preserves monotonicity when wall clock falls behind last_wall" do
      state = %{HLC.new(:node_a@host, max_clock_skew_ms: 100) | last_wall: 5000}

      # Wall clock is at 1000, but last_wall is far ahead at 5000.
      # HLC must preserve monotonicity: use last_wall and increment counter.
      {ts, _state} = HLC.now(state, 1000)

      assert {5000, 1, :node_a@host} = ts
    end

    test "bounds wall time to prevent forward runaway" do
      state = HLC.new(:node_a@host, max_clock_skew_ms: 100)

      # First generate a normal timestamp at wall=1000
      {_ts1, state} = HLC.now(state, 1000)

      # Now receive a timestamp that pushes last_wall to 1050 (within bounds)
      {:ok, _ts, state} = HLC.receive_timestamp(state, {1050, 0, :remote@host}, 1000)

      # Next call at wall=1000: effective = min(max(1000, 1050), 1100) = 1050
      # 1050 == last_wall, so counter increments
      {ts, _state} = HLC.now(state, 1000)

      assert {1050, _, :node_a@host} = ts
    end

    test "monotonicity across many calls" do
      state = HLC.new(:node_a@host)

      {timestamps, _final_state} =
        Enum.reduce(1..100, {[], state}, fn i, {acc, s} ->
          # Simulate time moving forward with occasional backward jumps
          wall = 1000 + div(i, 3)
          {ts, new_s} = HLC.now(s, wall)
          {[ts | acc], new_s}
        end)

      timestamps = Enum.reverse(timestamps)

      # Every timestamp should be strictly greater than the previous
      timestamps
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.each(fn [earlier, later] ->
        assert HLC.compare(earlier, later) == :lt,
               "Expected #{inspect(earlier)} < #{inspect(later)}"
      end)
    end
  end

  describe "now/1 (real clock)" do
    test "generates timestamp using system time" do
      state = HLC.new(:node_a@host)
      before = System.system_time(:millisecond)

      {timestamp, _state} = HLC.now(state)

      {wall, 0, :node_a@host} = timestamp
      after_ms = System.system_time(:millisecond)

      assert wall >= before
      assert wall <= after_ms
    end
  end

  describe "receive_timestamp/3" do
    test "advances state when remote is ahead within bounds" do
      state = HLC.new(:node_a@host)
      {_ts, state} = HLC.now(state, 1000)

      remote = {1500, 3, :node_b@host}

      assert {:ok, ts, new_state} = HLC.receive_timestamp(state, remote, 1000)

      # Should advance to remote wall time with counter > remote_counter
      assert {1500, 4, :node_a@host} = ts
      assert new_state.last_wall == 1500
      assert new_state.last_counter == 4
    end

    test "rejects remote timestamp beyond max skew" do
      state = HLC.new(:node_a@host, max_clock_skew_ms: 100)

      remote = {2000, 0, :node_b@host}

      assert {:error, :clock_skew_detected, skew} =
               HLC.receive_timestamp(state, remote, 1000)

      assert skew == 1000
    end

    test "accepts remote timestamp exactly at max skew" do
      state = HLC.new(:node_a@host, max_clock_skew_ms: 500)

      remote = {1500, 0, :node_b@host}

      assert {:ok, _ts, _state} = HLC.receive_timestamp(state, remote, 1000)
    end

    test "advances when local is ahead of remote" do
      state = HLC.new(:node_a@host)
      {_ts, state} = HLC.now(state, 2000)

      remote = {1500, 3, :node_b@host}

      assert {:ok, ts, _state} = HLC.receive_timestamp(state, remote, 2000)

      # Local wall (2000) is ahead, so should use local wall
      assert {2000, 1, :node_a@host} = ts
    end

    test "handles all three walls equal" do
      state = HLC.new(:node_a@host)
      {_ts, state} = HLC.now(state, 1000)

      remote = {1000, 5, :node_b@host}

      assert {:ok, ts, _state} = HLC.receive_timestamp(state, remote, 1000)

      # All walls equal: counter = max(local_counter, remote_counter) + 1
      # state.last_counter = 0, remote_counter = 5
      assert {1000, 6, :node_a@host} = ts
    end

    test "handles local wall equal to new_wall but remote different" do
      state = %{HLC.new(:node_a@host) | last_wall: 1500, last_counter: 3}

      remote = {1000, 7, :node_b@host}

      assert {:ok, ts, _state} = HLC.receive_timestamp(state, remote, 1500)

      # new_wall = max(1500, 1500, 1000) = 1500 = state.last_wall but != remote_wall
      assert {1500, 4, :node_a@host} = ts
    end
  end

  describe "compare/2" do
    test "compares by wall time first" do
      assert HLC.compare({2000, 0, :a@h}, {1000, 0, :a@h}) == :gt
      assert HLC.compare({1000, 0, :a@h}, {2000, 0, :a@h}) == :lt
    end

    test "compares by counter when wall times equal" do
      assert HLC.compare({1000, 5, :a@h}, {1000, 3, :a@h}) == :gt
      assert HLC.compare({1000, 3, :a@h}, {1000, 5, :a@h}) == :lt
    end

    test "compares by node_id for deterministic tiebreak" do
      assert HLC.compare({1000, 0, :b@h}, {1000, 0, :a@h}) == :gt
      assert HLC.compare({1000, 0, :a@h}, {1000, 0, :b@h}) == :lt
    end

    test "returns :eq for identical timestamps" do
      ts = {1000, 5, :a@h}
      assert HLC.compare(ts, ts) == :eq
    end
  end

  describe "merge/2" do
    test "returns higher timestamp" do
      higher = {2000, 0, :a@h}
      lower = {1000, 5, :a@h}

      assert HLC.merge(higher, lower) == higher
      assert HLC.merge(lower, higher) == higher
    end

    test "returns first when equal" do
      ts = {1000, 5, :a@h}
      assert HLC.merge(ts, ts) == ts
    end

    test "uses counter for tiebreak" do
      a = {1000, 5, :a@h}
      b = {1000, 3, :a@h}

      assert HLC.merge(a, b) == a
    end
  end

  describe "serialisation" do
    test "round-trip to_binary/from_binary" do
      timestamp = {1_706_000_000_000, 42, :node_a@host}

      binary = HLC.to_binary(timestamp)
      assert is_binary(binary)

      decoded = HLC.from_binary(binary)
      assert decoded == timestamp
    end

    test "round-trip with zero counter" do
      timestamp = {1_000, 0, :n@h}

      assert timestamp == timestamp |> HLC.to_binary() |> HLC.from_binary()
    end

    test "round-trip with large values" do
      timestamp =
        {9_999_999_999_999, 4_294_967_295, :"very-long-node-name@very-long-host.example.com"}

      assert timestamp == timestamp |> HLC.to_binary() |> HLC.from_binary()
    end

    test "binary preserves wall time ordering" do
      ts1 = {1000, 0, :a@h}
      ts2 = {2000, 0, :a@h}

      bin1 = HLC.to_binary(ts1)
      bin2 = HLC.to_binary(ts2)

      # Binary comparison should also be ordered by wall time
      assert bin1 < bin2
    end
  end

  describe "property: monotonicity" do
    @tag timeout: 10_000
    test "generating N timestamps always yields a sorted sequence" do
      for _ <- 1..50 do
        node_id = :"node_#{:rand.uniform(10)}@host"
        state = HLC.new(node_id)
        start_wall = :rand.uniform(1_000_000)

        {timestamps, _} =
          Enum.reduce(1..20, {[], state}, fn _i, {acc, s} ->
            # Random wall time progression (may go backward)
            wall = start_wall + :rand.uniform(100) - 30
            {ts, new_s} = HLC.now(s, wall)
            {[ts | acc], new_s}
          end)

        timestamps = Enum.reverse(timestamps)

        timestamps
        |> Enum.chunk_every(2, 1, :discard)
        |> Enum.each(fn [earlier, later] ->
          assert HLC.compare(earlier, later) == :lt,
                 "Monotonicity violated: #{inspect(earlier)} should be < #{inspect(later)}"
        end)
      end
    end
  end
end
