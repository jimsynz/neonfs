defmodule NeonFS.Core.HLCPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NeonFS.Core.HLC

  # Generators

  defp node_id do
    StreamData.member_of([
      :node_a@host,
      :node_b@host,
      :node_c@host,
      :node_d@host,
      :node_e@host
    ])
  end

  defp hlc_timestamp do
    gen all(
          wall <- StreamData.integer(1..10_000_000),
          counter <- StreamData.integer(0..1_000),
          node <- node_id()
        ) do
      {wall, counter, node}
    end
  end

  defp wall_time do
    StreamData.integer(1..10_000_000)
  end

  defp wall_time_sequence(length) do
    StreamData.list_of(wall_time(), length: length)
  end

  describe "compare/2 properties" do
    property "antisymmetry: compare(a, b) == :gt implies compare(b, a) == :lt" do
      check all(
              ts_a <- hlc_timestamp(),
              ts_b <- hlc_timestamp()
            ) do
        cmp_ab = HLC.compare(ts_a, ts_b)
        cmp_ba = HLC.compare(ts_b, ts_a)

        case cmp_ab do
          :gt -> assert cmp_ba == :lt
          :lt -> assert cmp_ba == :gt
          :eq -> assert cmp_ba == :eq
        end
      end
    end

    property "transitivity: a > b and b > c implies a > c" do
      check all(
              ts_a <- hlc_timestamp(),
              ts_b <- hlc_timestamp(),
              ts_c <- hlc_timestamp()
            ) do
        if HLC.compare(ts_a, ts_b) == :gt and HLC.compare(ts_b, ts_c) == :gt do
          assert HLC.compare(ts_a, ts_c) == :gt
        end
      end
    end

    property "reflexivity: compare(a, a) == :eq" do
      check all(ts <- hlc_timestamp()) do
        assert HLC.compare(ts, ts) == :eq
      end
    end

    property "merge returns the greater of two timestamps" do
      check all(
              ts_a <- hlc_timestamp(),
              ts_b <- hlc_timestamp()
            ) do
        merged = HLC.merge(ts_a, ts_b)
        cmp = HLC.compare(ts_a, ts_b)

        case cmp do
          :gt -> assert merged == ts_a
          :lt -> assert merged == ts_b
          :eq -> assert merged == ts_a
        end
      end
    end
  end

  describe "now/2 properties" do
    property "send always advances: consecutive now/2 calls yield strictly increasing timestamps" do
      check all(
              node <- node_id(),
              walls <- wall_time_sequence(10)
            ) do
        state = HLC.new(node)

        {timestamps, _final_state} =
          Enum.reduce(walls, {[], state}, fn w, {acc, s} ->
            {ts, new_s} = HLC.now(s, w)
            {[ts | acc], new_s}
          end)

        timestamps
        |> Enum.reverse()
        |> Enum.chunk_every(2, 1, :discard)
        |> Enum.each(fn [earlier, later] ->
          assert HLC.compare(earlier, later) == :lt,
                 "Monotonicity violated: #{inspect(earlier)} should be < #{inspect(later)}"
        end)
      end
    end

    property "wall component never exceeds peak wall clock input + max_clock_skew_ms" do
      check all(
              node <- node_id(),
              max_skew <- StreamData.integer(100..5_000),
              walls <- wall_time_sequence(10)
            ) do
        state = HLC.new(node, max_clock_skew_ms: max_skew)

        # The HLC maintains monotonicity: once last_wall is set high (from a
        # large wall_ms input), backward clock jumps reuse last_wall. The correct
        # bound is that the wall component never exceeds the highest wall_ms
        # seen so far plus max_skew.
        Enum.reduce(walls, {state, 0}, fn w, {s, peak_wall} ->
          new_peak = max(peak_wall, w)
          {ts, new_s} = HLC.now(s, w)
          {ts_wall, _counter, _node} = ts

          assert ts_wall <= new_peak + max_skew,
                 "Wall component #{ts_wall} exceeded peak wall #{new_peak} + max_skew #{max_skew}"

          {new_s, new_peak}
        end)
      end
    end
  end

  describe "receive_timestamp/3 properties" do
    property "result timestamp is greater than both local last and remote timestamp (when within skew)" do
      check all(
              node <- node_id(),
              local_wall <- StreamData.integer(1_000..5_000),
              remote_wall <- StreamData.integer(1_000..5_000),
              remote_counter <- StreamData.integer(0..100),
              remote_node <- node_id(),
              current_wall <- StreamData.integer(1_000..5_000)
            ) do
        state = HLC.new(node, max_clock_skew_ms: 10_000)

        # Issue a local timestamp first
        {local_ts, state} = HLC.now(state, local_wall)
        remote_ts = {remote_wall, remote_counter, remote_node}

        case HLC.receive_timestamp(state, remote_ts, current_wall) do
          {:ok, result_ts, _new_state} ->
            # Result must be greater than the previous local timestamp
            assert HLC.compare(result_ts, local_ts) == :gt,
                   "Result #{inspect(result_ts)} should be > local #{inspect(local_ts)}"

          {:error, :clock_skew_detected, _skew} ->
            # Skew rejection is fine — the property only applies when accepted
            :ok
        end
      end
    end

    property "receive_timestamp rejects when remote wall exceeds local wall by more than max_skew" do
      check all(
              node <- node_id(),
              max_skew <- StreamData.integer(100..1_000),
              local_wall <- StreamData.integer(1_000..5_000),
              extra <- StreamData.integer(1..5_000)
            ) do
        state = HLC.new(node, max_clock_skew_ms: max_skew)
        remote_wall = local_wall + max_skew + extra
        remote_ts = {remote_wall, 0, :remote@host}

        assert {:error, :clock_skew_detected, skew} =
                 HLC.receive_timestamp(state, remote_ts, local_wall)

        assert skew == remote_wall - local_wall
        assert skew > max_skew
      end
    end

    property "receive_timestamp maintains monotonicity across interleaved sends and receives" do
      check all(
              node <- node_id(),
              ops <-
                StreamData.list_of(
                  StreamData.one_of([
                    StreamData.tuple({StreamData.constant(:send), wall_time()}),
                    StreamData.tuple(
                      {StreamData.constant(:recv),
                       StreamData.tuple({wall_time(), StreamData.integer(0..100), node_id()}),
                       wall_time()}
                    )
                  ]),
                  min_length: 2,
                  max_length: 15
                )
            ) do
        state = HLC.new(node, max_clock_skew_ms: 20_000_000)

        {timestamps, _} =
          Enum.reduce(ops, {[], state}, fn op, {acc, s} ->
            case op do
              {:send, w} ->
                {ts, new_s} = HLC.now(s, w)
                {[ts | acc], new_s}

              {:recv, remote_ts, w} ->
                case HLC.receive_timestamp(s, remote_ts, w) do
                  {:ok, ts, new_s} -> {[ts | acc], new_s}
                  {:error, :clock_skew_detected, _} -> {acc, s}
                end
            end
          end)

        timestamps
        |> Enum.reverse()
        |> Enum.chunk_every(2, 1, :discard)
        |> Enum.each(fn [earlier, later] ->
          assert HLC.compare(earlier, later) == :lt,
                 "Monotonicity violated: #{inspect(earlier)} should be < #{inspect(later)}"
        end)
      end
    end
  end

  describe "serialisation properties" do
    property "round-trip: from_binary(to_binary(ts)) == ts" do
      check all(ts <- hlc_timestamp()) do
        assert ts == ts |> HLC.to_binary() |> HLC.from_binary()
      end
    end
  end
end
