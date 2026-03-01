defmodule NeonFS.Core.TieringManager.ColdnessScoreTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.TieringManager.ColdnessScore

  @now ~U[2026-02-22 12:00:00Z]

  describe "score/2" do
    test "recently accessed chunk scores higher than chunk accessed long ago" do
      recent = %{daily: 5, last_accessed: DateTime.add(@now, -3600, :second)}
      old = %{daily: 5, last_accessed: DateTime.add(@now, -86_400, :second)}

      recent_score = ColdnessScore.score(recent, now: @now)
      old_score = ColdnessScore.score(old, now: @now)

      assert recent_score > old_score
    end

    test "chunk with many daily accesses scores higher than chunk with few" do
      frequent = %{daily: 10, last_accessed: DateTime.add(@now, -7200, :second)}
      infrequent = %{daily: 1, last_accessed: DateTime.add(@now, -7200, :second)}

      frequent_score = ColdnessScore.score(frequent, now: @now)
      infrequent_score = ColdnessScore.score(infrequent, now: @now)

      assert frequent_score > infrequent_score
    end

    test "nil last_accessed produces lowest possible score" do
      never_accessed = %{daily: 0, last_accessed: nil}
      old = %{daily: 0, last_accessed: DateTime.add(@now, -604_800, :second)}

      never_score = ColdnessScore.score(never_accessed, now: @now)
      old_score = ColdnessScore.score(old, now: @now)

      assert never_score < old_score
    end

    test "formula matches spec: -hours_since + daily * weight" do
      # 2 hours ago, 3 daily accesses, weight 10
      stats = %{daily: 3, last_accessed: DateTime.add(@now, -7200, :second)}
      score = ColdnessScore.score(stats, now: @now, frequency_weight: 10)

      # -2.0 + 3 * 10 = 28.0
      assert_in_delta score, 28.0, 0.01
    end

    test "custom frequency_weight is applied" do
      stats = %{daily: 3, last_accessed: DateTime.add(@now, -3600, :second)}

      score_default = ColdnessScore.score(stats, now: @now)
      score_high = ColdnessScore.score(stats, now: @now, frequency_weight: 20)

      # With weight 10: -1 + 30 = 29
      # With weight 20: -1 + 60 = 59
      assert_in_delta score_default, 29.0, 0.01
      assert_in_delta score_high, 59.0, 0.01
    end

    test "eviction order matches expected coldness ranking" do
      # Build chunks with known stats, sorted coldest first
      chunks = [
        {:chunk_a, %{daily: 0, last_accessed: nil}},
        {:chunk_b, %{daily: 0, last_accessed: DateTime.add(@now, -172_800, :second)}},
        {:chunk_c, %{daily: 1, last_accessed: DateTime.add(@now, -86_400, :second)}},
        {:chunk_d, %{daily: 5, last_accessed: DateTime.add(@now, -3600, :second)}},
        {:chunk_e, %{daily: 20, last_accessed: DateTime.add(@now, -60, :second)}}
      ]

      sorted =
        chunks
        |> Enum.sort_by(fn {_id, stats} -> ColdnessScore.score(stats, now: @now) end, :asc)
        |> Enum.map(&elem(&1, 0))

      # nil last_accessed is coldest, then old+no accesses, etc.
      assert hd(sorted) == :chunk_a
      assert List.last(sorted) == :chunk_e
    end

    test "zero daily and zero hours_since gives score of zero" do
      stats = %{daily: 0, last_accessed: @now}
      score = ColdnessScore.score(stats, now: @now)
      assert_in_delta score, 0.0, 0.001
    end
  end
end
