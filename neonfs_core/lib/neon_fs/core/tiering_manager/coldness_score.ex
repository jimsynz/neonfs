defmodule NeonFS.Core.TieringManager.ColdnessScore do
  @moduledoc """
  Computes a coldness score for eviction decisions.

  Lower scores indicate colder chunks (evicted first). The formula
  blends access recency with access frequency:

      coldness = -(hours_since_last_access) + (daily_count * frequency_weight)

  A chunk accessed recently but infrequently scores low (cold). A chunk
  accessed long ago but frequently still scores high (warm) because the
  `frequency_weight` (default 10) amplifies the daily count.

  Chunks with `nil` last_accessed are treated as maximally cold.
  """

  @default_frequency_weight 10

  @doc """
  Computes a coldness score for a chunk's access stats.

  ## Parameters
  - `stats` — map with `:daily` count and `:last_accessed` (DateTime or nil)
  - `opts` — optional keyword list:
    - `:frequency_weight` — multiplier for daily access count (default #{@default_frequency_weight})
    - `:now` — reference time for recency calculation (default `DateTime.utc_now/0`)

  ## Returns
  A float score. Lower values = colder = evicted first.
  """
  @spec score(map(), keyword()) :: float()
  def score(stats, opts \\ []) do
    frequency_weight = Keyword.get(opts, :frequency_weight, @default_frequency_weight)
    now = Keyword.get(opts, :now, DateTime.utc_now())
    daily = Map.get(stats, :daily, 0)

    hours_since = hours_since_last_access(Map.get(stats, :last_accessed), now)

    -hours_since + daily * frequency_weight
  end

  # 10 years in hours — effectively infinite for chunks never accessed
  @max_hours 87_600.0

  defp hours_since_last_access(nil, _now), do: @max_hours

  defp hours_since_last_access(%DateTime{} = last, %DateTime{} = now) do
    DateTime.diff(now, last, :second) / 3600.0
  end
end
