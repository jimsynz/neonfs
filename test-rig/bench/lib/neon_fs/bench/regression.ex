defmodule NeonFS.Bench.Regression do
  @moduledoc """
  Rolling-window regression gate for the scheduled benchmark suite (#1524).

  Compares the freshest benchee run against the **median** of a rolling
  window of the last few runs (rather than a committed baseline or the single
  previous run): a single-previous comparison lets a slow weekly drift never
  trip a fixed gate and lets one anomalous week ratchet the reference, whereas
  a ~4-run median measures against ~4 weeks ago (so cumulative drift trips) and
  is robust to a single bad sample.

  A metric regresses when it moves the wrong way by **> 10% AND > 2σ** vs the
  window median, where σ is the standard deviation of the window's per-run
  values. Direction is per-metric: throughput/rate metrics (`seq_write`,
  `seq_read`, `small_files` — gated on benchee `ips`) regress when they
  **drop**; latency metrics (`stat_list`, `range_read` — gated on the average
  run time) regress when they **rise**.

  On regression it emits a Markdown issue body carrying the config, the
  regressed metric, its current value, the window median + σ, and a bounded
  `git bisect` range (`<oldest-run-in-window-sha>..<current-sha>`) — each run
  is SHA-stamped (#1520/#1523), and bisect stays manual (#1525).
  """

  @pct_threshold 0.10
  @sigma_multiple 2.0
  @higher_better ~w(seq_write seq_read small_files)
  @lower_better ~w(stat_list range_read)

  @doc """
  Entry point for the scheduled workflow. Loads the fresh run's result tree
  (`BENCH_CURRENT_DIR`) and the window of prior run trees (`BENCH_WINDOW_DIRS`,
  whitespace-separated), compares each config against the window of the **same
  config** (baseline-vs-baseline, etc.), and — if any metric regresses —
  writes a Markdown issue body to `BENCH_REGRESSION_REPORT`. Always exits 0: a
  regression opens an issue, it does not fail the build (bisect is manual).
  """
  @spec main() :: :ok
  def main do
    current_runs = run_dirs(System.fetch_env!("BENCH_CURRENT_DIR"))

    window_runs =
      System.get_env("BENCH_WINDOW_DIRS", "")
      |> String.split(~r/\s+/, trim: true)
      |> Enum.flat_map(&run_dirs/1)

    results = analyze_runs(current_runs, window_runs)

    if results == [] do
      IO.puts("bench regression gate: no regressions (#{length(window_runs)} window runs)")
    else
      body = issue_body(results, window_runs)
      IO.puts(body)

      case System.get_env("BENCH_REGRESSION_REPORT") do
        nil -> :ok
        path -> File.write!(path, body)
      end
    end

    :ok
  end

  @doc """
  Load every result dir at or under `root` — any directory holding a
  `meta.json` (a benchee run writes one such dir per config). Searched
  recursively so it's robust to however downloaded window artifacts nest the
  results tree inside their archive.
  """
  @spec run_dirs(String.t()) :: [map()]
  def run_dirs(root) do
    Path.wildcard(Path.join(root, "**/meta.json"))
    |> Enum.map(&Path.dirname/1)
    |> Enum.uniq()
    |> Enum.map(&load_run/1)
    |> Enum.reject(&is_nil/1)
  end

  @doc """
  Compare each current run against the window runs sharing its config label,
  returning `[%{run, regressions}]` for the configs that regressed.
  """
  @spec analyze_runs([map()], [map()]) :: [map()]
  def analyze_runs(current_runs, window_runs) do
    window_by_label = Enum.group_by(window_runs, & &1.label)

    for run <- current_runs,
        regressions = analyze(run, Map.get(window_by_label, run.label, [])),
        regressions != [] do
      %{run: run, regressions: regressions}
    end
  end

  @doc """
  Return the list of regressed metrics for `current` against `window`. A metric
  is only considered when the window holds at least two runs with that metric
  (σ needs ≥2 samples) and a positive median.
  """
  @spec analyze(map(), [map()]) :: [map()]
  def analyze(current, window) do
    for {name, %{value: cur, direction: dir}} <- current.metrics,
        values = window_values(window, name),
        length(values) >= 2,
        median = median(values),
        median > 0,
        sigma = std_dev(values),
        result = regression(dir, cur, median, sigma),
        result != nil do
      Map.merge(result, %{metric: name, current: cur, median: median, sigma: sigma})
    end
  end

  # nil when within tolerance; a map describing the breach otherwise.
  defp regression(:higher_better, cur, median, sigma) do
    drop = median - cur

    if cur < median and drop / median > @pct_threshold and drop > @sigma_multiple * sigma do
      %{direction: :drop, pct: drop / median}
    end
  end

  defp regression(:lower_better, cur, median, sigma) do
    rise = cur - median

    if cur > median and rise / median > @pct_threshold and rise > @sigma_multiple * sigma do
      %{direction: :rise, pct: rise / median}
    end
  end

  defp window_values(window, name) do
    for run <- window, %{value: v} = Map.get(run.metrics, name, %{value: nil}), v != nil, do: v
  end

  # --- run loading ----------------------------------------------------------

  @doc "Load a result dir into `%{sha, produced_at, label, metrics}` (nil if absent/unparseable)."
  @spec load_run(String.t()) :: map() | nil
  def load_run(dir) do
    meta = read_meta(dir)
    bench = read_bench(dir)

    if meta && bench do
      %{
        sha: meta["sha"] || "unknown",
        produced_at: meta["produced_at"] || "",
        label: meta["config_label"] || "",
        metrics: metrics(bench)
      }
    end
  rescue
    _ -> nil
  end

  defp read_meta(dir) do
    path = Path.join(dir, "meta.json")
    if File.exists?(path), do: Jason.decode!(File.read!(path))
  end

  defp read_bench(dir) do
    dir
    |> Path.join("*.json")
    |> Path.wildcard()
    |> Enum.reject(&(Path.basename(&1) == "meta.json"))
    |> List.first()
    |> case do
      nil -> nil
      path -> Jason.decode!(File.read!(path))
    end
  end

  # benchee_json emits a list of scenarios; key each by its "name" ("iface/op").
  defp metrics(scenarios) do
    for %{"name" => name, "run_time_data" => %{"statistics" => stats}} <- scenarios,
        {value, direction} = metric(name, stats),
        is_number(value),
        into: %{} do
      {name, %{value: value, direction: direction}}
    end
  end

  defp metric(name, stats) do
    op = name |> String.split("/") |> List.last()

    cond do
      op in @lower_better -> {stats["average"], :lower_better}
      op in @higher_better -> {stats["ips"], :higher_better}
      true -> {stats["ips"], :higher_better}
    end
  end

  # --- issue body -----------------------------------------------------------

  @doc false
  def issue_body(results, window_runs) do
    current_sha = results |> List.first() |> get_in([Access.key(:run), Access.key(:sha)])
    oldest = window_runs |> Enum.sort_by(& &1.produced_at) |> List.first()
    oldest_sha = (oldest && oldest.sha) || "<oldest-in-window>"

    sections =
      Enum.map_join(results, "\n\n", fn %{run: run, regressions: regs} ->
        rows =
          Enum.map_join(regs, "\n", fn r ->
            "| `#{r.metric}` | #{r.direction} | #{fmt(r.current)} | #{fmt(r.median)} | #{fmt(r.sigma)} | #{Float.round(r.pct * 100, 1)}% |"
          end)

        """
        ### Config: #{label_or(run.label)}

        | Metric | Direction | Current | Window median | σ | Δ vs median |
        | --- | --- | --- | --- | --- | --- |
        #{rows}
        """
      end)

    """
    ## Benchmark regression detected

    Current SHA `#{current_sha}` · window of #{length(window_runs)} runs. A metric
    is flagged when it moves the wrong way by **> 10% and > 2σ** vs the
    rolling-window median (throughput/rate `ips` dropping, latency rising).

    #{sections}

    ### Bisect range

    Each run is SHA-stamped, so the regression is bounded to `#{oldest_sha}..#{current_sha}`:

    ```
    git bisect start #{current_sha} #{oldest_sha}
    git bisect run test-rig/neonfs-rig bench --rev HEAD   # see #1525
    ```

    Bisect stays manual — full-VM benchmarks are too slow to auto-bisect.
    """
  end

  defp label_or(""), do: "(default)"
  defp label_or(label), do: label

  # --- stats ----------------------------------------------------------------

  @doc false
  def median(values) do
    sorted = Enum.sort(values)
    n = length(sorted)
    mid = div(n, 2)

    if rem(n, 2) == 1 do
      Enum.at(sorted, mid)
    else
      (Enum.at(sorted, mid - 1) + Enum.at(sorted, mid)) / 2
    end
  end

  @doc false
  def std_dev(values) do
    n = length(values)
    mean = Enum.sum(values) / n
    variance = Enum.sum(Enum.map(values, &:math.pow(&1 - mean, 2))) / n
    :math.sqrt(variance)
  end

  defp fmt(n) when is_float(n), do: :erlang.float_to_binary(n, decimals: 1)
  defp fmt(n), do: to_string(n)
end
