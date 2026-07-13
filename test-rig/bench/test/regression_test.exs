defmodule NeonFS.Bench.RegressionTest do
  use ExUnit.Case, async: true

  alias NeonFS.Bench.Regression

  defp current(name, value, direction),
    do: %{sha: "cur", metrics: %{name => %{value: value, direction: direction}}}

  defp window(name, values),
    do: Enum.map(values, &%{sha: "w", produced_at: "", metrics: %{name => %{value: &1}}})

  describe "median/1 and std_dev/1" do
    test "median of odd and even counts" do
      assert Regression.median([3, 1, 2]) == 2
      assert Regression.median([1, 2, 3, 4]) == 2.5
    end

    test "std_dev is population standard deviation" do
      assert_in_delta Regression.std_dev([100, 120, 80, 100]), 14.142, 0.01
      assert Regression.std_dev([100, 100, 100]) == 0.0
    end
  end

  describe "analyze/2 — higher-is-better (throughput/rate, ips)" do
    test "flags a drop that exceeds both 10% and 2σ" do
      cur = current("fuse/seq_write", 80.0, :higher_better)
      win = window("fuse/seq_write", [100.0, 100.0, 100.0, 100.0])

      assert [%{metric: "fuse/seq_write", direction: :drop}] = Regression.analyze(cur, win)
    end

    test "ignores a >10% drop that is within 2σ (noisy window)" do
      cur = current("fuse/seq_write", 85.0, :higher_better)
      win = window("fuse/seq_write", [100.0, 120.0, 80.0, 100.0])

      assert Regression.analyze(cur, win) == []
    end

    test "ignores a drop under 10%" do
      cur = current("fuse/seq_write", 95.0, :higher_better)
      win = window("fuse/seq_write", [100.0, 100.0, 100.0, 100.0])

      assert Regression.analyze(cur, win) == []
    end

    test "does not flag an improvement (higher ips)" do
      cur = current("fuse/seq_write", 130.0, :higher_better)
      win = window("fuse/seq_write", [100.0, 100.0, 100.0, 100.0])

      assert Regression.analyze(cur, win) == []
    end
  end

  describe "analyze/2 — lower-is-better (latency, average time)" do
    test "flags a rise that exceeds both 10% and 2σ" do
      cur = current("fuse/stat_list", 13.0, :lower_better)
      win = window("fuse/stat_list", [10.0, 10.0, 10.0, 10.0])

      assert [%{metric: "fuse/stat_list", direction: :rise}] = Regression.analyze(cur, win)
    end

    test "does not flag a latency improvement (lower time)" do
      cur = current("fuse/stat_list", 7.0, :lower_better)
      win = window("fuse/stat_list", [10.0, 10.0, 10.0, 10.0])

      assert Regression.analyze(cur, win) == []
    end
  end

  describe "analyze/2 — insufficient history" do
    test "skips metrics with fewer than two window samples" do
      cur = current("fuse/seq_write", 10.0, :higher_better)
      win = window("fuse/seq_write", [100.0])

      assert Regression.analyze(cur, win) == []
    end

    test "skips metrics absent from the window" do
      cur = current("fuse/seq_write", 10.0, :higher_better)
      win = window("nfs/seq_write", [100.0, 100.0])

      assert Regression.analyze(cur, win) == []
    end
  end

  describe "analyze_runs/2 — per-config grouping" do
    defp run(label, name, value, direction),
      do: %{
        sha: "cur",
        produced_at: "",
        label: label,
        metrics: %{name => %{value: value, direction: direction}}
      }

    defp win_run(label, name, value),
      do: %{sha: "w-#{label}", produced_at: "", label: label, metrics: %{name => %{value: value}}}

    test "compares each config only against the same-config window" do
      current = [run("baseline", "fuse/seq_write", 80.0, :higher_better)]

      window = [
        win_run("baseline", "fuse/seq_write", 100.0),
        win_run("baseline", "fuse/seq_write", 100.0),
        # a durable window that must NOT be mixed into the baseline comparison
        win_run("durable", "fuse/seq_write", 80.0),
        win_run("durable", "fuse/seq_write", 80.0)
      ]

      assert [%{run: %{label: "baseline"}, regressions: [%{metric: "fuse/seq_write"}]}] =
               analyze_runs_via(current, window)
    end

    test "no regression when the same-config window is within tolerance" do
      current = [run("durable", "fuse/seq_write", 80.0, :higher_better)]

      window = [
        win_run("durable", "fuse/seq_write", 80.0),
        win_run("durable", "fuse/seq_write", 82.0)
      ]

      assert analyze_runs_via(current, window) == []
    end
  end

  defp analyze_runs_via(c, w), do: Regression.analyze_runs(c, w)

  describe "load_run/1" do
    setup do
      dir = Path.join(System.tmp_dir!(), "regr_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf(dir) end)
      %{dir: dir}
    end

    test "parses meta.json + benchee JSON into metrics keyed by iface/op", %{dir: dir} do
      File.write!(
        Path.join(dir, "meta.json"),
        Jason.encode!(%{
          sha: "abc123",
          produced_at: "2026-07-13T00:00:00Z",
          config_label: "durable"
        })
      )

      File.write!(
        Path.join(dir, "abc123.json"),
        Jason.encode!([
          %{
            "name" => "fuse/seq_write",
            "run_time_data" => %{"statistics" => %{"ips" => 42.0, "average" => 1000.0}}
          },
          %{
            "name" => "fuse/stat_list",
            "run_time_data" => %{"statistics" => %{"ips" => 500.0, "average" => 2.0}}
          }
        ])
      )

      run = Regression.load_run(dir)

      assert run.sha == "abc123"
      assert run.label == "durable"
      # throughput op gates on ips, latency op gates on average time
      assert run.metrics["fuse/seq_write"] == %{value: 42.0, direction: :higher_better}
      assert run.metrics["fuse/stat_list"] == %{value: 2.0, direction: :lower_better}
    end

    test "returns nil for a dir without a benchee JSON", %{dir: dir} do
      File.write!(Path.join(dir, "meta.json"), Jason.encode!(%{sha: "x"}))
      assert Regression.load_run(dir) == nil
    end

    test "run_dirs finds run dirs nested at any depth (robust to artifact nesting)", %{dir: dir} do
      # mimic a downloaded artifact: results tree nested under extra dirs
      nested = Path.join([dir, "bench-abc", "test-rig", "bench", "results", "abc-baseline-x"])
      File.mkdir_p!(nested)

      File.write!(
        Path.join(nested, "meta.json"),
        Jason.encode!(%{sha: "abc", config_label: "baseline"})
      )

      File.write!(
        Path.join(nested, "abc.json"),
        Jason.encode!([
          %{"name" => "fuse/seq_write", "run_time_data" => %{"statistics" => %{"ips" => 10.0}}}
        ])
      )

      assert [%{sha: "abc", label: "baseline"}] = Regression.run_dirs(dir)
    end
  end
end
