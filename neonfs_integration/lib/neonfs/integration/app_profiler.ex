defmodule NeonFS.Integration.AppProfiler do
  @moduledoc """
  Per-application start-time profiler for use inside peer test nodes.

  `PeerCluster` currently starts each peer's applications by calling
  `:application.ensure_all_started(:neonfs_core)` — a single RPC that
  hides the cost of each constituent app. This module unwinds that by:

    1. Resolving the full transitive dependency tree from a root app.
    2. Topologically sorting the tree so every app starts after its
       dependencies are up.
    3. Starting each app individually via `:application.start/1` and
       timing the call with `:timer.tc/3`.

  The profiler is intentionally a **peer-node helper** — it must be
  run inside the peer BEAM where `:peer.call(peer, AppProfiler,
  :start_timed, [...])` can drive it. Running it on the test-runner
  BEAM would include apps the runner itself already started (logger,
  ssl, etc.) and produce noise.

  Used by #507 to identify per-app hot-spots in the ~1.4s average
  `start_applications` phase of peer-cluster integration tests.
  """

  @type timing :: %{
          app: atom(),
          duration_us: non_neg_integer(),
          result: :ok | :already_started | :load_failed | {:error, term()}
        }

  @doc """
  Times the individual start of every application in `root_app`'s
  transitive dependency tree, in topological order.

  Returns a list of `timing` maps in the order the apps were started.
  The sum of `duration_us` values is the total start time the caller
  would pay if they invoked `:application.ensure_all_started(root_app)`
  from the same starting state.

  Apps that are already running contribute near-zero durations —
  `:application.start/1` on an already-started app returns
  `{:error, {:already_started, _}}` quickly.
  """
  @spec start_timed(atom()) :: [timing()]
  def start_timed(root_app) do
    deps = topo_sorted_deps(root_app)

    Enum.map(deps, fn app ->
      {duration_us, raw_result} = :timer.tc(:application, :start, [app])

      %{
        app: app,
        duration_us: duration_us,
        result: normalise_result(raw_result)
      }
    end)
  end

  # ─── Dependency tree resolution ───────────────────────────────────

  @doc false
  @spec topo_sorted_deps(atom()) :: [atom()]
  def topo_sorted_deps(root_app) do
    # Collect only apps whose .app file could be loaded — optional
    # dependencies (e.g. `:castore` as an optional dep of `:mint`)
    # can appear in an ancestor's `:applications` list without their
    # .app file being on the code path. Including them would cause
    # `:application.start/1` to fail later with `{:error, {'no such
    # file or directory', 'X.app'}}`.
    apps = collect_deps(root_app, MapSet.new([root_app]))

    graph = :digraph.new()

    try do
      Enum.each(apps, fn app -> :digraph.add_vertex(graph, app) end)

      for app <- apps do
        for dep <- app_deps(app), MapSet.member?(apps, dep) do
          # `dep` must start before `app` → edge from dep to app
          :digraph.add_edge(graph, dep, app)
        end
      end

      case :digraph_utils.topsort(graph) do
        false -> raise "dependency cycle in #{inspect(root_app)} app graph"
        sorted -> sorted
      end
    after
      :digraph.delete(graph)
    end
  end

  defp collect_deps(app, acc) do
    # Load the app if it is not loaded yet so `:application.get_key/2`
    # can see its `:applications` entry. Apps whose .app file cannot
    # be found (optional deps not pulled in by mix) are dropped from
    # the accumulator so we don't try to start them later.
    case load_app(app) do
      :ok -> reduce_deps(app_deps(app), acc)
      {:error, _} -> MapSet.delete(acc, app)
    end
  end

  defp reduce_deps(deps, acc) do
    Enum.reduce(deps, acc, &visit_dep/2)
  end

  defp visit_dep(dep, acc) do
    if MapSet.member?(acc, dep) do
      acc
    else
      collect_deps(dep, MapSet.put(acc, dep))
    end
  end

  defp app_deps(app) do
    case :application.get_key(app, :applications) do
      {:ok, deps} -> deps
      :undefined -> []
    end
  end

  defp load_app(app) do
    case :application.load(app) do
      :ok -> :ok
      {:error, {:already_loaded, _}} -> :ok
      other -> other
    end
  end

  # ─── Result normalisation ─────────────────────────────────────────

  defp normalise_result(:ok), do: :ok
  defp normalise_result({:error, {:already_started, _}}), do: :already_started
  defp normalise_result({:error, reason}), do: {:error, reason}
  defp normalise_result(other), do: other

  @doc """
  Human-readable summary table for a list of timings.

  Apps with `duration_us < threshold_us` (default `1_000`, i.e. 1ms)
  are grouped into a single `<threshold` row to keep the output
  focused. Sorted by duration descending.
  """
  @spec format_summary([timing()], keyword()) :: String.t()
  def format_summary(timings, opts \\ []) do
    threshold_us = Keyword.get(opts, :threshold_us, 1_000)

    total_us = Enum.reduce(timings, 0, &(&2 + &1.duration_us))
    {hot, cold} = Enum.split_with(timings, &(&1.duration_us >= threshold_us))
    hot_sorted = Enum.sort_by(hot, & &1.duration_us, :desc)

    cold_total_us = Enum.reduce(cold, 0, &(&2 + &1.duration_us))

    lines = [
      "app                              duration     result",
      "-------------------------------- ------------ ----------"
    ]

    hot_lines =
      Enum.map(hot_sorted, fn t ->
        [
          String.pad_trailing(to_string(t.app), 32),
          " ",
          String.pad_leading(format_duration(t.duration_us), 12),
          " ",
          format_result(t.result)
        ]
        |> IO.iodata_to_binary()
      end)

    cold_line =
      if cold == [] do
        []
      else
        [
          [
            String.pad_trailing("<#{div(threshold_us, 1000)}ms (#{length(cold)} apps)", 32),
            " ",
            String.pad_leading(format_duration(cold_total_us), 12),
            " ",
            "collapsed"
          ]
          |> IO.iodata_to_binary()
        ]
      end

    total_line =
      [
        "-------------------------------- ------------ ----------",
        [
          String.pad_trailing("TOTAL", 32),
          " ",
          String.pad_leading(format_duration(total_us), 12),
          " ",
          "#{length(timings)} apps"
        ]
        |> IO.iodata_to_binary()
      ]

    (lines ++ hot_lines ++ cold_line ++ total_line)
    |> Enum.join("\n")
  end

  defp format_duration(us) when us >= 1_000_000,
    do: "#{:erlang.float_to_binary(us / 1_000_000, decimals: 2)}s"

  defp format_duration(us) when us >= 1_000,
    do: "#{:erlang.float_to_binary(us / 1_000, decimals: 1)}ms"

  defp format_duration(us), do: "#{us}µs"

  defp format_result(result) when is_atom(result), do: to_string(result)
  defp format_result(result), do: inspect(result)
end
