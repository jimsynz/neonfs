defmodule NeonFS.Client.HealthCheck do
  @moduledoc """
  Universal health check framework for all NeonFS node types.

  Each application registers its health checks at startup via `register/2`.
  The runner executes all registered checks in parallel and produces a
  unified report. Works on core, FUSE, NFS, and omnibus nodes.
  """

  @persistent_term_key {__MODULE__, :checks}
  @default_timeout_ms 5_000

  @type status :: :healthy | :degraded | :unhealthy

  @type subsystem_report :: %{required(:status) => status(), optional(atom()) => term()}

  @doc """
  Registers health checks for an application.

  Check names are prefixed with the app name to produce flat atoms
  (e.g., `core_cache`, `fuse_mounts`). Multiple calls merge into the
  existing registry — safe for omnibus mode where apps start sequentially.
  """
  @spec register(atom(), keyword((-> subsystem_report()))) :: :ok
  def register(app, checks) when is_atom(app) and is_list(checks) do
    prefixed =
      Enum.map(checks, fn {name, fun} ->
        {:"#{app}_#{name}", fun}
      end)

    existing = registered_checks()
    :persistent_term.put(@persistent_term_key, existing ++ prefixed)
    :ok
  end

  @doc """
  Returns a health report for all registered subsystems.
  """
  @spec check() :: map()
  def check, do: check([])

  @doc """
  Returns a health report with optional runtime overrides.

  Options:
  - `:timeout_ms` — per-subsystem timeout (default: `5000`)
  - `:checks` — override registered checks (for tests)
  """
  @spec check(keyword()) :: map()
  def check(opts) when is_list(opts) do
    timeout_ms = Keyword.get(opts, :timeout_ms, @default_timeout_ms)
    checks = Keyword.get(opts, :checks, registered_checks())
    subsystem_reports = run_checks(checks, timeout_ms)

    %{
      checked_at: DateTime.utc_now(),
      checks: subsystem_reports,
      node: Node.self(),
      status: overall_status(subsystem_reports)
    }
  end

  @doc """
  Produces a serialised report suitable for RPC transport to the CLI.
  """
  @spec handle_node_status() :: {:ok, map()}
  def handle_node_status do
    report = check()
    {:ok, serialise_report(report)}
  end

  @doc """
  Clears all registered checks. Use in test `on_exit` callbacks.
  """
  @spec reset() :: :ok
  def reset do
    :persistent_term.erase(@persistent_term_key)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Normalises a term for JSON serialisation over Erlang distribution.

  Converts `DateTime` to ISO 8601, structs to maps, atoms to strings,
  and tuples to their `inspect` representation.
  """
  @spec normalise_for_json(term()) :: term()
  def normalise_for_json(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  def normalise_for_json(%_{} = struct) do
    struct
    |> Map.from_struct()
    |> normalise_for_json()
  end

  def normalise_for_json(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {normalise_for_json(key), normalise_for_json(value)} end)
  end

  def normalise_for_json(list) when is_list(list), do: Enum.map(list, &normalise_for_json/1)

  def normalise_for_json(atom) when is_atom(atom) and atom not in [nil, true, false],
    do: Atom.to_string(atom)

  def normalise_for_json(tuple) when is_tuple(tuple), do: inspect(tuple)
  def normalise_for_json(value), do: value

  # Private

  defp registered_checks do
    :persistent_term.get(@persistent_term_key, [])
  end

  defp run_checks(checks, timeout_ms) do
    checks
    |> Enum.zip(
      Task.async_stream(
        checks,
        fn {name, fun} -> {name, safe_check(fun)} end,
        ordered: true,
        on_timeout: :kill_task,
        timeout: timeout_ms
      )
    )
    |> Enum.reduce(%{}, fn
      {{name, _fun}, {:ok, {_name, report}}}, acc ->
        Map.put(acc, name, report)

      {{name, _fun}, {:exit, :timeout}}, acc ->
        Map.put(acc, name, %{status: :unhealthy, reason: :timeout})

      {{name, _fun}, {:exit, reason}}, acc ->
        Map.put(acc, name, %{status: :unhealthy, reason: {:exit, inspect(reason)}})
    end)
  end

  defp safe_check(fun) do
    case fun.() do
      %{status: status} = report when status in [:healthy, :degraded, :unhealthy] ->
        report

      report ->
        %{status: :unhealthy, reason: {:invalid_response, inspect(report)}}
    end
  rescue
    error ->
      %{status: :unhealthy, reason: {:exception, Exception.message(error)}}
  catch
    kind, reason ->
      %{status: :unhealthy, reason: {kind, inspect(reason)}}
  end

  defp overall_status(subsystem_reports) do
    statuses = subsystem_reports |> Map.values() |> Enum.map(& &1.status)

    cond do
      :unhealthy in statuses -> :unhealthy
      :degraded in statuses -> :degraded
      true -> :healthy
    end
  end

  defp serialise_report(report) do
    %{
      node: normalise_for_json(report.node),
      status: normalise_for_json(report.status),
      checked_at: normalise_for_json(report.checked_at),
      checks:
        Map.new(report.checks, fn {name, check} ->
          {normalise_for_json(name), normalise_for_json(check)}
        end)
    }
  end
end
