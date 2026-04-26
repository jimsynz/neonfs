defmodule NeonFS.TestSupport.SupervisorStartTimer do
  @moduledoc """
  Test-only collector for `NeonFS.Core.Supervisor`'s per-child start
  telemetry (#510).

  `NeonFS.Core.Supervisor` wraps every child spec with a telemetry span
  rooted at `[:neonfs, :core, :supervisor, :child]`. This module attaches
  a handler that accumulates the per-child durations in a peer-local ETS
  table, which can then be read back via `:peer.call`.

  Usage (from an integration test):

      # Install the collector on a peer BEFORE :neonfs_core starts.
      :peer.call(peer, SupervisorStartTimer, :install, [])

      :peer.call(peer, :application, :ensure_all_started, [:neonfs_core])

      timings = :peer.call(peer, SupervisorStartTimer, :timings, [])

  `timings/0` returns `[%{id: child_id, duration_us: non_neg_integer()}]`
  sorted by duration descending. The list reflects every child that
  started after `install/0` was called; children already running at
  install time are not captured.
  """

  @handler_id {__MODULE__, :child_timer}
  @event [:neonfs, :core, :supervisor, :child]
  @term_key {__MODULE__, :timings}

  @doc """
  Reset the timings list and attach the telemetry handler.

  Idempotent; safe to call multiple times. Ensures `:telemetry` is
  started. Uses `:persistent_term` for storage because the peer-side
  invocation of this function happens from a short-lived `:peer.call`
  process — an ETS table owned by that process would be destroyed the
  moment the call returned, and the telemetry handler (invoked later,
  from supervisor processes) would fail on insert.
  """
  @spec install() :: :ok
  def install do
    {:ok, _} = Application.ensure_all_started(:telemetry)

    :persistent_term.put(@term_key, [])

    # `:telemetry.detach` with a non-existent handler returns
    # `{:error, :not_found}` — we don't care, we just want a clean
    # slate before attaching.
    _ = :telemetry.detach(@handler_id)

    :ok =
      :telemetry.attach(
        @handler_id,
        @event ++ [:stop],
        &__MODULE__.handle_event/4,
        nil
      )
  end

  @doc "Detach the handler and drop the timings."
  @spec uninstall() :: :ok
  def uninstall do
    _ = :telemetry.detach(@handler_id)
    _ = :persistent_term.erase(@term_key)
    :ok
  end

  @doc """
  Return the accumulated per-child timings, sorted by duration desc.
  """
  @spec timings() :: [%{id: term(), duration_us: non_neg_integer()}]
  def timings do
    @term_key
    |> :persistent_term.get([])
    |> Enum.map(fn {id, duration_us} -> %{id: id, duration_us: duration_us} end)
    |> Enum.sort_by(& &1.duration_us, :desc)
  end

  @doc false
  def handle_event(_event, measurements, metadata, _config) do
    duration_us =
      System.convert_time_unit(measurements.duration, :native, :microsecond)

    id = Map.get(metadata, :id, :unknown)
    current = :persistent_term.get(@term_key, [])
    :persistent_term.put(@term_key, [{id, duration_us} | current])
  end

  @doc """
  Render a human-readable sorted table of the current timings.
  """
  @spec format_summary([%{id: term(), duration_us: non_neg_integer()}]) :: String.t()
  def format_summary(timings) do
    total_us = Enum.reduce(timings, 0, &(&2 + &1.duration_us))

    lines = [
      "child                                          duration     %",
      "---------------------------------------------- ------------ ------"
    ]

    rows =
      Enum.map(timings, fn t ->
        pct = if total_us > 0, do: t.duration_us * 100 / total_us, else: 0.0

        [
          String.pad_trailing(format_id(t.id), 46),
          " ",
          String.pad_leading(format_duration(t.duration_us), 12),
          " ",
          String.pad_leading(:erlang.float_to_binary(pct, decimals: 1) <> "%", 6)
        ]
        |> IO.iodata_to_binary()
      end)

    footer = [
      "---------------------------------------------- ------------ ------",
      [
        String.pad_trailing("TOTAL (#{length(timings)} children)", 46),
        " ",
        String.pad_leading(format_duration(total_us), 12),
        " ",
        String.pad_leading("100.0%", 6)
      ]
      |> IO.iodata_to_binary()
    ]

    (lines ++ rows ++ footer) |> Enum.join("\n")
  end

  defp format_id(id) when is_atom(id), do: Atom.to_string(id)
  defp format_id(id), do: inspect(id)

  defp format_duration(us) when us >= 1_000_000,
    do: "#{:erlang.float_to_binary(us / 1_000_000, decimals: 2)}s"

  defp format_duration(us) when us >= 1_000,
    do: "#{:erlang.float_to_binary(us / 1_000, decimals: 1)}ms"

  defp format_duration(us), do: "#{us}µs"
end
