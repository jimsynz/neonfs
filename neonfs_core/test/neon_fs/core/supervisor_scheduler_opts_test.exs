defmodule NeonFS.Core.SupervisorSchedulerOptsTest do
  # Serial deliberately, and the one case where mutating application
  # environment in a test is the right thing: what is under test *is*
  # `NeonFS.Core.Supervisor` reading its scheduler intervals from that
  # environment, so there is nothing to pass explicitly instead. Kept out of
  # `supervisor_test.exs` so the rest of the supervisor's specs stay
  # `async: true` rather than being serialised by this file's needs.
  use ExUnit.Case, async: false

  alias NeonFS.Core.{GCScheduler, ScrubScheduler}
  alias NeonFS.Core.Supervisor, as: CoreSupervisor

  @keys [
    :gc_interval_ms,
    :gc_pressure_threshold,
    :gc_pressure_check_interval_ms,
    :scrub_check_interval_ms
  ]

  setup do
    on_exit(fn ->
      for key <- @keys, do: Application.delete_env(:neonfs_core, key)
    end)

    :ok
  end

  describe "GCScheduler options" do
    test "carry the configured values" do
      Application.put_env(:neonfs_core, :gc_interval_ms, 1_234)
      Application.put_env(:neonfs_core, :gc_pressure_threshold, 0.42)
      Application.put_env(:neonfs_core, :gc_pressure_check_interval_ms, 567)

      opts = scheduler_opts(GCScheduler)

      assert opts[:interval_ms] == 1_234
      assert opts[:pressure_threshold] == 0.42
      assert opts[:pressure_check_interval_ms] == 567
    end

    # The defaults are the operative configuration in every deployment that
    # does not override them, so a wrong one is a silent behaviour change
    # rather than a crash — a daily GC quietly becoming an hourly one.
    test "fall back to the shipped defaults when unset" do
      opts = scheduler_opts(GCScheduler)

      assert opts[:interval_ms] == 86_400_000
      assert opts[:pressure_threshold] == 0.85
      assert opts[:pressure_check_interval_ms] == 300_000
    end
  end

  describe "ScrubScheduler options" do
    test "carry the configured value" do
      Application.put_env(:neonfs_core, :scrub_check_interval_ms, 9_876)

      assert scheduler_opts(ScrubScheduler)[:check_interval_ms] == 9_876
    end

    test "falls back to the shipped default when unset" do
      assert scheduler_opts(ScrubScheduler)[:check_interval_ms] == 3_600_000
    end
  end

  # Every child goes through `timed_start/4` for its startup telemetry, so the
  # options a scheduler will actually receive are nested inside the spec's MFA
  # rather than sitting beside the module. Matching the whole shape means a
  # change to that wrapping fails here rather than silently reading the wrong
  # element.
  defp scheduler_opts(module) do
    %{start: {CoreSupervisor, :timed_start, [^module, ^module, :start_link, [opts]]}} =
      CoreSupervisor.child_spec_for(module)

    opts
  end
end
