defmodule NeonFS.Core.ReplicaAuditSchedulerTest do
  @moduledoc """
  Unit tests for the periodic driver behind `ReplicaAudit.audit/0`.

  The audit itself is stubbed — its behaviour is covered by
  `replica_audit_test.exs`. What matters here is that a tick runs it, that a
  tick which cannot safely run says so instead of running anyway, and that a
  slow run is never overlapped by the next tick.
  """

  # The stub modules read their behaviour from `:persistent_term` under
  # fixed keys, so the tests must be sequential.
  use ExUnit.Case, async: false

  alias NeonFS.Core.ReplicaAuditScheduler

  @events [
    [:neonfs, :replica_audit_scheduler, :completed],
    [:neonfs, :replica_audit_scheduler, :failed],
    [:neonfs, :replica_audit_scheduler, :skipped]
  ]

  @audit_key {__MODULE__, :audit_fun}
  @ra_key {__MODULE__, :ra_initialized}
  @recovering_key {__MODULE__, :recovering}

  defmodule AuditStub do
    @moduledoc false
    @spec audit() :: {:ok, map()} | {:error, term()}
    def audit, do: :persistent_term.get({NeonFS.Core.ReplicaAuditSchedulerTest, :audit_fun}).()
  end

  defmodule RaStub do
    @moduledoc false
    @spec initialized?() :: boolean()
    def initialized?,
      do: :persistent_term.get({NeonFS.Core.ReplicaAuditSchedulerTest, :ra_initialized})
  end

  defmodule ModeStub do
    @moduledoc false
    @spec recovering?() :: boolean()
    def recovering?,
      do: :persistent_term.get({NeonFS.Core.ReplicaAuditSchedulerTest, :recovering})
  end

  setup do
    :persistent_term.put(@ra_key, true)
    :persistent_term.put(@recovering_key, false)
    :persistent_term.put(@audit_key, fn -> {:ok, empty_report()} end)

    on_exit(fn ->
      Enum.each([@audit_key, @ra_key, @recovering_key], &:persistent_term.erase/1)
    end)

    {:ok, ref: :telemetry_test.attach_event_handlers(self(), @events)}
  end

  defp empty_report, do: %{volumes: [], under_replicated: [], sole_copy_drives: []}

  defp start_scheduler(opts \\ []) do
    defaults = [
      name: nil,
      interval_ms: 30,
      audit_mod: AuditStub,
      ra_server_mod: RaStub,
      cluster_mode_mod: ModeStub
    ]

    start_supervised!({ReplicaAuditScheduler, Keyword.merge(defaults, opts)}, restart: :temporary)
  end

  test "a tick runs the audit and reports what it found", %{ref: ref} do
    test_pid = self()

    :persistent_term.put(@audit_key, fn ->
      send(test_pid, :audited)
      {:ok, %{volumes: [:v1, :v2], under_replicated: [:v1], sole_copy_drives: []}}
    end)

    start_scheduler()

    assert_receive :audited, 2_000

    assert_receive {[:neonfs, :replica_audit_scheduler, :completed], ^ref,
                    %{volume_count: 2, under_replicated_count: 1}, _},
                   2_000
  end

  test "skips and says so when Ra is unavailable", %{ref: ref} do
    test_pid = self()
    :persistent_term.put(@ra_key, false)
    :persistent_term.put(@audit_key, fn -> send(test_pid, :audited) && {:ok, empty_report()} end)

    start_scheduler()

    assert_receive {[:neonfs, :replica_audit_scheduler, :skipped], ^ref, _,
                    %{reason: :ra_unavailable}},
                   2_000

    refute_received :audited
  end

  test "skips while the cluster is recovering", %{ref: ref} do
    :persistent_term.put(@recovering_key, true)

    start_scheduler()

    assert_receive {[:neonfs, :replica_audit_scheduler, :skipped], ^ref, _,
                    %{reason: :recovering}},
                   2_000
  end

  # The traversal reads every volume's chunk tree, so a run that outlasts the
  # interval must not have a second one started on top of it.
  test "a tick arriving mid-run is skipped, not queued", %{ref: ref} do
    test_pid = self()

    :persistent_term.put(@audit_key, fn ->
      send(test_pid, :started)

      receive do
        :release -> {:ok, empty_report()}
      end
    end)

    start_scheduler()

    assert_receive :started, 2_000

    assert_receive {[:neonfs, :replica_audit_scheduler, :skipped], ^ref, _,
                    %{reason: :already_running}},
                   2_000

    refute_received :started, "the audit must not have been entered a second time"
  end

  test "an audit that raises does not wedge the scheduler", %{ref: ref} do
    counter = :counters.new(1, [])

    :persistent_term.put(@audit_key, fn ->
      :counters.add(counter, 1, 1)

      if :counters.get(counter, 1) == 1 do
        raise "boom"
      else
        {:ok, empty_report()}
      end
    end)

    start_scheduler()

    assert_receive {[:neonfs, :replica_audit_scheduler, :failed], ^ref, _, _}, 2_000

    # A scheduler that left `task` set would skip every later tick as
    # `:already_running` and go permanently quiet — the exact failure this
    # scheduler exists to prevent.
    assert_receive {[:neonfs, :replica_audit_scheduler, :completed], ^ref, _, _}, 3_000
  end

  test "an audit returning an error is reported, not raised", %{ref: ref} do
    :persistent_term.put(@audit_key, fn -> {:error, :volume_registry_unavailable} end)

    start_scheduler()

    assert_receive {[:neonfs, :replica_audit_scheduler, :failed], ^ref, _,
                    %{reason: :volume_registry_unavailable}},
                   2_000
  end

  test "enabled: false schedules no ticks at all", %{ref: ref} do
    test_pid = self()
    :persistent_term.put(@audit_key, fn -> send(test_pid, :audited) && {:ok, empty_report()} end)

    start_scheduler(enabled: false, interval_ms: 20)

    refute_receive {[:neonfs, :replica_audit_scheduler, _], ^ref, _, _}, 300
    refute_received :audited
  end

  test "status/1 reports the configured interval and whether a run is in flight" do
    scheduler = start_scheduler(interval_ms: 60_000)

    assert %{interval_ms: 60_000, enabled?: true, running?: false} =
             ReplicaAuditScheduler.status(scheduler)
  end
end
