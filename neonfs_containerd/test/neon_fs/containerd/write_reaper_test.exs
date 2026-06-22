defmodule NeonFS.Containerd.WriteReaperTest do
  @moduledoc """
  Tests that `WriteReaper` periodically sweeps stalled `WriteSession`s
  via `WriteSession.abort_stale/1` (#1354). The session registry /
  supervisor are started globally in `test_helper.exs`; each test
  starts its own reaper with a tight interval so the sweep fires
  promptly, and ages a session's `updated_at` into the past so the
  sweep targets exactly that session without sleeping a real day.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Containerd.{StubChunkWriter, WriteReaper, WriteSession, WriteSupervisor}

  @sweep [:neonfs, :containerd, :write_reaper, :sweep]

  setup do
    StubChunkWriter.start({:ok, []})
    Application.put_env(:neonfs_containerd, :volume, "containerd-test")

    on_exit(fn ->
      StubChunkWriter.stop()
      Application.delete_env(:neonfs_containerd, :volume)
    end)

    :ok
  end

  test "the scheduled sweep reclaims a session idle beyond the max age" do
    ref = "reaper-stale-#{System.unique_integer([:positive])}"
    {:ok, pid} = start_session(ref)
    WriteSession.feed(pid, "x")

    age_session(pid, -100_000)

    tel = :telemetry_test.attach_event_handlers(self(), [@sweep])
    start_supervised!({WriteReaper, interval_ms: 20, max_age_seconds: 86_400})

    assert_receive {@sweep, ^tel, %{reaped_count: count}, %{refs: refs}}, 1_000
    assert ref in refs
    assert count >= 1
    refute Process.alive?(pid)
  end

  test "an actively-fed session is left running across sweeps" do
    ref = "reaper-fresh-#{System.unique_integer([:positive])}"
    {:ok, pid} = start_session(ref)
    WriteSession.feed(pid, "y")

    tel = :telemetry_test.attach_event_handlers(self(), [@sweep])
    start_supervised!({WriteReaper, interval_ms: 20, max_age_seconds: 86_400})

    # Two sweeps elapse; the fresh session never enters the cutoff.
    assert_receive {@sweep, ^tel, _, %{refs: refs1}}, 1_000
    assert_receive {@sweep, ^tel, _, %{refs: refs2}}, 1_000
    refute ref in refs1
    refute ref in refs2
    assert Process.alive?(pid)

    _ = WriteSession.abort(pid)
  end

  defp start_session(ref) do
    WriteSupervisor.start_session(ref, ref: ref, chunk_writer_module: StubChunkWriter)
  end

  defp age_session(pid, delta_seconds) do
    :sys.replace_state(pid, fn state ->
      %{state | updated_at: DateTime.add(DateTime.utc_now(), delta_seconds, :second)}
    end)
  end
end
