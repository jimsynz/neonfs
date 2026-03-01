defmodule NeonFS.Core.TelemetryPollerTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.TelemetryPoller

  describe "polling" do
    test "measurement callbacks emit expected telemetry events" do
      test_pid = self()

      handler_id = "telemetry-poller-test-#{System.unique_integer([:positive])}"

      :telemetry.attach_many(
        handler_id,
        [
          [:neonfs, :cache, :size],
          [:neonfs, :cluster, :nodes],
          [:neonfs, :cluster, :ra],
          [:neonfs, :storage, :drive_state],
          [:neonfs, :storage, :utilisation],
          [:neonfs, :worker, :queue_depth]
        ],
        fn event, measurements, metadata, pid ->
          send(pid, {:telemetry_event, event, measurements, metadata})
        end,
        test_pid
      )

      on_exit(fn ->
        :telemetry.detach(handler_id)
      end)

      opts =
        [
          background_worker_mod: __MODULE__.MockBackgroundWorker,
          chunk_cache_mod: __MODULE__.MockChunkCache,
          ra_mod: __MODULE__.MockRa,
          ra_supervisor_mod: __MODULE__.MockRaSupervisor,
          storage_metrics_mod: __MODULE__.MockStorageMetrics
        ]

      TelemetryPoller.measure_storage(opts)
      TelemetryPoller.measure_cache(opts)
      TelemetryPoller.measure_cluster(opts)
      TelemetryPoller.measure_worker(opts)

      events = drain_events([])

      assert event_emitted?(events, [:neonfs, :storage, :utilisation], &(&1.used == 400))

      assert event_emitted?(events, [:neonfs, :storage, :drive_state], fn measurements ->
               measurements.state == 1
             end)

      assert event_emitted?(events, [:neonfs, :cache, :size], fn measurements ->
               measurements.bytes == 2048 and measurements.entry_count == 3
             end)

      assert event_emitted?(events, [:neonfs, :cluster, :nodes], &(&1.count >= 1))

      assert event_emitted?(events, [:neonfs, :cluster, :ra], fn measurements ->
               measurements.term == 42 and measurements.leader == 1
             end)

      assert event_emitted?(events, [:neonfs, :worker, :queue_depth], &(&1.count == 1))
      assert event_emitted?(events, [:neonfs, :worker, :queue_depth], &(&1.count == 2))
      assert event_emitted?(events, [:neonfs, :worker, :queue_depth], &(&1.count == 3))
    end

    test "survives when queried subsystem is down" do
      tel_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :telemetry_poller, :measurement]
        ])

      {:ok, pid} =
        TelemetryPoller.start_link(
          period: 20,
          background_worker_mod: __MODULE__.DownBackgroundWorker,
          chunk_cache_mod: __MODULE__.DownChunkCache,
          ra_mod: __MODULE__.DownRa,
          ra_supervisor_mod: __MODULE__.DownRaSupervisor,
          storage_metrics_mod: __MODULE__.DownStorageMetrics
        )

      on_exit(fn ->
        Application.delete_env(:neonfs_core, :telemetry_poller_runtime_opts)
      end)

      ref = Process.monitor(pid)

      # Wait for several measurement cycles to complete (proves process survives)
      for _ <- 1..3 do
        assert_receive {[:neonfs, :telemetry_poller, :measurement], ^tel_ref, _, _}, 1_000
      end

      refute_received {:DOWN, ^ref, :process, ^pid, _reason}
      assert Process.alive?(pid)

      Process.exit(pid, :normal)
    end
  end

  defp drain_events(acc) do
    receive do
      {:telemetry_event, event, measurements, metadata} ->
        drain_events([{event, measurements, metadata} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp event_emitted?(events, event_name, measurement_matcher) do
    Enum.any?(events, fn {event, measurements, _metadata} ->
      event == event_name and measurement_matcher.(measurements)
    end)
  end

  defmodule MockBackgroundWorker do
    def status do
      %{by_priority: %{high: 1, low: 3, normal: 2}}
    end
  end

  defmodule MockChunkCache do
    def entry_count, do: 3
    def stats, do: %{memory_used: 2048}
  end

  defmodule MockRa do
    def key_metrics(_server_id), do: %{membership: :leader, term: 42}
  end

  defmodule MockRaSupervisor do
    def server_id, do: {:neonfs_meta, :node@localhost}
  end

  defmodule MockStorageMetrics do
    def cluster_capacity do
      %{
        drives: [
          %{
            capacity_bytes: 1_000,
            drive_id: "d1",
            node: :node@localhost,
            state: :active,
            tier: :hot,
            used_bytes: 400
          }
        ]
      }
    end
  end

  defmodule DownBackgroundWorker do
    def status, do: raise("background worker unavailable")
  end

  defmodule DownChunkCache do
    def stats, do: raise("chunk cache unavailable")
  end

  defmodule DownRa do
    def key_metrics(_server_id), do: raise("ra unavailable")
  end

  defmodule DownRaSupervisor do
    def server_id, do: raise("ra supervisor unavailable")
  end

  defmodule DownStorageMetrics do
    def cluster_capacity, do: raise("storage metrics unavailable")
  end
end
