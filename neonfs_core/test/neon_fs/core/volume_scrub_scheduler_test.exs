defmodule NeonFS.Core.VolumeScrubSchedulerTest do
  # Stubs share `:persistent_term` keyed by `:vscrub_test_tid` so the
  # tests must be sequential.
  use ExUnit.Case, async: false

  alias NeonFS.Core.{Job, VolumeScrubScheduler}
  alias NeonFS.Core.Job.Runners.Scrub
  alias NeonFS.Core.Volume.RootSegment

  defmodule Store do
    @moduledoc false

    @spec init() :: :ets.tid()
    def init, do: :ets.new(:vscrub_test_store, [:set, :public])

    @spec put_segment(:ets.tid(), binary(), RootSegment.t()) :: true
    def put_segment(tid, volume_id, segment),
      do: :ets.insert(tid, {{:segment, volume_id}, segment})

    @spec get_segment(:ets.tid(), binary()) :: RootSegment.t() | nil
    def get_segment(tid, volume_id) do
      case :ets.lookup(tid, {:segment, volume_id}) do
        [{{:segment, ^volume_id}, segment}] -> segment
        [] -> nil
      end
    end

    @spec append_write(:ets.tid(), map()) :: true
    def append_write(tid, entry) do
      :ets.insert(tid, {:writes, get_writes(tid) ++ [entry]})
    end

    @spec get_writes(:ets.tid()) :: [map()]
    def get_writes(tid) do
      case :ets.lookup(tid, :writes) do
        [{:writes, log}] -> log
        [] -> []
      end
    end

    @spec put_volumes(:ets.tid(), [map()]) :: true
    def put_volumes(tid, volumes), do: :ets.insert(tid, {:volumes, volumes})

    @spec get_volumes(:ets.tid()) :: [map()]
    def get_volumes(tid) do
      case :ets.lookup(tid, :volumes) do
        [{:volumes, vols}] -> vols
        [] -> []
      end
    end

    @spec put_running_jobs(:ets.tid(), [Job.t()]) :: true
    def put_running_jobs(tid, jobs), do: :ets.insert(tid, {:running_jobs, jobs})

    @spec get_running_jobs(:ets.tid()) :: [Job.t()]
    def get_running_jobs(tid) do
      case :ets.lookup(tid, :running_jobs) do
        [{:running_jobs, jobs}] -> jobs
        [] -> []
      end
    end

    @spec append_created_job(:ets.tid(), Job.t()) :: true
    def append_created_job(tid, job),
      do: :ets.insert(tid, {:created_jobs, get_created_jobs(tid) ++ [job]})

    @spec get_created_jobs(:ets.tid()) :: [Job.t()]
    def get_created_jobs(tid) do
      case :ets.lookup(tid, :created_jobs) do
        [{:created_jobs, jobs}] -> jobs
        [] -> []
      end
    end
  end

  defmodule Reader do
    @moduledoc false

    @spec resolve_segment_for_write(binary(), keyword()) ::
            {:ok, RootSegment.t(), map()} | {:error, :not_found}
    def resolve_segment_for_write(volume_id, opts) do
      tid = Keyword.fetch!(opts, :tid)

      case Store.get_segment(tid, volume_id) do
        nil -> {:error, :not_found}
        segment -> {:ok, segment, %{root_chunk_hash: <<0::256>>, drive_locations: []}}
      end
    end
  end

  defmodule Writer do
    @moduledoc false

    @spec update_schedule(binary(), atom(), map(), keyword()) ::
            {:ok, binary()} | {:error, :not_found}
    def update_schedule(volume_id, key, schedule, opts) do
      tid = Keyword.fetch!(opts, :tid)
      Store.append_write(tid, %{volume_id: volume_id, key: key, schedule: schedule})

      case Store.get_segment(tid, volume_id) do
        nil ->
          {:error, :not_found}

        segment ->
          updated = %{segment | schedules: Map.put(segment.schedules, key, schedule)}
          Store.put_segment(tid, volume_id, updated)
          {:ok, <<1::256>>}
      end
    end
  end

  defmodule Registry do
    @moduledoc false

    @spec list() :: [map()]
    def list do
      tid = :persistent_term.get(:vscrub_test_tid)
      Store.get_volumes(tid)
    end
  end

  defmodule JobTracker do
    @moduledoc false

    @spec list(keyword()) :: [Job.t()]
    def list(filters) do
      tid = :persistent_term.get(:vscrub_test_tid)

      Store.get_running_jobs(tid)
      |> filter_status(Keyword.get(filters, :status))
      |> filter_type(Keyword.get(filters, :type))
    end

    @spec create(module(), map()) :: {:ok, Job.t()}
    def create(type, params) do
      tid = :persistent_term.get(:vscrub_test_tid)

      job = %Job{
        id: "mock-#{System.unique_integer([:positive])}",
        type: type,
        params: params,
        status: :running,
        node: Node.self(),
        progress: %{total: 0, completed: 0, description: nil},
        state: %{},
        created_at: DateTime.utc_now(),
        started_at: DateTime.utc_now(),
        updated_at: DateTime.utc_now()
      }

      Store.append_created_job(tid, job)
      {:ok, job}
    end

    defp filter_status(jobs, nil), do: jobs
    defp filter_status(jobs, s) when is_atom(s), do: Enum.filter(jobs, &(&1.status == s))

    defp filter_type(jobs, nil), do: jobs
    defp filter_type(jobs, t), do: Enum.filter(jobs, &(&1.type == t))
  end

  defp segment(volume_id, scrub_schedule) do
    base =
      RootSegment.new(
        volume_id: volume_id,
        volume_name: "vol-#{volume_id}",
        cluster_id: "clust-test",
        cluster_name: "test-cluster",
        durability: %{type: :replicate, factor: 1, min_copies: 1}
      )

    %{base | schedules: Map.put(base.schedules, :scrub, scrub_schedule)}
  end

  defp setup_scheduler(opts) do
    tid = Store.init()
    Store.put_volumes(tid, opts[:volumes] || [])

    for {volume_id, segment} <- opts[:segments] || %{} do
      Store.put_segment(tid, volume_id, segment)
    end

    :persistent_term.put(:vscrub_test_tid, tid)

    on_exit(fn ->
      try do
        :persistent_term.erase(:vscrub_test_tid)
      rescue
        ArgumentError -> :ok
      end
    end)

    name = :"VScrubSched_#{System.unique_integer([:positive])}"

    {:ok, pid} =
      VolumeScrubScheduler.start_link(
        name: name,
        tick_interval_ms: opts[:tick_interval_ms] || 50,
        job_tracker_mod: JobTracker,
        volume_registry_mod: Registry,
        metadata_reader_mod: Reader,
        metadata_writer_mod: Writer,
        metadata_reader_opts: [tid: tid],
        metadata_writer_opts: [tid: tid]
      )

    %{pid: pid, name: name, tid: tid}
  end

  defp attach_telemetry(events) do
    parent = self()
    handler_id = "vscrub-test-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      events,
      fn event, measurements, metadata, _config ->
        send(parent, {event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    handler_id
  end

  describe "scheduled tick" do
    test "dispatches Scrub per due volume and updates last_run" do
      attach_telemetry([[:neonfs, :volume_scrub_scheduler, :triggered]])

      ctx =
        setup_scheduler(
          volumes: [%{id: "vol-due"}],
          segments: %{"vol-due" => segment("vol-due", %{interval_ms: 1, last_run: nil})}
        )

      assert_receive {[:neonfs, :volume_scrub_scheduler, :triggered], _,
                      %{volume_id: "vol-due", reason: :scheduled}},
                     2_000

      [job] = Store.get_created_jobs(ctx.tid)
      assert job.type == Scrub
      assert job.params == %{volume_id: "vol-due"}

      writes = Store.get_writes(ctx.tid)

      assert Enum.any?(writes, fn entry ->
               entry.volume_id == "vol-due" and entry.key == :scrub
             end)
    end

    test "skips a volume whose interval hasn't elapsed" do
      attach_telemetry([[:neonfs, :volume_scrub_scheduler, :triggered]])

      not_due_segment =
        segment("vol-not-due", %{interval_ms: 1_000_000_000, last_run: DateTime.utc_now()})

      _ctx =
        setup_scheduler(
          volumes: [%{id: "vol-not-due"}],
          segments: %{"vol-not-due" => not_due_segment}
        )

      refute_receive {[:neonfs, :volume_scrub_scheduler, :triggered], _,
                      %{volume_id: "vol-not-due"}},
                     500
    end

    test "does not double-dispatch when a Scrub job for the same volume is running" do
      attach_telemetry([
        [:neonfs, :volume_scrub_scheduler, :triggered],
        [:neonfs, :volume_scrub_scheduler, :skipped]
      ])

      ctx =
        setup_scheduler(
          volumes: [%{id: "vol-busy"}],
          segments: %{"vol-busy" => segment("vol-busy", %{interval_ms: 1, last_run: nil})}
        )

      Store.put_running_jobs(ctx.tid, [
        %Job{
          id: "running-1",
          type: Scrub,
          params: %{volume_id: "vol-busy"},
          status: :running,
          node: Node.self(),
          progress: %{total: 0, completed: 0, description: nil},
          state: %{},
          created_at: DateTime.utc_now(),
          started_at: DateTime.utc_now(),
          updated_at: DateTime.utc_now()
        }
      ])

      assert_receive {[:neonfs, :volume_scrub_scheduler, :skipped], _,
                      %{volume_id: "vol-busy", reason: :already_running}},
                     2_000

      assert Store.get_created_jobs(ctx.tid) == []
    end
  end

  describe "trigger_now/2" do
    test "dispatches an immediate Scrub job for the named volume" do
      attach_telemetry([[:neonfs, :volume_scrub_scheduler, :triggered]])

      ctx =
        setup_scheduler(
          tick_interval_ms: :timer.hours(24),
          volumes: [%{id: "vol-manual"}],
          segments: %{"vol-manual" => segment("vol-manual", %{interval_ms: 1, last_run: nil})}
        )

      assert {:ok, _job} = VolumeScrubScheduler.trigger_now("vol-manual", ctx.name)

      assert_receive {[:neonfs, :volume_scrub_scheduler, :triggered], _,
                      %{volume_id: "vol-manual", reason: :manual}},
                     1_000
    end
  end
end
