defmodule NeonFS.Core.VolumeGCSchedulerTest do
  # Stubs share `:persistent_term` keyed by `:vgc_test_tid` for the
  # current run, so the tests must be sequential.
  use ExUnit.Case, async: false

  alias NeonFS.Core.{Job, VolumeGCScheduler}
  alias NeonFS.Core.Job.Runners.GarbageCollection
  alias NeonFS.Core.Volume.RootSegment

  # ─── Static test stubs ─────────────────────────────────────────────
  #
  # The scheduler hits `metadata_reader_mod` / `metadata_writer_mod` /
  # `volume_registry_mod` / `job_tracker_mod` / `storage_metrics_mod` as
  # plain modules (no GenServer registry indirection). To stay async-
  # safe each test passes a unique ETS table name through opts; the
  # stub modules take that name from the opts/config they receive and
  # delegate to the table. No dynamic `Module.create/3` shenanigans.

  defmodule Store do
    @moduledoc false

    @spec init() :: :ets.tid()
    def init do
      :ets.new(:vgc_test_store, [:set, :public])
    end

    @spec put_segment(:ets.tid(), binary(), RootSegment.t()) :: true
    def put_segment(tid, volume_id, segment) do
      :ets.insert(tid, {{:segment, volume_id}, segment})
    end

    @spec get_segment(:ets.tid(), binary()) :: RootSegment.t() | nil
    def get_segment(tid, volume_id) do
      case :ets.lookup(tid, {:segment, volume_id}) do
        [{{:segment, ^volume_id}, segment}] -> segment
        [] -> nil
      end
    end

    @spec append_write(:ets.tid(), map()) :: true
    def append_write(tid, entry) do
      log = get_writes(tid)
      :ets.insert(tid, {:writes, log ++ [entry]})
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

    @spec put_capacity(:ets.tid(), map()) :: true
    def put_capacity(tid, capacity), do: :ets.insert(tid, {:capacity, capacity})

    @spec get_capacity(:ets.tid()) :: map()
    def get_capacity(tid) do
      case :ets.lookup(tid, :capacity) do
        [{:capacity, cap}] -> cap
        [] -> %{total_capacity: :unlimited, total_used: 0}
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
    def append_created_job(tid, job) do
      :ets.insert(tid, {:created_jobs, get_created_jobs(tid) ++ [job]})
    end

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
      tid = :persistent_term.get(:vgc_test_tid)
      Store.get_volumes(tid)
    end
  end

  defmodule Metrics do
    @moduledoc false

    @spec cluster_capacity() :: map()
    def cluster_capacity do
      tid = :persistent_term.get(:vgc_test_tid)
      Store.get_capacity(tid)
    end
  end

  defmodule JobTracker do
    @moduledoc false

    @spec list(keyword()) :: [Job.t()]
    def list(filters) do
      tid = :persistent_term.get(:vgc_test_tid)

      Store.get_running_jobs(tid)
      |> filter_status(Keyword.get(filters, :status))
      |> filter_type(Keyword.get(filters, :type))
    end

    @spec create(module(), map()) :: {:ok, Job.t()}
    def create(type, params) do
      tid = :persistent_term.get(:vgc_test_tid)

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

  # ─── Test setup ────────────────────────────────────────────────────

  defp segment(volume_id, gc_schedule) do
    base =
      RootSegment.new(
        volume_id: volume_id,
        volume_name: "vol-#{volume_id}",
        cluster_id: "clust-test",
        cluster_name: "test-cluster",
        durability: %{type: :replicate, factor: 1, min_copies: 1}
      )

    %{base | schedules: Map.put(base.schedules, :gc, gc_schedule)}
  end

  defp setup_scheduler(opts) do
    tid = Store.init()

    Store.put_volumes(tid, opts[:volumes] || [])
    Store.put_capacity(tid, opts[:capacity] || %{total_capacity: :unlimited, total_used: 0})

    for {volume_id, segment} <- opts[:segments] || %{} do
      Store.put_segment(tid, volume_id, segment)
    end

    # Registry / Metrics / JobTracker stubs read the tid from
    # `:persistent_term`. Tests are `async: false` so cross-test
    # interference can't happen.
    :persistent_term.put(:vgc_test_tid, tid)

    on_exit(fn ->
      try do
        :persistent_term.erase(:vgc_test_tid)
      rescue
        ArgumentError -> :ok
      end
    end)

    name = :"VGCSched_#{System.unique_integer([:positive])}"

    {:ok, pid} =
      VolumeGCScheduler.start_link(
        name: name,
        tick_interval_ms: opts[:tick_interval_ms] || 50,
        pressure_check_interval_ms: opts[:pressure_check_interval_ms] || :timer.hours(24),
        pressure_threshold: opts[:pressure_threshold] || 0.85,
        job_tracker_mod: JobTracker,
        storage_metrics_mod: Metrics,
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
    handler_id = "vgc-test-#{System.unique_integer([:positive])}"

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

  # ─── Tests ─────────────────────────────────────────────────────────

  describe "scheduled tick" do
    test "dispatches GarbageCollection per due volume and updates last_run" do
      attach_telemetry([[:neonfs, :volume_gc_scheduler, :triggered]])

      ctx =
        setup_scheduler(
          volumes: [%{id: "vol-due-1"}, %{id: "vol-due-2"}],
          segments: %{
            "vol-due-1" => segment("vol-due-1", %{interval_ms: 1, last_run: nil}),
            "vol-due-2" => segment("vol-due-2", %{interval_ms: 1, last_run: nil})
          }
        )

      assert_receive {[:neonfs, :volume_gc_scheduler, :triggered], _,
                      %{volume_id: "vol-due-1", reason: :scheduled}},
                     2_000

      assert_receive {[:neonfs, :volume_gc_scheduler, :triggered], _,
                      %{volume_id: "vol-due-2", reason: :scheduled}},
                     2_000

      created = Store.get_created_jobs(ctx.tid)
      types = created |> Enum.map(& &1.type) |> Enum.uniq()
      assert types == [GarbageCollection]

      writes = Store.get_writes(ctx.tid)
      volume_ids = writes |> Enum.map(& &1.volume_id) |> MapSet.new()
      assert MapSet.member?(volume_ids, "vol-due-1")
      assert MapSet.member?(volume_ids, "vol-due-2")
      assert Enum.all?(writes, fn entry -> entry.key == :gc end)
    end

    test "skips a volume whose interval hasn't elapsed" do
      attach_telemetry([[:neonfs, :volume_gc_scheduler, :triggered]])

      not_due_segment =
        segment("vol-not-due", %{interval_ms: 1_000_000_000, last_run: DateTime.utc_now()})

      _ctx =
        setup_scheduler(
          volumes: [%{id: "vol-not-due"}],
          segments: %{"vol-not-due" => not_due_segment}
        )

      refute_receive {[:neonfs, :volume_gc_scheduler, :triggered], _,
                      %{volume_id: "vol-not-due"}},
                     500
    end

    test "does not double-dispatch when a GC job for the same volume is already running" do
      attach_telemetry([
        [:neonfs, :volume_gc_scheduler, :triggered],
        [:neonfs, :volume_gc_scheduler, :skipped]
      ])

      ctx =
        setup_scheduler(
          volumes: [%{id: "vol-busy"}],
          segments: %{"vol-busy" => segment("vol-busy", %{interval_ms: 1, last_run: nil})}
        )

      Store.put_running_jobs(ctx.tid, [
        %Job{
          id: "running-1",
          type: GarbageCollection,
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

      assert_receive {[:neonfs, :volume_gc_scheduler, :skipped], _,
                      %{volume_id: "vol-busy", reason: :already_running}},
                     2_000

      assert Store.get_created_jobs(ctx.tid) == []
    end
  end

  describe "trigger_now/2" do
    test "dispatches an immediate job for the given volume" do
      attach_telemetry([[:neonfs, :volume_gc_scheduler, :triggered]])

      ctx =
        setup_scheduler(
          tick_interval_ms: :timer.hours(24),
          volumes: [%{id: "vol-manual"}],
          segments: %{"vol-manual" => segment("vol-manual", %{interval_ms: 1, last_run: nil})}
        )

      assert {:ok, _job} = VolumeGCScheduler.trigger_now("vol-manual", ctx.name)

      assert_receive {[:neonfs, :volume_gc_scheduler, :triggered], _,
                      %{volume_id: "vol-manual", reason: :manual}},
                     1_000
    end
  end

  describe "pressure check" do
    test "fans out a GC job to every volume when usage exceeds threshold" do
      attach_telemetry([[:neonfs, :volume_gc_scheduler, :triggered]])

      ctx =
        setup_scheduler(
          tick_interval_ms: :timer.hours(24),
          pressure_check_interval_ms: 50,
          pressure_threshold: 0.5,
          capacity: %{total_capacity: 100, total_used: 90},
          volumes: [%{id: "vol-press-1"}, %{id: "vol-press-2"}],
          segments: %{
            "vol-press-1" =>
              segment("vol-press-1", %{interval_ms: 1_000_000, last_run: DateTime.utc_now()}),
            "vol-press-2" =>
              segment("vol-press-2", %{interval_ms: 1_000_000, last_run: DateTime.utc_now()})
          }
        )

      assert_receive {[:neonfs, :volume_gc_scheduler, :triggered], _,
                      %{volume_id: "vol-press-1", reason: :pressure}},
                     2_000

      assert_receive {[:neonfs, :volume_gc_scheduler, :triggered], _,
                      %{volume_id: "vol-press-2", reason: :pressure}},
                     2_000

      created = Store.get_created_jobs(ctx.tid)
      assert length(created) >= 2
    end

    test "no fan-out when usage is below the threshold" do
      attach_telemetry([[:neonfs, :volume_gc_scheduler, :triggered]])

      _ctx =
        setup_scheduler(
          tick_interval_ms: :timer.hours(24),
          pressure_check_interval_ms: 50,
          pressure_threshold: 0.95,
          capacity: %{total_capacity: 100, total_used: 50},
          volumes: [%{id: "vol-press-low"}],
          segments: %{
            "vol-press-low" =>
              segment("vol-press-low", %{interval_ms: 1_000_000, last_run: DateTime.utc_now()})
          }
        )

      refute_receive {[:neonfs, :volume_gc_scheduler, :triggered], _,
                      %{volume_id: "vol-press-low"}},
                     500
    end
  end
end
