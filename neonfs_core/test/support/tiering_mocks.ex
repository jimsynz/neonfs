defmodule NeonFS.TestSupport.TieringMocks do
  @moduledoc """
  ETS-backed collaborator stubs for `NeonFS.Core.TieringManagerTest`.

  These lived at the bottom of that test file, which made it impossible to
  run `async: true`: `mix test` starts async modules with
  `ExUnit.async_run/0` *before* it finishes requiring the test files, so
  the suite began executing the test module while the modules defined
  after it were still compiling. The tests failed with
  `UndefinedFunctionError` on stubs that existed a moment later — a
  compile race, not a missing function, which is why the message suggested
  the very function that had been called.

  Compiled from `test/support`, they are ready before any test runs.

  Each keeps its state in a named ETS table, so the file that uses them
  still cannot run concurrently with another that touches the same tables
  — but nothing else does.
  """

  defmodule ChunkIndex do
    @moduledoc """
    Serves a fixed chunk list, set per test with `set_chunks/1`.
    """

    @table :mock_chunk_index_data

    def init do
      safe_delete_table()
      :ets.new(@table, [:named_table, :set, :public])
      :ets.insert(@table, {:chunks, []})
    end

    def cleanup do
      safe_delete_table()
    end

    defp safe_delete_table do
      :ets.delete(@table)
      :ok
    rescue
      ArgumentError -> :ok
    end

    def set_chunks(chunks) do
      :ets.insert(@table, {:chunks, chunks})
    end

    def list_by_node(_node) do
      case :ets.lookup(@table, :chunks) do
        [{:chunks, chunks}] -> chunks
        [] -> []
      end
    end
  end

  defmodule AccessTracker do
    @moduledoc """
    Returns per-chunk access stats seeded with `set_stats/2`.
    """

    @table :mock_access_tracker_data

    def init do
      safe_delete_table()
      :ets.new(@table, [:named_table, :set, :public])
    end

    def cleanup do
      safe_delete_table()
    end

    defp safe_delete_table do
      :ets.delete(@table)
      :ok
    rescue
      ArgumentError -> :ok
    end

    def set_stats(chunk_hash, stats) do
      :ets.insert(@table, {chunk_hash, stats})
    end

    def get_stats(chunk_hash) do
      case :ets.lookup(@table, chunk_hash) do
        [{^chunk_hash, stats}] -> stats
        [] -> %{hourly: 0, daily: 0, last_accessed: nil}
      end
    end
  end

  defmodule DriveRegistry do
    @moduledoc """
    Reports per-tier usage ratios and drive paths under a base dir.
    """

    @table :mock_drive_registry_data

    # The base dir lives in the mock's own table rather than in application
    # env. It was only ever a channel from `setup` to `list_drives/0` here,
    # and routing it through VM-global state let this file collide with any
    # other reading the same key.
    def init(base_dir) do
      safe_delete_table()
      :ets.new(@table, [:named_table, :set, :public])
      :ets.insert(@table, {:base_dir, base_dir})
    end

    def cleanup do
      safe_delete_table()
    end

    defp safe_delete_table do
      :ets.delete(@table)
      :ok
    rescue
      ArgumentError -> :ok
    end

    def set_tier_usage(tier, ratio) do
      :ets.insert(@table, {{:tier_usage, tier}, ratio})
    end

    def list_drives do
      [{:base_dir, base_dir}] = :ets.lookup(@table, :base_dir)

      :ets.tab2list(@table)
      |> Enum.filter(fn
        {{:tier_usage, _}, _} -> true
        _ -> false
      end)
      |> Enum.map(fn {{:tier_usage, tier}, ratio} ->
        capacity = 1_000_000_000
        used = trunc(capacity * ratio)

        %NeonFS.Core.Drive{
          id: "mock_#{tier}",
          node: Node.self(),
          path: Path.join(base_dir, "#{tier}"),
          tier: tier,
          capacity_bytes: capacity,
          used_bytes: used,
          state: :active
        }
      end)
    end
  end

  defmodule VolumeRegistry do
    @moduledoc """
    Resolves every volume id to the same stub volume.
    """

    def list, do: []
  end

  defmodule BackgroundWorker do
    @moduledoc """
    Records submitted work instead of running it, and can report a full queue.
    """

    @table :mock_bg_worker_data

    def init do
      safe_delete_table()
      :ets.new(@table, [:named_table, :set, :public])
      :ets.insert(@table, {:submitted, []})
      :ets.insert(@table, {:queue_full, false})
    end

    def cleanup do
      safe_delete_table()
    end

    defp safe_delete_table do
      :ets.delete(@table)
      :ok
    rescue
      ArgumentError -> :ok
    end

    def set_queue_full(full) do
      :ets.insert(@table, {:queue_full, full})
    end

    def get_submitted do
      case :ets.lookup(@table, :submitted) do
        [{:submitted, list}] -> list
        [] -> []
      end
    end

    def status do
      queue_full =
        case :ets.lookup(@table, :queue_full) do
          [{:queue_full, val}] -> val
          [] -> false
        end

      %{
        queued: if(queue_full, do: 100, else: 0),
        running: 0,
        completed: 0,
        by_priority: %{high: 0, normal: 0, low: 0}
      }
    end

    def submit(work_fn, opts) do
      existing =
        case :ets.lookup(@table, :submitted) do
          [{:submitted, list}] -> list
          [] -> []
        end

      :ets.insert(@table, {:submitted, existing ++ [{work_fn, opts}]})
      {:ok, "mock_work_#{:erlang.unique_integer([:positive])}"}
    end
  end
end
