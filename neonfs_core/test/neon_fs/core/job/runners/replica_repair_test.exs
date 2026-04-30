defmodule NeonFS.Core.Job.Runners.ReplicaRepairTest do
  @moduledoc """
  Unit tests for `NeonFS.Core.Job.Runners.ReplicaRepair` (#707).
  Stubs `NeonFS.Core.ReplicaRepair.repair_volume/2` via the
  `:replica_repair_mod` Application env so the runner is exercised
  in isolation.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Core.Job
  alias NeonFS.Core.Job.Runners.ReplicaRepair, as: Runner

  defmodule StubRepair do
    @moduledoc false

    def repair_volume(volume_id, opts) do
      parent = Application.fetch_env!(:neonfs_core, :replica_repair_test_parent)
      send(parent, {:repair_volume_called, volume_id, opts})

      Application.fetch_env!(:neonfs_core, :replica_repair_test_response)
    end
  end

  setup do
    Application.put_env(:neonfs_core, :replica_repair_mod, StubRepair)
    Application.put_env(:neonfs_core, :replica_repair_test_parent, self())

    on_exit(fn ->
      Application.delete_env(:neonfs_core, :replica_repair_mod)
      Application.delete_env(:neonfs_core, :replica_repair_test_parent)
      Application.delete_env(:neonfs_core, :replica_repair_test_response)
      Application.delete_env(:neonfs_core, :replica_repair_batch_size)
    end)

    :ok
  end

  defp stub_response(response) do
    Application.put_env(:neonfs_core, :replica_repair_test_response, response)
  end

  defp empty_job(params \\ %{volume_id: "vol-1"}) do
    now = DateTime.utc_now()

    %Job{
      id: "job-1",
      type: Runner,
      node: node(),
      params: params,
      state: %{},
      progress: %{total: 0, completed: 0, description: ""},
      status: :running,
      started_at: now,
      created_at: now,
      updated_at: now,
      completed_at: nil
    }
  end

  describe "label/0" do
    test "returns the canonical label" do
      assert Runner.label() == "replica-repair"
    end
  end

  describe "step/1" do
    test "passes the saved cursor to repair_volume and returns :continue with next_cursor" do
      stub_response({:ok, %{added: 2, removed: 0, errors: [], next_cursor: 100}})

      job = empty_job() |> Map.put(:state, %{cursor: 50})

      assert {:continue, updated} = Runner.step(job)

      assert_received {:repair_volume_called, "vol-1", opts}
      assert opts[:cursor] == 50
      assert opts[:batch_size] == 100

      assert updated.state[:cursor] == 100
      assert updated.state[:added] == 2
      assert updated.state[:removed] == 0
      assert updated.state[:errors] == []
    end

    test "uses cursor 0 when state has no cursor yet" do
      stub_response({:ok, %{added: 0, removed: 1, errors: [], next_cursor: 50}})

      assert {:continue, _updated} = Runner.step(empty_job())

      assert_received {:repair_volume_called, "vol-1", opts}
      assert opts[:cursor] == 0
    end

    test "honours :replica_repair_batch_size from app env" do
      Application.put_env(:neonfs_core, :replica_repair_batch_size, 25)
      stub_response({:ok, %{added: 0, removed: 0, errors: [], next_cursor: :done}})

      Runner.step(empty_job())

      assert_received {:repair_volume_called, "vol-1", opts}
      assert opts[:batch_size] == 25
    end

    test "returns :complete with cleared cursor when next_cursor is :done" do
      stub_response({:ok, %{added: 5, removed: 2, errors: [], next_cursor: :done}})

      assert {:complete, completed} = Runner.step(empty_job())

      assert completed.state[:cursor] == nil
      assert completed.state[:added] == 5
      assert completed.state[:removed] == 2
      assert completed.progress.description == "Complete"
    end

    test "accumulates added / removed / errors across multiple steps" do
      stub_response({:ok, %{added: 1, removed: 2, errors: [{"h1", :gone}], next_cursor: 10}})

      job = empty_job() |> Map.put(:state, %{added: 5, removed: 3, errors: [{"h0", :prev}]})

      assert {:continue, updated} = Runner.step(job)

      assert updated.state[:added] == 6
      assert updated.state[:removed] == 5
      assert updated.state[:errors] == [{"h0", :prev}, {"h1", :gone}]
    end

    test "returns {:error, _, job} on a pass-level failure" do
      stub_response({:error, :not_found})

      job = empty_job(%{volume_id: "vol-missing"})

      assert {:error, :not_found, ^job} = Runner.step(job)
    end

    test "raises on missing :volume_id param" do
      stub_response({:ok, %{added: 0, removed: 0, errors: [], next_cursor: :done}})

      assert_raise ArgumentError, ~r/volume_id/, fn ->
        Runner.step(empty_job(%{}))
      end
    end

    test "accepts string volume_id key (resumed-from-storage shape)" do
      stub_response({:ok, %{added: 0, removed: 0, errors: [], next_cursor: :done}})

      assert {:complete, _} = Runner.step(empty_job(%{"volume_id" => "vol-2"}))

      assert_received {:repair_volume_called, "vol-2", _}
    end
  end

  describe "on_cancel/1" do
    test "is a no-op" do
      assert :ok = Runner.on_cancel(empty_job())
    end
  end
end
