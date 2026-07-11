defmodule NeonFS.Core.PlacementBarrierTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.PlacementBarrier

  describe "drain/1 with no supervisor running" do
    test "reports nothing outstanding" do
      refute Process.whereis(NeonFS.Core.PlacementTaskSupervisor)
      assert %{drained: 0, remaining: 0, timed_out: false} = PlacementBarrier.drain(0)
    end
  end

  describe "drain/1 with placements in flight" do
    setup do
      start_supervised!({Task.Supervisor, name: NeonFS.Core.PlacementTaskSupervisor})
      :ok
    end

    test "returns immediately when nothing is outstanding" do
      assert %{drained: 0, remaining: 0, timed_out: false} = PlacementBarrier.drain(1_000)
    end

    test "blocks until every outstanding placement completes" do
      test_pid = self()

      for _ <- 1..3 do
        {:ok, _pid} =
          PlacementBarrier.run(fn ->
            receive do
              :release -> send(test_pid, :done)
            end
          end)
      end

      drainer = Task.async(fn -> PlacementBarrier.drain(5_000) end)

      # The drain must still be waiting while the placements are parked.
      refute Task.yield(drainer, 100)

      send_to_children(:release)

      assert %{drained: 3, remaining: 0, timed_out: false} = Task.await(drainer)
      assert_received :done
    end

    test "bounds out on a hung placement and reports the shortfall" do
      PlacementBarrier.run(fn ->
        receive do
          :never -> :ok
        end
      end)

      assert %{drained: 0, remaining: 1, timed_out: true} = PlacementBarrier.drain(50)
    end
  end

  defp send_to_children(msg) do
    NeonFS.Core.PlacementTaskSupervisor
    |> Task.Supervisor.children()
    |> Enum.each(&send(&1, msg))
  end
end
