defmodule NeonFS.Client.CostFunctionTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.{Connection, CostFunction, Discovery}

  setup do
    # CostFunction depends on Discovery which depends on Connection
    start_supervised!({Connection, bootstrap_nodes: []})
    start_supervised!(Discovery)
    start_supervised!(CostFunction)
    :ok
  end

  describe "select_core_node/0" do
    test "returns error when no nodes are known" do
      assert {:error, :no_core_nodes} = CostFunction.select_core_node()
    end
  end

  describe "select_core_node/1" do
    test "returns error with prefer_leader when no nodes are known" do
      assert {:error, :no_core_nodes} = CostFunction.select_core_node(prefer_leader: true)
    end
  end

  describe "node_costs/0" do
    test "returns empty list when no nodes are known" do
      assert CostFunction.node_costs() == []
    end
  end

  describe "draining deprioritisation (#1324)" do
    test "prefers a cheaper active node over a draining one" do
      inject_costs(%{
        active@host: node_cost(:active@host, 0.2, false),
        draining@host: node_cost(:draining@host, 1.2, true)
      })

      assert {:ok, :active@host} = CostFunction.select_core_node()
    end

    test "still selects a draining node when it is the only option" do
      # Deprioritise, not exclude: a lone draining node must still route.
      inject_costs(%{draining@host: node_cost(:draining@host, 1.05, true)})

      assert {:ok, :draining@host} = CostFunction.select_core_node()
    end
  end

  defp inject_costs(costs) do
    :sys.replace_state(CostFunction, fn state -> %{state | costs: costs} end)
  end

  defp node_cost(node, total_cost, off_duty?) do
    %{
      node: node,
      latency_ms: 1,
      load_score: 0.0,
      queue_score: 0.0,
      off_duty: off_duty?,
      total_cost: total_cost
    }
  end
end
