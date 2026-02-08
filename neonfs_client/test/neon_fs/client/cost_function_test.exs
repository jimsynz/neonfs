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
end
