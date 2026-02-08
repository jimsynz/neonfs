defmodule NeonFS.Client.RouterTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.{Connection, CostFunction, Discovery, Router}

  setup do
    start_supervised!({Connection, bootstrap_nodes: []})
    start_supervised!(Discovery)
    start_supervised!(CostFunction)
    :ok
  end

  describe "call/4" do
    test "returns error when no core nodes are reachable" do
      assert {:error, :all_nodes_unreachable} =
               Router.call(SomeModule, :some_function, [])
    end
  end

  describe "metadata_call/3" do
    test "returns error when no core nodes are reachable" do
      assert {:error, :all_nodes_unreachable} =
               Router.metadata_call(SomeModule, :some_function, [])
    end
  end
end
