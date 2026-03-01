defmodule NeonFS.Client.RouterTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.{Connection, CostFunction, Discovery, Router}
  alias NeonFS.Error.Unavailable

  setup do
    start_supervised!({Connection, bootstrap_nodes: []})
    start_supervised!(Discovery)
    start_supervised!(CostFunction)
    :ok
  end

  describe "call/4" do
    test "returns error when no core nodes are reachable" do
      assert {:error, %Unavailable{}} =
               Router.call(SomeModule, :some_function, [])
    end
  end

  describe "metadata_call/3" do
    test "returns error when no core nodes are reachable" do
      assert {:error, %Unavailable{}} =
               Router.metadata_call(SomeModule, :some_function, [])
    end
  end
end
