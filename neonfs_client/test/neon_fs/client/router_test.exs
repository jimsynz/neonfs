defmodule NeonFS.Client.RouterTest do
  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Client.{Connection, CostFunction, Discovery, RootPlacement, Router}
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

  describe "volume_metadata_call/4" do
    test "dispatches to a reachable root-holding node" do
      stub(RootPlacement, :get, fn "vol" -> {:ok, [Node.self()]} end)
      stub(Discovery, :get_core_nodes, fn -> [Node.self()] end)

      # RPC to the local node executes the real function.
      assert 3 == Router.volume_metadata_call("vol", Kernel, :+, [1, 2])
    end

    test "falls back to metadata_call when no root holder is reachable" do
      stub(RootPlacement, :get, fn "vol" -> {:ok, [:unreachable@nohost]} end)
      stub(Discovery, :get_core_nodes, fn -> [] end)

      assert {:error, %Unavailable{}} =
               Router.volume_metadata_call("vol", SomeModule, :some_function, [])
    end

    test "falls back to metadata_call when the root lookup fails" do
      stub(RootPlacement, :get, fn "vol" -> {:error, :not_found} end)

      assert {:error, %Unavailable{}} =
               Router.volume_metadata_call("vol", SomeModule, :some_function, [])
    end
  end
end
