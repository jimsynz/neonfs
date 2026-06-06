defmodule NeonFS.Client.RootPlacementTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.RootPlacement

  setup do
    start_supervised!(RootPlacement)
    :ok
  end

  defp counting_resolver(reply) do
    test_pid = self()

    fn volume_name ->
      send(test_pid, {:resolved, volume_name})
      reply
    end
  end

  describe "get/2" do
    test "resolves and caches on a miss, then serves subsequent reads from cache" do
      resolver = counting_resolver({:ok, [:core1@host]})

      assert {:ok, [:core1@host]} = RootPlacement.get("vol", resolver: resolver)
      assert_received {:resolved, "vol"}

      assert {:ok, [:core1@host]} = RootPlacement.get("vol", resolver: resolver)
      refute_received {:resolved, "vol"}
    end

    test "does not cache errors" do
      err_resolver = counting_resolver({:error, :not_found})

      assert {:error, :not_found} = RootPlacement.get("vol", resolver: err_resolver)
      assert_received {:resolved, "vol"}

      ok_resolver = counting_resolver({:ok, [:core1@host]})
      assert {:ok, [:core1@host]} = RootPlacement.get("vol", resolver: ok_resolver)
      assert_received {:resolved, "vol"}
    end

    test "re-resolves once the entry's TTL has elapsed" do
      resolver = counting_resolver({:ok, [:core1@host]})

      assert {:ok, _} = RootPlacement.get("vol", resolver: resolver, ttl_ms: 0)
      assert_received {:resolved, "vol"}

      assert {:ok, _} = RootPlacement.get("vol", resolver: resolver)
      assert_received {:resolved, "vol"}
    end

    test "wraps an unexpected resolver reply as an error" do
      assert {:error, {:unexpected_root_nodes_reply, :weird}} =
               RootPlacement.get("vol", resolver: fn _ -> :weird end)
    end
  end

  describe "invalidate/1" do
    test "forces the next get to re-resolve" do
      resolver = counting_resolver({:ok, [:core1@host]})

      assert {:ok, _} = RootPlacement.get("vol", resolver: resolver)
      assert_received {:resolved, "vol"}

      assert :ok = RootPlacement.invalidate("vol")

      assert {:ok, _} = RootPlacement.get("vol", resolver: resolver)
      assert_received {:resolved, "vol"}
    end
  end
end
