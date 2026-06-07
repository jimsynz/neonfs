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

  describe "get_by_id/2 (#1087)" do
    test "resolves + caches under a key namespace distinct from get/2" do
      id_resolver = counting_resolver({:ok, [:core1@host]})

      assert {:ok, [:core1@host]} = RootPlacement.get_by_id("vol-id", resolver: id_resolver)
      assert_received {:resolved, "vol-id"}

      # Second by-id read is a cache hit.
      assert {:ok, [:core1@host]} = RootPlacement.get_by_id("vol-id", resolver: id_resolver)
      refute_received {:resolved, "vol-id"}

      # A by-name lookup for the same string is a separate cache entry (must
      # still resolve), proving the {:name, _} / {:id, _} namespaces don't clash.
      name_resolver = counting_resolver({:ok, [:core2@host]})
      assert {:ok, [:core2@host]} = RootPlacement.get("vol-id", resolver: name_resolver)
      assert_received {:resolved, "vol-id"}
    end
  end
end
