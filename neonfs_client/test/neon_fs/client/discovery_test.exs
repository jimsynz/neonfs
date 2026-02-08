defmodule NeonFS.Client.DiscoveryTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.{Connection, Discovery}

  setup do
    # Discovery depends on Connection for refresh
    start_supervised!({Connection, bootstrap_nodes: []})
    start_supervised!(Discovery)
    :ok
  end

  describe "get_core_nodes/0" do
    test "returns empty list when no services are cached" do
      assert Discovery.get_core_nodes() == []
    end
  end

  describe "list_by_type/1" do
    test "returns empty list for uncached type" do
      assert Discovery.list_by_type(:core) == []
      assert Discovery.list_by_type(:fuse) == []
      assert Discovery.list_by_type(:s3) == []
    end
  end

  describe "refresh/0" do
    test "does not crash when no core connection exists" do
      Discovery.refresh()
      Process.sleep(50)

      assert Process.whereis(Discovery) != nil
    end
  end

  describe "nodedown handling" do
    test "handles nodedown without crashing" do
      pid = Process.whereis(Discovery)
      send(pid, {:nodedown, :unknown@host, []})
      Process.sleep(50)

      assert Process.alive?(pid)
    end

    test "handles nodeup without crashing" do
      pid = Process.whereis(Discovery)
      send(pid, {:nodeup, :unknown@host, []})
      Process.sleep(50)

      assert Process.alive?(pid)
    end
  end
end
