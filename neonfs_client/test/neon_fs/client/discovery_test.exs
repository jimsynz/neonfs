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
      :sys.get_state(Discovery)

      assert Process.whereis(Discovery) != nil
    end
  end

  describe "peer_sync_interval configuration" do
    test "uses default refresh interval when not configured" do
      pid = Process.whereis(Discovery)
      state = :sys.get_state(pid)

      assert state.refresh_ms == 5_000
    end

    test "reads peer_sync_interval from app env" do
      stop_supervised!(Discovery)

      Application.put_env(:neonfs_client, :peer_sync_interval, 60_000)

      on_exit(fn ->
        Application.delete_env(:neonfs_client, :peer_sync_interval)
      end)

      start_supervised!(Discovery)
      pid = Process.whereis(Discovery)
      state = :sys.get_state(pid)

      assert state.refresh_ms == 60_000
    end

    test "opts override app env" do
      stop_supervised!(Discovery)

      Application.put_env(:neonfs_client, :peer_sync_interval, 60_000)

      on_exit(fn ->
        Application.delete_env(:neonfs_client, :peer_sync_interval)
      end)

      start_supervised!({Discovery, refresh_ms: 15_000})
      pid = Process.whereis(Discovery)
      state = :sys.get_state(pid)

      assert state.refresh_ms == 15_000
    end
  end

  describe "nodedown handling" do
    test "handles nodedown without crashing" do
      pid = Process.whereis(Discovery)
      send(pid, {:nodedown, :unknown@host, []})
      :sys.get_state(pid)

      assert Process.alive?(pid)
    end

    test "handles nodeup without crashing" do
      pid = Process.whereis(Discovery)
      send(pid, {:nodeup, :unknown@host, []})
      :sys.get_state(pid)

      assert Process.alive?(pid)
    end
  end
end
