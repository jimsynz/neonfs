defmodule NeonFS.Client.ConnectionTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.{Connection, ServiceInfo}

  describe "start_link/1" do
    test "starts with empty bootstrap nodes" do
      pid = start_supervised!({Connection, bootstrap_nodes: []})
      assert Process.alive?(pid)
    end
  end

  describe "connected_core_node/0" do
    test "returns error when no connections exist" do
      start_supervised!({Connection, bootstrap_nodes: []})

      assert {:error, :no_connection} = Connection.connected_core_node()
    end
  end

  describe "bootstrap_nodes/0" do
    test "returns the configured bootstrap nodes" do
      # Use localhost node names so Node.connect/1 fails fast
      nodes = [:core@localhost, :other@localhost]
      start_supervised!({Connection, bootstrap_nodes: nodes})

      # Wait for handle_continue(:connect) to complete
      assert Connection.bootstrap_nodes() == nodes
    end

    test "returns empty list when none configured" do
      start_supervised!({Connection, bootstrap_nodes: []})

      assert Connection.bootstrap_nodes() == []
    end
  end

  describe "peer_connect_timeout configuration" do
    test "uses default when not configured" do
      pid = start_supervised!({Connection, bootstrap_nodes: []})
      state = :sys.get_state(pid)

      assert state.peer_connect_timeout == 10_000
    end

    test "reads from opts" do
      pid = start_supervised!({Connection, bootstrap_nodes: [], peer_connect_timeout: 20_000})
      state = :sys.get_state(pid)

      assert state.peer_connect_timeout == 20_000
    end

    test "reads from app env when not in opts" do
      Application.put_env(:neonfs_client, :peer_connect_timeout, 15_000)

      on_exit(fn ->
        Application.delete_env(:neonfs_client, :peer_connect_timeout)
      end)

      pid = start_supervised!({Connection, bootstrap_nodes: []})
      state = :sys.get_state(pid)

      assert state.peer_connect_timeout == 15_000
    end
  end

  describe "sync_services/1" do
    test "tracks desired nodes from discovered services" do
      pid = start_supervised!({Connection, bootstrap_nodes: [:bootstrap@localhost]})

      Connection.sync_services([
        ServiceInfo.new(:neonfs_core@localhost, :core),
        ServiceInfo.new(:neonfs_nfs@localhost, :nfs)
      ])

      state = :sys.get_state(pid)

      assert MapSet.equal?(
               state.desired_nodes,
               MapSet.new([:bootstrap@localhost, :neonfs_core@localhost, :neonfs_nfs@localhost])
             )

      assert MapSet.equal?(
               state.core_nodes,
               MapSet.new([:bootstrap@localhost, :neonfs_core@localhost])
             )
    end

    test "ignores the local node in discovered services" do
      pid = start_supervised!({Connection, bootstrap_nodes: []})

      Connection.sync_services([
        ServiceInfo.new(Node.self(), :nfs),
        ServiceInfo.new(:neonfs_core@localhost, :core)
      ])

      state = :sys.get_state(pid)

      assert MapSet.equal?(state.desired_nodes, MapSet.new([:neonfs_core@localhost]))
      assert MapSet.equal?(state.core_nodes, MapSet.new([:neonfs_core@localhost]))
    end
  end

  describe "connected_core_node/0 with full mesh state" do
    test "returns only connected core nodes" do
      pid = start_supervised!({Connection, bootstrap_nodes: []})

      :sys.replace_state(pid, fn state ->
        %{
          state
          | connected_nodes: MapSet.new([:neonfs_nfs@localhost, :neonfs_core@localhost]),
            core_nodes: MapSet.new([:neonfs_core@localhost])
        }
      end)

      assert {:ok, :neonfs_core@localhost} = Connection.connected_core_node()
    end
  end

  describe "nodedown handling" do
    test "handles 2-tuple nodedown for untracked node without crashing" do
      pid = start_supervised!({Connection, bootstrap_nodes: []})

      send(pid, {:nodedown, :unknown@host})
      :sys.get_state(pid)

      assert Process.alive?(pid)
    end

    test "handles 3-tuple nodedown for untracked node without crashing" do
      pid = start_supervised!({Connection, bootstrap_nodes: []})

      send(pid, {:nodedown, :unknown@host, []})
      :sys.get_state(pid)

      assert Process.alive?(pid)
    end

    test "keeps desired nodes after a connected node goes down" do
      pid = start_supervised!({Connection, bootstrap_nodes: []})
      ref = make_ref()

      :sys.replace_state(pid, fn state ->
        %{
          state
          | connected_nodes: MapSet.new([:neonfs_core@localhost]),
            desired_nodes: MapSet.new([:neonfs_core@localhost]),
            core_nodes: MapSet.new([:neonfs_core@localhost]),
            monitors: %{ref => :neonfs_core@localhost}
        }
      end)

      send(pid, {:nodedown, :neonfs_core@localhost})
      state = :sys.get_state(pid)

      refute MapSet.member?(state.connected_nodes, :neonfs_core@localhost)
      assert MapSet.member?(state.desired_nodes, :neonfs_core@localhost)
      assert state.monitors == %{}
    end
  end
end
