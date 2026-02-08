defmodule NeonFS.Client.ConnectionTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.Connection

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

  describe "nodedown handling" do
    test "handles nodedown for untracked node without crashing" do
      pid = start_supervised!({Connection, bootstrap_nodes: []})

      send(pid, {:nodedown, :unknown@host, []})
      Process.sleep(50)

      assert Process.alive?(pid)
    end
  end
end
