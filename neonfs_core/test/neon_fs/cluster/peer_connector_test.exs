defmodule NeonFS.Cluster.PeerConnectorTest do
  use ExUnit.Case, async: true

  alias NeonFS.Cluster.PeerConnector

  describe "startup connection" do
    test "dials every supplied peer once" do
      test = self()

      connect = fn node ->
        send(test, {:connect, node})
        true
      end

      start_supervised!({PeerConnector, peers: [:core1@host, :core2@host], connect: connect})

      assert_receive {:connect, :core1@host}, 1_000
      assert_receive {:connect, :core2@host}, 1_000
    end

    test "logs but does not crash when a peer is unreachable" do
      test = self()

      connect = fn node ->
        send(test, {:connect, node})
        false
      end

      pid = start_supervised!({PeerConnector, peers: [:down@host], connect: connect})

      assert_receive {:connect, :down@host}, 1_000
      assert Process.alive?(pid)
    end

    test "is a no-op with no known peers" do
      pid = start_supervised!({PeerConnector, peers: []})
      assert Process.alive?(pid)
    end
  end
end
