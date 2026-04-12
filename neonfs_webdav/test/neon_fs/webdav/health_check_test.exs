defmodule NeonFS.WebDAV.HealthCheckTest do
  use ExUnit.Case, async: true

  alias NeonFS.WebDAV.HealthCheck

  describe "cluster_status/1" do
    test "returns ok when core nodes are available" do
      status = HealthCheck.cluster_status(fn -> [:core@node1, :core@node2] end)

      assert status.status == :ok
      assert status.writable == true
      assert status.readable == true
      assert status.reason == nil
    end

    test "returns unavailable when no core nodes" do
      status = HealthCheck.cluster_status(fn -> [] end)

      assert status.status == :unavailable
      assert status.writable == false
      assert status.readable == false
      assert status.reason == "no-core-nodes"
    end
  end
end
