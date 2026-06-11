defmodule NeonFS.Client.CLIHandlerTest do
  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Client.CLIHandler
  alias NeonFS.Client.Join

  describe "join_cluster/3 type resolution (#1161)" do
    test "rejects a type string that is not a service type" do
      assert {:error, {:invalid_service_type, "bogus_service"}} =
               CLIHandler.join_cluster("nfs_inv_x_9_y", "node1:9568", "bogus_service")
    end

    test "rejects a known atom that is not a service type" do
      assert {:error, {:invalid_service_type, "ok"}} =
               CLIHandler.join_cluster("nfs_inv_x_9_y", "node1:9568", "ok")
    end

    test "detection fails cleanly when no NeonFS service application is running" do
      # The client test VM runs none of the service applications, so
      # the nil-type default has nothing to detect.
      assert {:error, :unknown_service_type} =
               CLIHandler.join_cluster("nfs_inv_x_9_y", "node1:9568")
    end
  end

  describe "join_cluster/3 dispatch" do
    test "an interface type drives NeonFS.Client.Join and shapes the CLI reply" do
      stub(Join, :join_cluster, fn "tok", "node1:9568", :nfs ->
        {:ok, :joining}
      end)

      assert {:ok, reply} = CLIHandler.join_cluster("tok", "node1:9568", "nfs")
      assert reply["status"] == "joining"
      assert reply["type"] == "nfs"
      assert reply["via_address"] == "node1:9568"
      assert reply["node_name"] == Atom.to_string(Node.self())
    end

    test "join errors pass through" do
      stub(Join, :join_cluster, fn _, _, _ -> {:error, {:http_error, 403}} end)

      assert {:error, {:http_error, 403}} =
               CLIHandler.join_cluster("tok", "node1:9568", "s3")
    end
  end
end
