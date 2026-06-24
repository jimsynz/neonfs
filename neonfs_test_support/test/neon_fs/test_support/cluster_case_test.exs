defmodule NeonFS.TestSupport.ClusterCaseTest do
  use ExUnit.Case, async: true

  alias NeonFS.TestSupport.ClusterCase

  describe "handle_cluster_init_result/2" do
    test "accepts a successful init" do
      assert :ok = ClusterCase.handle_cluster_init_result({:ok, %{cluster_id: "abc"}}, :node1)
    end

    test "treats an already-initialised cluster as success (#1388 retry idempotency)" do
      # `cluster_init` can time out transiently while actually completing; the
      # retry then returns this. The cluster is initialised, so it must not blow
      # up the caller's setup_all.
      already = {:error, %{message: "Cluster already initialised"}}
      assert :ok = ClusterCase.handle_cluster_init_result(already, :node1)
    end

    test "raises on a genuine init error" do
      genuine = {:error, %{message: "No drives available"}}

      assert_raise RuntimeError, ~r/cluster_init on node1 failed/, fn ->
        ClusterCase.handle_cluster_init_result(genuine, :node1)
      end
    end

    test "raises on a terminal badrpc result" do
      assert_raise RuntimeError, ~r/cluster_init on node2 failed/, fn ->
        ClusterCase.handle_cluster_init_result({:badrpc, :timeout}, :node2)
      end
    end
  end
end
