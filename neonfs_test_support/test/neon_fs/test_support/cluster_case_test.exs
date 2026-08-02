defmodule NeonFS.TestSupport.ClusterCaseTest do
  use ExUnit.Case, async: true

  alias NeonFS.TestSupport.ClusterCase

  describe "handle_cluster_init_result/2" do
    test "accepts a successful init" do
      assert :ok = ClusterCase.handle_cluster_init_result({:ok, %{cluster_id: "abc"}}, :node1)
    end

    test "treats an already-initialised cluster as success (retry idempotency)" do
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

  describe "handle_join_result/2" do
    test "accepts a successful join" do
      assert :ok = ClusterCase.handle_join_result({:ok, %{cluster_name: "test"}}, :node2)
    end

    test "treats an already-joined node as success" do
      # `rpc_until_ready/6` retries a transient `:timeout`, and a join that got
      # as far as writing `cluster.json` has already succeeded — the retry is
      # refused by `validate_not_in_cluster/0`. Failing here would report a
      # formation failure for a cluster that formed.
      assert :ok = ClusterCase.handle_join_result({:error, :already_in_cluster}, :node2)
    end

    test "raises on a genuine join rejection" do
      genuine = {:error, {:join_rejected, :invalid_token}}

      assert_raise RuntimeError, ~r/join_cluster_rpc on node2 failed/, fn ->
        ClusterCase.handle_join_result(genuine, :node2)
      end
    end

    test "raises on a terminal badrpc result" do
      assert_raise RuntimeError, ~r/join_cluster_rpc on node3 failed/, fn ->
        ClusterCase.handle_join_result({:badrpc, :nodedown}, :node3)
      end
    end
  end

  describe "raise_incomplete_registration/4" do
    test "names the services that never registered" do
      assert_raise RuntimeError, ~r/missing: +\[s3: :node2@localhost\]/, fn ->
        ClusterCase.raise_incomplete_registration(
          :node1,
          [{:s3, :node2@localhost}],
          [{:core, :node1@localhost}],
          30_000
        )
      end
    end

    test "reports what did register, so a partial formation is distinguishable" do
      assert_raise RuntimeError, ~r/registered: \[core: :node1@localhost\]/, fn ->
        ClusterCase.raise_incomplete_registration(
          :node1,
          [{:webdav, :node3@localhost}],
          [{:core, :node1@localhost}],
          30_000
        )
      end
    end

    test "attributes the failure to formation rather than to the caller's assertion" do
      assert_raise RuntimeError, ~r/Cluster formation did not complete/, fn ->
        ClusterCase.raise_incomplete_registration(:node1, [{:nfs, :node2@localhost}], [], 1_000)
      end
    end
  end
end
