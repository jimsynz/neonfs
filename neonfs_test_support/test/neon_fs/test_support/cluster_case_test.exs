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

    test "still raises once the CA retries are exhausted" do
      # The retry lives in `join_cluster_idempotent/4`; if every attempt loses
      # the race the rejection must surface rather than be swallowed.
      exhausted =
        {:error,
         {:join_rejected, {:cert_signing_failed, %{file_path: "/tls/ca.crt", class: :not_found}}}}

      assert_raise RuntimeError, ~r/join_cluster_rpc on node2 failed/, fn ->
        ClusterCase.handle_join_result(exhausted, :node2)
      end
    end
  end

  describe "ca_not_yet_readable?/1" do
    test "classifies a join rejected because the CA is not readable yet" do
      rejection =
        {:error,
         {:join_rejected, {:cert_signing_failed, %{file_path: "/tls/ca.crt", class: :not_found}}}}

      assert ClusterCase.ca_not_yet_readable?(rejection)
    end

    test "does not classify other cert-signing rejections" do
      # Retrying these turns a clear failure into a slow one.
      refute ClusterCase.ca_not_yet_readable?(
               {:error, {:join_rejected, {:cert_signing_failed, :invalid_csr}}}
             )

      refute ClusterCase.ca_not_yet_readable?(
               {:error, {:join_rejected, {:cert_signing_failed, %{file_path: "/tls/serial"}}}}
             )
    end

    test "does not classify unrelated results" do
      refute ClusterCase.ca_not_yet_readable?({:ok, %{cluster_name: "test"}})
      refute ClusterCase.ca_not_yet_readable?({:error, :already_in_cluster})
      refute ClusterCase.ca_not_yet_readable?({:badrpc, :nodedown})
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

  describe "wait_until/2" do
    test "returns immediately when the condition is already true" do
      assert :ok = ClusterCase.wait_until(fn -> true end, timeout: 0)
    end

    test "gives up on a condition that never becomes true" do
      assert {:error, :timeout} =
               ClusterCase.wait_until(fn -> false end, timeout: 100, max_interval: 20)
    end

    test "succeeds once the condition flips" do
      counter = :counters.new(1, [])

      assert :ok =
               ClusterCase.wait_until(
                 fn ->
                   :counters.add(counter, 1, 1)
                   :counters.get(counter, 1) >= 5
                 end,
                 timeout: 5_000,
                 interval: 1,
                 max_interval: 1
               )
    end

    # The regression this guards. A wall-clock deadline is spent by a slow
    # condition rather than by waiting, so the same budget buys far fewer
    # attempts on a loaded runner than on an idle one — which is the whole
    # failure mode, since a cluster condition is an RPC and contention is
    # exactly what makes it slow. Charging only for sleep decouples the two.
    #
    # Asserted as a ratio rather than an absolute count, because an absolute
    # count is itself load-sensitive and would reintroduce the flake in the
    # test for the fix.
    test "a slow condition does not consume the waiting budget" do
      attempts_when = fn body ->
        counter = :counters.new(1, [])

        {:error, :timeout} =
          ClusterCase.wait_until(
            fn ->
              :counters.add(counter, 1, 1)
              body.()
              false
            end,
            timeout: 200,
            interval: 10,
            max_interval: 10
          )

        :counters.get(counter, 1)
      end

      fast = attempts_when.(fn -> :ok end)
      slow = attempts_when.(fn -> Process.sleep(20) end)

      assert slow >= div(fast, 2),
             "slow condition got #{slow} attempts against #{fast} for a fast one"
    end

    test "the wall-clock ceiling still bounds a pathologically slow condition" do
      started = System.monotonic_time(:millisecond)

      assert {:error, :timeout} =
               ClusterCase.wait_until(
                 fn ->
                   Process.sleep(200)
                   false
                 end,
                 timeout: 100,
                 interval: 5,
                 max_interval: 5
               )

      # 100ms of sleep at 5ms a poll is 20 attempts, each costing 200ms in the
      # condition — over 4s with no ceiling. The 5x ceiling stops it far short.
      assert System.monotonic_time(:millisecond) - started < 2_000
    end
  end
end
