defmodule NeonFS.TestSupport.PeerClusterTest do
  use ExUnit.Case, async: true

  alias NeonFS.TestSupport.PeerCluster

  describe "transient_rpc_error?/1 (#1396)" do
    test "retries bring-up failures expressed as bare atoms" do
      for reason <- [:nodedown, :timeout, :noconnection, :noproc] do
        assert PeerCluster.transient_rpc_error?(reason), "expected #{inspect(reason)} transient"
      end
    end

    test "retries tagged-tuple forms surfaced by :rpc.call" do
      # `GenServer.call` timeout from a still-starting process comes back as a
      # `{:timeout, mfa}` tuple, not the bare `:timeout` atom (the gap that let
      # `neonfs_docker` setup_all flake under load).
      assert PeerCluster.transient_rpc_error?({:timeout, {GenServer, :call, [:x, :y, 5000]}})
      assert PeerCluster.transient_rpc_error?({:nodedown, :node1@host})
      assert PeerCluster.transient_rpc_error?({:noproc, {GenServer, :call, [:idx, :req, 10_000]}})
    end

    test "unwraps {:EXIT, reason} from a remote exit" do
      assert PeerCluster.transient_rpc_error?({:EXIT, {:noproc, {GenServer, :call, []}}})
      assert PeerCluster.transient_rpc_error?({:EXIT, {:timeout, {GenServer, :call, []}}})
    end

    test "does not retry genuine errors" do
      refute PeerCluster.transient_rpc_error?(:undef)
      refute PeerCluster.transient_rpc_error?({:undef, [{Mod, :fun, 1, []}]})
      refute PeerCluster.transient_rpc_error?(:killed)
      refute PeerCluster.transient_rpc_error?({:EXIT, :killed})
      refute PeerCluster.transient_rpc_error?(%ArgumentError{message: "bad"})
    end
  end
end
