defmodule NeonFS.TestSupport.PeerClusterTest do
  use ExUnit.Case, async: true

  alias NeonFS.TestSupport.PeerCluster

  describe "transient_rpc_error?/1" do
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

  describe "allocate_peer_port/0" do
    test "assigns from below the kernel's ephemeral range" do
      {:ok, range} = File.read("/proc/sys/net/ipv4/ip_local_port_range")
      [ephemeral_floor, _ceiling] = range |> String.split() |> Enum.map(&String.to_integer/1)

      port = PeerCluster.allocate_peer_port()

      # The kernel never auto-assigns below this floor, so a port handed out
      # here cannot be taken by an outgoing connection before the peer that
      # was given it gets around to binding.
      assert port < ephemeral_floor
    end

    # Interleaved with unrelated `unique_integer` traffic, which is how a real
    # suite calls this. Back-to-back allocations cannot catch the failure this
    # guards: deriving the offset from the VM-wide `unique_integer` counter
    # aliases two allocations onto one port whenever they are separated by a
    # multiple of the band width, and only intervening consumers create gaps
    # that large. The naive version produced 13 duplicates in 400.
    test "never repeats a port, even with other unique_integer consumers interleaved" do
      ports =
        for _ <- 1..400 do
          for _ <- 1..:rand.uniform(40), do: System.unique_integer([:positive, :monotonic])
          PeerCluster.allocate_peer_port()
        end

      assert length(Enum.uniq(ports)) == length(ports)
    end

    test "assigns something bindable" do
      port = PeerCluster.allocate_peer_port()

      assert {:ok, socket} = :gen_tcp.listen(port, [])
      :gen_tcp.close(socket)
    end
  end
end
