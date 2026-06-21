defmodule NeonFS.Client.JoinPeersTest do
  use ExUnit.Case, async: true

  alias NeonFS.Client.Join
  alias NeonFS.Cluster.State

  defp peer(name, port \\ 9100) do
    %{id: "id-#{name}", name: name, last_seen: DateTime.utc_now(), dist_port: port}
  end

  describe "State.sanitise_peers/2" do
    test "drops the excluded node and collapses duplicate names" do
      peers = [peer(:a@h), peer(:b@h), peer(:a@h, 9200)]

      result = State.sanitise_peers(peers, :b@h)

      assert Enum.map(result, & &1.name) == [:a@h]
    end

    test "never keeps an entry for the node itself" do
      peers = [peer(:self@h), peer(:peer@h)]

      result = State.sanitise_peers(peers, :self@h)

      assert Enum.map(result, & &1.name) == [:peer@h]
    end
  end

  describe "joiner_ra_members/3" do
    test "core node lists itself once even when already advertised" do
      members = [:c@h, :b@h, :a@h]

      assert Join.joiner_ra_members(members, :c@h, :core) == [:c@h, :b@h, :a@h]
    end

    test "core node adds itself when absent from the advertised members" do
      assert Join.joiner_ra_members([:a@h], :b@h, :core) == [:b@h, :a@h]
    end

    test "non-core node is not added to Ra membership" do
      assert Join.joiner_ra_members([:a@h], :fuse@h, :fuse) == [:a@h]
    end
  end

  describe "ensure_redeem_port/1" do
    test "appends the default port to a bare host" do
      assert Join.ensure_redeem_port("core1") == "core1:9568"
    end

    test "leaves an explicit port untouched" do
      assert Join.ensure_redeem_port("core1:9568") == "core1:9568"
      assert Join.ensure_redeem_port("core1:80") == "core1:80"
    end

    test "appends the default port to a bracketed IPv6 address" do
      assert Join.ensure_redeem_port("[::1]") == "[::1]:9568"
    end

    test "leaves a bracketed IPv6 address with a port untouched" do
      assert Join.ensure_redeem_port("[::1]:9568") == "[::1]:9568"
    end
  end
end
