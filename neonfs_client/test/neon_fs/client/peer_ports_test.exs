defmodule NeonFS.Client.PeerPortsTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.PeerPorts

  setup do
    PeerPorts.reset()
    on_exit(&PeerPorts.reset/0)
    :ok
  end

  describe "publish/1" do
    test "publishes an entry a peer can be resolved by" do
      assert %{"a@h1" => 9100} = PeerPorts.publish(%{:a@h1 => 9100})
      assert System.get_env("NEONFS_PEER_PORTS") == "a@h1:9100"
    end

    # The whole reason this module exists. Join, Formation and Discovery each
    # know about different peers; the two that predate this used to
    # `System.put_env/2` the lot, so whichever ran last silently dropped the
    # others' entries and a dial failed with `:nxdomain` until the next
    # refresh put them back.
    test "merges rather than replacing" do
      PeerPorts.publish(%{:core@h1 => 9100})
      PeerPorts.publish(%{:sibling@h2 => 9102})

      assert PeerPorts.current() == %{"core@h1" => 9100, "sibling@h2" => 9102}
    end

    test "a later port for the same node wins" do
      PeerPorts.publish(%{:a@h1 => 9100})
      PeerPorts.publish(%{:a@h1 => 9200})

      assert PeerPorts.current() == %{"a@h1" => 9200}
    end

    # Publishing a zero would resolve a sibling to a broken port. Omitting it
    # lets `NeonFS.Epmd` fall through to `cluster.json`, which may know.
    test "drops a node with no distribution port" do
      PeerPorts.publish([{:has_port@h1, 9100}, {:no_port@h2, 0}])

      assert PeerPorts.current() == %{"has_port@h1" => 9100}
    end

    test "accepts string node names and a list of pairs" do
      PeerPorts.publish([{"a@h1", 9100}, {"b@h2", 9101}])

      assert PeerPorts.current() == %{"a@h1" => 9100, "b@h2" => 9101}
    end

    test "publishing nothing leaves the variable untouched" do
      assert PeerPorts.publish([]) == %{}
      refute System.get_env("NEONFS_PEER_PORTS")
    end

    # An IPv6 host has colons of its own, so a naive split would read the
    # wrong one as the port.
    test "round-trips a host containing colons" do
      PeerPorts.publish(%{:"a@::1" => 9100})

      assert PeerPorts.current() == %{"a@::1" => 9100}
    end

    # Otherwise a republish of the same set reorders the variable and looks
    # like a change to anything watching it.
    test "is stable across republishes of the same set" do
      PeerPorts.publish(%{:b@h2 => 9101, :a@h1 => 9100})
      first = System.get_env("NEONFS_PEER_PORTS")

      PeerPorts.publish(%{:a@h1 => 9100, :b@h2 => 9101})

      assert System.get_env("NEONFS_PEER_PORTS") == first
    end

    test "preserves entries set by something outside this module" do
      System.put_env("NEONFS_PEER_PORTS", "legacy@h9:9999")

      PeerPorts.publish(%{:a@h1 => 9100})

      assert PeerPorts.current() == %{"legacy@h9" => 9999, "a@h1" => 9100}
    end

    test "ignores an unparseable existing entry rather than losing the rest" do
      System.put_env("NEONFS_PEER_PORTS", "nonsense,good@h1:9100")

      PeerPorts.publish(%{:a@h2 => 9200})

      assert PeerPorts.current() == %{"good@h1" => 9100, "a@h2" => 9200}
    end
  end
end
