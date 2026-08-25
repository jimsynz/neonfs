defmodule NeonFS.Client.ServiceInfoTest do
  use ExUnit.Case, async: true

  alias NeonFS.Client.ServiceInfo

  describe "new/3" do
    test "creates with required fields and defaults" do
      info = ServiceInfo.new(:node@host, :core)

      assert info.node == :node@host
      assert info.type == :core
      assert %DateTime{} = info.registered_at
      assert info.metadata == %{}
      assert info.status == :online
      assert info.dist_port == 0
    end

    # A field, not a metadata entry: every node has a distribution port, and
    # `NeonFS.Client.PeerPorts` reads it off every registration without
    # having to guess whether this one carries it.
    test "carries the node's distribution port through a Ra round trip" do
      info = ServiceInfo.new(:node@host, :core, dist_port: 9101)

      assert info.dist_port == 9101
      assert info |> ServiceInfo.to_map() |> ServiceInfo.from_map() |> Map.get(:dist_port) == 9101
    end

    # A registration stored before the field existed reads as zero, which is
    # what "not dialable by a sibling" is spelled as everywhere else.
    test "reads a stored map with no distribution port as zero" do
      stored = %{node: :node@host, type: :core, status: :online, metadata: %{}}

      assert ServiceInfo.from_map(stored).dist_port == 0
    end

    test "accepts custom options" do
      now = DateTime.utc_now()

      info =
        ServiceInfo.new(:fuse@host, :fuse,
          registered_at: now,
          metadata: %{version: "1.0"},
          status: :draining
        )

      assert info.node == :fuse@host
      assert info.type == :fuse
      assert info.registered_at == now
      assert info.metadata == %{version: "1.0"}
      assert info.status == :draining
    end
  end

  describe "for_self/2" do
    setup do
      saved = System.get_env("NEONFS_DIST_PORT")

      on_exit(fn ->
        case saved do
          nil -> System.delete_env("NEONFS_DIST_PORT")
          value -> System.put_env("NEONFS_DIST_PORT", value)
        end
      end)

      :ok
    end

    # Four of the five places that build a registration describe this node,
    # and each would otherwise have to remember the port. One that forgot
    # would register a node that discovers and routes fine and cannot be
    # dialled by a sibling — invisible until something tries.
    test "fills in this node and its distribution port" do
      System.put_env("NEONFS_DIST_PORT", "9101")

      info = ServiceInfo.for_self(:csi, metadata: %{mode: :controller})

      assert info.node == Node.self()
      assert info.type == :csi
      assert info.dist_port == 9101
      assert info.metadata == %{mode: :controller}
    end

    test "is zero when the node has no distribution port configured" do
      System.delete_env("NEONFS_DIST_PORT")

      assert ServiceInfo.for_self(:csi).dist_port == 0
    end

    test "an explicit port wins over the environment" do
      System.put_env("NEONFS_DIST_PORT", "9101")

      assert ServiceInfo.for_self(:csi, dist_port: 9999).dist_port == 9999
    end
  end

  describe "to_map/1" do
    test "converts struct to plain map" do
      info = ServiceInfo.new(:node@host, :core, metadata: %{cap: :full})
      map = ServiceInfo.to_map(info)

      assert is_map(map)
      refute is_struct(map)
      assert map.node == :node@host
      assert map.type == :core
      assert map.metadata == %{cap: :full}
      assert map.status == :online
      assert %DateTime{} = map.registered_at
    end
  end

  describe "from_map/1" do
    test "passes through existing ServiceInfo structs unchanged" do
      info = ServiceInfo.new(:node@host, :core)
      assert ServiceInfo.from_map(info) == info
    end

    test "reconstructs from a plain map" do
      now = DateTime.utc_now()

      map = %{
        node: :node@host,
        type: :fuse,
        registered_at: now,
        metadata: %{v: "2"},
        status: :offline
      }

      info = ServiceInfo.from_map(map)

      assert %ServiceInfo{} = info
      assert info.node == :node@host
      assert info.type == :fuse
      assert info.registered_at == now
      assert info.metadata == %{v: "2"}
      assert info.status == :offline
    end

    test "fills defaults for missing optional fields" do
      map = %{node: :node@host, type: :core}
      info = ServiceInfo.from_map(map)

      assert info.node == :node@host
      assert info.type == :core
      assert %DateTime{} = info.registered_at
      assert info.metadata == %{}
      assert info.status == :online
      assert info.dist_port == 0
    end

    # A field, not a metadata entry: every node has a distribution port, and
    # `NeonFS.Client.PeerPorts` reads it off every registration without
    # having to guess whether this one carries it.
    test "carries the node's distribution port through a Ra round trip" do
      info = ServiceInfo.new(:node@host, :core, dist_port: 9101)

      assert info.dist_port == 9101
      assert info |> ServiceInfo.to_map() |> ServiceInfo.from_map() |> Map.get(:dist_port) == 9101
    end

    # A registration stored before the field existed reads as zero, which is
    # what "not dialable by a sibling" is spelled as everywhere else.
    test "reads a stored map with no distribution port as zero" do
      stored = %{node: :node@host, type: :core, status: :online, metadata: %{}}

      assert ServiceInfo.from_map(stored).dist_port == 0
    end

    test "round-trips through to_map and from_map" do
      original = ServiceInfo.new(:node@host, :s3, metadata: %{region: "nz"})
      round_tripped = original |> ServiceInfo.to_map() |> ServiceInfo.from_map()

      assert round_tripped.node == original.node
      assert round_tripped.type == original.type
      assert round_tripped.registered_at == original.registered_at
      assert round_tripped.metadata == original.metadata
      assert round_tripped.status == original.status
    end
  end
end
