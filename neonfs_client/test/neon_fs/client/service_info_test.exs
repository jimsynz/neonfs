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
