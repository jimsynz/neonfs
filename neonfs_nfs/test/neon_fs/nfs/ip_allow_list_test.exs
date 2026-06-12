defmodule NeonFS.NFS.IpAllowListTest do
  use ExUnit.Case, async: true

  alias NeonFS.NFS.IpAllowList

  describe "allowed?/2" do
    test "an empty allow-list permits everyone (allow-all)" do
      assert IpAllowList.allowed?({203, 0, 113, 7}, [])
      assert IpAllowList.allowed?(nil, [])
    end

    test "a nil peer is denied when the allow-list is non-empty" do
      refute IpAllowList.allowed?(nil, ["10.0.0.0/8"])
    end

    test "exact IPv4 match" do
      assert IpAllowList.allowed?({192, 168, 1, 5}, ["192.168.1.5"])
      refute IpAllowList.allowed?({192, 168, 1, 6}, ["192.168.1.5"])
    end

    test "IPv4 CIDR match" do
      assert IpAllowList.allowed?({10, 1, 2, 3}, ["10.0.0.0/8"])
      assert IpAllowList.allowed?({10, 255, 255, 255}, ["10.0.0.0/8"])
      refute IpAllowList.allowed?({11, 0, 0, 1}, ["10.0.0.0/8"])
    end

    test "/32 is exact, /0 matches anything in-family" do
      assert IpAllowList.allowed?({10, 0, 0, 1}, ["10.0.0.1/32"])
      refute IpAllowList.allowed?({10, 0, 0, 2}, ["10.0.0.1/32"])
      assert IpAllowList.allowed?({8, 8, 8, 8}, ["0.0.0.0/0"])
    end

    test "matches against any entry in the list" do
      list = ["192.168.0.0/16", "203.0.113.7"]
      assert IpAllowList.allowed?({203, 0, 113, 7}, list)
      assert IpAllowList.allowed?({192, 168, 5, 5}, list)
      refute IpAllowList.allowed?({10, 0, 0, 1}, list)
    end

    test "IPv6 CIDR match" do
      peer = {0x2001, 0x0DB8, 0, 0, 0, 0, 0, 1}
      assert IpAllowList.allowed?(peer, ["2001:db8::/32"])
      refute IpAllowList.allowed?({0x2001, 0x0DB9, 0, 0, 0, 0, 0, 1}, ["2001:db8::/32"])
    end

    test "address families don't cross-match" do
      refute IpAllowList.allowed?({10, 0, 0, 1}, ["2001:db8::/32"])
      refute IpAllowList.allowed?({0x2001, 0x0DB8, 0, 0, 0, 0, 0, 1}, ["10.0.0.0/8"])
    end

    test "malformed entries are ignored (never match)" do
      refute IpAllowList.allowed?({10, 0, 0, 1}, ["not-an-ip", "10.0.0.0/999"])
    end
  end
end
