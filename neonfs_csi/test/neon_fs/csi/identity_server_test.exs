defmodule NeonFS.CSI.IdentityServerTest do
  use ExUnit.Case, async: false
  use Mimic

  alias Csi.V1.{
    GetPluginCapabilitiesRequest,
    GetPluginInfoRequest,
    PluginCapability,
    ProbeRequest
  }

  alias NeonFS.Client.Discovery
  alias NeonFS.CSI.IdentityServer

  setup :verify_on_exit!

  describe "GetPluginInfo" do
    test "returns the canonical CSI driver name" do
      reply = IdentityServer.get_plugin_info(%GetPluginInfoRequest{}, nil)
      assert reply.name == "neonfs.csi.harton.dev"
    end

    test "reports the plugin version from the application spec" do
      reply = IdentityServer.get_plugin_info(%GetPluginInfoRequest{}, nil)
      assert is_binary(reply.vendor_version)
      refute reply.vendor_version == ""
    end

    test "echoes the configured mode in the manifest map" do
      Application.put_env(:neonfs_csi, :mode, :node)

      try do
        reply = IdentityServer.get_plugin_info(%GetPluginInfoRequest{}, nil)
        assert reply.manifest["mode"] == "node"
      after
        Application.delete_env(:neonfs_csi, :mode)
      end
    end
  end

  describe "GetPluginCapabilities" do
    test "controller mode advertises CONTROLLER_SERVICE" do
      Application.put_env(:neonfs_csi, :mode, :controller)

      try do
        reply = IdentityServer.get_plugin_capabilities(%GetPluginCapabilitiesRequest{}, nil)

        assert [
                 %PluginCapability{
                   type: {:service, %PluginCapability.Service{type: :CONTROLLER_SERVICE}}
                 }
               ] =
                 reply.capabilities
      after
        Application.delete_env(:neonfs_csi, :mode)
      end
    end

    test "node mode advertises no plugin-level capabilities" do
      Application.put_env(:neonfs_csi, :mode, :node)

      try do
        reply = IdentityServer.get_plugin_capabilities(%GetPluginCapabilitiesRequest{}, nil)
        assert reply.capabilities == []
      after
        Application.delete_env(:neonfs_csi, :mode)
      end
    end
  end

  describe "Probe" do
    test "ready when Discovery sees at least one core node" do
      stub(Discovery, :get_core_nodes, fn -> [:core@host] end)

      reply = IdentityServer.probe(%ProbeRequest{}, nil)
      assert reply.ready.value == true
    end

    test "not ready when Discovery sees no core nodes" do
      stub(Discovery, :get_core_nodes, fn -> [] end)

      reply = IdentityServer.probe(%ProbeRequest{}, nil)
      assert reply.ready.value == false
    end

    test "not ready when Discovery raises (e.g. not started yet)" do
      stub(Discovery, :get_core_nodes, fn -> raise "no discovery" end)

      reply = IdentityServer.probe(%ProbeRequest{}, nil)
      assert reply.ready.value == false
    end
  end
end
