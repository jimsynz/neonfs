defmodule NeonFS.ClientTest do
  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Client
  alias NeonFS.Client.Router

  describe "core_call/3 routing (#1076)" do
    test "routes volume-scoped NeonFS.Core metadata writes to a root holder" do
      stub(Router, :volume_metadata_call, fn vol, mod, fun, args ->
        {:root, vol, mod, fun, args}
      end)

      assert {:root, "vol", NeonFS.Core, :delete_file, ["vol", "/x"]} =
               Client.core_call(NeonFS.Core, :delete_file, ["vol", "/x"])

      assert {:root, "vol", NeonFS.Core, :write_file_at, ["vol", "/x", 0, "d", []]} =
               Client.core_call(NeonFS.Core, :write_file_at, ["vol", "/x", 0, "d", []])
    end

    test "routes NeonFS.Core reads via cost-based Router.call" do
      stub(Router, :call, fn mod, fun, args -> {:cost_based, mod, fun, args} end)

      assert {:cost_based, NeonFS.Core, :get_file_meta, ["vol", "/x"]} =
               Client.core_call(NeonFS.Core, :get_file_meta, ["vol", "/x"])
    end

    test "routes volume-lifecycle ops (create/delete_volume) via cost-based Router.call" do
      stub(Router, :call, fn mod, fun, args -> {:cost_based, mod, fun, args} end)

      assert {:cost_based, NeonFS.Core, :delete_volume, ["vol"]} =
               Client.core_call(NeonFS.Core, :delete_volume, ["vol"])
    end

    test "routes non-NeonFS.Core modules via cost-based Router.call" do
      stub(Router, :call, fn mod, fun, args -> {:cost_based, mod, fun, args} end)

      assert {:cost_based, NeonFS.Core.VolumeRegistry, :get_by_name, ["vol"]} =
               Client.core_call(NeonFS.Core.VolumeRegistry, :get_by_name, ["vol"])
    end

    test "a write op with empty args falls back to cost-based routing (no crash)" do
      stub(Router, :call, fn mod, fun, args -> {:cost_based, mod, fun, args} end)

      assert {:cost_based, NeonFS.Core, :delete_file, []} =
               Client.core_call(NeonFS.Core, :delete_file, [])
    end
  end
end
