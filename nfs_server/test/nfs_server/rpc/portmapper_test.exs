defmodule NFSServer.RPC.PortmapperTest do
  use ExUnit.Case, async: true

  alias NFSServer.RPC.{Auth, Portmapper}
  alias NFSServer.XDR

  describe "PMAPPROC_NULL" do
    test "ping returns an empty body" do
      assert {:ok, <<>>} = Portmapper.handle_call(0, <<>>, %Auth.None{}, %{})
    end
  end

  describe "PMAPPROC_GETPORT" do
    test "returns the registered port for (program, version, proto)" do
      args =
        XDR.encode_uint(100_003) <>
          XDR.encode_uint(3) <>
          XDR.encode_uint(6) <>
          XDR.encode_uint(0)

      ctx = %{portmap_mappings: %{{100_003, 3, 6} => 2049}}

      assert {:ok, body} = Portmapper.handle_call(3, args, %Auth.None{}, ctx)
      assert {:ok, 2049, <<>>} = XDR.decode_uint(body)
    end

    test "returns 0 when the mapping is unknown" do
      args =
        XDR.encode_uint(100_999) <>
          XDR.encode_uint(1) <>
          XDR.encode_uint(6) <>
          XDR.encode_uint(0)

      assert {:ok, body} = Portmapper.handle_call(3, args, %Auth.None{}, %{})
      assert {:ok, 0, <<>>} = XDR.decode_uint(body)
    end

    test "returns :garbage_args on a too-short payload" do
      assert :garbage_args = Portmapper.handle_call(3, <<0, 0>>, %Auth.None{}, %{})
    end
  end

  describe "unknown procedures" do
    test "PMAPPROC_SET / UNSET / DUMP / CALLIT all return :proc_unavail" do
      for proc <- [1, 2, 4, 5] do
        assert :proc_unavail = Portmapper.handle_call(proc, <<>>, %Auth.None{}, %{})
      end
    end
  end
end
