defmodule NFSServer.RPC.AuthTest do
  use ExUnit.Case, async: true

  alias NFSServer.RPC.Auth
  alias NFSServer.XDR

  doctest Auth

  describe "AUTH_NONE" do
    test "encodes flavor=0 with empty body" do
      assert <<0::32, 0::32>> == Auth.encode_opaque_auth(%Auth.None{})
    end

    test "decodes flavor=0 with empty body to %None{}" do
      bytes = <<0::32, 0::32, "trailing">>
      assert {:ok, %Auth.None{}, "trailing"} = Auth.decode_opaque_auth(bytes)
    end

    test "rejects AUTH_NONE with a non-empty body" do
      bytes = XDR.encode_uint(0) <> XDR.encode_var_opaque("oops")
      assert {:error, {:auth_badcred, :auth_none_with_body}} = Auth.decode_opaque_auth(bytes)
    end
  end

  describe "AUTH_SYS" do
    test "round-trips a fully-populated credential" do
      cred = %Auth.Sys{
        stamp: 0xDEADBEEF,
        machinename: "client.example.com",
        uid: 1000,
        gid: 1000,
        gids: [1000, 1001, 1002, 27]
      }

      assert {:ok, ^cred, <<>>} = cred |> Auth.encode_opaque_auth() |> Auth.decode_opaque_auth()
    end

    test "round-trips a credential with more than 16 supplementary gids" do
      gids = Enum.to_list(2000..2050)

      cred = %Auth.Sys{
        stamp: 1,
        machinename: "host",
        uid: 500,
        gid: 500,
        gids: gids
      }

      assert {:ok, ^cred, <<>>} = cred |> Auth.encode_opaque_auth() |> Auth.decode_opaque_auth()
    end

    test "rejects unsupported flavors with :auth_badcred" do
      bytes = XDR.encode_uint(6) <> XDR.encode_var_opaque(<<>>)
      assert {:error, {:auth_badcred, {:unsupported, 6}}} = Auth.decode_opaque_auth(bytes)
    end
  end
end
