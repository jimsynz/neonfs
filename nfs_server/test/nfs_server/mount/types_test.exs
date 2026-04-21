defmodule NFSServer.Mount.TypesTest do
  use ExUnit.Case, async: true

  alias NFSServer.Mount.Types
  alias NFSServer.Mount.Types.{ExportNode, MountList}

  describe "mountstat3 round-trip" do
    test "encodes / decodes every defined status" do
      for stat <- [
            :ok,
            :perm,
            :noent,
            :io,
            :acces,
            :notdir,
            :inval,
            :nametoolong,
            :notsupp,
            :serverfault
          ] do
        encoded = Types.encode_stat(stat)
        assert {:ok, ^stat, <<>>} = Types.decode_stat(encoded)
      end
    end

    test "decodes an unknown code as {:unknown, n}" do
      encoded = NFSServer.XDR.encode_uint(99_999)
      assert {:ok, {:unknown, 99_999}, <<>>} = Types.decode_stat(encoded)
    end
  end

  describe "fhandle3 length cap" do
    test "round-trips bytes up to 64" do
      for len <- [0, 1, 32, 64] do
        fh = :crypto.strong_rand_bytes(len)
        assert {:ok, ^fh, <<>>} = fh |> Types.encode_fhandle3() |> Types.decode_fhandle3()
      end
    end

    test "rejects an over-long fhandle on decode" do
      over = :crypto.strong_rand_bytes(65)
      bytes = NFSServer.XDR.encode_var_opaque(over)
      assert {:error, :fhandle_too_long} = Types.decode_fhandle3(bytes)
    end
  end

  describe "dirpath / name length caps" do
    test "decode rejects a path > 1024 bytes" do
      long = String.duplicate("a", 1025)
      bytes = NFSServer.XDR.encode_string(long)
      assert {:error, :path_too_long} = Types.decode_dirpath(bytes)
    end

    test "decode rejects a name > 255 bytes" do
      long = String.duplicate("n", 256)
      bytes = NFSServer.XDR.encode_string(long)
      assert {:error, :name_too_long} = Types.decode_name(bytes)
    end
  end

  describe "mountres3 encoding" do
    test "OK reply carries fhandle and auth_flavors" do
      bytes = Types.encode_mountres3_ok("FH", [1])
      # status (4) + opaque len (4) + opaque + pad to 4 + array len (4) + 1 uint (4)
      assert <<0::32, 2::32, "FH", 0, 0, 1::32, 1::32>> == bytes
    end

    test "error reply is just the status" do
      assert <<2::32>> == Types.encode_mountres3_err(:noent)
    end
  end

  describe "encode_chain / decode_chain" do
    test "empty list round-trips as a single FALSE flag" do
      bytes = Types.encode_chain([], &Types.encode_name/1)
      assert <<0, 0, 0, 0>> == bytes
      assert {:ok, [], <<>>} = Types.decode_chain(bytes, &Types.decode_name/1)
    end

    test "round-trips a multi-item list" do
      items = ["alice", "bob", "charlie"]
      bytes = Types.encode_chain(items, &Types.encode_name/1)
      assert {:ok, ^items, <<>>} = Types.decode_chain(bytes, &Types.decode_name/1)
    end
  end

  describe "mountlist entry round-trip" do
    test "encodes hostname + directory" do
      entry = %MountList{hostname: "client.example.com", directory: "/exports/foo"}

      assert {:ok, ^entry, <<>>} =
               entry |> Types.encode_mountlist_entry() |> Types.decode_mountlist_entry()
    end
  end

  describe "exportnode round-trip" do
    test "encodes directory + group list" do
      node = %ExportNode{dir: "/exports/bar", groups: ["everyone", "admins"]}
      assert {:ok, ^node, <<>>} = node |> Types.encode_exportnode() |> Types.decode_exportnode()
    end

    test "encodes an exportnode with no groups (empty group list)" do
      node = %ExportNode{dir: "/exports/empty", groups: []}
      assert {:ok, ^node, <<>>} = node |> Types.encode_exportnode() |> Types.decode_exportnode()
    end
  end
end
