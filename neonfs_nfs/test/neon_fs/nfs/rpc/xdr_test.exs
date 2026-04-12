defmodule NeonFS.NFS.RPC.XDRTest do
  use ExUnit.Case, async: true

  alias NeonFS.NFS.RPC.XDR

  describe "int" do
    test "encode/decode round-trips" do
      for val <- [0, 1, -1, 2_147_483_647, -2_147_483_648] do
        encoded = XDR.encode_int(val)
        assert {:ok, ^val, <<>>} = XDR.decode_int(encoded)
      end
    end

    test "decode returns truncated on insufficient data" do
      assert {:error, :truncated} = XDR.decode_int(<<1, 2, 3>>)
      assert {:error, :truncated} = XDR.decode_int(<<>>)
    end

    test "preserves trailing data" do
      data = XDR.encode_int(42) <> "trailing"
      assert {:ok, 42, "trailing"} = XDR.decode_int(data)
    end
  end

  describe "uint" do
    test "encode/decode round-trips" do
      for val <- [0, 1, 4_294_967_295] do
        encoded = XDR.encode_uint(val)
        assert {:ok, ^val, <<>>} = XDR.decode_uint(encoded)
      end
    end
  end

  describe "hyper_uint" do
    test "encode/decode round-trips for 64-bit values" do
      for val <- [0, 1, 0xFFFFFFFFFFFFFFFF, 1_099_511_627_776] do
        encoded = XDR.encode_hyper_uint(val)
        assert {:ok, ^val, <<>>} = XDR.decode_hyper_uint(encoded)
      end
    end

    test "decode returns truncated on insufficient data" do
      assert {:error, :truncated} = XDR.decode_hyper_uint(<<1, 2, 3, 4, 5, 6, 7>>)
    end
  end

  describe "hyper_int" do
    test "encode/decode round-trips for signed 64-bit values" do
      for val <- [0, -1, 9_223_372_036_854_775_807, -9_223_372_036_854_775_808] do
        encoded = XDR.encode_hyper_int(val)
        assert {:ok, ^val, <<>>} = XDR.decode_hyper_int(encoded)
      end
    end
  end

  describe "bool" do
    test "encode/decode round-trips" do
      assert {:ok, true, <<>>} = XDR.decode_bool(XDR.encode_bool(true))
      assert {:ok, false, <<>>} = XDR.decode_bool(XDR.encode_bool(false))
    end

    test "rejects invalid boolean values" do
      assert {:error, :invalid} = XDR.decode_bool(<<2::big-32>>)
    end

    test "returns truncated on insufficient data" do
      assert {:error, :truncated} = XDR.decode_bool(<<1, 2>>)
    end
  end

  describe "opaque" do
    test "encode/decode round-trips" do
      data = "hello"
      encoded = XDR.encode_opaque(data)
      assert {:ok, ^data, <<>>} = XDR.decode_opaque(encoded)
    end

    test "pads to 4-byte boundary" do
      data = "abc"
      encoded = XDR.encode_opaque(data)
      # 4 (length) + 3 (data) + 1 (padding) = 8
      assert byte_size(encoded) == 8
      assert {:ok, ^data, <<>>} = XDR.decode_opaque(encoded)
    end

    test "handles empty opaque" do
      encoded = XDR.encode_opaque(<<>>)
      assert {:ok, <<>>, <<>>} = XDR.decode_opaque(encoded)
    end

    test "handles opaque aligned to 4 bytes" do
      data = "abcd"
      encoded = XDR.encode_opaque(data)
      # 4 (length) + 4 (data) + 0 (no padding) = 8
      assert byte_size(encoded) == 8
      assert {:ok, ^data, <<>>} = XDR.decode_opaque(encoded)
    end

    test "returns truncated when body is incomplete" do
      # Length says 100 but only 4 bytes follow
      assert {:error, :truncated} = XDR.decode_opaque(<<100::big-32, 1, 2, 3, 4>>)
    end

    test "preserves trailing data after opaque" do
      data = "hi"
      encoded = XDR.encode_opaque(data) <> "rest"
      assert {:ok, ^data, "rest"} = XDR.decode_opaque(encoded)
    end
  end

  describe "string" do
    test "encode/decode round-trips" do
      str = "NeonFS"
      encoded = XDR.encode_string(str)
      assert {:ok, ^str, <<>>} = XDR.decode_string(encoded)
    end
  end

  describe "void" do
    test "decode consumes nothing" do
      assert {:ok, nil, "data"} = XDR.decode_void("data")
      assert {:ok, nil, <<>>} = XDR.decode_void(<<>>)
    end

    test "encode produces empty binary" do
      assert <<>> = XDR.encode_void()
    end
  end

  describe "xdr_pad" do
    test "calculates correct padding" do
      assert XDR.xdr_pad(0) == 0
      assert XDR.xdr_pad(1) == 3
      assert XDR.xdr_pad(2) == 2
      assert XDR.xdr_pad(3) == 1
      assert XDR.xdr_pad(4) == 0
      assert XDR.xdr_pad(5) == 3
    end
  end
end
