defmodule NeonFS.NFS.NLM.CodecTest do
  use ExUnit.Case, async: true

  alias NeonFS.NFS.NLM.Codec
  alias NeonFS.NFS.RPC.XDR

  describe "decode_lockargs" do
    test "decodes valid lock arguments" do
      data =
        XDR.encode_opaque("cookie123") <>
          XDR.encode_bool(true) <>
          XDR.encode_bool(true) <>
          encode_test_lock() <>
          XDR.encode_bool(false) <>
          XDR.encode_int(1)

      assert {:ok, args} = Codec.decode_lockargs(data)
      assert args.cookie == "cookie123"
      assert args.block == true
      assert args.exclusive == true
      assert args.lock.caller_name == "testhost"
      assert args.lock.fh == <<1, 2, 3, 4>>
      assert args.lock.oh == <<5, 6>>
      assert args.lock.svid == 42
      assert args.lock.offset == 100
      assert args.lock.length == 200
      assert args.reclaim == false
      assert args.state == 1
    end

    test "returns error on truncated data" do
      assert {:error, :truncated} = Codec.decode_lockargs(<<>>)
    end
  end

  describe "decode_testargs" do
    test "decodes valid test arguments" do
      data =
        XDR.encode_opaque("testcookie") <>
          XDR.encode_bool(false) <>
          encode_test_lock()

      assert {:ok, args} = Codec.decode_testargs(data)
      assert args.cookie == "testcookie"
      assert args.exclusive == false
      assert args.lock.caller_name == "testhost"
      assert args.lock.svid == 42
      assert args.lock.offset == 100
      assert args.lock.length == 200
    end
  end

  describe "decode_unlockargs" do
    test "decodes valid unlock arguments" do
      data =
        XDR.encode_opaque("ucookie") <>
          encode_test_lock()

      assert {:ok, args} = Codec.decode_unlockargs(data)
      assert args.cookie == "ucookie"
      assert args.lock.caller_name == "testhost"
    end
  end

  describe "decode_cancargs" do
    test "decodes valid cancel arguments" do
      data =
        XDR.encode_opaque("ccookie") <>
          XDR.encode_bool(true) <>
          XDR.encode_bool(false) <>
          encode_test_lock()

      assert {:ok, args} = Codec.decode_cancargs(data)
      assert args.cookie == "ccookie"
      assert args.block == true
      assert args.exclusive == false
    end
  end

  describe "decode_notify" do
    test "decodes valid notify arguments" do
      data = XDR.encode_string("crashedhost") <> XDR.encode_int(5)

      assert {:ok, args} = Codec.decode_notify(data)
      assert args.name == "crashedhost"
      assert args.state == 5
    end
  end

  describe "encode_res" do
    test "encodes granted result" do
      result = Codec.encode_res("cookie", :granted)

      # Decode to verify
      assert {:ok, "cookie", rest} = XDR.decode_opaque(result)
      assert {:ok, 0, <<>>} = XDR.decode_int(rest)
    end

    test "encodes denied result" do
      result = Codec.encode_res("cookie", :denied)

      assert {:ok, "cookie", rest} = XDR.decode_opaque(result)
      assert {:ok, 1, <<>>} = XDR.decode_int(rest)
    end

    test "encodes all status values" do
      statuses = [
        {:granted, 0},
        {:denied, 1},
        {:denied_nolocks, 2},
        {:blocked, 3},
        {:denied_grace_period, 4},
        {:deadlck, 5},
        {:rofs, 6},
        {:stale_fh, 7},
        {:fbig, 8},
        {:failed, 9}
      ]

      for {stat, expected_val} <- statuses do
        result = Codec.encode_res(<<>>, stat)
        assert {:ok, <<>>, rest} = XDR.decode_opaque(result)
        assert {:ok, ^expected_val, <<>>} = XDR.decode_int(rest)
      end
    end
  end

  describe "encode_testres" do
    test "encodes granted test result" do
      result = Codec.encode_testres("cookie", :granted)

      assert {:ok, "cookie", rest} = XDR.decode_opaque(result)
      # discriminant 0 = GRANTED
      assert {:ok, 0, <<>>} = XDR.decode_int(rest)
    end

    test "encodes denied test result with holder info" do
      holder = %{
        exclusive: true,
        svid: 99,
        oh: <<7, 8>>,
        offset: 500,
        length: 1000
      }

      result = Codec.encode_testres("cookie", {:denied, holder})

      assert {:ok, "cookie", rest} = XDR.decode_opaque(result)
      # discriminant 1 = DENIED
      assert {:ok, 1, rest} = XDR.decode_int(rest)
      assert {:ok, true, rest} = XDR.decode_bool(rest)
      assert {:ok, 99, rest} = XDR.decode_int(rest)
      assert {:ok, <<7, 8>>, rest} = XDR.decode_opaque(rest)
      assert {:ok, 500, rest} = XDR.decode_hyper_uint(rest)
      assert {:ok, 1000, <<>>} = XDR.decode_hyper_uint(rest)
    end
  end

  ## Test helpers

  defp encode_test_lock do
    XDR.encode_string("testhost") <>
      XDR.encode_opaque(<<1, 2, 3, 4>>) <>
      XDR.encode_opaque(<<5, 6>>) <>
      XDR.encode_int(42) <>
      XDR.encode_hyper_uint(100) <>
      XDR.encode_hyper_uint(200)
  end
end
