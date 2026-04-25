defmodule NFSServer.RPC.RecordMarkingTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NFSServer.RPC.RecordMarking

  doctest RecordMarking

  describe "encode/1" do
    test "wraps a body in a single final fragment" do
      assert <<1::1, 5::31, "hello">> ==
               IO.iodata_to_binary(RecordMarking.encode("hello"))
    end

    test "wraps an empty body" do
      assert <<1::1, 0::31>> == IO.iodata_to_binary(RecordMarking.encode(<<>>))
    end

    test "accepts iodata so streaming bodies pass through unflattened" do
      iodata = ["he", ["llo"], <<>>]
      encoded = RecordMarking.encode(iodata)
      assert <<1::1, 5::31, "hello">> == IO.iodata_to_binary(encoded)
    end
  end

  describe "decode_fragment/1" do
    test "decodes a single final fragment" do
      bytes = <<1::1, 5::31, "hello">>
      assert {:ok, true, "hello", <<>>} = RecordMarking.decode_fragment(bytes)
    end

    test "decodes a non-final fragment and leaves the rest" do
      bytes = <<0::1, 3::31, "abc", "trailing">>
      assert {:ok, false, "abc", "trailing"} = RecordMarking.decode_fragment(bytes)
    end

    test "returns :incomplete on partial header" do
      assert :incomplete = RecordMarking.decode_fragment(<<0, 0>>)
    end

    test "returns :incomplete when header says more bytes than we have" do
      assert :incomplete = RecordMarking.decode_fragment(<<1::1, 100::31, "only-a-bit">>)
    end
  end

  describe "decode_message/1 (multi-fragment reassembly)" do
    test "joins two fragments into a single message" do
      bytes =
        IO.iodata_to_binary([
          RecordMarking.encode_fragment("foo", false),
          RecordMarking.encode_fragment("bar", true)
        ])

      assert {:ok, "foobar", <<>>} = RecordMarking.decode_message(bytes)
    end

    test "handles a message split across many fragments" do
      payload = String.duplicate("x", 1000)
      fragments = RecordMarking.encode_fragments(payload, 100) |> IO.iodata_to_binary()

      assert {:ok, ^payload, <<>>} = RecordMarking.decode_message(fragments)
    end

    test "leaves trailing bytes for the next message" do
      bytes =
        IO.iodata_to_binary([
          RecordMarking.encode("first"),
          RecordMarking.encode("second")
        ])

      assert {:ok, "first", rest} = RecordMarking.decode_message(bytes)
      assert {:ok, "second", <<>>} = RecordMarking.decode_message(rest)
    end

    test "returns :incomplete when no terminating fragment yet" do
      bytes = RecordMarking.encode_fragment("partial", false) |> IO.iodata_to_binary()
      assert :incomplete = RecordMarking.decode_message(bytes)
    end

    test "returns :incomplete when a header is split mid-frame" do
      bytes = RecordMarking.encode("hello") |> IO.iodata_to_binary()
      <<head::binary-size(7), _::binary>> = bytes
      assert :incomplete = RecordMarking.decode_message(head)
    end
  end

  describe "property: encode then decode round-trips" do
    property "any body up to 16 KiB round-trips through one fragment" do
      check all(body <- binary(max_length: 16 * 1024)) do
        encoded = body |> RecordMarking.encode() |> IO.iodata_to_binary()
        assert {:ok, ^body, <<>>} = RecordMarking.decode_message(encoded)
      end
    end

    property "any body up to 16 KiB round-trips through fragmented encoding" do
      check all(
              body <- binary(min_length: 1, max_length: 16 * 1024),
              max_frag <- integer(64..2048)
            ) do
        encoded = body |> RecordMarking.encode_fragments(max_frag) |> IO.iodata_to_binary()
        assert {:ok, ^body, <<>>} = RecordMarking.decode_message(encoded)
      end
    end
  end
end
