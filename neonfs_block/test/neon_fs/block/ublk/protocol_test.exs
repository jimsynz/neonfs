defmodule NeonFS.Block.Ublk.ProtocolTest do
  @moduledoc """
  The codec's half of the wire the helper's `protocol.rs` implements.

  Both halves are hand-rolled against the same written layout, so each is
  tested against the bytes rather than against the other: a shared round-trip
  would agree with itself even if both had drifted from the documented frame.
  """

  use ExUnit.Case, async: true

  alias NeonFS.Block.Ublk.Protocol

  @version 1

  describe "decoding a request" do
    test "reads each operation's header" do
      for {op, code} <- [read: 0, write: 1, flush: 2, discard: 3, write_zeroes: 4] do
        assert Protocol.op_code(op) == code

        payload = if op == :write, do: :binary.copy(<<0xA1>>, 512), else: <<>>
        frame = <<@version::8, code::8, 7::16, 8192::64, 512::32>> <> payload

        assert {:ok, request} = Protocol.decode_request(frame)
        assert request.op == op
        assert request.tag == 7
        assert request.offset == 8192
        assert request.length == 512
        assert request.data == payload
      end
    end

    test "refuses a version it does not speak, rather than reinterpreting it" do
      frame = <<@version + 1::8, 0::8, 0::16, 0::64, 512::32>>

      assert {:error, {:unsupported_version, next, @version}} = Protocol.decode_request(frame)
      assert next == @version + 1
    end

    test "refuses an operation code it has no meaning for" do
      frame = <<@version::8, 99::8, 0::16, 0::64, 512::32>>

      assert {:error, {:unknown_op, 99}} = Protocol.decode_request(frame)
    end

    # The header's length is what the guest asked to be written; a payload
    # that disagrees means one of the two is not what it claims, and picking
    # either lands bytes at an offset nothing intended.
    test "refuses a write whose payload is not as long as its header claims" do
      short = <<@version::8, 1::8, 0::16, 0::64, 512::32>> <> :binary.copy(<<0>>, 511)

      assert {:error, {:payload_length_mismatch, 512, 511}} = Protocol.decode_request(short)
    end

    test "refuses a payload on an operation that carries none" do
      frame = <<@version::8, 0::8, 0::16, 0::64, 512::32>> <> <<1, 2, 3>>

      assert {:error, {:unexpected_payload, :read, 3}} = Protocol.decode_request(frame)
    end

    test "refuses a frame too short to hold a header" do
      assert {:error, {:malformed_request, 4}} = Protocol.decode_request(<<1, 2, 3, 4>>)
    end
  end

  describe "encoding a reply" do
    test "a success carries its payload's length, not the request's" do
      data = :binary.copy(<<0xB2>>, 4096)

      assert <<@version::8, 0::8, 3::16, length::32, ^data::binary>> =
               Protocol.encode_ok(3, data) |> IO.iodata_to_binary()

      assert length == 4096
    end

    test "a success with no payload is a bare header" do
      assert <<@version::8, 0::8, 9::16, 0::32>> =
               Protocol.encode_ok(9) |> IO.iodata_to_binary()
    end

    test "a failure carries the errno and echoes the tag" do
      assert <<@version::8, 11::8, 12::16, 0::32>> =
               Protocol.encode_error(12, :stale_chunks) |> IO.iodata_to_binary()
    end
  end

  # These are the numbers the kernel hands the guest, so a wrong one is a
  # wrong diagnosis of a real failure — not a cosmetic slip.
  describe "errno mapping" do
    test "contention is retryable, not a device fault" do
      assert Protocol.errno(:stale_chunks) == 11
    end

    test "a fence says the server stopped serving, not that the disk broke" do
      assert Protocol.errno({:fenced, 4}) == 108
    end

    test "a bad request is invalid rather than a fault" do
      assert Protocol.errno({:out_of_range, 0, 512, 256}) == 22
      assert Protocol.errno({:unaligned_request, 1, 512}) == 22
    end

    test "a refusal is a permission problem" do
      assert Protocol.errno(%{class: :forbidden}) == 1
    end

    test "anything unmapped is EIO" do
      assert Protocol.errno(:something_new) == 5
    end
  end

  test "the header sizes the helper frames against" do
    assert Protocol.header_bytes() == %{request: 16, reply: 8}
    assert Protocol.version() == @version
  end
end
