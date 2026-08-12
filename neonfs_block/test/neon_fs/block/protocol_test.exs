defmodule NeonFS.Block.ProtocolTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NeonFS.Block.Protocol

  doctest NeonFS.Block.Protocol

  @request_magic 0x25609513
  @ihaveopt 0x49484156454F5054
  @simple_reply_magic 0x67446698

  @export %{
    size: 8 * 1024 * 1024,
    read_only: false,
    logical_block_size: 4096,
    physical_block_size: 4096
  }

  describe "the handshake greeting" do
    test "advertises fixed-newstyle, without which no client can proceed" do
      assert <<0x4E42444D41474943::64, 0x49484156454F5054::64, flags::16>> =
               Protocol.server_greeting()

      assert Bitwise.band(flags, 1) == 1, "NBD_FLAG_FIXED_NEWSTYLE must be set"
      assert Bitwise.band(flags, 2) == 2, "NBD_FLAG_NO_ZEROES is offered"
    end

    test "accepts client flags that set fixed-newstyle, and reports the no-zeroes ask" do
      assert {:ok, %{fixed_newstyle: true, no_zeroes: false}, "tail"} =
               Protocol.decode_client_flags(<<1::32, "tail">>)

      assert {:ok, %{fixed_newstyle: true, no_zeroes: true}, ""} =
               Protocol.decode_client_flags(<<3::32>>)
    end

    test "refuses a client that does not speak fixed-newstyle" do
      assert {:error, :fixed_newstyle_required} = Protocol.decode_client_flags(<<0::32>>)
    end

    test "waits for the whole flags field rather than guessing" do
      assert :incomplete = Protocol.decode_client_flags(<<1::16>>)
      assert :incomplete = Protocol.decode_client_flags(<<>>)
    end
  end

  describe "option decoding" do
    test "reads NBD_OPT_EXPORT_NAME's name" do
      frame = option_frame(1, "neonfs")
      assert {:ok, {:export_name, "neonfs"}, ""} = Protocol.decode_option(frame)
    end

    test "reads NBD_OPT_GO's name and info requests" do
      payload = <<6::32, "neonfs", 2::16, 0::16, 3::16>>

      assert {:ok, {:go, %{name: "neonfs", info_requests: [0, 3]}}, ""} =
               Protocol.decode_option(option_frame(7, payload))
    end

    test "reads NBD_OPT_INFO the same way as GO" do
      payload = <<6::32, "neonfs", 0::16>>

      assert {:ok, {:info, %{name: "neonfs", info_requests: []}}, ""} =
               Protocol.decode_option(option_frame(6, payload))
    end

    test "names the payload-free options" do
      assert {:ok, {:abort, nil}, ""} = Protocol.decode_option(option_frame(2, ""))
      assert {:ok, {:list, nil}, ""} = Protocol.decode_option(option_frame(3, ""))
      assert {:ok, {:structured_reply, nil}, ""} = Protocol.decode_option(option_frame(8, ""))
    end

    # The numbers, spelled out against the specification rather than against
    # this module: a code map shifted by one still round-trips through its own
    # encoder, so only the constants themselves can catch it. A real client
    # sending NBD_OPT_GO (7) into a map that reads 7 as NBD_OPT_INFO gets a
    # correct-looking info reply and a connection that never transmits.
    test "reads each option at the code the specification assigns it" do
      for {code, option} <- [
            {1, :export_name},
            {2, :abort},
            {3, :list},
            {5, :starttls},
            {6, :info},
            {7, :go},
            {8, :structured_reply}
          ] do
        assert {:ok, decoded, ""} =
                 Protocol.decode_option(option_frame(code, payload_for(option)))

        assert elem(decoded, 0) == option, "option #{code} decoded as #{inspect(decoded)}"
      end
    end

    test "returns an unknown option as unknown rather than as an error" do
      # The protocol's answer to an unrecognised option is a reply, not a
      # disconnect, so the caller needs the code to reply about.
      assert {:ok, {:unknown, %{code: 4242, payload: "x"}}, ""} =
               Protocol.decode_option(option_frame(4242, "x"))
    end

    test "keeps what follows a complete option" do
      frame = option_frame(2, "") <> "next"
      assert {:ok, {:abort, nil}, "next"} = Protocol.decode_option(frame)
    end

    test "waits for a payload that has not all arrived" do
      assert :incomplete = Protocol.decode_option(<<@ihaveopt::64, 1::32, 16::32, "short">>)
      assert :incomplete = Protocol.decode_option(<<@ihaveopt::64, 1::32>>)
      assert :incomplete = Protocol.decode_option(<<>>)
    end

    test "rejects a complete frame with the wrong magic" do
      assert {:error, {:bad_option_magic, _}} =
               Protocol.decode_option(<<0::64, 1::32, 0::32, "">>)
    end

    test "treats a malformed GO payload as unknown rather than raising" do
      # A name length that overruns its own payload: complete frame, nonsense
      # contents. Answering `NBD_REP_ERR_INVALID` needs the option code, so it
      # comes back as unknown rather than as an error.
      assert {:ok, {:unknown, %{code: 7}}, ""} =
               Protocol.decode_option(option_frame(7, <<99::32, "no">>))
    end
  end

  describe "option replies" do
    test "carry the option being answered and the reply type" do
      reply = Protocol.encode_option_reply(:go, :ack)
      assert <<0x0003E889045565A9::64, 7::32, 1::32, 0::32>> = reply
    end

    test "refuse structured replies with the code the protocol requires" do
      assert <<_magic::64, 8::32, 0x80000001::32, 0::32>> =
               Protocol.encode_option_reply(:structured_reply, :err_unsup)
    end

    test "carry their payload's length" do
      payload = Protocol.encode_info_export(@export)
      reply = Protocol.encode_option_reply(:go, :info, payload)

      assert <<_magic::64, 7::32, 3::32, length::32, rest::binary>> = reply
      assert length == byte_size(payload)
      assert rest == payload
    end
  end

  describe "export information" do
    test "NBD_INFO_EXPORT carries the size and the transmission flags" do
      assert <<0::16, size::64, flags::16>> = Protocol.encode_info_export(@export)
      assert size == @export.size
      assert Bitwise.band(flags, 1) == 1, "NBD_FLAG_HAS_FLAGS is mandatory"
      assert Bitwise.band(flags, 4) == 4, "flush must be offered"
      assert Bitwise.band(flags, 8) == 8, "FUA must be offered"
      assert Bitwise.band(flags, 32) == 32, "trim must be offered"
      assert Bitwise.band(flags, 64) == 64, "write zeroes must be offered"
      assert Bitwise.band(flags, 2) == 0, "a writable export is not read-only"
      refute Protocol.rotational?(flags)
    end

    test "a read-only export says so" do
      assert <<0::16, _size::64, flags::16>> =
               Protocol.encode_info_export(%{@export | read_only: true})

      assert Bitwise.band(flags, 2) == 2
    end

    test "NBD_INFO_BLOCK_SIZE carries minimum, preferred and maximum" do
      assert <<3::16, minimum::32, preferred::32, maximum::32>> =
               Protocol.encode_info_block_size(@export, 32 * 1024 * 1024)

      assert minimum == 4096
      assert preferred == 4096
      assert maximum == 32 * 1024 * 1024
    end

    test "the export-name reply pads unless the client asked not to" do
      padded = Protocol.encode_export_name_reply(@export)
      bare = Protocol.encode_export_name_reply(@export, no_zeroes: true)

      assert byte_size(padded) == 8 + 2 + 124
      assert byte_size(bare) == 8 + 2
      assert binary_part(padded, 0, 10) == bare
    end
  end

  describe "request decoding" do
    test "reads a command that carries no payload" do
      assert {:ok, request, ""} =
               Protocol.decode_request(request_frame(:read, 0, 42, 4096, 8192))

      assert request.type == :read
      assert request.cookie == 42
      assert request.offset == 4096
      assert request.length == 8192
      assert request.data == nil
      assert request.flags == []
    end

    test "reads a write's payload as part of its frame" do
      payload = :binary.copy(<<0xAB>>, 512)
      frame = request_frame(:write, 0, 7, 0, 512) <> payload

      assert {:ok, %{type: :write, data: ^payload, length: 512}, ""} =
               Protocol.decode_request(frame)
    end

    test "waits for a write whose payload has not all arrived" do
      frame = request_frame(:write, 0, 7, 0, 512) <> :binary.copy(<<0>>, 100)
      assert :incomplete = Protocol.decode_request(frame)
    end

    test "names the command flags" do
      assert {:ok, %{flags: [:fua]}, ""} =
               Protocol.decode_request(request_frame(:flush, 1, 1, 0, 0))

      assert {:ok, %{flags: [:no_hole]}, ""} =
               Protocol.decode_request(request_frame(:write_zeroes, 2, 1, 0, 4096))

      assert {:ok, %{flags: [:fua, :no_hole]}, ""} =
               Protocol.decode_request(request_frame(:write_zeroes, 3, 1, 0, 4096))
    end

    test "decodes every command this server implements" do
      for {type, code} <- [read: 0, write: 1, disconnect: 2, flush: 3, trim: 4, write_zeroes: 6] do
        frame = <<@request_magic::32, 0::16, code::16, 1::64, 0::64, 0::32>>
        assert {:ok, %{type: ^type}, ""} = Protocol.decode_request(frame)
      end
    end

    test "reports an unknown command with the cookie needed to answer it" do
      frame = <<@request_magic::32, 0::16, 99::16, 314::64, 0::64, 0::32>>
      assert {:error, {:unknown_command, 99, 314}} = Protocol.decode_request(frame)
    end

    test "rejects a complete header with the wrong magic" do
      frame = <<0xDEADBEEF::32, 0::16, 0::16, 1::64, 0::64, 0::32>>
      assert {:error, {:bad_request_magic, 0xDEADBEEF}} = Protocol.decode_request(frame)
    end

    test "waits for a truncated header" do
      full = request_frame(:read, 0, 1, 0, 4096)

      for take <- 0..(byte_size(full) - 1) do
        assert :incomplete = Protocol.decode_request(binary_part(full, 0, take)),
               "a #{take}-byte prefix of a request header is incomplete, not an error"
      end
    end

    test "keeps what follows a complete request" do
      frame = request_frame(:flush, 0, 1, 0, 0) <> "next"
      assert {:ok, %{type: :flush}, "next"} = Protocol.decode_request(frame)
    end
  end

  describe "simple replies" do
    test "echo the cookie and carry the error" do
      assert <<@simple_reply_magic::32, 0::32, 42::64>> =
               Protocol.encode_simple_reply(:ok, 42)

      assert <<@simple_reply_magic::32, 5::32, 42::64>> =
               Protocol.encode_simple_reply(:eio, 42)
    end

    test "carry read data after the header" do
      data = :binary.copy(<<0xCD>>, 4096)

      assert <<@simple_reply_magic::32, 0::32, 9::64, ^data::binary>> =
               Protocol.encode_simple_reply(:ok, 9, data)
    end

    test "know the errors a caller can map onto" do
      for error <- [:ok, :eperm, :eio, :enospc, :einval, :enotsup] do
        assert error in Protocol.errors()
      end
    end
  end

  describe "round-tripping" do
    property "a decoded request has the fields it was built from" do
      check all(
              type <- member_of([:read, :flush, :trim, :write_zeroes, :disconnect]),
              cookie <- integer(0..0xFFFFFFFFFFFFFFFF),
              offset <- integer(0..0xFFFFFFFFFFFF),
              length <- integer(0..0xFFFFFFFF),
              fua <- boolean(),
              no_hole <- boolean()
            ) do
        flags = Bitwise.bor(if(fua, do: 1, else: 0), if(no_hole, do: 2, else: 0))
        frame = request_frame(type, flags, cookie, offset, length)

        assert {:ok, request, ""} = Protocol.decode_request(frame)
        assert request.type == type
        assert request.cookie == cookie
        assert request.offset == offset
        assert request.length == length
        assert :fua in request.flags == fua
        assert :no_hole in request.flags == no_hole
      end
    end

    property "a write's payload survives decoding intact" do
      check all(
              cookie <- integer(0..0xFFFFFFFF),
              offset <- integer(0..0xFFFFFFFF),
              data <- binary(max_length: 2048)
            ) do
        frame = request_frame(:write, 0, cookie, offset, byte_size(data)) <> data

        assert {:ok, %{type: :write, data: ^data, cookie: ^cookie, offset: ^offset}, ""} =
                 Protocol.decode_request(frame)
      end
    end

    property "any prefix of a request is incomplete rather than an error" do
      check all(
              type <- member_of([:read, :write, :flush]),
              length <- integer(0..512),
              take <- integer(0..27)
            ) do
        frame = request_frame(type, 0, 1, 0, length)
        assert :incomplete = Protocol.decode_request(binary_part(frame, 0, take))
      end
    end

    property "a decoded option keeps whatever followed it" do
      check all(
              code <- member_of([2, 3, 8]),
              trailing <- binary(max_length: 64)
            ) do
        frame = option_frame(code, "") <> trailing
        assert {:ok, {_name, nil}, ^trailing} = Protocol.decode_option(frame)
      end
    end

    property "an export's advertised size is the size it was given" do
      check all(size <- integer(0..0xFFFFFFFFFFFF)) do
        export = %{@export | size: size}
        assert <<0::16, ^size::64, _flags::16>> = Protocol.encode_info_export(export)
      end
    end
  end

  # NBD_OPT_GO and NBD_OPT_INFO carry a name and an info-request list; the
  # rest of the options this server knows carry nothing.
  defp payload_for(option) when option in [:go, :info], do: <<6::32, "neonfs", 0::16>>
  defp payload_for(:export_name), do: "neonfs"
  defp payload_for(_option), do: ""

  defp option_frame(code, payload) do
    <<@ihaveopt::64, code::32, byte_size(payload)::32, payload::binary>>
  end

  defp request_frame(type, flags, cookie, offset, length) do
    code =
      case type do
        :read -> 0
        :write -> 1
        :disconnect -> 2
        :flush -> 3
        :trim -> 4
        :write_zeroes -> 6
      end

    <<@request_magic::32, flags::16, code::16, cookie::64, offset::64, length::32>>
  end
end
