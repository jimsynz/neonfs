defmodule NeonFS.Block.Ublk.Protocol do
  @moduledoc """
  The wire between the ublk helper process and this node.

  A fixed binary header plus a raw payload, `{packet, 4}`-framed by the
  socket. Not ETF: the ublk work exists for its numbers, and encoding and
  decoding a term per 4 KiB IO would put the codec in the measurement. The
  cost is a hand-rolled format on both sides, which is why it has tests on
  both sides.

  ## Requests, helper to BEAM

      <<version::8, op::8, tag::16, offset::64, length::32>> <> payload

  `tag` is ublk's own per-queue IO tag, echoed back so a reply can be
  matched without the helper keeping its own table. `payload` is present
  only on a write.

  ## Replies, BEAM to helper

      <<version::8, status::8, tag::16, length::32>> <> payload

  `status` is `0` for success and a positive errno otherwise — the helper
  hands it to the kernel as the IO's result, so it has to be an errno
  rather than a name. `payload` is present only on a successful read, and
  `length` is its size so the helper can size the completion without
  re-deriving it from the request it has already retired.

  ## The version byte is first, and is checked

  A helper and a node are separate artefacts that a partial upgrade can
  pair unevenly. A frame whose version this node does not know is refused
  rather than reinterpreted: the alternative is decoding one release's
  offsets with another's layout, which lands writes at the wrong place on
  the device rather than failing.
  """

  @version 1

  @ops %{read: 0, write: 1, flush: 2, discard: 3, write_zeroes: 4}
  @op_names Map.new(@ops, fn {name, code} -> {code, name} end)

  @request_header_bytes 16
  @reply_header_bytes 8

  @type op :: :read | :write | :flush | :discard | :write_zeroes
  @type tag :: 0..65_535

  @type request :: %{
          op: op(),
          tag: tag(),
          offset: non_neg_integer(),
          length: non_neg_integer(),
          data: binary()
        }

  @doc "The protocol version this node speaks."
  @spec version() :: pos_integer()
  def version, do: @version

  @doc "The wire code for `op`, and its inverse — for the helper's tests."
  @spec op_code(op()) :: non_neg_integer()
  def op_code(op), do: Map.fetch!(@ops, op)

  @doc """
  Decodes one request frame.

  `:incomplete` is not a case here: `{packet, 4}` delivers whole frames, so
  a short frame is a malformed one rather than a partial read — the
  distinction that matters for NBD's byte stream does not arise.
  """
  @spec decode_request(binary()) :: {:ok, request()} | {:error, term()}
  def decode_request(<<@version::8, op::8, tag::16, offset::64, length::32, data::binary>>) do
    case Map.fetch(@op_names, op) do
      {:ok, name} -> decoded(name, tag, offset, length, data)
      :error -> {:error, {:unknown_op, op}}
    end
  end

  def decode_request(<<version::8, _rest::binary>>) when version != @version,
    do: {:error, {:unsupported_version, version, @version}}

  def decode_request(frame), do: {:error, {:malformed_request, byte_size(frame)}}

  # A write's payload has to be exactly as long as its header claims. A
  # short one would write the header's length from a buffer that does not
  # have it; a long one means the framing has slipped, and guessing which
  # end is authoritative is how bytes land at the wrong offset.
  defp decoded(:write, tag, offset, length, data) when byte_size(data) == length,
    do: {:ok, %{op: :write, tag: tag, offset: offset, length: length, data: data}}

  defp decoded(:write, _tag, _offset, length, data),
    do: {:error, {:payload_length_mismatch, length, byte_size(data)}}

  defp decoded(op, tag, offset, length, <<>>),
    do: {:ok, %{op: op, tag: tag, offset: offset, length: length, data: <<>>}}

  defp decoded(op, _tag, _offset, _length, data),
    do: {:error, {:unexpected_payload, op, byte_size(data)}}

  @doc """
  Encodes a successful reply, with `data` for a read and empty otherwise.
  """
  @spec encode_ok(tag(), binary()) :: iodata()
  def encode_ok(tag, data \\ <<>>) do
    [<<@version::8, 0::8, tag::16, byte_size(data)::32>>, data]
  end

  @doc """
  Encodes a failure as the errno the helper will hand the kernel.

  A reason with no mapping becomes `EIO`, which is the honest default for
  "this device could not serve that" — but the mappings that exist matter:
  `EAGAIN` for contention the guest may retry, and `ESHUTDOWN` for a device
  this node has stopped serving, which is not the same claim as a broken
  disk.
  """
  @spec encode_error(tag(), term()) :: iodata()
  def encode_error(tag, reason) do
    [<<@version::8, errno(reason)::8, tag::16, 0::32>>]
  end

  @doc "The errno a failure reason becomes on the wire."
  @spec errno(term()) :: pos_integer()
  def errno(:stale_chunks), do: 11
  def errno({:fenced, _current}), do: 108
  def errno({:out_of_range, _offset, _length, _size}), do: 22
  def errno({:unaligned_request, _offset, _length}), do: 22
  def errno(%{class: :forbidden}), do: 1
  def errno(_reason), do: 5

  @doc "Header sizes, for the helper's own framing tests."
  @spec header_bytes() :: %{request: pos_integer(), reply: pos_integer()}
  def header_bytes, do: %{request: @request_header_bytes, reply: @reply_header_bytes}
end
