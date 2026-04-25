defmodule NFSServer.RPC.RecordMarking do
  @moduledoc """
  ONC RPC record-marking framing — [RFC 5531 §11](https://www.rfc-editor.org/rfc/rfc5531#section-11).

  Each RPC message on a stream transport (TCP) is split into one or
  more **fragments**. Each fragment is preceded by a 4-byte marker:

      <<last::1, length::31>>

  When `last` is `1`, the fragment ends the RPC message; when `0`,
  more fragments follow. `length` is the number of payload bytes in
  the fragment (NOT including the marker itself).

  In practice, almost every NFSv3 client emits exactly one fragment
  per message — but the protocol mandates fragment-aware decoding,
  and our reassembler handles the multi-fragment case as well.

  ## Encoding

  `encode/1` wraps a single message as one final-fragment. Use
  `encode_fragments/2` if you need to split a large reply across
  multiple fragments — for NFSv3 message sizes (typically ≤ 1 MiB)
  the single-fragment path is fine.

  ## Decoding

  `decode_fragment/1` peels one fragment off the front of a buffer
  and tells you whether it was the last fragment.

  `decode_message/1` reassembles a complete message — call it after
  enough TCP bytes have arrived; return value `:incomplete` means
  more bytes are needed and the caller should buffer them and try
  again. The returned `body` is the concatenated fragment payloads
  (i.e. the raw RPC message body, ready for `RPC.Message.decode/1`).
  """

  @typedoc "Result of decoding a single fragment."
  @type fragment_result ::
          {:ok, last? :: boolean(), body :: binary(), rest :: binary()}
          | :incomplete
          | {:error, term()}

  @typedoc "Result of decoding a complete message."
  @type message_result ::
          {:ok, body :: binary(), rest :: binary()}
          | :incomplete
          | {:error, term()}

  # 31 bits max per fragment per RFC 5531; in practice we never need
  # to split below the protocol-defined ceiling.
  @max_fragment_size 0x7FFF_FFFF

  @doc """
  Wrap `body` as a single final fragment.

  Accepts iodata so streaming replies (e.g. NFSv3 READ chunks pulled
  from a backend `Enumerable`) can flow through the encode path
  without flattening to a single binary. The 4-byte marker is built
  from `:erlang.iolist_size/1` and prepended; `:gen_tcp.send/2`
  accepts iolist directly.
  """
  @spec encode(iodata()) :: iodata()
  def encode(body) do
    encode_fragment(body, true)
  end

  @doc """
  Encode `body` as a single fragment with the given `last?` flag.

  Accepts iodata; returns iodata. The fragment marker is computed
  from `:erlang.iolist_size/1` so the caller does not need to
  pre-flatten.
  """
  @spec encode_fragment(iodata(), boolean()) :: iodata()
  def encode_fragment(body, last?) when is_boolean(last?) do
    length = :erlang.iolist_size(body)

    if length > @max_fragment_size do
      raise ArgumentError, "fragment length #{length} exceeds RFC 5531 maximum"
    end

    last_bit = if last?, do: 1, else: 0
    [<<last_bit::1, length::31>>, body]
  end

  @doc """
  Encode `body` split into fragments of at most `max_fragment_size`
  bytes each. The final fragment carries the `last?` flag set.
  """
  @spec encode_fragments(binary(), pos_integer()) :: iodata()
  def encode_fragments(body, max_fragment_size)
      when is_binary(body) and is_integer(max_fragment_size) and max_fragment_size > 0 do
    do_encode_fragments(body, max_fragment_size, [])
  end

  defp do_encode_fragments(<<>>, _max, acc) do
    # Empty body — emit a single empty terminating fragment.
    case acc do
      [] -> [encode_fragment(<<>>, true)]
      _ -> Enum.reverse(acc)
    end
  end

  defp do_encode_fragments(body, max, acc) when byte_size(body) <= max do
    Enum.reverse([:erlang.iolist_to_binary(encode_fragment(body, true)) | acc])
  end

  defp do_encode_fragments(body, max, acc) do
    <<chunk::binary-size(max), rest::binary>> = body

    do_encode_fragments(rest, max, [:erlang.iolist_to_binary(encode_fragment(chunk, false)) | acc])
  end

  @doc """
  Peel a single fragment off the front of `buffer`. Returns
  `:incomplete` if fewer than `4 + length` bytes are available.
  """
  @spec decode_fragment(binary()) :: fragment_result()
  def decode_fragment(<<last_bit::1, length::31, rest::binary>>) when byte_size(rest) >= length do
    <<body::binary-size(length), tail::binary>> = rest
    {:ok, last_bit == 1, body, tail}
  end

  def decode_fragment(<<_marker::32, _rest::binary>>), do: :incomplete
  def decode_fragment(_short), do: :incomplete

  @doc """
  Reassemble fragments into a single message body. Returns
  `:incomplete` if more bytes are needed.
  """
  @spec decode_message(binary()) :: message_result()
  def decode_message(buffer) when is_binary(buffer) do
    do_decode_message(buffer, [])
  end

  defp do_decode_message(buffer, acc) do
    case decode_fragment(buffer) do
      {:ok, true, body, rest} ->
        {:ok, IO.iodata_to_binary(Enum.reverse([body | acc])), rest}

      {:ok, false, body, rest} ->
        do_decode_message(rest, [body | acc])

      :incomplete ->
        :incomplete
    end
  end
end
