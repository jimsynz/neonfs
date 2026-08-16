defmodule NeonFS.Block.Protocol do
  @moduledoc """
  Network Block Device wire protocol — encoding and decoding only.

  No sockets and no device state live here: every function takes bytes and
  returns terms, or takes terms and returns bytes. That keeps the parts of NBD
  that are easy to get subtly wrong — field widths, byte order, which reply
  shape answers which request — testable without a client, a kernel or a
  cluster.

  ## The two phases

  A connection starts in **handshake**: the server greets with a magic pair and
  its handshake flags, the client answers with its own flags, and then the two
  haggle over options until the client asks to go into transmission
  (`NBD_OPT_GO`, or the older `NBD_OPT_EXPORT_NAME`). Only fixed-newstyle is
  spoken — `NBD_FLAG_FIXED_NEWSTYLE` is always advertised and the oldstyle
  handshake is not implemented, because every client that matters has spoken
  newstyle for a decade.

  Structured replies are **not** negotiated. A `NBD_OPT_STRUCTURED_REPLY`
  request is answered `NBD_REP_ERR_UNSUP`, which the protocol requires a client
  to tolerate, and simple replies then carry every response. They cost nothing
  here: this server has no need to split a read into chunks or to report a hole
  separately, since a sparse region reads as zeroes either way.

  In **transmission**, each request is a fixed 28-byte header — magic, command
  flags, type, a cookie the reply echoes, offset and length — optionally
  followed by a payload for a write. Each reply is a fixed 16-byte header
  carrying the same cookie, optionally followed by read data.

  ## Decoding is incremental

  `decode_option/1` and `decode_request/1` return `{:ok, term, rest}` when a
  whole frame is present and `:incomplete` when it is not, so a caller can hand
  them whatever has arrived so far and keep the remainder. They never raise on
  a short buffer — a truncated frame at the tail of a TCP segment is normal,
  not exceptional. A frame that is complete but wrong (bad magic, unknown
  command) returns `{:error, reason}`, which is a different thing and deserves
  a different answer on the wire.

  ## References

  The protocol document (`nbd/doc/proto.md` upstream) is the authority. Field
  names here follow it, so `NBD_CMD_WRITE_ZEROES` is `:write_zeroes` and its
  `NBD_CMD_FLAG_NO_HOLE` is `:no_hole`.
  """

  import Bitwise

  # Handshake magic: the server's greeting and the client's option prefix.
  @nbdmagic 0x4E42444D41474943
  @ihaveopt 0x49484156454F5054

  @option_reply_magic 0x0003E889045565A9

  @request_magic 0x25609513
  @simple_reply_magic 0x67446698

  # Handshake flags (server → client).
  @flag_fixed_newstyle 1
  @flag_no_zeroes 2

  # Transmission flags (server → client, per export).
  @flag_has_flags 1
  @flag_read_only 2
  @flag_send_flush 4
  @flag_send_fua 8
  @flag_rotational 16
  @flag_send_trim 32
  @flag_send_write_zeroes 64

  @options %{
    1 => :export_name,
    2 => :abort,
    3 => :list,
    5 => :starttls,
    6 => :info,
    7 => :go,
    8 => :structured_reply
  }

  @option_replies %{
    ack: 1,
    server: 2,
    info: 3,
    err_unsup: 0x80000001,
    err_policy: 0x80000002,
    err_invalid: 0x80000003,
    err_platform: 0x80000004,
    err_tls_reqd: 0x80000005,
    err_unknown: 0x80000006,
    err_shutdown: 0x80000007,
    err_block_size_reqd: 0x80000008,
    err_too_big: 0x80000009
  }

  @info_types %{export: 0, name: 1, description: 2, block_size: 3}

  @commands %{
    0 => :read,
    1 => :write,
    2 => :disconnect,
    3 => :flush,
    4 => :trim,
    6 => :write_zeroes
  }

  # Command flags (client → server, per request).
  @cmd_flag_fua 1
  @cmd_flag_no_hole 2

  @errors %{
    ok: 0,
    eperm: 1,
    eio: 5,
    eagain: 11,
    enomem: 12,
    einval: 22,
    enospc: 28,
    eoverflow: 75,
    enotsup: 95,
    eshutdown: 108
  }

  @type option ::
          {:export_name, String.t()}
          | {:go, %{name: String.t(), info_requests: [non_neg_integer()]}}
          | {:info, %{name: String.t(), info_requests: [non_neg_integer()]}}
          | {:abort, nil}
          | {:list, nil}
          | {:starttls, nil}
          | {:structured_reply, nil}
          | {:unknown, %{code: non_neg_integer(), payload: binary()}}

  @type request :: %{
          type: :read | :write | :disconnect | :flush | :trim | :write_zeroes,
          flags: [:fua | :no_hole],
          cookie: non_neg_integer(),
          offset: non_neg_integer(),
          length: non_neg_integer(),
          data: binary() | nil
        }

  @type export :: %{
          size: non_neg_integer(),
          read_only: boolean(),
          logical_block_size: pos_integer(),
          physical_block_size: pos_integer()
        }

  @doc """
  The server's opening greeting: magic, and the handshake flags it supports.

  `NBD_FLAG_NO_ZEROES` is advertised so a client can ask to skip the 124 bytes
  of padding the older handshake ends with.
  """
  @spec server_greeting() :: binary()
  def server_greeting do
    <<@nbdmagic::64, @ihaveopt::64, @flag_fixed_newstyle ||| @flag_no_zeroes::16>>
  end

  @doc """
  Decodes the client's flags, which answer the greeting.

  Fixed-newstyle is mandatory: a client that does not set it is speaking a
  dialect this server does not implement, and saying so here is better than
  failing to parse its first option.
  """
  @spec decode_client_flags(binary()) ::
          {:ok, %{fixed_newstyle: boolean(), no_zeroes: boolean()}, binary()}
          | {:error, :fixed_newstyle_required}
          | :incomplete
  def decode_client_flags(<<flags::32, rest::binary>>) do
    if (flags &&& @flag_fixed_newstyle) == 0 do
      {:error, :fixed_newstyle_required}
    else
      {:ok, %{fixed_newstyle: true, no_zeroes: (flags &&& @flag_no_zeroes) != 0}, rest}
    end
  end

  def decode_client_flags(_partial), do: :incomplete

  @doc """
  Decodes one option request from the handshake.

  Returns `{:ok, option, rest}`, `:incomplete` when the option's payload has
  not all arrived, or `{:error, reason}` when the frame is present and wrong.
  An option this server does not know is returned as `{:unknown, _}` rather
  than an error, because the protocol's answer to one is a reply
  (`NBD_REP_ERR_UNSUP`) rather than a disconnect.
  """
  @spec decode_option(binary()) :: {:ok, option(), binary()} | {:error, term()} | :incomplete
  def decode_option(<<magic::64, _code::32, _length::32, _rest::binary>>)
      when magic != @ihaveopt do
    {:error, {:bad_option_magic, magic}}
  end

  def decode_option(<<@ihaveopt::64, code::32, length::32, rest::binary>>)
      when byte_size(rest) >= length do
    <<payload::binary-size(^length), remainder::binary>> = rest
    {:ok, decode_option_payload(Map.get(@options, code), code, payload), remainder}
  end

  def decode_option(_partial), do: :incomplete

  @doc """
  Encodes an option reply: the option being answered, the reply type, and any
  payload that type carries.
  """
  @spec encode_option_reply(atom() | non_neg_integer(), atom(), binary()) :: binary()
  def encode_option_reply(option, reply_type, payload \\ <<>>) do
    code = option_code(option)
    reply = Map.fetch!(@option_replies, reply_type)
    <<@option_reply_magic::64, code::32, reply::32, byte_size(payload)::32, payload::binary>>
  end

  @doc """
  The `NBD_INFO_EXPORT` payload: the export's size and its transmission flags.

  Flags are derived from the export rather than passed in, so a read-only
  device cannot accidentally advertise that it accepts writes.
  """
  @spec encode_info_export(export()) :: binary()
  def encode_info_export(export) do
    <<Map.fetch!(@info_types, :export)::16, export.size::64, transmission_flags(export)::16>>
  end

  @doc """
  The `NBD_INFO_BLOCK_SIZE` payload: the smallest, preferred and largest block
  sizes this export accepts.

  The minimum is the logical block size, the preferred the physical one, and
  the maximum bounds a single request — a client that respects it never asks
  for more than the server is willing to hold in memory at once.
  """
  @spec encode_info_block_size(export(), pos_integer()) :: binary()
  def encode_info_block_size(export, maximum_request_bytes) do
    <<Map.fetch!(@info_types, :block_size)::16, export.logical_block_size::32,
      export.physical_block_size::32, maximum_request_bytes::32>>
  end

  @doc """
  The reply to `NBD_OPT_EXPORT_NAME`, which has no reply header at all: the
  size, the transmission flags, and 124 zero bytes unless the client asked for
  `NBD_FLAG_NO_ZEROES`.
  """
  @spec encode_export_name_reply(export(), keyword()) :: binary()
  def encode_export_name_reply(export, opts \\ []) do
    zeroes = if Keyword.get(opts, :no_zeroes, false), do: <<>>, else: <<0::size(124)-unit(8)>>
    <<export.size::64, transmission_flags(export)::16, zeroes::binary>>
  end

  @doc """
  Decodes one transmission-phase request.

  A write's payload is part of its frame, so a write whose data has not all
  arrived is `:incomplete` — the caller keeps reading rather than acting on a
  partial write.
  """
  @spec decode_request(binary()) :: {:ok, request(), binary()} | {:error, term()} | :incomplete
  def decode_request(<<magic::32, _rest::binary-size(24), _tail::binary>>)
      when magic != @request_magic do
    {:error, {:bad_request_magic, magic}}
  end

  def decode_request(
        <<@request_magic::32, flags::16, type::16, cookie::64, offset::64, length::32,
          rest::binary>>
      ) do
    case Map.get(@commands, type) do
      nil ->
        {:error, {:unknown_command, type, cookie}}

      :write when byte_size(rest) < length ->
        :incomplete

      :write ->
        <<data::binary-size(^length), remainder::binary>> = rest

        {:ok,
         %{
           type: :write,
           flags: command_flags(flags),
           cookie: cookie,
           offset: offset,
           length: length,
           data: data
         }, remainder}

      command ->
        {:ok,
         %{
           type: command,
           flags: command_flags(flags),
           cookie: cookie,
           offset: offset,
           length: length,
           data: nil
         }, rest}
    end
  end

  def decode_request(_partial), do: :incomplete

  @doc """
  A simple reply: the error (`:ok` for success), the cookie being answered, and
  any data the command returns.

  The data belongs to a read; every other command replies with a header alone.
  """
  @spec encode_simple_reply(atom(), non_neg_integer(), binary()) :: binary()
  def encode_simple_reply(error, cookie, data \\ <<>>) do
    <<@simple_reply_magic::32, Map.fetch!(@errors, error)::32, cookie::64, data::binary>>
  end

  @doc """
  Every error name a reply can carry, for callers mapping their own failures
  onto the wire.
  """
  @spec errors() :: [atom()]
  def errors, do: Map.keys(@errors)

  @doc """
  The transmission flags an export advertises.

  `NBD_FLAG_HAS_FLAGS` is mandatory. Flush and FUA are always offered because
  the backing store has a real durability barrier to map them onto, and a
  device that cannot be flushed is one a filesystem cannot safely use. TRIM and
  WRITE_ZEROES are offered because zeroing is cheap on a content-addressed
  store. `NBD_FLAG_ROTATIONAL` is never set.
  """
  @spec transmission_flags(export()) :: non_neg_integer()
  def transmission_flags(export) do
    base =
      @flag_has_flags ||| @flag_send_flush ||| @flag_send_fua ||| @flag_send_trim |||
        @flag_send_write_zeroes

    if export.read_only, do: base ||| @flag_read_only, else: base
  end

  @doc """
  Whether these flags mark a rotational device — always false, and here so the
  meaning of the unset bit is documented rather than implied.
  """
  @spec rotational?(non_neg_integer()) :: boolean()
  def rotational?(flags), do: (flags &&& @flag_rotational) != 0

  defp decode_option_payload(:export_name, _code, payload), do: {:export_name, payload}

  defp decode_option_payload(option, _code, payload) when option in [:go, :info] do
    case payload do
      <<name_length::32, rest::binary>> when byte_size(rest) >= name_length + 2 ->
        <<name::binary-size(^name_length), request_count::16, requests::binary>> = rest
        {option, %{name: name, info_requests: info_requests(requests, request_count)}}

      _malformed ->
        {:unknown, %{code: option_code(option), payload: payload}}
    end
  end

  defp decode_option_payload(nil, code, payload),
    do: {:unknown, %{code: code, payload: payload}}

  defp decode_option_payload(option, _code, _payload), do: {option, nil}

  defp info_requests(binary, count) do
    for <<request::16 <- binary_part(binary, 0, min(count * 2, byte_size(binary)))>>, do: request
  end

  defp command_flags(flags) do
    [{@cmd_flag_fua, :fua}, {@cmd_flag_no_hole, :no_hole}]
    |> Enum.filter(fn {bit, _name} -> (flags &&& bit) != 0 end)
    |> Enum.map(fn {_bit, name} -> name end)
  end

  defp option_code(option) when is_integer(option), do: option

  defp option_code(option) when is_atom(option) do
    {code, _name} = Enum.find(@options, fn {_code, name} -> name == option end)
    code
  end
end
