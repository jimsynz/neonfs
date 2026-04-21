defmodule NFSServer.XDR do
  @moduledoc """
  Pure-Elixir encoder / decoder for the XDR (External Data
  Representation) standard — [RFC 4506](https://www.rfc-editor.org/rfc/rfc4506).

  XDR is the wire format for ONC RPC and every NFSv3 data structure,
  so everything in the downstream NFS stack depends on this module.

  ## Design

  - **Big-endian.** XDR is always big-endian on the wire, regardless
    of host endianness. All integer and float encodings use
    `big-signed-integer` / `big-unsigned-integer` / `big-float`
    bit specifiers.
  - **4-byte alignment.** Length-prefixed and variable-length encodings
    (opaque, string, variable arrays) are padded with zero bytes so
    the encoded length is a multiple of 4. Decoders skip the matching
    padding.
  - **Pair-of-functions per type.** Primitives expose `encode_<type>/1`
    and `decode_<type>/1` as sibling public functions. Decoders
    consume a prefix of the binary and return
    `{:ok, value, rest} | {:error, reason}`, mirroring the existing
    binary-pattern-matching style elsewhere in NeonFS. This keeps
    composition trivial:

        with {:ok, major, rest} <- XDR.decode_uint(binary),
             {:ok, minor, rest} <- XDR.decode_uint(rest),
             {:ok, name, rest} <- XDR.decode_string(rest) do
          {:ok, %MyRecord{major: major, minor: minor, name: name}, rest}
        end

  - **Composite helpers.** `encode_array/3`, `decode_var_array/2`,
    `encode_optional/2`, `decode_union/3` etc. let you assemble
    composite types from primitives without a full macro DSL. The DSL
    can be added later if the NFSv3 type hand-rolling becomes
    repetitive.

  ## Errors

  All decoders return `{:error, reason}` on a bad input. Reasons are:

    * `:short_binary` — fewer bytes than the wire format requires.
    * `{:bad_bool, n}` — a bool-encoded integer was neither 0 nor 1.
    * `{:bad_pad, bytes}` — alignment padding bytes were non-zero.
    * `{:bad_discriminant, n}` — a discriminated-union discriminant
      wasn't recognised by the caller's arm map.
  """

  # ——— Primitive types ————————————————————————————————————————————

  @doc "Encode a 32-bit signed integer (§4.1)."
  @spec encode_int(integer()) :: binary()
  def encode_int(n) when is_integer(n) and n >= -0x80000000 and n <= 0x7FFFFFFF do
    <<n::big-signed-32>>
  end

  @doc "Decode a 32-bit signed integer."
  @spec decode_int(binary()) :: {:ok, integer(), binary()} | {:error, term()}
  def decode_int(<<n::big-signed-32, rest::binary>>), do: {:ok, n, rest}
  def decode_int(_), do: {:error, :short_binary}

  @doc "Encode a 32-bit unsigned integer (§4.2)."
  @spec encode_uint(non_neg_integer()) :: binary()
  def encode_uint(n) when is_integer(n) and n >= 0 and n <= 0xFFFFFFFF do
    <<n::big-unsigned-32>>
  end

  @doc "Decode a 32-bit unsigned integer."
  @spec decode_uint(binary()) :: {:ok, non_neg_integer(), binary()} | {:error, term()}
  def decode_uint(<<n::big-unsigned-32, rest::binary>>), do: {:ok, n, rest}
  def decode_uint(_), do: {:error, :short_binary}

  @doc "Encode a 64-bit signed integer (`hyper`, §4.5)."
  @spec encode_hyper(integer()) :: binary()
  def encode_hyper(n)
      when is_integer(n) and n >= -0x80000000_00000000 and n <= 0x7FFFFFFF_FFFFFFFF do
    <<n::big-signed-64>>
  end

  @doc "Decode a 64-bit signed integer (`hyper`)."
  @spec decode_hyper(binary()) :: {:ok, integer(), binary()} | {:error, term()}
  def decode_hyper(<<n::big-signed-64, rest::binary>>), do: {:ok, n, rest}
  def decode_hyper(_), do: {:error, :short_binary}

  @doc "Encode a 64-bit unsigned integer (`unsigned hyper`)."
  @spec encode_uhyper(non_neg_integer()) :: binary()
  def encode_uhyper(n) when is_integer(n) and n >= 0 and n <= 0xFFFFFFFF_FFFFFFFF do
    <<n::big-unsigned-64>>
  end

  @doc "Decode a 64-bit unsigned integer."
  @spec decode_uhyper(binary()) :: {:ok, non_neg_integer(), binary()} | {:error, term()}
  def decode_uhyper(<<n::big-unsigned-64, rest::binary>>), do: {:ok, n, rest}
  def decode_uhyper(_), do: {:error, :short_binary}

  @doc "Encode an IEEE 754 single-precision float (§4.6)."
  @spec encode_float(float()) :: binary()
  def encode_float(f) when is_float(f), do: <<f::big-float-32>>

  @doc "Decode an IEEE 754 single-precision float."
  @spec decode_float(binary()) :: {:ok, float(), binary()} | {:error, term()}
  def decode_float(<<f::big-float-32, rest::binary>>), do: {:ok, f, rest}
  def decode_float(_), do: {:error, :short_binary}

  @doc "Encode an IEEE 754 double-precision float (§4.7)."
  @spec encode_double(float()) :: binary()
  def encode_double(f) when is_float(f), do: <<f::big-float-64>>

  @doc "Decode an IEEE 754 double-precision float."
  @spec decode_double(binary()) :: {:ok, float(), binary()} | {:error, term()}
  def decode_double(<<f::big-float-64, rest::binary>>), do: {:ok, f, rest}
  def decode_double(_), do: {:error, :short_binary}

  @doc "Encode a boolean as a 32-bit TRUE(1) / FALSE(0) (§4.4)."
  @spec encode_bool(boolean()) :: binary()
  def encode_bool(true), do: <<1::big-unsigned-32>>
  def encode_bool(false), do: <<0::big-unsigned-32>>

  @doc "Decode a boolean."
  @spec decode_bool(binary()) :: {:ok, boolean(), binary()} | {:error, term()}
  def decode_bool(<<0::big-unsigned-32, rest::binary>>), do: {:ok, false, rest}
  def decode_bool(<<1::big-unsigned-32, rest::binary>>), do: {:ok, true, rest}
  def decode_bool(<<n::big-unsigned-32, _::binary>>), do: {:error, {:bad_bool, n}}
  def decode_bool(_), do: {:error, :short_binary}

  @doc "Encode an enum as a 32-bit signed int (§4.3)."
  @spec encode_enum(integer()) :: binary()
  def encode_enum(n), do: encode_int(n)

  @doc "Decode an enum (same wire format as int)."
  @spec decode_enum(binary()) :: {:ok, integer(), binary()} | {:error, term()}
  def decode_enum(binary), do: decode_int(binary)

  @doc """
  Encode `void` (§4.16) — zero bytes. Accepts any argument and returns
  `<<>>`, so it can be plugged into composite encoders uniformly.
  """
  @spec encode_void(any()) :: binary()
  def encode_void(_), do: <<>>

  @doc "Decode `void` — consumes zero bytes."
  @spec decode_void(binary()) :: {:ok, :void, binary()}
  def decode_void(binary) when is_binary(binary), do: {:ok, :void, binary}

  # ——— Opaque data and strings ———————————————————————————————————

  @doc """
  Encode a fixed-length opaque byte array (§4.9). The binary's byte
  size must equal `length`; the output is the binary padded with
  zeros to a 4-byte boundary.
  """
  @spec encode_fixed_opaque(binary(), non_neg_integer()) :: binary()
  def encode_fixed_opaque(binary, length)
      when is_binary(binary) and byte_size(binary) == length do
    padding = pad_bytes(length)
    <<binary::binary, 0::size(padding * 8)>>
  end

  @doc "Decode a fixed-length opaque byte array (§4.9)."
  @spec decode_fixed_opaque(binary(), non_neg_integer()) ::
          {:ok, binary(), binary()} | {:error, term()}
  def decode_fixed_opaque(binary, length) when is_binary(binary) and length >= 0 do
    pad = pad_bytes(length)

    case binary do
      <<data::binary-size(length), padding::binary-size(pad), rest::binary>> ->
        check_pad(data, padding, rest)

      _ ->
        {:error, :short_binary}
    end
  end

  @doc """
  Encode a variable-length opaque byte array (§4.10). A 4-byte length
  prefix precedes the bytes and its trailing alignment padding.
  """
  @spec encode_var_opaque(binary()) :: binary()
  def encode_var_opaque(binary) when is_binary(binary) do
    length = byte_size(binary)
    encode_uint(length) <> encode_fixed_opaque(binary, length)
  end

  @doc "Decode a variable-length opaque byte array (§4.10)."
  @spec decode_var_opaque(binary()) :: {:ok, binary(), binary()} | {:error, term()}
  def decode_var_opaque(binary) do
    with {:ok, length, rest} <- decode_uint(binary) do
      decode_fixed_opaque(rest, length)
    end
  end

  @doc """
  Encode a string (§4.11). Identical wire format to variable-length
  opaque; accepts any binary the caller considers a string (XDR does
  not mandate an encoding).
  """
  @spec encode_string(binary()) :: binary()
  def encode_string(s) when is_binary(s), do: encode_var_opaque(s)

  @doc "Decode a string (same wire format as variable-length opaque)."
  @spec decode_string(binary()) :: {:ok, binary(), binary()} | {:error, term()}
  def decode_string(binary), do: decode_var_opaque(binary)

  # ——— Helpers ——————————————————————————————————————————————————

  @compile {:inline, pad_bytes: 1}
  defp pad_bytes(length), do: rem(4 - rem(length, 4), 4)

  defp check_pad(data, padding, rest) do
    if padding == <<0::size(byte_size(padding) * 8)>> do
      {:ok, data, rest}
    else
      {:error, {:bad_pad, padding}}
    end
  end

  # ——— Composite type helpers ——————————————————————————————————

  @typedoc """
  An encoder function — takes a value, returns its wire bytes.
  """
  @type encoder :: (term() -> binary())

  @typedoc """
  A decoder function — takes a binary, returns the parsed value and
  the remaining bytes (or an error tuple).
  """
  @type decoder :: (binary() -> {:ok, term(), binary()} | {:error, term()})

  @doc """
  Encode a fixed-length array (§4.12). `length` must equal the list
  length. Every element is encoded with `encoder` back-to-back — no
  array-level length prefix.
  """
  @spec encode_fixed_array([term()], non_neg_integer(), encoder()) :: binary()
  def encode_fixed_array(list, length, encoder)
      when is_list(list) and is_integer(length) and is_function(encoder, 1) do
    if length(list) != length do
      raise ArgumentError,
            "expected fixed array of length #{length}, got #{length(list)}"
    end

    Enum.map_join(list, "", encoder)
  end

  @doc """
  Decode a fixed-length array (§4.12) of `length` elements using
  `decoder`.
  """
  @spec decode_fixed_array(binary(), non_neg_integer(), decoder()) ::
          {:ok, [term()], binary()} | {:error, term()}
  def decode_fixed_array(binary, length, decoder)
      when is_integer(length) and length >= 0 and is_function(decoder, 1) do
    do_decode_fixed_array(binary, length, decoder, [])
  end

  defp do_decode_fixed_array(rest, 0, _decoder, acc), do: {:ok, Enum.reverse(acc), rest}

  defp do_decode_fixed_array(binary, n, decoder, acc) when n > 0 do
    case decoder.(binary) do
      {:ok, elem, rest} -> do_decode_fixed_array(rest, n - 1, decoder, [elem | acc])
      {:error, _} = err -> err
    end
  end

  @doc """
  Encode a variable-length array (§4.13): a 4-byte count followed by
  `count` elements, no trailing padding (each element handles its
  own alignment).
  """
  @spec encode_var_array([term()], encoder()) :: binary()
  def encode_var_array(list, encoder) when is_list(list) and is_function(encoder, 1) do
    encode_uint(length(list)) <> Enum.map_join(list, "", encoder)
  end

  @doc "Decode a variable-length array (§4.13)."
  @spec decode_var_array(binary(), decoder()) ::
          {:ok, [term()], binary()} | {:error, term()}
  def decode_var_array(binary, decoder) when is_function(decoder, 1) do
    with {:ok, count, rest} <- decode_uint(binary) do
      decode_fixed_array(rest, count, decoder)
    end
  end

  @doc """
  Encode an optional value (§4.19): a boolean presence flag followed
  by the value when present. `nil` encodes as `false` (no body).
  """
  @spec encode_optional(term() | nil, encoder()) :: binary()
  def encode_optional(nil, _encoder), do: encode_bool(false)

  def encode_optional(value, encoder) when is_function(encoder, 1),
    do: encode_bool(true) <> encoder.(value)

  @doc "Decode an optional value (§4.19)."
  @spec decode_optional(binary(), decoder()) ::
          {:ok, term() | nil, binary()} | {:error, term()}
  def decode_optional(binary, decoder) when is_function(decoder, 1) do
    case decode_bool(binary) do
      {:ok, false, rest} -> {:ok, nil, rest}
      {:ok, true, rest} -> decoder.(rest)
      {:error, _} = err -> err
    end
  end

  @doc """
  Encode a discriminated union (§4.15).

  `arms` maps each possible discriminant value to the encoder for its
  arm body. The discriminant is encoded as a 32-bit signed integer
  (per the most common NFSv3 usage); pass a pre-encoded discriminant
  via `encode_union_with/3` if your protocol uses a different type.

  The `void` arm (§4.16) encodes as no body — use `&XDR.encode_void/1`
  in the arm map.
  """
  @spec encode_union(integer(), term(), %{required(integer()) => encoder()}) :: binary()
  def encode_union(discriminant, value, arms)
      when is_integer(discriminant) and is_map(arms) do
    encoder = Map.fetch!(arms, discriminant)
    encode_int(discriminant) <> encoder.(value)
  end

  @doc """
  Decode a discriminated union (§4.15). `arms` maps each discriminant
  value to its arm decoder; an unknown discriminant surfaces as
  `{:error, {:bad_discriminant, n}}`.
  """
  @spec decode_union(binary(), %{required(integer()) => decoder()}) ::
          {:ok, {integer(), term()}, binary()} | {:error, term()}
  def decode_union(binary, arms) when is_map(arms) do
    with {:ok, disc, rest} <- decode_int(binary),
         {:ok, decoder} <- fetch_arm(arms, disc),
         {:ok, value, rest2} <- decoder.(rest) do
      {:ok, {disc, value}, rest2}
    end
  end

  defp fetch_arm(arms, disc) do
    case Map.fetch(arms, disc) do
      {:ok, decoder} -> {:ok, decoder}
      :error -> {:error, {:bad_discriminant, disc}}
    end
  end
end
