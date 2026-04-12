defmodule NeonFS.NFS.RPC.XDR do
  @moduledoc """
  XDR (External Data Representation) encode/decode primitives per RFC 4506.

  All XDR types are big-endian, 4-byte aligned. This module provides the
  building blocks for encoding and decoding ONC RPC messages and NLM types.
  """

  @type decode_result(t) :: {:ok, t, binary()} | {:error, :truncated | :invalid}

  @doc """
  Decodes a signed 32-bit integer.
  """
  @spec decode_int(binary()) :: decode_result(integer())
  def decode_int(<<value::big-signed-32, rest::binary>>), do: {:ok, value, rest}
  def decode_int(_), do: {:error, :truncated}

  @doc """
  Encodes a signed 32-bit integer.
  """
  @spec encode_int(integer()) :: binary()
  def encode_int(value) when is_integer(value), do: <<value::big-signed-32>>

  @doc """
  Decodes an unsigned 32-bit integer.
  """
  @spec decode_uint(binary()) :: decode_result(non_neg_integer())
  def decode_uint(<<value::big-unsigned-32, rest::binary>>), do: {:ok, value, rest}
  def decode_uint(_), do: {:error, :truncated}

  @doc """
  Encodes an unsigned 32-bit integer.
  """
  @spec encode_uint(non_neg_integer()) :: binary()
  def encode_uint(value) when is_integer(value) and value >= 0, do: <<value::big-unsigned-32>>

  @doc """
  Decodes an unsigned 64-bit integer (hyper).
  """
  @spec decode_hyper_uint(binary()) :: decode_result(non_neg_integer())
  def decode_hyper_uint(<<value::big-unsigned-64, rest::binary>>), do: {:ok, value, rest}
  def decode_hyper_uint(_), do: {:error, :truncated}

  @doc """
  Encodes an unsigned 64-bit integer (hyper).
  """
  @spec encode_hyper_uint(non_neg_integer()) :: binary()
  def encode_hyper_uint(value) when is_integer(value) and value >= 0,
    do: <<value::big-unsigned-64>>

  @doc """
  Decodes a signed 64-bit integer (hyper).
  """
  @spec decode_hyper_int(binary()) :: decode_result(integer())
  def decode_hyper_int(<<value::big-signed-64, rest::binary>>), do: {:ok, value, rest}
  def decode_hyper_int(_), do: {:error, :truncated}

  @doc """
  Encodes a signed 64-bit integer (hyper).
  """
  @spec encode_hyper_int(integer()) :: binary()
  def encode_hyper_int(value) when is_integer(value), do: <<value::big-signed-64>>

  @doc """
  Decodes an XDR boolean (0 = false, 1 = true).
  """
  @spec decode_bool(binary()) :: decode_result(boolean())
  def decode_bool(<<0::big-32, rest::binary>>), do: {:ok, false, rest}
  def decode_bool(<<1::big-32, rest::binary>>), do: {:ok, true, rest}
  def decode_bool(<<_::big-32, _::binary>>), do: {:error, :invalid}
  def decode_bool(_), do: {:error, :truncated}

  @doc """
  Encodes an XDR boolean.
  """
  @spec encode_bool(boolean()) :: binary()
  def encode_bool(true), do: <<1::big-32>>
  def encode_bool(false), do: <<0::big-32>>

  @doc """
  Decodes a variable-length opaque (length-prefixed byte array).
  """
  @spec decode_opaque(binary()) :: decode_result(binary())
  def decode_opaque(<<len::big-unsigned-32, rest::binary>>) do
    pad = xdr_pad(len)

    case rest do
      <<data::binary-size(len), _padding::binary-size(pad), rest2::binary>> ->
        {:ok, data, rest2}

      _ ->
        {:error, :truncated}
    end
  end

  def decode_opaque(_), do: {:error, :truncated}

  @doc """
  Encodes a variable-length opaque.
  """
  @spec encode_opaque(binary()) :: binary()
  def encode_opaque(data) when is_binary(data) do
    len = byte_size(data)
    pad = xdr_pad(len)
    <<len::big-unsigned-32, data::binary, 0::size(pad * 8)>>
  end

  @doc """
  Decodes an XDR string (same wire format as variable-length opaque).
  """
  @spec decode_string(binary()) :: decode_result(String.t())
  def decode_string(data), do: decode_opaque(data)

  @doc """
  Encodes an XDR string.
  """
  @spec encode_string(String.t()) :: binary()
  def encode_string(str) when is_binary(str), do: encode_opaque(str)

  @doc """
  Decodes void (consumes nothing).
  """
  @spec decode_void(binary()) :: decode_result(nil)
  def decode_void(rest), do: {:ok, nil, rest}

  @doc """
  Encodes void.
  """
  @spec encode_void :: binary()
  def encode_void, do: <<>>

  @doc """
  Calculates the padding needed to align to a 4-byte boundary.
  """
  @spec xdr_pad(non_neg_integer()) :: non_neg_integer()
  def xdr_pad(len) do
    case rem(len, 4) do
      0 -> 0
      r -> 4 - r
    end
  end
end
