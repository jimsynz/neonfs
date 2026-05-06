defmodule NeonFS.Core.Volume.MetadataValue do
  @moduledoc """
  Serialisation contract for index-tree values (#819).

  The IndexTree (#781) stores opaque bytes — the caller decides the
  encoding. Per-volume metadata uses ETF (`:erlang.term_to_binary/1`)
  so values round-trip BEAM struct shapes without a separate schema:

  - Symmetric with the existing `Volume.RootSegment` (#780) format.
  - Lossless for atoms, datetimes, nested maps — the kind of fields
    `FileMeta` / `ChunkMeta` / `Stripe` carry.
  - `:safe` mode on decode rejects atoms not yet known to the VM,
    so a malformed (or actively malicious) chunk can't introduce
    new atoms or run arbitrary terms.

  Per-type wrappers in `NeonFS.Core.Volume.MetadataReader` apply
  this codec before handing values to callers.

  Decode failures surface as `{:error, {:malformed_value, reason}}`
  so the caller can distinguish a torn read from a missing key.
  """

  @type decode_error :: {:error, {:malformed_value, term()}}

  @doc """
  Encodes a struct or map as ETF bytes for storage in the index
  tree. The caller is responsible for keeping the term shape stable
  across encode / decode pairs.
  """
  @spec encode(term()) :: binary()
  def encode(value), do: :erlang.term_to_binary(value, minor_version: 2)

  @doc """
  Decodes ETF bytes into the term they were `encode/1`-ed from.

  Uses `:safe` so an attacker-controlled chunk can't introduce
  unknown atoms.
  """
  @spec decode(binary()) :: {:ok, term()} | decode_error()
  def decode(bytes) when is_binary(bytes) do
    {:ok, :erlang.binary_to_term(bytes, [:safe])}
  rescue
    e -> {:error, {:malformed_value, Exception.message(e)}}
  end
end
