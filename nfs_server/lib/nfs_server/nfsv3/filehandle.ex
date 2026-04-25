defmodule NFSServer.NFSv3.Filehandle do
  @moduledoc """
  Default opaque file-handle scheme for NFSv3 backends.

  RFC 1813 §2.5 defines `nfsfh3` as an opaque variable-length byte
  string up to 64 bytes. The handler treats handles as fully opaque
  — every interpretation (validation, lookup, refusal) lives in the
  backend. Most backends still need *some* packing, so this module
  provides one.

  ## Layout

      ┌─────────────┬─────────────┬────────────────┬──────────────┐
      │ tag (1 B)   │ vol_id      │ fileid (8 B)   │ generation   │
      │             │ (16 B UUID) │                │ (4 B)        │
      └─────────────┴─────────────┴────────────────┴──────────────┘
                       29 bytes total — well under NFS3_FHSIZE = 64

  - **tag** — `0x01` for "v1 default scheme". Lets future schemes
    coexist without ambiguity.
  - **vol_id** — 16 raw bytes; meant for a UUID, but any 16-byte
    cluster-stable identifier works.
  - **fileid** — 64-bit big-endian fileid (NeonFS's `FileMeta.inode`,
    or any equivalent persistent inode).
  - **generation** — 32-bit big-endian generation counter; bumped on
    delete-and-recreate so a stale handle returns NFS3ERR_STALE
    instead of pointing at a different file. Pass `0` if the
    backend doesn't track generations yet.

  Wire encoding is just the 29-byte tuple — `NFSServer.NFSv3.Types.encode_fhandle3/1`
  wraps it in the variable-opaque XDR envelope.

  ## Backends that want a different scheme

  Skip this module entirely and pack whatever you like into the
  64-byte budget. The Backend behaviour treats `fhandle3()` as
  `binary()`.
  """

  @tag_v1 0x01
  @vol_id_size 16
  @fileid_size 8
  @generation_size 4
  @packed_size 1 + @vol_id_size + @fileid_size + @generation_size

  @typedoc "Decoded form of a v1 default-scheme handle."
  @type decoded :: %{
          required(:vol_id) => <<_::128>>,
          required(:fileid) => non_neg_integer(),
          required(:generation) => non_neg_integer()
        }

  @typedoc "Reasons `decode/1` may refuse with."
  @type decode_error :: :stale | :badhandle

  @doc """
  Pack `{vol_id, fileid, generation}` into a v1 default-scheme handle.

  `vol_id` must be exactly 16 bytes. `fileid` and `generation` are
  64-bit and 32-bit unsigned respectively. Raises `ArgumentError`
  on malformed input — every callsite passes already-validated
  cluster-internal values.
  """
  @spec encode(<<_::128>>, non_neg_integer(), non_neg_integer()) :: binary()
  def encode(vol_id, fileid, generation \\ 0)
      when is_binary(vol_id) and byte_size(vol_id) == @vol_id_size and
             is_integer(fileid) and fileid >= 0 and fileid <= 0xFFFFFFFFFFFFFFFF and
             is_integer(generation) and generation >= 0 and generation <= 0xFFFFFFFF do
    <<@tag_v1::8, vol_id::binary, fileid::big-unsigned-64, generation::big-unsigned-32>>
  end

  @doc """
  Unpack a v1 default-scheme handle. Returns `{:error, :badhandle}`
  for any handle that isn't this scheme — including handles that are
  too short, too long, or use a different tag byte. Backends that
  pack with a different scheme should not call this function.
  """
  @spec decode(binary()) :: {:ok, decoded()} | {:error, decode_error()}
  def decode(
        <<@tag_v1::8, vol_id::binary-size(@vol_id_size), fileid::big-unsigned-64,
          generation::big-unsigned-32>>
      ) do
    {:ok, %{vol_id: vol_id, fileid: fileid, generation: generation}}
  end

  def decode(<<@tag_v1::8, _rest::binary>>), do: {:error, :badhandle}

  def decode(binary) when is_binary(binary) and byte_size(binary) == @packed_size,
    do: {:error, :badhandle}

  def decode(_), do: {:error, :badhandle}

  @doc """
  Check whether a decoded handle's `vol_id` matches the expected
  volume. The handler doesn't enforce this — it's a backend
  helper for the common case of "refuse a handle that's for a
  different volume" with NFS3ERR_STALE.
  """
  @spec same_volume?(decoded(), <<_::128>>) :: boolean()
  def same_volume?(%{vol_id: id}, expected)
      when is_binary(expected) and byte_size(expected) == 16,
      do: id == expected

  @doc "The fixed packed-handle byte size for this scheme."
  @spec packed_size() :: 29
  def packed_size, do: @packed_size

  @doc "The tag byte for v1 default-scheme handles."
  @spec tag() :: byte()
  def tag, do: @tag_v1
end
