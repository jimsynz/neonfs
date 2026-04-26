defmodule NeonFS.NFS.Filehandle do
  @moduledoc """
  Packing and unpacking for NFSv3 file handles produced by
  `NeonFS.NFS.NFSv3Backend`.

  The handler-side type `NFSServer.NFSv3.Types.fhandle3()` is an opaque
  variable-length binary capped at 64 bytes (RFC 1813 §2.5). This
  module gives that binary a stable internal layout:

      | Bytes | Field      | Notes                                   |
      |-------|------------|-----------------------------------------|
      |   16  | volume_id  | NeonFS volume UUID, raw 128-bit form    |
      |    8  | fileid     | 64-bit `NeonFS.NFS.InodeTable` inode    |
      |    4  | generation | POSIX-like; bumps on delete + recreate. |
      |   36  | reserved   | zero-filled, decodes to `:reserved`     |

  Total: exactly 64 bytes. The reserved trailing bytes are forward-
  compatibility padding — anything other than zeros today decodes as
  `{:error, :stale}` so the handler can return `NFS3ERR_STALE`. Sub-
  issue #532; cf. #284 (NFSv3 epic) and #113 (native-BEAM NFS epic).

  ## Why pack like this

    * Volume id up front lets the backend resolve volume routing
      without consulting any local mapping table — the file handle
      *is* the volume reference.
    * The 64-bit inode is deterministically derived from
      `(volume_name, path)` via `NeonFS.NFS.InodeTable`, so handles
      are stable across NFS-node restarts and portable behind a
      load balancer.
    * Generation is reserved for future use (currently always 0).
      Once `FileMeta` carries a real generation counter, the
      backend can validate it on every callback and surface
      `NFS3ERR_STALE` for delete-and-recreate races.

  Wrong-volume handles (the on-wire fhandle decodes cleanly but the
  embedded volume id doesn't match the export the handler is bound
  to) decode fine here — the backend rejects them at the next layer.
  """

  import Bitwise, only: [<<<: 2]

  @fhandle3_size 64
  @volume_id_bytes 16
  @fileid_bytes 8
  @generation_bytes 4
  @reserved_bytes @fhandle3_size - @volume_id_bytes - @fileid_bytes - @generation_bytes
  @padding <<0::size(@reserved_bytes * 8)>>

  @typedoc "Decoded view of a filehandle."
  @type t :: %{
          volume_id: <<_::128>>,
          fileid: non_neg_integer(),
          generation: non_neg_integer()
        }

  @doc """
  Pack a `volume_id` (16-byte binary), `fileid` (64-bit unsigned), and
  optional `generation` (32-bit unsigned, default 0) into the on-wire
  64-byte filehandle.
  """
  @spec encode(<<_::128>>, non_neg_integer(), non_neg_integer()) :: binary()
  def encode(volume_id, fileid, generation \\ 0)
      when is_binary(volume_id) and byte_size(volume_id) == @volume_id_bytes and
             is_integer(fileid) and fileid >= 0 and fileid < 1 <<< 64 and
             is_integer(generation) and generation >= 0 and generation < 1 <<< 32 do
    <<volume_id::binary-size(@volume_id_bytes), fileid::64-big-unsigned,
      generation::32-big-unsigned, @padding::binary>>
  end

  @doc """
  Decode the 64-byte filehandle binary back into `t/0`. Returns
  `{:error, :stale}` for any handle whose size doesn't match or whose
  reserved trailing bytes are non-zero — the handler maps both to
  `NFS3ERR_STALE`.
  """
  @spec decode(binary()) :: {:ok, t()} | {:error, :stale}
  def decode(
        <<volume_id::binary-size(@volume_id_bytes), fileid::64-big-unsigned,
          generation::32-big-unsigned, reserved::binary-size(@reserved_bytes)>>
      )
      when reserved == @padding do
    {:ok, %{volume_id: volume_id, fileid: fileid, generation: generation}}
  end

  def decode(_), do: {:error, :stale}

  @doc """
  Convert a NeonFS volume UUID string (hyphenated form, e.g.
  `\"019dc5d8-3fcf-7d13-b4fa-832c4390b0a0\"`) into the 16-byte binary
  used by `encode/3`. Returns `{:error, :invalid}` for malformed
  input.
  """
  @spec volume_uuid_to_binary(String.t()) :: {:ok, <<_::128>>} | {:error, :invalid}
  def volume_uuid_to_binary(uuid) when is_binary(uuid) do
    hex = String.replace(uuid, "-", "")

    case Base.decode16(hex, case: :mixed) do
      {:ok, <<bin::binary-size(@volume_id_bytes)>>} -> {:ok, bin}
      _ -> {:error, :invalid}
    end
  end

  def volume_uuid_to_binary(_), do: {:error, :invalid}

  @doc """
  Reverse of `volume_uuid_to_binary/1`: pack a 16-byte binary into the
  hyphenated UUID string used by `NeonFS.Core` APIs.
  """
  @spec volume_uuid_from_binary(<<_::128>>) :: String.t()
  def volume_uuid_from_binary(<<a::32, b::16, c::16, d::16, e::48>>) do
    [a, b, c, d, e]
    |> Enum.zip([8, 4, 4, 4, 12])
    |> Enum.map_join("-", fn {n, w} ->
      n |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(w, "0")
    end)
  end

  @doc "Returns the wire size of a filehandle (always 64 bytes)."
  @spec size() :: pos_integer()
  def size, do: @fhandle3_size
end
