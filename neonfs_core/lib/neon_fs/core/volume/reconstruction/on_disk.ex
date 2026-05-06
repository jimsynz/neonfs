defmodule NeonFS.Core.Volume.Reconstruction.OnDisk do
  @moduledoc """
  Production on-disk chunk walker for `Volume.Reconstruction` (#839
  / part of #788).

  Walks a drive's blob directory in pure Elixir — bypasses
  `BlobStore` entirely so reconstruction works even when the
  cluster's GenServers can't start (the disaster scenario this is
  designed for).

  Chunks live at:

      <drive>/blobs/<tier>/<prefix1>/.../<prefix_n>/<hash_hex>.<codec_suffix_hex>

  Where `<tier>` is `hot` / `warm` / `cold`, `<prefix_i>` is two
  hex chars from the start of `<hash_hex>`, and `<codec_suffix_hex>`
  is a 16-char hex digest of the chunk's codec choice. Root segment
  chunks are written via `BlobStoreChunkStore.put` (#813) with
  default options (Compression::None, encryption=None) so the
  bytes on disk are the raw ETF payload — no decompression /
  decryption needed before `RootSegment.decode/1`.

  `list_candidate_hashes/1` extracts every distinct hash that
  appears as a filename prefix across `hot` / `warm` / `cold`.
  `read_chunk/2` reads the first matching file's bytes.
  """

  @hash_hex_len 64

  @doc """
  Returns every distinct chunk hash (raw 32-byte binary) found
  under `<drive_path>/blobs/`.
  """
  @spec list_candidate_hashes(String.t()) :: [binary()]
  def list_candidate_hashes(drive_path) when is_binary(drive_path) do
    [drive_path, "blobs", "**", "*"]
    |> Path.join()
    |> Path.wildcard()
    |> Stream.map(&extract_hash_hex/1)
    |> Stream.reject(&is_nil/1)
    |> Stream.uniq()
    |> Stream.map(&decode_hex/1)
    |> Stream.reject(&is_nil/1)
    |> Enum.to_list()
  end

  @doc """
  Reads the bytes of a chunk. Returns `{:ok, bytes}` from the
  first matching file under `<drive_path>/blobs/`, or
  `{:error, :not_found}` if no file matches.
  """
  @spec read_chunk(String.t(), binary()) :: {:ok, binary()} | {:error, :not_found | term()}
  def read_chunk(drive_path, hash) when is_binary(drive_path) and is_binary(hash) do
    hex = Base.encode16(hash, case: :lower)

    [drive_path, "blobs", "**", "#{hex}.*"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.reject(&tmpfile?/1)
    |> case do
      [] -> {:error, :not_found}
      [path | _rest] -> File.read(path)
    end
  end

  ## Internals

  defp extract_hash_hex(path) do
    base = Path.basename(path)

    case String.split(base, ".", parts: 2) do
      [hex, _suffix] when byte_size(hex) == @hash_hex_len -> hex
      _ -> nil
    end
  end

  defp decode_hex(hex) do
    case Base.decode16(hex, case: :lower) do
      {:ok, bytes} when byte_size(bytes) == 32 -> bytes
      _ -> nil
    end
  end

  defp tmpfile?(path) do
    base = Path.basename(path)
    String.contains?(base, ".tmp.") or String.starts_with?(base, "tmp.")
  end
end
