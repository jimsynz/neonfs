defmodule NeonFS.Core.BlobStore.Codec do
  @moduledoc """
  Pure resolution of a chunk's codec (compression + encryption-nonce) settings
  into the shapes the blob NIFs and callers expect.

  Extracted from `NeonFS.Core.BlobStore` (#1207): these functions hold no
  process state and depend only on their arguments, so they live apart from the
  stateful drive-management GenServer. The codec suffix on disk is a fingerprint
  of `(compression, encryption-nonce)` — see `native/neonfs_blob/src/codec.rs`.
  """

  @doc """
  Derives the codec locator opts for a chunk from its `ChunkMeta`.

  Handles both `:none` / `:zstd` atoms and `"zstd:3"`-style compression strings.
  The encryption key is not part of the codec suffix — only the nonce is — so a
  zero-bytes placeholder key is emitted alongside the real nonce to keep the
  NIF's "encrypted" branch active for callers that only need to locate the file
  (delete, `chunk_exists`, migrate).
  """
  @spec opts_for_chunk(map()) :: keyword()
  def opts_for_chunk(chunk_meta) when is_map(chunk_meta) do
    compression =
      case Map.get(chunk_meta, :compression) do
        nil -> []
        :none -> [compression: :none]
        :zstd -> [compression: :zstd]
        other -> [compression: other]
      end

    case Map.get(chunk_meta, :crypto) do
      %{nonce: nonce} when is_binary(nonce) ->
        compression ++ [key: <<0::256>>, nonce: nonce]

      _ ->
        compression
    end
  end

  @doc """
  Normalises a keyword list's `:compression` option into the
  `{compression_string, level}` pair the blob NIFs take.
  """
  @spec compression_args(keyword()) :: {String.t(), integer()}
  def compression_args(opts) do
    normalise_compression(Keyword.get(opts, :compression), opts)
  end

  defp normalise_compression(nil, _opts), do: {"", 0}
  defp normalise_compression("", _opts), do: {"", 0}
  defp normalise_compression(:none, _opts), do: {"none", 0}
  defp normalise_compression("none", _opts), do: {"none", 0}
  defp normalise_compression(:zstd, opts), do: {"zstd", default_zstd_level(opts)}
  defp normalise_compression("zstd", opts), do: {"zstd", default_zstd_level(opts)}
  defp normalise_compression({:zstd, level}, _opts), do: {"zstd", level}

  defp normalise_compression("zstd:" <> level_str, opts),
    do: {"zstd", parse_level(level_str, opts)}

  defp normalise_compression(value, opts) when is_atom(value),
    do: normalise_compression(Atom.to_string(value), opts)

  defp default_zstd_level(opts), do: Keyword.get(opts, :compression_level, 3)

  defp parse_level(level_str, opts) do
    case Integer.parse(level_str) do
      {level, ""} -> level
      _ -> default_zstd_level(opts)
    end
  end
end
