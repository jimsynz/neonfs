defmodule NeonFS.Client.Chunker.Native do
  @moduledoc """
  Native Rust NIF bindings for client-side chunking.

  This module binds to the `neonfs_chunker` crate in
  `neonfs_client/native/`. It exposes the chunker surface that
  `NeonFS.Client.ChunkWriter` (#450) uses to split a byte stream into
  content-addressed chunks before shipping each chunk to a data-plane
  replica (see the #408 design note, the #449 scaffold issue, and the
  #299 streaming-writes epic).

  The chunker output is byte-for-byte compatible with
  `NeonFS.Core.Blob.Native` — dedup across packages depends on
  identical `(hash, offset, size)` tuples for the same input. The
  parity property test in
  `test/neon_fs/client/chunker/parity_test.exs` exercises that
  invariant.
  """

  use Rustler,
    otp_app: :neonfs_client,
    crate: :neonfs_chunker

  @type hash :: binary()
  @type chunker :: reference()
  @type chunk_strategy :: String.t()
  @type chunk_result :: {binary(), hash(), non_neg_integer(), non_neg_integer()}

  @doc """
  Computes the SHA-256 hash of the given binary data.

  The hash is computed on the raw data as provided — no compression
  or encryption is applied. This keeps content addressing consistent
  regardless of storage format.
  """
  @spec compute_hash(binary()) :: hash()
  def compute_hash(_data), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Determines the appropriate chunking strategy for the given data
  length.

  - `< 64KB`: `{"single", 0}` (avoid chunking overhead).
  - `64KB - 1MB`: `{"fixed", 262_144}` (256KB blocks).
  - `>= 1MB`: `{"fastcdc", 262_144}` (content-defined, 256KB average).
  """
  @spec auto_chunk_strategy(non_neg_integer()) :: {chunk_strategy(), non_neg_integer()}
  def auto_chunk_strategy(data_len), do: nif_auto_chunk_strategy(data_len)

  @doc """
  Stateless chunker — splits `data` according to
  `{strategy, strategy_param}` and returns the full chunk list.
  """
  @spec chunk_data(binary(), chunk_strategy(), non_neg_integer()) ::
          {:ok, [chunk_result()]} | {:error, String.t()}
  def chunk_data(data, strategy, strategy_param),
    do: nif_chunk_data(data, strategy, strategy_param)

  @doc """
  Creates an incremental chunker that emits chunks as data is fed in.

  Use this when the total input size is unknown or larger than memory.
  Feed slices via `chunker_feed/2` and call `chunker_finish/1` once
  all input has been provided to flush any remaining buffered data.
  """
  @spec chunker_init(chunk_strategy(), non_neg_integer()) ::
          {:ok, chunker()} | {:error, String.t()}
  def chunker_init(_strategy, _strategy_param), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Feeds a slice of data into the chunker.

  Returns any chunks that became complete as a result of this slice.
  Bytes that may still belong to a future chunk remain buffered
  inside the resource — the working set is bounded by the strategy's
  maximum chunk size, regardless of how much data is fed in total.
  """
  @spec chunker_feed(chunker(), binary()) :: [chunk_result()]
  def chunker_feed(_chunker, _data), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Flushes any remaining buffered data as the final chunks.

  After `chunker_finish/1`, the chunker may be reused with further
  `chunker_feed/2` calls; offsets continue from where they left off.
  """
  @spec chunker_finish(chunker()) :: [chunk_result()]
  def chunker_finish(_chunker), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec nif_auto_chunk_strategy(non_neg_integer()) :: {chunk_strategy(), non_neg_integer()}
  def nif_auto_chunk_strategy(_data_len), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec nif_chunk_data(binary(), chunk_strategy(), non_neg_integer()) ::
          {:ok, [chunk_result()]} | {:error, String.t()}
  def nif_chunk_data(_data, _strategy, _strategy_param),
    do: :erlang.nif_error(:nif_not_loaded)
end
