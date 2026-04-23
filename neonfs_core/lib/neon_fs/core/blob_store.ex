defmodule NeonFS.Core.BlobStore do
  @moduledoc """
  High-level Elixir wrapper around the blob store NIF.

  This GenServer manages multiple blob store NIF handles — one per configured
  physical drive. A node with 2 NVMes and 8 SATA disks gets 10 separate
  `Native.store_open/2` calls and 10 handles.

  ## Drive Configuration

  Drives are configured via application environment or `start_link/1` options:

      config :neonfs_core, :drives, [
        %{id: "nvme0", path: "/data/nvme0", tier: :hot, capacity: 1_000_000_000_000},
        %{id: "sata0", path: "/data/sata0", tier: :cold, capacity: 4_000_000_000_000}
      ]

  At least one drive must be configured; startup fails if no drives are present.

  ## Telemetry Events

  The following telemetry events are emitted:

  - `[:neonfs, :blob_store, :write_chunk, :start]` - Before chunk write
  - `[:neonfs, :blob_store, :write_chunk, :stop]` - After chunk write
  - `[:neonfs, :blob_store, :write_chunk, :exception]` - On write error
  - `[:neonfs, :blob_store, :read_chunk, :start]` - Before chunk read
  - `[:neonfs, :blob_store, :read_chunk, :stop]` - After chunk read
  - `[:neonfs, :blob_store, :read_chunk, :exception]` - On read error
  - `[:neonfs, :blob_store, :delete_chunk, :start]` - Before chunk delete
  - `[:neonfs, :blob_store, :delete_chunk, :stop]` - After chunk delete
  - `[:neonfs, :blob_store, :delete_chunk, :exception]` - On delete error
  - `[:neonfs, :blob_store, :migrate_chunk, :start]` - Before chunk migration
  - `[:neonfs, :blob_store, :migrate_chunk, :stop]` - After chunk migration
  - `[:neonfs, :blob_store, :migrate_chunk, :exception]` - On migration error

  All events include `drive_id` in metadata.

  ## Example

      {:ok, _pid} = NeonFS.Core.BlobStore.start_link(
        drives: [%{id: "default", path: "/tmp/blobs", tier: :hot, capacity: 100_000_000}],
        prefix_depth: 2
      )

      data = "hello world"
      {:ok, hash, info} = NeonFS.Core.BlobStore.write_chunk(data, "default", "hot")
      {:ok, ^data} = NeonFS.Core.BlobStore.read_chunk(hash, "default", tier: "hot")

  """

  use GenServer
  require Logger

  alias NeonFS.Core.Blob.Native
  alias NeonFS.Core.DriveState
  alias NeonFS.Core.Volume
  alias NeonFS.Core.VolumeRegistry

  @type chunk_hash :: binary()
  @type tier :: String.t()
  @type drive_id :: String.t()
  @type chunk_info :: %{
          original_size: non_neg_integer(),
          stored_size: non_neg_integer(),
          compression: String.t()
        }
  @type drive_config :: %{
          id: String.t(),
          path: String.t(),
          tier: atom(),
          capacity: non_neg_integer()
        }

  @tiers ["hot", "warm", "cold"]

  ## Client API

  @doc """
  Starts the blob store GenServer.

  ## Options

    * `:drives` (required) - List of drive configurations, each with `:id`, `:path`, `:tier`, `:capacity`
    * `:prefix_depth` (optional) - Number of prefix directory levels, defaults to 2
    * `:name` (optional) - Name for the GenServer, defaults to `__MODULE__`

  Falls back to `Application.get_env(:neonfs_core, :drives)` if `:drives` not provided.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Writes a chunk to a specific drive in the blob store.

  ## Parameters

    * `data` - Binary data to write
    * `drive_id` - Target drive identifier
    * `tier` - Storage tier: "hot", "warm", or "cold"
    * `opts` - Optional keyword list:
      * `:compression` - Compression type: "none" (default) or "zstd"
      * `:compression_level` - Compression level for zstd (1-19), defaults to 3
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, hash, chunk_info}` - Hash and chunk metadata on success
    * `{:error, reason}` - On failure

  """
  @spec write_chunk(binary(), drive_id(), tier(), keyword()) ::
          {:ok, chunk_hash(), chunk_info()} | {:error, String.t()}
  def write_chunk(data, drive_id, tier, opts \\ []) do
    DriveState.ensure_active(drive_id)
    {server, opts} = Keyword.pop(opts, :server, __MODULE__)
    GenServer.call(server, {:write_chunk, data, drive_id, tier, opts})
  end

  @doc """
  Reads a chunk from a specific drive in the blob store.

  ## Parameters

    * `hash` - SHA-256 hash of the chunk (32 bytes)
    * `drive_id` - Drive identifier to read from
    * `opts` - Optional keyword list:
      * `:tier` - Storage tier (default: "hot")
      * `:verify` - Verify data integrity (default: false)
      * `:decompress` - Decompress data (default: false)
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, data}` - The chunk data on success
    * `{:error, reason}` - On failure

  """
  @spec read_chunk(chunk_hash(), drive_id(), keyword()) :: {:ok, binary()} | {:error, String.t()}
  def read_chunk(hash, drive_id, opts \\ []) do
    DriveState.ensure_active(drive_id)
    {server, opts} = Keyword.pop(opts, :server, __MODULE__)
    tier = Keyword.get(opts, :tier, "hot")
    verify = Keyword.get(opts, :verify, false)
    decompress = Keyword.get(opts, :decompress, false)
    key = Keyword.get(opts, :key, <<>>)
    nonce = Keyword.get(opts, :nonce, <<>>)
    codec_opts = Keyword.take(opts, [:compression, :compression_level])

    GenServer.call(
      server,
      {:read_chunk, hash, drive_id, tier, verify, decompress, key, nonce, codec_opts}
    )
  end

  @doc """
  Reads a chunk from a specific drive with explicit tier and options.

  ## Parameters

    * `hash` - SHA-256 hash of the chunk (32 bytes)
    * `drive_id` - Drive identifier to read from
    * `tier` - Storage tier: "hot", "warm", or "cold"
    * `read_opts` - Keyword list:
      * `:verify` - Verify data integrity (default: false)
      * `:decompress` - Decompress data (default: false)
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, data}` - The chunk data on success
    * `{:error, reason}` - On failure

  """
  @spec read_chunk_with_options(chunk_hash(), drive_id(), tier(), keyword()) ::
          {:ok, binary()} | {:error, String.t()}
  def read_chunk_with_options(hash, drive_id, tier, read_opts) do
    DriveState.ensure_active(drive_id)
    {server, read_opts} = Keyword.pop(read_opts, :server, __MODULE__)
    verify = Keyword.get(read_opts, :verify, false)
    decompress = Keyword.get(read_opts, :decompress, false)
    key = Keyword.get(read_opts, :key, <<>>)
    nonce = Keyword.get(read_opts, :nonce, <<>>)
    codec_opts = Keyword.take(read_opts, [:compression, :compression_level])

    GenServer.call(
      server,
      {:read_chunk, hash, drive_id, tier, verify, decompress, key, nonce, codec_opts}
    )
  end

  @doc """
  Deletes a chunk from a specific drive.

  Searches all tier subdirectories within the drive to find and delete the chunk.

  ## Parameters

    * `hash` - SHA-256 hash of the chunk (32 bytes)
    * `drive_id` - Drive identifier
    * `opts` - Optional keyword list:
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, bytes_freed}` - Number of bytes freed on success
    * `{:error, reason}` - On failure

  """
  @spec delete_chunk(chunk_hash(), drive_id(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, String.t()}
  def delete_chunk(hash, drive_id, opts \\ []) do
    server = Keyword.pop(opts, :server, __MODULE__) |> elem(0)
    opts = Keyword.delete(opts, :server)
    GenServer.call(server, {:delete_chunk, hash, drive_id, opts})
  end

  @doc """
  Checks if a chunk exists on any drive and tier, returning its tier and stored size.

  Iterates all drives and tiers to locate the chunk. Returns the first match.

  ## Returns

    * `{:ok, tier, size}` — chunk found with its tier and stored byte size
    * `{:error, :not_found}` — chunk not present on any drive/tier
  """
  @spec chunk_info(chunk_hash(), keyword()) ::
          {:ok, tier(), non_neg_integer()} | {:error, :not_found}
  def chunk_info(hash, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:chunk_info, hash})
  end

  @doc """
  Resolves volume-level write options for a put_chunk invocation.

  Called by `NeonFS.Transport.Handler` when an interface node sends
  plaintext chunks on the new 8-tuple put_chunk frame (see the #408
  design note). The resolved options are then passed straight through
  to `write_chunk/4` as the fourth argument — matching compression and,
  in future, encryption settings per volume.

  ## Current scope (MVP)

    * Compression — resolved from `volume.compression` if the
      algorithm is not `:none` AND the input size exceeds
      `volume.compression.min_size`. Otherwise returns an empty list
      (chunks stored as-is, matching legacy behaviour).
    * Encryption — **not yet applied on this path**. Encrypted
      volumes currently still store raw bytes via this handler, which
      is the same behaviour as before this function existed. Tracked
      for a follow-up.

  ## Returns

    * `[]` when no volume processing applies (volume uncompressed,
      unknown volume, or encrypted — pending follow-up).
    * Keyword list with `:compression` and `:compression_level` keys
      when compression is configured for the volume.
  """
  @spec resolve_put_chunk_opts(binary()) :: keyword()
  def resolve_put_chunk_opts(volume_id) when is_binary(volume_id) do
    case VolumeRegistry.get(volume_id) do
      {:ok, %Volume{} = volume} -> volume_write_opts(volume)
      _ -> []
    end
  end

  defp volume_write_opts(%Volume{compression: %{algorithm: :zstd, level: level}}) do
    [compression: "zstd", compression_level: level]
  end

  defp volume_write_opts(_volume), do: []

  @doc """
  Migrates a chunk between tiers within a single drive.

  The operation is atomic: the chunk is copied to the destination tier,
  verified for integrity, and only then deleted from the source tier.

  ## Parameters

    * `hash` - SHA-256 hash of the chunk (32 bytes)
    * `drive_id` - Drive identifier (migration is within this drive)
    * `from_tier` - Source tier: "hot", "warm", or "cold"
    * `to_tier` - Destination tier: "hot", "warm", or "cold"
    * `opts` - Optional keyword list:
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, {}}` - On success
    * `{:error, reason}` - On failure

  """
  @spec migrate_chunk(chunk_hash(), drive_id(), tier(), tier(), keyword()) ::
          {:ok, {}} | {:error, String.t()}
  def migrate_chunk(hash, drive_id, from_tier, to_tier, opts \\ []) do
    {server, opts} = Keyword.pop(opts, :server, __MODULE__)
    GenServer.call(server, {:migrate_chunk, hash, drive_id, from_tier, to_tier, opts})
  end

  @doc """
  Re-encrypts a chunk in place with new encryption keys.

  Decrypts with the old key/nonce and re-encrypts with the new key/nonce
  atomically in Rust — no chunk data crosses the NIF boundary.

  ## Parameters

    * `hash` - SHA-256 hash of the chunk (32 bytes)
    * `drive_id` - Drive identifier containing the chunk
    * `tier` - Storage tier: "hot", "warm", or "cold"
    * `old_key` - Old encryption key (32 bytes)
    * `old_nonce` - Old nonce (12 bytes)
    * `new_key` - New encryption key (32 bytes)
    * `new_nonce` - New nonce (12 bytes)
    * `opts` - Optional keyword list:
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, stored_size}` - The stored size of the re-encrypted chunk
    * `{:error, reason}` - On failure

  """
  @spec reencrypt_chunk(
          chunk_hash(),
          drive_id(),
          tier(),
          binary(),
          binary(),
          binary(),
          binary(),
          keyword()
        ) :: {:ok, non_neg_integer()} | {:error, String.t()}
  def reencrypt_chunk(hash, drive_id, tier, old_key, old_nonce, new_key, new_nonce, opts \\ []) do
    {server, opts} = Keyword.pop(opts, :server, __MODULE__)

    GenServer.call(
      server,
      {:reencrypt_chunk, hash, drive_id, tier, old_key, old_nonce, new_key, new_nonce, opts}
    )
  end

  # Metadata namespace operations

  @doc """
  Writes metadata to the blob store's metadata namespace.

  ## Parameters

    * `segment_id` - 32-byte binary segment identifier (hex-encoded before NIF call)
    * `key_hash` - 32-byte binary hash of the metadata key
    * `data` - Binary metadata to write
    * `drive_id` - Target drive identifier
    * `opts` - Optional keyword list:
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, {}}` - On success
    * `{:error, reason}` - On failure
  """
  @spec write_metadata(binary(), binary(), binary(), drive_id(), keyword()) ::
          {:ok, {}} | {:error, String.t()}
  def write_metadata(segment_id, key_hash, data, drive_id, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    timeout = Keyword.get(opts, :timeout, 5_000)
    GenServer.call(server, {:write_metadata, segment_id, key_hash, data, drive_id}, timeout)
  end

  @doc """
  Reads metadata from the blob store's metadata namespace.

  ## Parameters

    * `segment_id` - 32-byte binary segment identifier
    * `key_hash` - 32-byte binary hash of the metadata key
    * `drive_id` - Drive identifier to read from
    * `opts` - Optional keyword list:
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, data}` - The metadata as a binary
    * `{:error, reason}` - On failure
  """
  @spec read_metadata(binary(), binary(), drive_id(), keyword()) ::
          {:ok, binary()} | {:error, String.t()}
  def read_metadata(segment_id, key_hash, drive_id, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    timeout = Keyword.get(opts, :timeout, 5_000)
    GenServer.call(server, {:read_metadata, segment_id, key_hash, drive_id}, timeout)
  end

  @doc """
  Deletes metadata from the blob store's metadata namespace.

  ## Parameters

    * `segment_id` - 32-byte binary segment identifier
    * `key_hash` - 32-byte binary hash of the metadata key
    * `drive_id` - Drive identifier
    * `opts` - Optional keyword list:
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, {}}` - On success
    * `{:error, reason}` - On failure
  """
  @spec delete_metadata(binary(), binary(), drive_id(), keyword()) ::
          {:ok, {}} | {:error, String.t()}
  def delete_metadata(segment_id, key_hash, drive_id, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    timeout = Keyword.get(opts, :timeout, 5_000)
    GenServer.call(server, {:delete_metadata, segment_id, key_hash, drive_id}, timeout)
  end

  @doc """
  Lists all metadata key hashes in a segment.

  ## Parameters

    * `segment_id` - 32-byte binary segment identifier
    * `drive_id` - Drive identifier
    * `opts` - Optional keyword list:
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, hashes}` - List of 32-byte binary key hashes
    * `{:error, reason}` - On failure
  """
  @spec list_metadata_segment(binary(), drive_id(), keyword()) ::
          {:ok, [binary()]} | {:error, String.t()}
  def list_metadata_segment(segment_id, drive_id, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    timeout = Keyword.get(opts, :timeout, 5_000)
    GenServer.call(server, {:list_metadata_segment, segment_id, drive_id}, timeout)
  end

  @doc """
  Returns configured drives and their status.

  ## Returns

    * `{:ok, drives}` - Map of `%{drive_id => drive_config}`

  """
  @spec list_drives(keyword()) :: {:ok, %{String.t() => drive_config()}}
  def list_drives(opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, :list_drives)
  end

  @doc """
  Opens a new blob store for a drive at runtime.

  Creates the NIF handle and adds it to the BlobStore state.

  ## Parameters

    * `drive_config` - Drive configuration map with `:id`, `:path`, `:tier`, `:capacity`
    * `opts` - Optional keyword list:
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, drive_id}` - On success
    * `{:error, reason}` - On failure
  """
  @spec open_store(drive_config(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def open_store(drive_config, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    timeout = Keyword.get(opts, :timeout, 5_000)
    GenServer.call(server, {:open_store, drive_config}, timeout)
  end

  @doc """
  Closes a blob store for a drive at runtime.

  Removes the NIF handle from state. The Rust ResourceArc is GC'd once dereferenced.

  ## Parameters

    * `drive_id` - Drive identifier to close
    * `opts` - Optional keyword list:
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `:ok` - On success
    * `{:error, :unknown_drive}` - Drive not found
  """
  @spec close_store(String.t(), keyword()) :: :ok | {:error, :unknown_drive}
  def close_store(drive_id, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    timeout = Keyword.get(opts, :timeout, 5_000)
    GenServer.call(server, {:close_store, drive_id}, timeout)
  end

  @doc """
  Checks if a drive contains any data by inspecting tier subdirectories.

  Walks the drive's tier subdirectories (`hot/`, `warm/`, `cold/`) and checks
  for the presence of any prefix subdirectories containing files.

  ## Parameters

    * `drive_id` - Drive identifier
    * `opts` - Optional keyword list:
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, boolean()}` - Whether the drive contains data
    * `{:error, :unknown_drive}` - Drive not found
  """
  @spec drive_has_data?(String.t(), keyword()) :: {:ok, boolean()} | {:error, :unknown_drive}
  def drive_has_data?(drive_id, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    timeout = Keyword.get(opts, :timeout, 5_000)
    GenServer.call(server, {:drive_has_data, drive_id}, timeout)
  end

  @doc """
  Splits data into chunks using the specified strategy.

  ## Parameters

    * `data` - Binary data to split
    * `strategy` - Chunking strategy:
      * `{:single}` - Store as a single chunk
      * `{:fixed, size}` - Split into fixed-size chunks
      * `{:fastcdc, avg_size}` - Use content-defined chunking
      * `:auto` - Automatically select based on data length
    * `opts` - Optional keyword list:
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, chunks}` - List of chunk results on success, where each chunk is
      `{data, hash, offset, size}`
    * `{:error, reason}` - On failure

  """
  @spec chunk_data(binary(), chunk_strategy, keyword()) ::
          {:ok, [chunk_result]} | {:error, String.t()}
        when chunk_strategy:
               :auto | {:single} | {:fixed, pos_integer()} | {:fastcdc, pos_integer()},
             chunk_result: {binary(), chunk_hash(), non_neg_integer(), non_neg_integer()}
  def chunk_data(data, strategy, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:chunk_data, data, strategy})
  end

  @typedoc """
  Opaque handle to an incremental chunker; pass it to `chunker_feed/2` and
  `chunker_finish/1`.
  """
  @type chunker :: reference()

  @doc """
  Initialises an incremental chunker for streaming writes.

  Unlike `chunk_data/3`, this returns a stateful handle that the caller
  feeds bytes into as they arrive. Each emitted chunk has the same shape
  (`{data, hash, offset, size}`) as a `chunk_data` result; the working
  set is bounded by the strategy's maximum chunk size.

  When `:auto` is given without a size hint, the chunker defaults to
  `:fastcdc` with the standard parameters — the auto strategy needs the
  total size to pick between single/fixed/fastcdc, which streaming
  callers don't have up front. Pass an explicit strategy to override.

  This call does not go through the BlobStore GenServer — the chunker
  resource is process-safe and held directly by the caller.
  """
  @spec chunker_init(chunk_strategy) :: {:ok, chunker()} | {:error, String.t()}
        when chunk_strategy:
               :auto | {:single} | {:fixed, pos_integer()} | {:fastcdc, pos_integer()}
  def chunker_init(strategy) do
    {strategy_str, param} = native_strategy(strategy)
    Native.chunker_init(strategy_str, param)
  end

  @doc """
  Feeds a slice into the chunker; returns chunks completed by this slice.
  """
  @spec chunker_feed(chunker(), binary()) :: [chunk_result]
        when chunk_result: {binary(), chunk_hash(), non_neg_integer(), non_neg_integer()}
  def chunker_feed(chunker, data), do: Native.chunker_feed(chunker, data)

  @doc """
  Flushes any remaining buffered data as the final chunks.
  """
  @spec chunker_finish(chunker()) :: [chunk_result]
        when chunk_result: {binary(), chunk_hash(), non_neg_integer(), non_neg_integer()}
  def chunker_finish(chunker), do: Native.chunker_finish(chunker)

  defp native_strategy(:auto), do: {"fastcdc", 0}
  defp native_strategy({:single}), do: {"single", 0}
  defp native_strategy({:fixed, size}), do: {"fixed", size}
  defp native_strategy({:fastcdc, avg_size}), do: {"fastcdc", avg_size}

  ## GenServer Callbacks

  @impl true
  def init(opts) do
    drives_config = Keyword.get(opts, :drives) || Application.get_env(:neonfs_core, :drives, [])
    prefix_depth = Keyword.get(opts, :prefix_depth, 2)

    if drives_config == [] do
      Logger.info(
        "BlobStore starting with no drives — use DriveManager.add_drive/1 to add drives"
      )

      {:ok, %{stores: %{}, drives: %{}, prefix_depth: prefix_depth}}
    else
      case open_all_stores(drives_config, prefix_depth) do
        {:ok, stores, drives} ->
          {:ok, %{stores: stores, drives: drives, prefix_depth: prefix_depth}}

        {:error, reason} ->
          {:stop, {:error, reason}}
      end
    end
  end

  @impl true
  def handle_call({:write_chunk, data, drive_id, tier, opts}, _from, state) do
    case get_store(state, drive_id) do
      {:ok, store} ->
        result = do_write_chunk(store, data, drive_id, tier, opts)
        if match?({:ok, _, _}, result), do: DriveState.record_io(drive_id)
        {:reply, result, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call(
        {:read_chunk, hash, drive_id, tier, verify, decompress, key, nonce, codec_opts},
        _from,
        state
      ) do
    case get_store(state, drive_id) do
      {:ok, store} ->
        read_args = %{
          hash: hash,
          drive_id: drive_id,
          tier: tier,
          verify: verify,
          decompress: decompress,
          key: key,
          nonce: nonce,
          codec_opts: codec_opts
        }

        result = do_read_chunk(store, read_args)

        if match?({:ok, _}, result), do: DriveState.record_io(drive_id)
        {:reply, result, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:delete_chunk, hash, drive_id, codec_opts}, _from, state) do
    case get_store(state, drive_id) do
      {:ok, store} ->
        result = do_delete_chunk(store, hash, drive_id, codec_opts)
        if match?({:ok, _}, result), do: DriveState.record_io(drive_id)
        {:reply, result, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:chunk_info, hash}, _from, state) do
    result = do_chunk_info(state.stores, hash)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:migrate_chunk, hash, drive_id, from_tier, to_tier, codec_opts}, _from, state) do
    case get_store(state, drive_id) do
      {:ok, store} ->
        result = do_migrate_chunk(store, hash, drive_id, from_tier, to_tier, codec_opts)
        if match?({:ok, _}, result), do: DriveState.record_io(drive_id)
        {:reply, result, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call(
        {:reencrypt_chunk, hash, drive_id, tier, old_key, old_nonce, new_key, new_nonce,
         codec_opts},
        _from,
        state
      ) do
    case get_store(state, drive_id) do
      {:ok, store} ->
        compression = codec_compression_args(codec_opts)

        result =
          Native.store_reencrypt_chunk(
            store,
            hash,
            tier,
            compression,
            {old_key, old_nonce},
            {new_key, new_nonce}
          )

        if match?({:ok, _}, result), do: DriveState.record_io(drive_id)
        {:reply, result, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call(:list_drives, _from, state) do
    {:reply, {:ok, state.drives}, state}
  end

  @impl true
  def handle_call({:open_store, drive_config}, _from, state) do
    drive = normalize_drive_config(drive_config)

    if Map.has_key?(state.stores, drive.id) do
      {:reply, {:error, {:already_open, drive.id}}, state}
    else
      case Native.store_open(drive.path, state.prefix_depth) do
        {:ok, handle} ->
          new_stores = Map.put(state.stores, drive.id, handle)
          new_drives = Map.put(state.drives, drive.id, drive)
          {:reply, {:ok, drive.id}, %{state | stores: new_stores, drives: new_drives}}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end
  end

  @impl true
  def handle_call({:close_store, drive_id}, _from, state) do
    if Map.has_key?(state.stores, drive_id) do
      new_stores = Map.delete(state.stores, drive_id)
      new_drives = Map.delete(state.drives, drive_id)
      {:reply, :ok, %{state | stores: new_stores, drives: new_drives}}
    else
      {:reply, {:error, :unknown_drive}, state}
    end
  end

  @impl true
  def handle_call({:drive_has_data, drive_id}, _from, state) do
    case Map.fetch(state.drives, drive_id) do
      {:ok, drive} ->
        has_data = check_drive_has_data(drive.path)
        {:reply, {:ok, has_data}, state}

      :error ->
        {:reply, {:error, :unknown_drive}, state}
    end
  end

  @impl true
  def handle_call({:chunk_data, data, strategy}, _from, state) do
    {strategy_str, param} =
      case strategy do
        :auto ->
          Native.auto_chunk_strategy(byte_size(data))

        {:single} ->
          {"single", 0}

        {:fixed, size} ->
          {"fixed", size}

        {:fastcdc, avg_size} ->
          {"fastcdc", avg_size}
      end

    result = Native.chunk_data(data, strategy_str, param)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:write_metadata, segment_id, key_hash, data, drive_id}, _from, state) do
    case get_store(state, drive_id) do
      {:ok, store} ->
        segment_hex = Base.encode16(segment_id, case: :lower)
        result = Native.metadata_write(store, segment_hex, key_hash, data)
        {:reply, result, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:read_metadata, segment_id, key_hash, drive_id}, _from, state) do
    case get_store(state, drive_id) do
      {:ok, store} ->
        segment_hex = Base.encode16(segment_id, case: :lower)
        result = Native.metadata_read(store, segment_hex, key_hash)
        {:reply, result, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:delete_metadata, segment_id, key_hash, drive_id}, _from, state) do
    case get_store(state, drive_id) do
      {:ok, store} ->
        segment_hex = Base.encode16(segment_id, case: :lower)
        result = Native.metadata_delete(store, segment_hex, key_hash)
        {:reply, result, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:list_metadata_segment, segment_id, drive_id}, _from, state) do
    case get_store(state, drive_id) do
      {:ok, store} ->
        segment_hex = Base.encode16(segment_id, case: :lower)
        result = Native.metadata_list_segment(store, segment_hex)
        {:reply, result, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  ## Codec helpers

  @doc """
  Extracts codec opts from a `ChunkMeta`-shaped map so delete/migrate/read
  calls resolve to the correct on-disk variant. Handles both `:none`/`:zstd`
  atoms and `"zstd:3"`-style strings on the compression field.
  """
  @spec codec_opts_for_chunk(map()) :: keyword()
  def codec_opts_for_chunk(chunk_meta) when is_map(chunk_meta) do
    compression =
      case Map.get(chunk_meta, :compression) do
        nil -> []
        :none -> [compression: :none]
        :zstd -> [compression: :zstd]
        other -> [compression: other]
      end

    case Map.get(chunk_meta, :crypto) do
      %{nonce: nonce} when is_binary(nonce) ->
        # The key is not part of the codec suffix — only the nonce is. The
        # NIF uses (key, nonce) emptiness to detect unencrypted reads, so
        # pass a zero-bytes placeholder to keep the "encrypted" branch
        # active for callers that only need to locate the file (delete,
        # chunk_exists, migrate).
        compression ++ [key: <<0::256>>, nonce: nonce]

      _ ->
        compression
    end
  end

  @doc false
  @spec codec_compression_args(keyword()) :: {String.t(), integer()}
  def codec_compression_args(opts) do
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

  ## Private: Store lookup

  defp get_store(state, drive_id) do
    case Map.fetch(state.stores, drive_id) do
      {:ok, store} -> {:ok, store}
      :error -> {:error, "unknown drive_id: #{drive_id}"}
    end
  end

  ## Private: Init helpers

  defp open_all_stores(drives_config, prefix_depth) do
    results =
      Enum.map(drives_config, fn drive ->
        drive = normalize_drive_config(drive)

        case Native.store_open(drive.path, prefix_depth) do
          {:ok, handle} -> {:ok, drive.id, handle, drive}
          {:error, reason} -> {:error, drive.id, reason}
        end
      end)

    errors = Enum.filter(results, &match?({:error, _, _}, &1))

    if Enum.empty?(errors) do
      stores = Map.new(results, fn {:ok, id, handle, _} -> {id, handle} end)
      drives = Map.new(results, fn {:ok, id, _, config} -> {id, config} end)
      {:ok, stores, drives}
    else
      [{:error, drive_id, reason} | _] = errors
      Logger.error("Failed to open store for drive", drive_id: drive_id, reason: inspect(reason))
      {:error, {:drive_open_failed, drive_id, reason}}
    end
  end

  defp normalize_drive_config(drive) when is_map(drive) do
    %{
      id: to_string(drive[:id] || drive["id"]),
      path: to_string(drive[:path] || drive["path"]),
      tier: normalize_tier(drive[:tier] || drive["tier"] || :hot),
      capacity: drive[:capacity] || drive["capacity"] || 0
    }
  end

  defp normalize_tier(tier) when is_atom(tier), do: tier
  defp normalize_tier(tier) when is_binary(tier), do: String.to_existing_atom(tier)

  ## Private: Write

  defp do_write_chunk(store, data, drive_id, tier, opts) do
    compression = Keyword.get(opts, :compression, "none")
    compression_level = Keyword.get(opts, :compression_level, 3)
    key = Keyword.get(opts, :key, <<>>)
    nonce = Keyword.get(opts, :nonce, <<>>)

    metadata = %{
      drive_id: drive_id,
      tier: tier,
      data_size: byte_size(data),
      compression: compression
    }

    start_time = System.monotonic_time()

    :telemetry.execute(
      [:neonfs, :blob_store, :write_chunk, :start],
      %{system_time: System.system_time()},
      metadata
    )

    result =
      try do
        hash = Native.compute_hash(data)

        case Native.store_write_chunk_compressed(
               store,
               hash,
               data,
               tier,
               compression,
               compression_level,
               key,
               nonce
             ) do
          {:ok, {original_size, stored_size, comp_type, _enc_algo, _enc_nonce}} ->
            duration = System.monotonic_time() - start_time

            :telemetry.execute(
              [:neonfs, :blob_store, :write_chunk, :stop],
              %{duration: duration, bytes_written: stored_size},
              metadata
            )

            info = %{
              original_size: original_size,
              stored_size: stored_size,
              compression: comp_type
            }

            {:ok, hash, info}

          {:error, reason} = error ->
            duration = System.monotonic_time() - start_time

            :telemetry.execute(
              [:neonfs, :blob_store, :write_chunk, :exception],
              %{duration: duration},
              Map.put(metadata, :error, reason)
            )

            error
        end
      rescue
        e ->
          duration = System.monotonic_time() - start_time

          :telemetry.execute(
            [:neonfs, :blob_store, :write_chunk, :exception],
            %{duration: duration},
            Map.put(metadata, :error, Exception.message(e))
          )

          {:error, Exception.message(e)}
      end

    result
  end

  ## Private: Read

  defp do_read_chunk(store, %{
         hash: hash,
         drive_id: drive_id,
         tier: tier,
         verify: verify,
         decompress: decompress,
         key: key,
         nonce: nonce,
         codec_opts: codec_opts
       }) do
    metadata = %{
      drive_id: drive_id,
      tier: tier,
      hash: Base.encode16(hash, case: :lower),
      verify: verify,
      decompress: decompress
    }

    start_time = System.monotonic_time()

    :telemetry.execute(
      [:neonfs, :blob_store, :read_chunk, :start],
      %{system_time: System.system_time()},
      metadata
    )

    {comp_str, comp_level} = codec_compression_args(codec_opts)
    locator = {comp_str, comp_level, key, nonce}

    result =
      try do
        case Native.store_read_chunk_with_options(store, hash, tier, verify, decompress, locator) do
          {:ok, data} ->
            duration = System.monotonic_time() - start_time

            :telemetry.execute(
              [:neonfs, :blob_store, :read_chunk, :stop],
              %{duration: duration, bytes_read: byte_size(data)},
              metadata
            )

            {:ok, data}

          {:error, reason} = error ->
            duration = System.monotonic_time() - start_time

            :telemetry.execute(
              [:neonfs, :blob_store, :read_chunk, :exception],
              %{duration: duration},
              Map.put(metadata, :error, reason)
            )

            error
        end
      rescue
        e ->
          duration = System.monotonic_time() - start_time

          :telemetry.execute(
            [:neonfs, :blob_store, :read_chunk, :exception],
            %{duration: duration},
            Map.put(metadata, :error, Exception.message(e))
          )

          {:error, Exception.message(e)}
      end

    result
  end

  ## Private: Delete (searches all tiers)

  defp do_chunk_info(stores, hash) do
    Enum.reduce_while(stores, {:error, :not_found}, fn {_drive_id, store}, acc ->
      case find_chunk_on_store(store, hash) do
        {:ok, _tier, _size} = found -> {:halt, found}
        {:error, :not_found} -> {:cont, acc}
      end
    end)
  end

  defp find_chunk_on_store(store, hash) do
    Enum.reduce_while(@tiers, {:error, :not_found}, fn tier, acc ->
      case chunk_found_at_tier(store, hash, tier) do
        {:ok, _size} = found -> {:halt, {:ok, tier, elem(found, 1)}}
        :not_found -> {:cont, acc}
      end
    end)
  end

  defp chunk_found_at_tier(store, hash, tier) do
    case Native.store_chunk_exists_any_codec(store, hash, tier) do
      {:ok, true} -> {:ok, chunk_any_codec_size(store, hash, tier)}
      _ -> :not_found
    end
  end

  defp chunk_any_codec_size(store, hash, tier) do
    case Native.store_chunk_any_codec_size(store, hash, tier) do
      {:ok, n} -> n
      _ -> 0
    end
  end

  defp do_delete_chunk(store, hash, drive_id, codec_opts) do
    metadata = %{drive_id: drive_id, hash: Base.encode16(hash, case: :lower)}
    start_time = System.monotonic_time()

    :telemetry.execute(
      [:neonfs, :blob_store, :delete_chunk, :start],
      %{system_time: System.system_time()},
      metadata
    )

    result =
      try do
        case try_delete_from_tiers(store, hash, codec_opts) do
          {:ok, bytes_freed} ->
            duration = System.monotonic_time() - start_time

            :telemetry.execute(
              [:neonfs, :blob_store, :delete_chunk, :stop],
              %{duration: duration, bytes_freed: bytes_freed},
              metadata
            )

            {:ok, bytes_freed}

          {:error, reason} = error ->
            duration = System.monotonic_time() - start_time

            :telemetry.execute(
              [:neonfs, :blob_store, :delete_chunk, :exception],
              %{duration: duration},
              Map.put(metadata, :error, reason)
            )

            error
        end
      rescue
        e ->
          duration = System.monotonic_time() - start_time

          :telemetry.execute(
            [:neonfs, :blob_store, :delete_chunk, :exception],
            %{duration: duration},
            Map.put(metadata, :error, Exception.message(e))
          )

          {:error, Exception.message(e)}
      end

    result
  end

  defp try_delete_from_tiers(store, hash, codec_opts) do
    {comp_str, comp_level} = codec_compression_args(codec_opts)
    key = Keyword.get(codec_opts, :key, <<>>)
    nonce = Keyword.get(codec_opts, :nonce, <<>>)

    # Try each tier, return success on first hit. When codec_opts are empty
    # (the common orphan-delete case), fall back to "any codec" so we clean
    # up every variant at the hit tier.
    any_codec? = comp_str == "" and key == <<>> and nonce == <<>>

    Enum.reduce_while(@tiers, {:error, "chunk not found in any tier"}, fn tier, acc ->
      result =
        if any_codec? do
          Native.store_delete_chunk_any_codec(store, hash, tier)
        else
          Native.store_delete_chunk(store, hash, tier, comp_str, comp_level, key, nonce)
        end

      case result do
        {:ok, bytes_freed} -> {:halt, {:ok, bytes_freed}}
        {:error, _} -> {:cont, acc}
      end
    end)
  end

  ## Private: Drive data check

  defp check_drive_has_data(drive_path) do
    # The NIF stores data under {drive_path}/blobs/{tier}/{prefix}/{hash}
    blobs_path = Path.join(drive_path, "blobs")

    Enum.any?(@tiers, fn tier ->
      tier_path = Path.join(blobs_path, tier)

      case File.ls(tier_path) do
        {:ok, entries} -> Enum.any?(entries, &prefix_dir_has_files?(tier_path, &1))
        {:error, _} -> false
      end
    end)
  end

  defp prefix_dir_has_files?(parent, entry) do
    path = Path.join(parent, entry)

    case File.stat(path) do
      {:ok, %{type: :directory}} ->
        case File.ls(path) do
          {:ok, [_ | _]} -> true
          _ -> false
        end

      _ ->
        # A file directly in the tier dir also counts as data
        true
    end
  end

  ## Private: Migrate

  defp do_migrate_chunk(store, hash, drive_id, from_tier, to_tier, codec_opts) do
    metadata = %{
      drive_id: drive_id,
      hash: Base.encode16(hash, case: :lower),
      from_tier: from_tier,
      to_tier: to_tier
    }

    start_time = System.monotonic_time()

    :telemetry.execute(
      [:neonfs, :blob_store, :migrate_chunk, :start],
      %{system_time: System.system_time()},
      metadata
    )

    {comp_str, comp_level} = codec_compression_args(codec_opts)
    key = Keyword.get(codec_opts, :key, <<>>)
    nonce = Keyword.get(codec_opts, :nonce, <<>>)

    result =
      try do
        case Native.store_migrate_chunk(
               store,
               hash,
               from_tier,
               to_tier,
               comp_str,
               comp_level,
               key,
               nonce
             ) do
          {:ok, {}} = success ->
            duration = System.monotonic_time() - start_time

            :telemetry.execute(
              [:neonfs, :blob_store, :migrate_chunk, :stop],
              %{duration: duration},
              metadata
            )

            success

          {:error, reason} = error ->
            duration = System.monotonic_time() - start_time

            :telemetry.execute(
              [:neonfs, :blob_store, :migrate_chunk, :exception],
              %{duration: duration},
              Map.put(metadata, :error, reason)
            )

            error
        end
      rescue
        e ->
          duration = System.monotonic_time() - start_time

          :telemetry.execute(
            [:neonfs, :blob_store, :migrate_chunk, :exception],
            %{duration: duration},
            Map.put(metadata, :error, Exception.message(e))
          )

          {:error, Exception.message(e)}
      end

    result
  end
end
