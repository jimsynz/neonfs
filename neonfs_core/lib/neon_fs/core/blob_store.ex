defmodule NeonFS.Core.BlobStore do
  @moduledoc """
  High-level Elixir wrapper around the blob store NIF.

  This GenServer manages the blob store resource lifecycle and provides
  a clean API for chunk operations with proper error handling and telemetry.

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

  All events include metadata with operation details.

  ## Example

      {:ok, _pid} = NeonFS.Core.BlobStore.start_link(
        base_dir: "/tmp/blobs",
        prefix_depth: 2
      )

      data = "hello world"
      {:ok, hash, info} = NeonFS.Core.BlobStore.write_chunk(data, "hot")
      {:ok, ^data} = NeonFS.Core.BlobStore.read_chunk(hash, "hot")

  """

  use GenServer
  require Logger

  alias NeonFS.Core.Blob.Native

  @type chunk_hash :: binary()
  @type tier :: String.t()
  @type chunk_info :: %{
          original_size: non_neg_integer(),
          stored_size: non_neg_integer(),
          compression: String.t()
        }

  ## Client API

  @doc """
  Starts the blob store GenServer.

  ## Options

    * `:base_dir` (required) - Base directory for blob storage
    * `:prefix_depth` (optional) - Number of prefix directory levels, defaults to 2
    * `:name` (optional) - Name for the GenServer, defaults to `__MODULE__`

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Writes a chunk to the blob store.

  The chunk is hashed using SHA-256, and the hash is used for content-addressed
  storage. Optional compression can be applied.

  ## Parameters

    * `data` - Binary data to write
    * `tier` - Storage tier: "hot", "warm", or "cold"
    * `opts` - Optional keyword list:
      * `:compression` - Compression type: "none" (default) or "zstd"
      * `:compression_level` - Compression level for zstd (1-19), defaults to 3
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, hash, chunk_info}` - Hash and chunk metadata on success
    * `{:error, reason}` - On failure

  ## Example

      {:ok, hash, info} = BlobStore.write_chunk("hello", "hot")
      {:ok, hash, info} = BlobStore.write_chunk(data, "warm",
        compression: "zstd",
        compression_level: 5
      )

  """
  @spec write_chunk(binary(), tier(), keyword()) ::
          {:ok, chunk_hash(), chunk_info()} | {:error, String.t()}
  def write_chunk(data, tier, opts \\ []) do
    {server, opts} = Keyword.pop(opts, :server, __MODULE__)
    GenServer.call(server, {:write_chunk, data, tier, opts})
  end

  @doc """
  Reads a chunk from the blob store by hash.

  ## Parameters

    * `hash` - SHA-256 hash of the chunk (32 bytes)
    * `tier` - Storage tier: "hot", "warm", or "cold"
    * `opts` - Optional keyword list:
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, data}` - The chunk data on success
    * `{:error, reason}` - On failure

  ## Example

      {:ok, data} = BlobStore.read_chunk(hash, "hot")

  """
  @spec read_chunk(chunk_hash(), tier(), keyword()) :: {:ok, binary()} | {:error, String.t()}
  def read_chunk(hash, tier, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:read_chunk, hash, tier, []})
  end

  @doc """
  Reads a chunk from the blob store with options.

  ## Parameters

    * `hash` - SHA-256 hash of the chunk (32 bytes)
    * `tier` - Storage tier: "hot", "warm", or "cold"
    * `read_opts` - Keyword list:
      * `:verify` - Verify data integrity (default: false)
      * `:decompress` - Decompress data (default: false)
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, data}` - The chunk data on success
    * `{:error, reason}` - On failure

  ## Example

      {:ok, data} = BlobStore.read_chunk(hash, "hot", verify: true, decompress: true)

  """
  @spec read_chunk_with_options(chunk_hash(), tier(), keyword()) ::
          {:ok, binary()} | {:error, String.t()}
  def read_chunk_with_options(hash, tier, read_opts) do
    {server, read_opts} = Keyword.pop(read_opts, :server, __MODULE__)
    GenServer.call(server, {:read_chunk, hash, tier, read_opts})
  end

  @doc """
  Deletes a chunk from the blob store.

  ## Parameters

    * `hash` - SHA-256 hash of the chunk (32 bytes)
    * `tier` - Storage tier: "hot", "warm", or "cold"
    * `opts` - Optional keyword list:
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, {}}` - On success
    * `{:error, reason}` - On failure

  ## Example

      {:ok, {}} = BlobStore.delete_chunk(hash, "hot")

  """
  @spec delete_chunk(chunk_hash(), tier(), keyword()) :: {:ok, {}} | {:error, String.t()}
  def delete_chunk(hash, tier, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:delete_chunk, hash, tier})
  end

  @doc """
  Migrates a chunk from one storage tier to another.

  The operation is atomic: the chunk is copied to the destination tier,
  verified for integrity, and only then deleted from the source tier.

  ## Parameters

    * `hash` - SHA-256 hash of the chunk (32 bytes)
    * `from_tier` - Source tier: "hot", "warm", or "cold"
    * `to_tier` - Destination tier: "hot", "warm", or "cold"
    * `opts` - Optional keyword list:
      * `:server` - GenServer name, defaults to `__MODULE__`

  ## Returns

    * `{:ok, {}}` - On success
    * `{:error, reason}` - On failure

  ## Example

      {:ok, {}} = BlobStore.migrate_chunk(hash, "hot", "cold")

  """
  @spec migrate_chunk(chunk_hash(), tier(), tier(), keyword()) ::
          {:ok, {}} | {:error, String.t()}
  def migrate_chunk(hash, from_tier, to_tier, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:migrate_chunk, hash, from_tier, to_tier})
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

  ## Example

      {:ok, chunks} = BlobStore.chunk_data(data, :auto)
      {:ok, chunks} = BlobStore.chunk_data(data, {:fixed, 256 * 1024})

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

  ## GenServer Callbacks

  @impl true
  def init(opts) do
    base_dir = Keyword.fetch!(opts, :base_dir)
    prefix_depth = Keyword.get(opts, :prefix_depth, 2)

    case Native.store_open(base_dir, prefix_depth) do
      {:ok, store} ->
        {:ok, %{store: store, base_dir: base_dir, prefix_depth: prefix_depth}}

      {:error, reason} ->
        Logger.error("Failed to open blob store at #{base_dir}: #{reason}")
        {:stop, {:error, reason}}
    end
  end

  @impl true
  def handle_call({:write_chunk, data, tier, opts}, _from, state) do
    compression = Keyword.get(opts, :compression, "none")
    compression_level = Keyword.get(opts, :compression_level, 3)

    metadata = %{tier: tier, data_size: byte_size(data), compression: compression}
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
               state.store,
               hash,
               data,
               tier,
               compression,
               compression_level
             ) do
          {:ok, {original_size, stored_size, comp_type}} ->
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

    {:reply, result, state}
  end

  @impl true
  def handle_call({:read_chunk, hash, tier, read_opts}, _from, state) do
    verify = Keyword.get(read_opts, :verify, false)
    decompress = Keyword.get(read_opts, :decompress, false)

    metadata = %{
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

    result =
      try do
        case Native.store_read_chunk_with_options(state.store, hash, tier, verify, decompress) do
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

    {:reply, result, state}
  end

  @impl true
  def handle_call({:delete_chunk, hash, tier}, _from, state) do
    metadata = %{tier: tier, hash: Base.encode16(hash, case: :lower)}
    start_time = System.monotonic_time()

    :telemetry.execute(
      [:neonfs, :blob_store, :delete_chunk, :start],
      %{system_time: System.system_time()},
      metadata
    )

    result =
      try do
        case Native.store_delete_chunk(state.store, hash, tier) do
          {:ok, {}} = success ->
            duration = System.monotonic_time() - start_time

            :telemetry.execute(
              [:neonfs, :blob_store, :delete_chunk, :stop],
              %{duration: duration},
              metadata
            )

            success

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

    {:reply, result, state}
  end

  @impl true
  def handle_call({:migrate_chunk, hash, from_tier, to_tier}, _from, state) do
    metadata = %{
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

    result =
      try do
        case Native.store_migrate_chunk(state.store, hash, from_tier, to_tier) do
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

    {:reply, result, state}
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
end
