defmodule NeonFS.Core.ChunkFetcher do
  @moduledoc """
  Handles fetching chunks from local or remote nodes.

  When a chunk isn't available locally, this module fetches it from a remote
  node that has it. This enables location-transparent storage - reads work
  regardless of which node the data is physically on.

  ## Telemetry Events

  The following telemetry events are emitted:

  - `[:neonfs, :chunk_fetcher, :local_hit]` - Chunk found locally
  - `[:neonfs, :chunk_fetcher, :remote_fetch, :start]` - Starting remote fetch
  - `[:neonfs, :chunk_fetcher, :remote_fetch, :stop]` - Remote fetch completed
  - `[:neonfs, :chunk_fetcher, :remote_fetch, :exception]` - Remote fetch failed

  All events include metadata with operation details.
  """

  alias NeonFS.Core.{BlobStore, ChunkIndex}
  require Logger

  @type fetch_result :: {:ok, binary(), source()} | {:error, term()}
  @type source :: :local | {:remote, atom()}

  @doc """
  Fetches a chunk by hash, from local storage or a remote node.

  First checks the local blob store. If not found locally, looks up the chunk's
  locations in the metadata and fetches from a remote node.

  ## Parameters

    * `hash` - SHA-256 hash of the chunk (32 bytes)
    * `opts` - Optional keyword list:
      * `:tier` - Storage tier hint (default: "hot")
      * `:verify` - Verify data integrity (default: false)
      * `:decompress` - Decompress data (default: false)
      * `:cache_remote` - Cache remotely fetched chunks locally (default: false)

  ## Returns

    * `{:ok, data, :local}` - Chunk fetched from local storage
    * `{:ok, data, {:remote, node}}` - Chunk fetched from remote node
    * `{:error, :chunk_not_found}` - Chunk not in metadata
    * `{:error, :all_replicas_failed}` - All remote fetches failed
    * `{:error, reason}` - Other error

  ## Examples

      {:ok, data, :local} = ChunkFetcher.fetch_chunk(hash)
      {:ok, data, {:remote, :node2@host}} = ChunkFetcher.fetch_chunk(hash, tier: "warm")

  """
  @spec fetch_chunk(binary(), keyword()) :: fetch_result()
  def fetch_chunk(hash, opts \\ []) when is_binary(hash) do
    tier = Keyword.get(opts, :tier, "hot")
    verify = Keyword.get(opts, :verify, false)
    decompress = Keyword.get(opts, :decompress, false)

    start_time = System.monotonic_time()

    # Try local fetch first
    case try_local_fetch(hash, tier, verify, decompress) do
      {:ok, data} ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:neonfs, :chunk_fetcher, :local_hit],
          %{duration: duration, bytes: byte_size(data)},
          %{hash: Base.encode16(hash, case: :lower)}
        )

        {:ok, data, :local}

      {:error, _reason} ->
        # Not found locally, try remote fetch
        try_remote_fetch(hash, tier, verify, decompress, start_time, opts)
    end
  end

  # Private Functions

  defp try_local_fetch(hash, tier, verify, decompress) do
    read_opts = [verify: verify, decompress: decompress]

    case BlobStore.read_chunk_with_options(hash, tier, read_opts) do
      {:ok, data} -> {:ok, data}
      {:error, reason} -> {:error, reason}
    end
  end

  defp try_remote_fetch(hash, tier, verify, decompress, start_time, opts) do
    cache_remote = Keyword.get(opts, :cache_remote, false)

    :telemetry.execute(
      [:neonfs, :chunk_fetcher, :remote_fetch, :start],
      %{system_time: System.system_time()},
      %{hash: Base.encode16(hash, case: :lower)}
    )

    result =
      case ChunkIndex.get(hash) do
        {:error, :not_found} ->
          {:error, :chunk_not_found}

        {:ok, chunk_meta} ->
          fetch_from_remote(hash, chunk_meta.locations, tier, verify, decompress, cache_remote)
      end

    duration = System.monotonic_time() - start_time

    case result do
      {:ok, data, {:remote, node}} ->
        :telemetry.execute(
          [:neonfs, :chunk_fetcher, :remote_fetch, :stop],
          %{duration: duration, bytes: byte_size(data)},
          %{hash: Base.encode16(hash, case: :lower), node: node}
        )

      {:error, reason} ->
        :telemetry.execute(
          [:neonfs, :chunk_fetcher, :remote_fetch, :exception],
          %{duration: duration},
          %{hash: Base.encode16(hash, case: :lower), error: reason}
        )
    end

    result
  end

  defp fetch_from_remote(hash, locations, tier, verify, decompress, cache_remote) do
    # Filter out local node from locations
    remote_locations =
      locations
      |> Enum.reject(&(&1.node == Node.self()))
      |> sort_locations_by_preference()

    # Try each location until one succeeds
    Enum.find_value(remote_locations, {:error, :all_replicas_failed}, fn location ->
      try_fetch_from_location(location, hash, tier, verify, decompress, cache_remote)
    end)
  end

  defp try_fetch_from_location(location, hash, tier, verify, decompress, cache_remote) do
    case rpc_read_chunk(location.node, hash, tier, verify, decompress) do
      {:ok, data} ->
        maybe_cache_remotely_fetched_chunk(hash, data, tier, cache_remote)
        {:ok, data, {:remote, location.node}}

      {:error, reason} ->
        Logger.debug(
          "Failed to fetch chunk from #{location.node}: #{inspect(reason)}, trying next location"
        )

        # Return nil to continue to next location
        nil
    end
  end

  defp maybe_cache_remotely_fetched_chunk(hash, data, tier, cache_remote) do
    if cache_remote do
      spawn(fn -> cache_chunk_locally(hash, data, tier) end)
    end
  end

  defp sort_locations_by_preference(locations) do
    # For Phase 2, simple sorting: prefer any available node
    # In future phases, could consider:
    # - Same rack/zone (from location metadata)
    # - Network proximity (latency measurements)
    # - Node load (current request count)
    # For now, just shuffle to distribute load
    Enum.shuffle(locations)
  end

  defp rpc_read_chunk(node, hash, tier, verify, decompress) do
    read_opts = [verify: verify, decompress: decompress]

    # Use :rpc to call BlobStore.read_chunk_with_options on the remote node
    case :rpc.call(
           node,
           BlobStore,
           :read_chunk_with_options,
           [hash, tier, read_opts],
           10_000
         ) do
      {:ok, data} ->
        {:ok, data}

      {:error, reason} ->
        {:error, reason}

      {:badrpc, reason} ->
        Logger.warning("RPC failed when reading chunk from #{node}: #{inspect(reason)}")
        {:error, {:rpc_failed, reason}}
    end
  end

  defp cache_chunk_locally(hash, data, tier) do
    # Store the chunk locally for future reads
    case BlobStore.write_chunk(data, tier) do
      {:ok, written_hash, _info} ->
        if written_hash == hash do
          Logger.debug("Successfully cached remote chunk #{Base.encode16(hash)} locally")
          :ok
        else
          Logger.error("Hash mismatch when caching chunk locally")
          {:error, :hash_mismatch}
        end

      {:error, reason} ->
        Logger.warning("Failed to cache remote chunk locally: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
