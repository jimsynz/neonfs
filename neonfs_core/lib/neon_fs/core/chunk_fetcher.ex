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

  alias NeonFS.Client.Router
  alias NeonFS.Core.{BlobStore, ChunkAccessTracker, ChunkCache, ChunkIndex, DriveRegistry}
  alias NeonFS.IO.{Operation, Scheduler}
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
      * `:key` - Decryption key (default: <<>>)
      * `:nonce` - Decryption nonce (default: <<>>)
      * `:cache_reason` - Why this chunk should be cached (e.g. `:reconstructed` for EC-reconstructed data)

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
    volume_id = Keyword.get(opts, :volume_id)
    start_time = System.monotonic_time()

    # Try cache first (only for transformed chunks)
    result =
      case try_cache_fetch(volume_id, hash) do
        {:ok, data} ->
          {:ok, data, :local}

        :miss ->
          fetch_from_storage(hash, start_time, opts)
      end

    # Record access for tiering decisions and populate cache
    case result do
      {:ok, data, source} ->
        ChunkAccessTracker.record_access(hash)
        maybe_cache_result(volume_id, hash, data, source, opts)

      _ ->
        :ok
    end

    result
  end

  @doc """
  Sorts locations by preference score. Lower scores are preferred.

  Locations with the same score are shuffled randomly to avoid hotspotting.
  This is a pure function suitable for testing without GenServer dependencies.

  ## Scoring hierarchy

  | Location type          | Score |
  |------------------------|-------|
  | Local SSD              |   0   |
  | Remote SSD             |  10   |
  | Local HDD active       |  20   |
  | Remote HDD active      |  30   |
  | HDD standby (any)      |  50   |
  | Unknown drive           | 100   |
  """
  @spec sort_locations_by_score([map()], node()) :: [map()]
  def sort_locations_by_score(locations, local_node) do
    locations
    |> Enum.map(fn loc -> {score_location(loc, local_node), :rand.uniform(), loc} end)
    |> Enum.sort_by(fn {score, rand, _loc} -> {score, rand} end)
    |> Enum.map(fn {_score, _rand, loc} -> loc end)
  end

  @doc """
  Computes a preference score for a chunk location.

  Lower scores are preferred. The `:hot` tier is treated as SSD-class storage;
  all other tiers (`:warm`, `:cold`) are treated as HDD-class. The gaps between
  scores (multiples of 10) allow future fine-tuning without reshuffling the hierarchy.
  """
  @spec score_location(map(), node()) :: non_neg_integer()
  def score_location(location_info, local_node) do
    tier = Map.get(location_info, :tier)
    state = Map.get(location_info, :state)
    local? = Map.get(location_info, :node) == local_node

    drive_score(tier, state, local?)
  end

  # ─── Private Functions ──────────────────────────────────────────────

  defp try_cache_fetch(nil, _hash), do: :miss

  defp try_cache_fetch(volume_id, hash) do
    ChunkCache.get(volume_id, hash)
  rescue
    ArgumentError -> :miss
  end

  defp maybe_cache_result(nil, _hash, _data, _source, _opts), do: :ok

  defp maybe_cache_result(volume_id, hash, data, source, opts) do
    chunk_types = cache_chunk_types(source, opts)

    if chunk_types != [] do
      ChunkCache.put(volume_id, hash, data, chunk_type: chunk_types)
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp cache_chunk_types(source, opts) do
    decompress = Keyword.get(opts, :decompress, false)
    reconstructed = Keyword.get(opts, :cache_reason) == :reconstructed
    remote = match?({:remote, _}, source)

    []
    |> maybe_add_type(:transformed, decompress)
    |> maybe_add_type(:reconstructed, reconstructed)
    |> maybe_add_remote_type(remote)
  end

  defp maybe_add_type(types, _type, false), do: types
  defp maybe_add_type(types, type, true), do: [type | types]

  defp maybe_add_remote_type(types, false), do: types
  defp maybe_add_remote_type([], true), do: []
  defp maybe_add_remote_type(types, true), do: [:remote | types]

  defp fetch_from_storage(hash, start_time, opts) do
    exclude_nodes = Keyword.get(opts, :exclude_nodes, [])

    if node() in exclude_nodes do
      # Skip local fetch entirely — go straight to remote
      try_remote_fetch(hash, start_time, opts, :excluded)
    else
      fetch_from_local_then_remote(hash, start_time, opts)
    end
  end

  defp fetch_from_local_then_remote(hash, start_time, opts) do
    read_opts = build_read_opts(opts)
    drive_id = Keyword.get(opts, :drive_id, "default")
    tier = Keyword.get(opts, :tier, "hot")
    volume_id = Keyword.get(opts, :volume_id, "_system")

    case try_local_fetch(hash, drive_id, tier, read_opts, volume_id) do
      {:ok, data} ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:neonfs, :chunk_fetcher, :local_hit],
          %{duration: duration, bytes: byte_size(data)},
          %{hash: Base.encode16(hash, case: :lower)}
        )

        {:ok, data, :local}

      {:error, "encryption error" <> _ = reason} ->
        # Decryption failures mean the chunk was found locally but couldn't be
        # authenticated. Retrying on a remote node with the same key/nonce would
        # fail identically, so propagate immediately.
        {:error, reason}

      {:error, local_reason} ->
        # Not found locally, try remote fetch
        try_remote_fetch(hash, start_time, opts, local_reason)
    end
  end

  defp try_local_fetch(hash, drive_id, tier, read_opts, volume_id) do
    op =
      Operation.new(
        priority: :user_read,
        volume_id: volume_id,
        drive_id: drive_id,
        type: :read,
        callback: fn -> BlobStore.read_chunk_with_options(hash, drive_id, tier, read_opts) end
      )

    Scheduler.submit_sync(op)
  end

  defp try_remote_fetch(hash, start_time, opts, local_reason) do
    read_opts = build_read_opts(opts)
    tier = Keyword.get(opts, :tier, "hot")
    cache_remote = Keyword.get(opts, :cache_remote, false)

    :telemetry.execute(
      [:neonfs, :chunk_fetcher, :remote_fetch, :start],
      %{system_time: System.system_time()},
      %{hash: Base.encode16(hash, case: :lower)}
    )

    exclude_nodes = Keyword.get(opts, :exclude_nodes, [])

    result =
      case ChunkIndex.get(hash) do
        {:error, :not_found} ->
          {:error, :chunk_not_found}

        {:ok, chunk_meta} ->
          fetch_from_remote(
            hash,
            chunk_meta.locations,
            tier,
            read_opts,
            cache_remote,
            local_reason,
            exclude_nodes
          )
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

  defp fetch_from_remote(
         hash,
         locations,
         tier,
         read_opts,
         cache_remote,
         local_reason,
         exclude_nodes
       ) do
    # Filter out local node and any explicitly excluded nodes from locations
    excluded = [Node.self() | exclude_nodes] |> Enum.uniq()

    remote_locations =
      locations
      |> Enum.reject(&(&1.node in excluded))
      |> sort_locations_by_preference()

    # When no remote locations exist (single-node), surface the local error
    # instead of the misleading :all_replicas_failed
    default_error =
      if remote_locations == [] do
        {:error, {:local_read_failed, local_reason}}
      else
        {:error, :all_replicas_failed}
      end

    # Try each location until one succeeds
    Enum.find_value(remote_locations, default_error, fn location ->
      try_fetch_from_location(location, hash, tier, read_opts, cache_remote)
    end)
  end

  defp try_fetch_from_location(location, hash, tier, read_opts, cache_remote) do
    drive_id = Map.get(location, :drive_id, "default")

    case read_remote_chunk(location.node, hash, drive_id, tier, read_opts) do
      {:ok, data} ->
        maybe_cache_remotely_fetched_chunk(hash, data, tier, cache_remote)
        {:ok, data, {:remote, location.node}}

      {:error, reason} ->
        Logger.debug("Failed to fetch chunk from node, trying next location",
          node: location.node,
          reason: inspect(reason)
        )

        # Return nil to continue to next location
        nil
    end
  end

  defp maybe_cache_remotely_fetched_chunk(hash, data, tier, cache_remote) do
    if cache_remote do
      op =
        Operation.new(
          priority: :replication,
          volume_id: "_cache",
          drive_id: "default",
          type: :write,
          callback: fn -> cache_chunk_locally(hash, data, tier) end
        )

      Scheduler.submit_async(op)
    end
  end

  defp sort_locations_by_preference(locations) do
    local_node = Node.self()

    locations
    |> Enum.map(&enrich_location_with_drive_info/1)
    |> sort_locations_by_score(local_node)
  end

  defp enrich_location_with_drive_info(location) do
    node = location.node
    drive_id = Map.get(location, :drive_id, "default")

    case safe_get_drive(node, drive_id) do
      {:ok, drive} ->
        Map.merge(location, %{tier: drive.tier, state: drive.state})

      {:error, _} ->
        Logger.debug("Drive info unavailable, assigning worst score",
          drive_id: drive_id,
          node: node
        )

        location
    end
  end

  defp safe_get_drive(node, drive_id) do
    DriveRegistry.get_drive(node, drive_id)
  rescue
    ArgumentError -> {:error, :registry_unavailable}
  catch
    :exit, _ -> {:error, :registry_unavailable}
  end

  defp drive_score(nil, _state, _local?), do: 100
  defp drive_score(:hot, _state, true), do: 0
  defp drive_score(:hot, _state, false), do: 10
  defp drive_score(_hdd, :standby, _local?), do: 50
  defp drive_score(_hdd, _state, true), do: 20
  defp drive_score(_hdd, _state, false), do: 30

  defp build_read_opts(opts) do
    [
      verify: Keyword.get(opts, :verify, false),
      decompress: Keyword.get(opts, :decompress, false),
      key: Keyword.get(opts, :key, <<>>),
      nonce: Keyword.get(opts, :nonce, <<>>)
    ]
  end

  defp read_remote_chunk(node, hash, drive_id, tier, read_opts) do
    if needs_remote_processing?(read_opts) do
      # Chunks needing decompression or decryption must use RPC because
      # the data plane returns raw stored bytes without transformation
      rpc_read_chunk(node, hash, drive_id, tier, read_opts)
    else
      data_call_read_chunk(node, hash, drive_id, tier, read_opts)
    end
  end

  defp needs_remote_processing?(read_opts) do
    Keyword.get(read_opts, :decompress, false) or
      Keyword.get(read_opts, :key, <<>>) != <<>>
  end

  defp data_call_read_chunk(node, hash, drive_id, tier, read_opts) do
    case Router.data_call(node, :get_chunk, [hash: hash, volume_id: drive_id, tier: tier],
           timeout: 10_000
         ) do
      {:ok, data} ->
        {:ok, data}

      {:error, :no_data_endpoint} ->
        Logger.info("No data endpoint, falling back to distribution RPC for read", node: node)
        rpc_read_chunk(node, hash, drive_id, tier, read_opts)

      {:error, :not_found} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp rpc_read_chunk(node, hash, drive_id, tier, read_opts) do
    case :rpc.call(
           node,
           BlobStore,
           :read_chunk_with_options,
           [hash, drive_id, tier, read_opts],
           10_000
         ) do
      {:ok, data} ->
        {:ok, data}

      {:error, reason} ->
        {:error, reason}

      {:badrpc, reason} ->
        Logger.warning("RPC failed when reading chunk from node",
          node: node,
          reason: inspect(reason)
        )

        {:error, {:rpc_failed, reason}}
    end
  end

  defp cache_chunk_locally(hash, data, tier) do
    # Store the chunk locally for future reads
    drive_id = "default"

    case BlobStore.write_chunk(data, drive_id, tier) do
      {:ok, written_hash, _info} ->
        if written_hash == hash do
          Logger.debug("Successfully cached remote chunk locally",
            chunk_hash: Base.encode16(hash)
          )

          :ok
        else
          Logger.error("Hash mismatch when caching chunk locally")
          {:error, :hash_mismatch}
        end

      {:error, reason} ->
        Logger.warning("Failed to cache remote chunk locally", reason: inspect(reason))
        {:error, reason}
    end
  end
end
