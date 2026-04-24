defmodule NeonFS.Client.ChunkWriter do
  @moduledoc """
  Streams a byte sequence into a cluster volume by chunking locally and
  pushing each chunk over the TLS data plane to a core node.

  The write-side counterpart to `NeonFS.Client.ChunkReader`. Interface
  nodes (S3, WebDAV, NFS, FUSE) that receive a streaming upload call
  `write_file_stream/4` instead of buffering the whole payload and
  round-tripping it through `NeonFS.Core.write_file_streamed/4` over the
  control plane — bytes stay off Erlang distribution and the working
  set is bounded by the chunker's maximum chunk size.

  The flow per chunk is:

    1. Emit chunks from `NeonFS.Client.Chunker.Native` as the stream
       is consumed.
    2. `NeonFS.Client.Router.data_call(target.node, :put_chunk, …)` ships
       each chunk to a core node over TLS. `:processing_volume_id` is
       set so the receiving handler applies the volume's compression /
       encryption codecs before writing to the blob store (the
       interface-side chunking path from the #408 design note).
    3. A per-chunk `%{hash:, location:, size:}` ref is collected and
       returned in order so the caller can subsequently invoke
       `NeonFS.Core.commit_chunks/4` with `:locations` and
       `:total_size`.

  ## Return shape

  The spec in #450 listed `{:ok, [chunk_hash]}` — the primary output is
  the ordered hash list. In practice `commit_chunks/4` also needs the
  locations map (`%{hash => [%{node, drive_id, tier}]}`) and the file's
  total byte length, both of which only the writer knows. This module
  therefore returns a list of per-chunk refs; callers derive the hash
  list, locations map, and total size from it. See `chunk_refs_to_commit_opts/1`.

  ## Replica fan-out

  Each chunk is shipped to a single core node — the receiving
  `put_chunk` handler kicks off the remaining `durability.factor - 1`
  replicas via `NeonFS.Core.Replication.replicate_chunk/4` and returns
  the full location list so the commit carries every replica that
  acknowledged (#478). The interface never fans out directly; this
  keeps a single data-plane hop from the interface per chunk and reuses
  the core-side replication path the co-located writer already has.

  Volumes with `durability.factor == 1` produce a single-location ref.
  Partial acceptance — e.g. 2 of 3 replicas succeed on a `:quorum`
  volume that only requires 2 — surfaces as a ref whose `:locations`
  list has fewer than `durability.factor` entries but meets
  `durability.min_copies`. Volumes that cannot reach `min_copies`
  return `{:error, _}` from `put_chunk` and the writer aborts
  every already-accepted replica.

  ## Abort

  The data-plane surface is put/get/has only — there is no delete. On
  error the writer falls back to a best-effort
  `:rpc.call(node, BlobStore, :delete_chunk, …)` against every node
  that accepted a chunk — including each replica, not just the
  primary. Failures are logged and swallowed; durable orphan cleanup
  is core-side GC's job. Interface-side durable abort tracking
  (`PendingWriteLog`) is explicitly deferred per #450.
  """

  require Logger

  alias NeonFS.Client.Chunker.Native
  alias NeonFS.Client.{Discovery, Router}
  alias NeonFS.Core.Volume
  alias NeonFS.Error.Unavailable

  @default_strategy "auto"
  @default_timeout 30_000
  @default_drive_id "default"

  @type location :: %{
          required(:node) => node(),
          required(:drive_id) => String.t(),
          required(:tier) => Volume.tier()
        }

  # `NeonFS.Core.ChunkMeta.compression/0` lives in neonfs_core and
  # therefore can't be referenced from neonfs_client (pure type-only
  # dependency inversion is the reason the PLT flagged this with
  # `unknown_type`). Mirror the literal here — the receiving
  # `CommitChunks.create_chunk_meta/3` maps it back onto
  # `ChunkMeta.compression()`.
  @type compression :: :none | :zstd

  @type codec_info :: %{
          required(:compression) => compression(),
          required(:crypto) => NeonFS.Core.ChunkCrypto.t() | nil,
          required(:original_size) => non_neg_integer()
        }

  @type chunk_ref :: %{
          required(:hash) => binary(),
          required(:locations) => [location()],
          required(:size) => non_neg_integer(),
          required(:codec) => codec_info()
        }

  @type abort_fn :: (location(), binary() -> any())

  @type write_opts :: [
          tier: Volume.tier(),
          drive_id: String.t(),
          target_node: node(),
          strategy: String.t(),
          strategy_param: non_neg_integer(),
          timeout: timeout(),
          exclude_nodes: [node()],
          abort_fn: abort_fn()
        ]

  @doc """
  Chunks an `Enumerable.t()` of binary segments locally and pushes each
  chunk to a core node over the TLS data plane.

  Returns `{:ok, [chunk_ref()]}` in byte order on success. The ref list
  is everything `NeonFS.Core.commit_chunks/4` needs: pass it through
  `chunk_refs_to_commit_opts/1` for `:locations` and `:total_size`,
  and `Enum.map(&(&1.hash))` for the hash list.

  ## Options

    * `:tier` — target storage tier (default: `volume.tiering.initial_tier`).
    * `:drive_id` — target drive on the chosen node (default: `"default"`).
    * `:target_node` — pin the target core node; otherwise pick the
      first available from `Discovery.get_core_nodes/0`.
    * `:strategy` — chunker strategy: `"auto"` (default), `"fastcdc"`,
      `"fixed"`, or `"single"`.
    * `:strategy_param` — strategy-specific parameter; defaults to 0
      (which lets `auto` / `fastcdc` fall back to sensible sizes).
    * `:timeout` — per-chunk `data_call` timeout in ms (default: 30_000).
    * `:exclude_nodes` — nodes to skip when selecting a target.
    * `:abort_fn` — override the best-effort abort callback (arity 2:
      `location, hash`). Default issues a 5-second
      `:rpc.call(node, NeonFS.Core.BlobStore, :delete_chunk, [hash,
      drive_id, []])`. Useful for tests and for interface nodes that
      want to queue aborts onto their own supervisor.

  ## Errors

    * `{:error, :no_core_nodes_available}` — no discoverable target.
    * `{:error, %NeonFS.Error.Unavailable{}}` — volume lookup RPC failed.
    * `{:error, {:put_chunk_failed, reason}}` — data-plane write failed;
      successfully-written chunks have been best-effort aborted.
    * `{:error, reason}` — propagated from `get_volume` / chunker NIF.
  """
  @spec write_file_stream(String.t(), String.t(), Enumerable.t(), write_opts()) ::
          {:ok, [chunk_ref()]} | {:error, term()}
  def write_file_stream(volume_name, path, stream, opts \\ []) do
    Logger.metadata(component: :chunk_writer, volume_id: volume_name, request_id: path)

    with {:ok, volume} <- fetch_volume(volume_name),
         {:ok, target} <- select_target(volume, opts),
         {:ok, chunker} <- init_chunker(opts) do
      do_stream(stream, chunker, volume, target, opts)
    end
  end

  @doc """
  Converts the writer's chunk-ref list into the `:total_size` and
  `:locations` options expected by `NeonFS.Core.commit_chunks/4`.

  Preserves chunk order via the hash list so a subsequent
  `commit_chunks(volume, path, hashes, opts)` call reconstructs the
  file in the byte order the writer produced.
  """
  @spec chunk_refs_to_commit_opts([chunk_ref()]) :: %{
          hashes: [binary()],
          locations: %{binary() => [location()]},
          chunk_codecs: %{binary() => codec_info()},
          total_size: non_neg_integer()
        }
  def chunk_refs_to_commit_opts(refs) do
    hashes = Enum.map(refs, & &1.hash)

    locations =
      refs
      |> Enum.group_by(& &1.hash, & &1.locations)
      |> Map.new(fn {hash, location_lists} ->
        {hash, location_lists |> List.flatten() |> Enum.uniq()}
      end)

    # Multiple refs for the same hash can appear when a chunk is
    # replicated to several nodes. Every write of a given hash is
    # deterministic in compression (volume-derived) but random in
    # nonce — any representative suffices since the on-disk codec
    # fingerprint is constant for the hash within one volume.
    chunk_codecs = Map.new(refs, fn ref -> {ref.hash, ref.codec} end)

    total_size = Enum.reduce(refs, 0, fn ref, acc -> acc + ref.size end)

    %{
      hashes: hashes,
      locations: locations,
      chunk_codecs: chunk_codecs,
      total_size: total_size
    }
  end

  ## Internal

  defp fetch_volume(volume_name) do
    case Router.call(NeonFS.Core, :get_volume, [volume_name]) do
      {:ok, %Volume{} = volume} ->
        {:ok, volume}

      {:ok, volume} when is_map(volume) ->
        {:ok, volume}

      {:error, _} = err ->
        err

      other ->
        {:error, Unavailable.exception(message: "Failed to fetch volume: #{inspect(other)}")}
    end
  end

  defp select_target(volume, opts) do
    tier = Keyword.get(opts, :tier, volume.tiering.initial_tier)
    drive_id = Keyword.get(opts, :drive_id, @default_drive_id)
    exclude = Keyword.get(opts, :exclude_nodes, [])

    case Keyword.fetch(opts, :target_node) do
      {:ok, node} ->
        {:ok, %{node: node, drive_id: drive_id, tier: tier}}

      :error ->
        discover_target(tier, drive_id, exclude)
    end
  end

  defp discover_target(tier, drive_id, exclude) do
    case Enum.reject(Discovery.get_core_nodes(), &(&1 in exclude)) do
      [] ->
        {:error, :no_core_nodes_available}

      [node | _] ->
        {:ok, %{node: node, drive_id: drive_id, tier: tier}}
    end
  end

  defp init_chunker(opts) do
    strategy = Keyword.get(opts, :strategy, @default_strategy)
    param = Keyword.get(opts, :strategy_param, 0)
    Native.chunker_init(strategy, param)
  end

  defp do_stream(stream, chunker, volume, target, opts) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    abort_fn = Keyword.get(opts, :abort_fn, &default_abort/2)
    initial = %{refs: [], written: []}

    feed_result =
      Enum.reduce_while(stream, {:ok, initial}, fn segment, {:ok, acc} ->
        feed_segment(segment, chunker, volume, target, timeout, acc)
      end)

    with {:ok, acc} <- feed_result,
         tail = Native.chunker_finish(chunker),
         {:ok, acc} <- process_emitted(tail, volume, target, timeout, acc) do
      {:ok, Enum.reverse(acc.refs)}
    else
      {:error, reason, acc} ->
        abort_written(acc.written, abort_fn)
        {:error, reason}
    end
  end

  defp feed_segment(segment, chunker, volume, target, timeout, acc) do
    case Native.chunker_feed(chunker, segment) do
      [] -> {:cont, {:ok, acc}}
      emitted -> continue_with_emitted(emitted, volume, target, timeout, acc)
    end
  end

  defp continue_with_emitted(emitted, volume, target, timeout, acc) do
    case process_emitted(emitted, volume, target, timeout, acc) do
      {:ok, acc} -> {:cont, {:ok, acc}}
      {:error, reason, acc} -> {:halt, {:error, reason, acc}}
    end
  end

  defp process_emitted([], _volume, _target, _timeout, acc), do: {:ok, acc}

  defp process_emitted([{data, hash, _offset, size} | rest], volume, target, timeout, acc) do
    case put_chunk(data, hash, volume, target, timeout) do
      {:ok, codec_info, locations} ->
        ref = %{
          hash: hash,
          locations: locations,
          size: size,
          codec: codec_info
        }

        updated = %{
          refs: [ref | acc.refs],
          written: [{hash, locations} | acc.written]
        }

        process_emitted(rest, volume, target, timeout, updated)

      {:error, reason} ->
        {:error, {:put_chunk_failed, reason}, acc}
    end
  end

  defp put_chunk(data, hash, volume, target, timeout) do
    args = [
      hash: hash,
      volume_id: target.drive_id,
      processing_volume_id: volume.id,
      write_id: nil,
      tier: tier_to_string(target.tier),
      data: data
    ]

    case Router.data_call(target.node, :put_chunk, args, timeout: timeout) do
      {:ok, %{compression: _, crypto: _, original_size: _} = codec_info} ->
        {codec, locations} = extract_codec_and_locations(codec_info, target)
        {:ok, codec, locations}

      :ok ->
        # Handler on older core nodes doesn't send codec info back.
        # Fall back to the pre-#481 assumption (compression=:none,
        # crypto=nil) — still wrong for compressed/encrypted volumes
        # but at least forward-compatible once every node is
        # upgraded. `original_size` equals the plaintext length we
        # just sent; there's no codec on the legacy path. Same
        # forward-compat applies to `:locations` (absent from the
        # pre-#478 response), which falls back to the single primary
        # target the writer picked.
        {:ok, %{compression: :none, crypto: nil, original_size: byte_size(data)}, [target]}

      {:error, _} = err ->
        err
    end
  end

  # Extracts replica locations from the codec_info reply, falling back
  # to the writer's primary `target` when the handler didn't include a
  # `:locations` key (pre-#478 core nodes). The `:locations` key is
  # stripped from the codec_info before it's handed back so the codec
  # struct matches the shape downstream `ChunkMeta` builders expect.
  defp extract_codec_and_locations(codec_info, target) do
    case Map.pop(codec_info, :locations) do
      {nil, codec} -> {codec, [target]}
      {[], codec} -> {codec, [target]}
      {locations, codec} when is_list(locations) -> {codec, locations}
    end
  end

  defp abort_written([], _abort_fn), do: :ok

  defp abort_written(written, abort_fn) do
    Enum.each(written, fn {hash, locations} ->
      Enum.each(locations, fn location ->
        try do
          abort_fn.(location, hash)
        rescue
          error ->
            Logger.debug("Abort callback raised", reason: inspect(error))
            :ok
        catch
          :exit, reason ->
            Logger.debug("Abort callback exited", reason: inspect(reason))
            :ok
        end
      end)
    end)

    :ok
  end

  defp default_abort(target, hash) do
    :rpc.call(
      target.node,
      NeonFS.Core.BlobStore,
      :delete_chunk,
      [hash, target.drive_id, []],
      5_000
    )
  end

  defp tier_to_string(tier) when is_atom(tier), do: Atom.to_string(tier)
  defp tier_to_string(tier) when is_binary(tier), do: tier
end
