defmodule NeonFS.Transport.Handler do
  @moduledoc """
  Handles an accepted TLS connection for inbound chunk operations.

  Processes `put_chunk`, `get_chunk`, and `has_chunk` requests from peer nodes,
  dispatching to the local blob store. Uses `{active, N}` for backpressure —
  the BEAM delivers N frames then pauses until re-armed via `:ssl_passive`.

  ## Options

    * `:socket` (required) — accepted TLS socket
    * `:dispatch_module` — module implementing the blob store interface
      (default: `NeonFS.Core.BlobStore`)

  """

  use GenServer, restart: :temporary

  require Logger

  @active_n 10
  @default_dispatch NeonFS.Core.BlobStore

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  # GenServer callbacks

  @impl GenServer
  def init(opts) do
    socket = Keyword.fetch!(opts, :socket)
    dispatch = Keyword.get(opts, :dispatch_module, @default_dispatch)
    {:ok, %{socket: socket, dispatch: dispatch}}
  end

  @impl GenServer
  def handle_info(:activate, state) do
    :ssl.setopts(state.socket, [{:active, @active_n}])
    {:noreply, state}
  end

  def handle_info({:ssl, _socket, data}, state) do
    message = :erlang.binary_to_term(data, [:safe])
    response = dispatch(message, state)
    :ssl.send(state.socket, :erlang.term_to_binary(response))
    {:noreply, state}
  end

  def handle_info({:ssl_passive, socket}, state) do
    :ssl.setopts(socket, [{:active, @active_n}])
    {:noreply, state}
  end

  def handle_info({:ssl_closed, _}, state) do
    {:stop, :normal, state}
  end

  def handle_info({:ssl_error, _, _reason}, state) do
    {:stop, :normal, state}
  end

  # Dispatch functions

  # New 8-tuple shape carries a real `volume_id` alongside `drive_id`.
  # When present, the handler resolves the volume's compression /
  # encryption settings via the dispatch module and applies them
  # before storing the chunk — the interface-side chunking path
  # established by the #408 design note. Frame layout:
  # `{:put_chunk, ref, hash, volume_id, drive_id, write_id, tier, data}`.
  #
  # `resolve_put_chunk_opts/1` may return `{:error, reason}` — for
  # example, an encrypted volume whose current key is unavailable.
  # That failure is surfaced directly to the interface node: storing
  # the chunk as plaintext on an encrypted volume would be a data-at-
  # rest leak, not a recoverable fallback.
  #
  # The reply carries the codec the handler actually used (compression
  # atom + optional `ChunkCrypto`) so `CommitChunks.create_chunk_meta/3`
  # on the receiving core can stamp the matching `ChunkMeta` rather
  # than hard-coding `compression: :none, crypto: nil` (#481).
  #
  # After the local put succeeds the handler also kicks off replication
  # for volumes with `durability.factor > 1` via the dispatch module's
  # optional `replicate_after_put/5` callback (#478). The returned
  # location list — local plus every successful replica — is stamped
  # into the response so the interface-side `ChunkWriter` can record
  # every replica in the eventual `commit_chunks` payload rather than
  # waiting for background reconciliation to patch the locations in.
  defp dispatch(
         {:put_chunk, ref, hash, volume_id, drive_id, _write_id, tier, data},
         state
       )
       when is_binary(volume_id) do
    with {:ok, opts} <- resolve_volume_opts(state.dispatch, volume_id),
         {:ok, _hash, _info} <- state.dispatch.write_chunk(data, drive_id, tier, opts),
         {:ok, locations} <-
           replicate_if_supported(state.dispatch, hash, data, volume_id, drive_id, tier) do
      {:ok, ref, codec_info_from_opts(opts, byte_size(data), locations)}
    else
      {:error, reason} -> {:error, ref, reason}
    end
  end

  # Legacy 7-tuple shape: the "volume_id" field is actually drive_id.
  # Callers that don't know (or shouldn't apply) volume-level
  # processing — replication, internal replay — stay on this path.
  # Stored bytes are written as-is with empty opts.
  defp dispatch({:put_chunk, ref, _hash, drive_id, _write_id, tier, data}, state) do
    case state.dispatch.write_chunk(data, drive_id, tier, []) do
      {:ok, _hash, _info} -> {:ok, ref}
      {:error, reason} -> {:error, ref, reason}
    end
  end

  defp dispatch({:get_chunk, ref, hash, volume_id}, state) do
    dispatch({:get_chunk, ref, hash, volume_id, "hot"}, state)
  end

  defp dispatch({:get_chunk, ref, hash, volume_id, tier}, state) do
    case state.dispatch.read_chunk(hash, volume_id, tier: tier) do
      {:ok, chunk_bytes} -> {:ok, ref, chunk_bytes}
      {:error, _reason} -> {:error, ref, :not_found}
    end
  end

  defp dispatch({:has_chunk, ref, hash}, state) do
    case state.dispatch.chunk_info(hash) do
      {:ok, tier, size} -> {:ok, ref, tier, size}
      {:error, _reason} -> {:error, ref, :not_found}
    end
  end

  defp resolve_volume_opts(dispatch, volume_id) do
    if function_exported?(dispatch, :resolve_put_chunk_opts, 1) do
      case dispatch.resolve_put_chunk_opts(volume_id) do
        {:error, _} = err -> err
        opts when is_list(opts) -> {:ok, opts}
      end
    else
      {:ok, []}
    end
  end

  # Calls the dispatch module's `replicate_after_put/5` to fan out the
  # just-written chunk to the `durability.factor - 1` additional
  # replicas. Dispatches that don't expose this callback (older test
  # mocks, single-responsibility blob-store shims) fall back to the
  # single-location response the pre-#478 path produced.
  defp replicate_if_supported(dispatch, hash, data, volume_id, drive_id, tier) do
    tier_atom = tier_to_atom(tier)
    local_location = %{node: Node.self(), drive_id: drive_id, tier: tier_atom}

    if function_exported?(dispatch, :replicate_after_put, 5) do
      dispatch.replicate_after_put(hash, data, volume_id, local_location, [])
    else
      {:ok, [local_location]}
    end
  end

  defp tier_to_atom(tier) when is_atom(tier), do: tier
  defp tier_to_atom("hot"), do: :hot
  defp tier_to_atom("warm"), do: :warm
  defp tier_to_atom("cold"), do: :cold
  defp tier_to_atom(tier) when is_binary(tier), do: String.to_atom(tier)

  # Builds the codec descriptor returned to the interface node.
  # Mirrors the shape the co-located write path stamps on `ChunkMeta`
  # via `WriteOperation.build_chunk_crypto/2` so the two paths agree.
  # The plaintext `original_size` travels alongside the codec so the
  # receiving `CommitChunks.create_chunk_meta/3` can populate
  # `ChunkMeta.original_size` without trying to reverse-engineer it
  # from `has_chunk`'s on-disk byte count.
  #
  # The `:locations` field carries the full replica location list from
  # `replicate_after_put/5` so `ChunkWriter.chunk_refs_to_commit_opts/1`
  # can emit an accurate multi-entry `:locations` map for the
  # `commit_chunks` payload (#478).
  defp codec_info_from_opts(opts, original_size, locations) do
    %{
      compression: compression_atom(Keyword.get(opts, :compression)),
      crypto: crypto_from_opts(opts),
      original_size: original_size,
      locations: locations
    }
  end

  defp compression_atom("zstd"), do: :zstd
  defp compression_atom(:zstd), do: :zstd
  defp compression_atom(_), do: :none

  defp crypto_from_opts(opts) do
    key = Keyword.get(opts, :key, <<>>)
    nonce = Keyword.get(opts, :nonce, <<>>)

    case {key, nonce} do
      {<<>>, _} ->
        nil

      {_, <<>>} ->
        nil

      {_, nonce} when is_binary(nonce) ->
        %NeonFS.Core.ChunkCrypto{
          algorithm: :aes_256_gcm,
          nonce: nonce,
          key_version: Keyword.fetch!(opts, :key_version)
        }
    end
  end
end
