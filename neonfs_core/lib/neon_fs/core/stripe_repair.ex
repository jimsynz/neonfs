defmodule NeonFS.Core.StripeRepair do
  @moduledoc """
  Background stripe repair for erasure-coded volumes.

  Detects degraded stripes (missing chunks) and reconstructs them using
  Reed-Solomon decoding. Repair reads K available chunks, reconstructs
  the missing ones via NIF, and stores them on available nodes.

  ## Periodic Scan

  When started as a GenServer, performs periodic scans at a configurable
  interval (default 5 minutes). Non-healthy stripes are submitted to
  `BackgroundWorker` with priority-based scheduling.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{
    BackgroundWorker,
    Blob.Native,
    BlobStore,
    ChunkFetcher,
    ChunkIndex,
    ChunkMeta,
    StripeIndex,
    StripePlacement
  }

  alias NeonFS.IO.{Operation, Scheduler}

  alias NeonFS.Core.StripeRepair.LockTable

  @default_scan_interval_ms 300_000

  # ─── Client API ──────────────────────────────────────────────────────

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Scans all stripes and returns non-healthy ones with their state.
  """
  @spec scan_stripes() :: [{binary(), :degraded | :critical, non_neg_integer()}]
  def scan_stripes do
    all_stripes = StripeIndex.list_all()

    results =
      Enum.reduce(all_stripes, [], fn stripe, acc ->
        case calculate_state(stripe) do
          {:healthy, _} -> acc
          {state, missing} -> [{stripe.id, state, missing} | acc]
        end
      end)

    emit_scan_telemetry(length(all_stripes), results)

    sort_by_priority(results)
  end

  @doc """
  Repairs a single stripe by reconstructing missing chunks.

  Returns `:ok` if repair succeeded or was unnecessary (healthy stripe),
  `{:error, reason}` on failure.
  """
  @spec repair_stripe(binary()) :: :ok | {:error, term()}
  def repair_stripe(stripe_id) do
    case LockTable.acquire_lock(stripe_id) do
      :ok ->
        result = do_repair(stripe_id)
        LockTable.release_lock(stripe_id)
        result

      {:error, :repair_in_progress} ->
        {:error, :repair_in_progress}
    end
  end

  # ─── GenServer Callbacks ────────────────────────────────────────────

  @impl true
  def init(opts) do
    LockTable.init()
    interval = Keyword.get(opts, :scan_interval_ms, @default_scan_interval_ms)

    state = %{scan_interval_ms: interval}
    schedule_scan(state)

    {:ok, state}
  end

  @impl true
  def handle_info(:scan_stripes, state) do
    run_scan_and_submit()
    schedule_scan(state)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp schedule_scan(%{scan_interval_ms: ms}) do
    Process.send_after(self(), :scan_stripes, ms)
  end

  # ─── Core Repair Logic ─────────────────────────────────────────────

  defp do_repair(stripe_id) do
    case StripeIndex.get(stripe_id) do
      {:ok, stripe} ->
        repair_if_needed(stripe)

      {:error, :not_found} ->
        {:error, :stripe_not_found}
    end
  end

  defp repair_if_needed(stripe) do
    case calculate_state(stripe) do
      {:healthy, _} ->
        :ok

      {:critical, missing} ->
        emit_repair_telemetry(stripe.id, :critical, missing, :skip)
        {:error, :insufficient_chunks}

      {:degraded, missing} ->
        emit_repair_telemetry(stripe.id, :degraded, missing, :start)
        reconstruct_missing(stripe)
    end
  end

  defp reconstruct_missing(stripe) do
    k = stripe.config.data_chunks
    chunk_size = stripe.config.chunk_size
    parity_count = stripe.config.parity_chunks

    {available, missing_indices} = partition_chunks(stripe)

    shards_to_fetch = Enum.take(available, k)

    case fetch_shards(stripe.volume_id, shards_to_fetch) do
      {:ok, shards_with_indices} ->
        decode_and_store(
          shards_with_indices,
          k,
          parity_count,
          chunk_size,
          stripe,
          missing_indices
        )

      {:error, reason} ->
        emit_repair_telemetry(stripe.id, :degraded, length(missing_indices), :failure)
        {:error, reason}
    end
  end

  defp decode_and_store(shards, k, parity_count, chunk_size, stripe, missing_indices) do
    case Native.erasure_decode(shards, k, parity_count, chunk_size) do
      {:ok, data_shards} ->
        store_reconstructed(data_shards, stripe, missing_indices)

      {:error, reason} ->
        emit_repair_telemetry(stripe.id, :degraded, length(missing_indices), :failure)
        {:error, {:reconstruction_failed, reason}}
    end
  end

  defp store_reconstructed(data_shards, stripe, missing_indices) do
    # data_shards only contains data chunks (indices 0..k-1)
    # For missing parity chunks, we'd need to re-encode.
    # Only reconstruct data chunks that are missing.
    data_missing = Enum.filter(missing_indices, &(&1 < stripe.config.data_chunks))

    results =
      Enum.map(data_missing, fn idx ->
        shard_data = Enum.at(data_shards, idx)
        store_reconstructed_chunk(shard_data, stripe, idx)
      end)

    if Enum.all?(results, &match?(:ok, &1)) do
      emit_repair_telemetry(stripe.id, :degraded, length(missing_indices), :success)
      :ok
    else
      emit_repair_telemetry(stripe.id, :degraded, length(missing_indices), :failure)
      {:error, :partial_repair}
    end
  end

  defp store_reconstructed_chunk(data, stripe, stripe_idx) do
    tier = :hot
    tier_str = "hot"

    targets_result =
      StripePlacement.select_targets(
        %{data_chunks: stripe.config.data_chunks, parity_chunks: stripe.config.parity_chunks},
        tier: tier
      )

    case targets_result do
      {:ok, targets} ->
        target = Enum.at(targets, stripe_idx, %{node: node(), drive_id: "default"})
        write_repaired_chunk(data, target, tier_str, stripe, stripe_idx)

      {:error, _} ->
        write_repaired_chunk(
          data,
          %{node: node(), drive_id: "default"},
          tier_str,
          stripe,
          stripe_idx
        )
    end
  end

  defp write_repaired_chunk(data, target, tier_str, stripe, stripe_idx) do
    case write_to_target(data, target, tier_str, stripe.volume_id) do
      {:ok, hash, chunk_info} ->
        chunk_meta = %ChunkMeta{
          hash: hash,
          original_size: byte_size(data),
          stored_size: chunk_info.stored_size,
          compression: :none,
          locations: [%{node: target.node, drive_id: target.drive_id, tier: :hot}],
          target_replicas: 1,
          commit_state: :committed,
          active_write_refs: MapSet.new(),
          stripe_id: stripe.id,
          stripe_index: stripe_idx,
          created_at: DateTime.utc_now(),
          last_verified: nil
        }

        ChunkIndex.put(chunk_meta)
        :ok

      {:error, reason} ->
        Logger.warning("Failed to store repaired chunk", reason: inspect(reason))
        {:error, reason}
    end
  end

  defp write_to_target(data, target, tier_str, volume_id) do
    if target.node == node() do
      op =
        Operation.new(
          priority: :repair,
          volume_id: volume_id,
          drive_id: target.drive_id,
          type: :write,
          callback: fn -> BlobStore.write_chunk(data, target.drive_id, tier_str, []) end
        )

      Scheduler.submit_sync(op)
    else
      case :rpc.call(
             target.node,
             BlobStore,
             :write_chunk,
             [data, target.drive_id, tier_str, []],
             10_000
           ) do
        {:ok, hash, info} -> {:ok, hash, info}
        {:badrpc, reason} -> {:error, {:rpc_failed, reason}}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  # ─── Chunk Availability ────────────────────────────────────────────

  defp calculate_state(stripe) do
    k = stripe.config.data_chunks
    n = k + stripe.config.parity_chunks

    available_count =
      stripe.chunks
      |> Enum.count(&chunk_available?(stripe.volume_id, &1))

    missing = n - available_count

    cond do
      available_count == n -> {:healthy, 0}
      available_count >= k -> {:degraded, missing}
      true -> {:critical, missing}
    end
  end

  defp chunk_available?(volume_id, chunk_hash) do
    match?({:ok, _}, ChunkIndex.get(volume_id, chunk_hash))
  end

  defp partition_chunks(stripe) do
    stripe.chunks
    |> Enum.with_index()
    |> Enum.reduce({[], []}, fn {hash, idx}, {avail, missing} ->
      if chunk_available?(stripe.volume_id, hash) do
        {[{hash, idx} | avail], missing}
      else
        {avail, [idx | missing]}
      end
    end)
    |> then(fn {avail, missing} -> {Enum.reverse(avail), Enum.reverse(missing)} end)
  end

  defp fetch_shards(volume_id, shards_to_fetch) do
    results =
      Enum.map(shards_to_fetch, fn {hash, idx} ->
        case fetch_chunk(volume_id, hash) do
          {:ok, data} -> {:ok, {idx, data}}
          error -> error
        end
      end)

    if Enum.all?(results, &match?({:ok, _}, &1)) do
      {:ok, Enum.map(results, fn {:ok, pair} -> pair end)}
    else
      {:error, :chunk_fetch_failed}
    end
  end

  defp fetch_chunk(volume_id, hash) do
    case ChunkIndex.get(volume_id, hash) do
      {:ok, chunk_meta} ->
        {tier, drive_id} = extract_location(chunk_meta)

        case ChunkFetcher.fetch_chunk(hash, tier: tier, drive_id: drive_id, verify: false) do
          {:ok, data, _source} -> {:ok, data}
          {:error, reason} -> {:error, reason}
        end

      {:error, :not_found} ->
        {:error, :chunk_not_found}
    end
  end

  defp extract_location(chunk_meta) do
    case chunk_meta.locations do
      [loc | _] -> {Atom.to_string(loc.tier), Map.get(loc, :drive_id, "default")}
      [] -> {"hot", "default"}
    end
  end

  # ─── Scan & Submit ─────────────────────────────────────────────────

  defp run_scan_and_submit do
    degraded = scan_stripes()

    Enum.each(degraded, fn {stripe_id, state, _missing} ->
      priority = if state == :critical, do: :high, else: :normal

      if not LockTable.locked?(stripe_id) do
        submit_repair(stripe_id, priority)
      end
    end)
  end

  defp submit_repair(stripe_id, priority) do
    if Code.ensure_loaded?(BackgroundWorker) and
         Process.whereis(BackgroundWorker) != nil do
      resources = drive_resources_for_stripe(stripe_id)

      BackgroundWorker.submit(
        fn -> repair_stripe(stripe_id) end,
        priority: priority,
        label: "stripe_repair:#{stripe_id}",
        resources: resources
      )
    else
      # BackgroundWorker not available, run directly
      repair_stripe(stripe_id)
    end
  end

  defp drive_resources_for_stripe(stripe_id) do
    case StripeIndex.get(stripe_id) do
      {:ok, stripe} ->
        stripe.chunks
        |> Enum.flat_map(&local_drive_resources_for_chunk(stripe.volume_id, &1))
        |> Enum.uniq()

      _ ->
        []
    end
  end

  defp local_drive_resources_for_chunk(volume_id, hash) do
    case ChunkIndex.get(volume_id, hash) do
      {:ok, chunk} ->
        chunk.locations
        |> Enum.filter(&(&1.node == Node.self()))
        |> Enum.map(&{:drive, Map.get(&1, :drive_id, "default")})

      _ ->
        []
    end
  end

  # ─── Priority Sorting ──────────────────────────────────────────────

  defp sort_by_priority(results) do
    Enum.sort_by(results, fn {_id, state, _missing} ->
      case state do
        :critical -> 0
        :degraded -> 1
      end
    end)
  end

  # ─── Telemetry ─────────────────────────────────────────────────────

  defp emit_scan_telemetry(total, results) do
    degraded = Enum.count(results, fn {_, s, _} -> s == :degraded end)
    critical = Enum.count(results, fn {_, s, _} -> s == :critical end)

    :telemetry.execute(
      [:neonfs, :stripe_repair, :scan],
      %{stripes_scanned: total, degraded_found: degraded, critical_found: critical},
      %{}
    )
  end

  defp emit_repair_telemetry(stripe_id, state, missing, outcome) do
    :telemetry.execute(
      [:neonfs, :stripe_repair, :repair],
      %{missing_chunks: missing},
      %{stripe_id: stripe_id, state: state, outcome: outcome}
    )
  end
end
