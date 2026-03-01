defmodule NeonFS.Core.Job.Runners.Scrub do
  @moduledoc """
  Job runner for integrity verification (scrubbing).

  Walks the local `ChunkIndex`, reads each chunk from `BlobStore`, and verifies
  its SHA-256 hash matches the stored hash. Processes chunks in batches,
  making it resumable after restart.

  ## Params

  - `:volume_id` (optional) — restrict scrubbing to a single volume's chunks

  ## Batch resumption

  Each verified chunk gets its `last_verified` timestamp updated. On resume,
  chunks already verified after the job's start time are skipped, so no work
  is repeated.

  ## Encrypted chunks

  Encrypted chunks are decrypted using the volume's encryption key from
  `KeyManager` before hash verification. The chunk's stored `key_version`
  determines which key version to use. If the key is unavailable, the chunk
  is reported as `:key_unavailable` (not corruption).

  Each node scrubs only its own local replicas.
  """

  @behaviour NeonFS.Core.Job.Runner

  require Logger

  alias NeonFS.Core.{BlobStore, ChunkIndex, FileIndex, KeyManager}
  alias NeonFS.IO.{Operation, Scheduler}

  @default_batch_size 500
  @max_corrupted 1000

  @impl NeonFS.Core.Job.Runner
  def label, do: "scrub"

  @impl NeonFS.Core.Job.Runner
  def step(job) do
    started_at = job.state[:started_at] || DateTime.utc_now()
    job = %{job | state: Map.put_new(job.state, :started_at, started_at)}

    skipped_hashes = job.state[:skipped_hashes] || MapSet.new()
    remaining = chunks_needing_scrub(job.params, started_at, skipped_hashes)
    volume_map = job.state[:volume_map] || build_volume_map(job.params, remaining)
    job = %{job | state: Map.put_new(job.state, :volume_map, volume_map)}

    scrub_remaining(job, remaining, volume_map)
  end

  # Private

  defp scrub_remaining(job, [], _volume_map) do
    finish(job)
  end

  defp scrub_remaining(job, remaining, volume_map) do
    batch = Enum.take(remaining, batch_size())

    {new_corrupted, batch_encrypted, batch_key_unavail, batch_skipped} =
      verify_batch(batch, volume_map)

    existing_corrupted = job.state[:corrupted] || []
    all_corrupted = Enum.take(existing_corrupted ++ new_corrupted, @max_corrupted)

    existing_skipped = job.state[:skipped_hashes] || MapSet.new()
    all_skipped = Enum.reduce(batch_skipped, existing_skipped, &MapSet.put(&2, &1))

    encrypted_verified = (job.state[:encrypted_verified] || 0) + batch_encrypted
    key_unavailable_count = (job.state[:key_unavailable_count] || 0) + batch_key_unavail

    completed = job.progress.completed + length(batch)
    total = max(job.progress.total, completed + length(remaining) - length(batch))

    updated = %{
      job
      | progress: %{
          total: total,
          completed: completed,
          description: "Verifying chunks"
        },
        state:
          Map.merge(job.state, %{
            corrupted: all_corrupted,
            corruption_count: length(all_corrupted),
            encrypted_verified: encrypted_verified,
            key_unavailable_count: key_unavailable_count,
            skipped_hashes: all_skipped
          })
    }

    {:continue, updated}
  end

  defp chunks_needing_scrub(params, started_at, skipped_hashes) do
    list_chunks(params)
    |> filter_local()
    |> Enum.filter(fn chunk ->
      chunk.commit_state == :committed and
        not MapSet.member?(skipped_hashes, chunk.hash) and
        (chunk.last_verified == nil or DateTime.compare(chunk.last_verified, started_at) == :lt)
    end)
  end

  defp list_chunks(%{volume_id: volume_id}), do: ChunkIndex.get_chunks_for_volume(volume_id)
  defp list_chunks(_), do: ChunkIndex.list_all()

  defp filter_local(chunks) do
    Enum.filter(chunks, fn chunk ->
      Enum.any?(chunk.locations, &(&1.node == node()))
    end)
  end

  defp verify_batch(chunks, volume_map) do
    init = {_corrupted = [], _encrypted = 0, _key_unavail = 0, _skipped = []}

    Enum.reduce(chunks, init, fn chunk, {corrupted, encrypted, key_unavail, skipped} ->
      volume_id = Map.get(volume_map, chunk.hash)
      encrypted_inc = if(chunk.crypto, do: 1, else: 0)

      case verify_chunk(chunk, volume_id) do
        :ok ->
          mark_verified(chunk)
          {corrupted, encrypted + encrypted_inc, key_unavail, skipped}

        :corrupt ->
          mark_verified(chunk)
          emit_corruption_telemetry(chunk)
          {corrupted ++ [chunk.hash], encrypted + encrypted_inc, key_unavail, skipped}

        :key_unavailable ->
          emit_key_unavailable_telemetry(chunk, volume_id)
          {corrupted, encrypted, key_unavail + 1, [chunk.hash | skipped]}
      end
    end)
  end

  defp verify_chunk(%{crypto: %{key_version: key_version, nonce: nonce}} = chunk, volume_id)
       when is_binary(volume_id) do
    case key_manager_mod().get_volume_key(volume_id, key_version) do
      {:ok, key} -> read_and_verify(chunk, volume_id, key: key, nonce: nonce)
      {:error, _reason} -> :key_unavailable
    end
  end

  defp verify_chunk(%{crypto: crypto} = _chunk, _volume_id) when crypto != nil do
    # Encrypted chunk but no volume_id mapping found
    :key_unavailable
  end

  defp verify_chunk(chunk, volume_id) do
    read_and_verify(chunk, volume_id || "_scrub", [])
  end

  defp read_and_verify(chunk, volume_id, extra_opts) do
    local_loc = Enum.find(chunk.locations, &(&1.node == node()))
    tier = Atom.to_string(local_loc.tier)
    decompress = chunk.compression != :none
    opts = [verify: true, decompress: decompress, tier: tier] ++ extra_opts

    op =
      Operation.new(
        priority: :scrub,
        volume_id: volume_id,
        drive_id: local_loc.drive_id,
        type: :read,
        callback: fn -> BlobStore.read_chunk(chunk.hash, local_loc.drive_id, opts) end
      )

    case Scheduler.submit_sync(op) do
      {:ok, _data} -> :ok
      {:error, _reason} -> :corrupt
    end
  end

  defp build_volume_map(%{volume_id: volume_id}, chunks) do
    Map.new(chunks, fn chunk -> {chunk.hash, volume_id} end)
  end

  defp build_volume_map(_params, chunks) do
    encrypted_hashes =
      chunks
      |> Enum.filter(& &1.crypto)
      |> MapSet.new(& &1.hash)

    if MapSet.size(encrypted_hashes) == 0 do
      %{}
    else
      build_volume_map_from_file_index(encrypted_hashes)
    end
  end

  defp build_volume_map_from_file_index(encrypted_hashes) do
    FileIndex.list_all()
    |> Enum.flat_map(fn file ->
      file.chunks
      |> Enum.filter(&MapSet.member?(encrypted_hashes, &1))
      |> Enum.map(&{&1, file.volume_id})
    end)
    |> Map.new()
  end

  defp mark_verified(chunk) do
    ChunkIndex.put(%{chunk | last_verified: DateTime.utc_now()})
  end

  defp emit_corruption_telemetry(chunk) do
    :telemetry.execute(
      [:neonfs, :scrub, :corruption_detected],
      %{},
      %{hash: chunk.hash, node: node()}
    )

    Logger.warning(
      "Scrub: corruption detected in chunk #{Base.encode16(chunk.hash, case: :lower)}"
    )
  end

  defp emit_key_unavailable_telemetry(chunk, volume_id) do
    :telemetry.execute(
      [:neonfs, :scrub, :key_unavailable],
      %{},
      %{hash: chunk.hash, volume_id: volume_id, node: node()}
    )

    Logger.warning(
      "Scrub: key unavailable for encrypted chunk #{Base.encode16(chunk.hash, case: :lower)}"
    )
  end

  defp finish(job) do
    corruption_count = length(job.state[:corrupted] || [])
    encrypted_verified = job.state[:encrypted_verified] || 0
    key_unavailable_count = job.state[:key_unavailable_count] || 0
    total = job.progress.completed

    :telemetry.execute(
      [:neonfs, :scrub, :complete],
      %{
        total: total,
        corrupted: corruption_count,
        encrypted_verified: encrypted_verified,
        key_unavailable: key_unavailable_count
      },
      %{}
    )

    Logger.info(
      "Scrub complete: #{total} chunks verified, #{corruption_count} corrupted, " <>
        "#{encrypted_verified} encrypted, #{key_unavailable_count} key unavailable"
    )

    {:complete, %{job | progress: %{job.progress | description: "Complete"}}}
  end

  defp batch_size do
    Application.get_env(:neonfs_core, :scrub_batch_size, @default_batch_size)
  end

  defp key_manager_mod do
    Application.get_env(:neonfs_core, :key_manager_mod, KeyManager)
  end
end
