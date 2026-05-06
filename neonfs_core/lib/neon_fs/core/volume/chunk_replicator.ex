defmodule NeonFS.Core.Volume.ChunkReplicator do
  @moduledoc """
  Writes a chunk to a set of drives concurrently, returning success
  once a quorum of replicas is durable.

  The drive set comes from `NeonFS.Core.Volume.DriveSelector` (#803);
  this module turns that list into actual `BlobStore.write_chunk`
  calls — across distribution if a drive lives on a remote node.

  Concurrency is via `Task.async_stream` so a slow drive doesn't
  serialise writes to the others. The function returns once all
  drives have responded (or timed out), reports per-drive success
  / failure for AntiEntropy to act on, and emits telemetry events
  on partial-quorum and insufficient-quorum outcomes.

  This is the lower-level primitive that `create_volume`'s root
  segment write (#805) and the per-volume metadata write path
  (#785) both build on. It is intentionally durability-agnostic:
  the caller passes `:min_copies` directly so erasure shards (where
  the minimum is `data_chunks` rather than `min_copies`) can use the
  same helper.
  """

  alias NeonFS.Core.BlobStore
  alias NeonFS.Core.MetadataStateMachine

  @type drive_entry :: MetadataStateMachine.drive_entry()
  @type drive_id :: String.t()
  @type chunk_hash :: binary()

  @type write_summary :: %{
          successful: [drive_id()],
          failed: [{drive_id(), term()}]
        }

  @type write_result ::
          {:ok, chunk_hash(), write_summary()}
          | {:error, :insufficient_replicas,
             %{successful: [drive_id()], failed: [{drive_id(), term()}], needed: pos_integer()}}

  @default_timeout_ms 30_000
  @default_tier "hot"

  @doc """
  Writes `data` to every drive in `drives`, concurrently. Returns
  `{:ok, hash, summary}` if at least `:min_copies` drives accepted
  the write, otherwise `{:error, :insufficient_replicas, info}`.

  Required opts:

  - `:min_copies` — quorum threshold.

  Optional opts:

  - `:tier` — `"hot"` (default) | `"warm"` | `"cold"`.
  - `:compression`, `:compression_level` — passed through to
    `BlobStore.write_chunk/4`.
  - `:writer_fn` — an `(data, drive_id, tier, write_opts) -> result`
    callback. Defaults to a thunk that calls
    `NeonFS.Core.BlobStore.write_chunk/4` on the drive's owning node
    (via `:rpc.call` if the drive lives elsewhere). Tests inject a
    fake here.
  - `:timeout` — per-drive timeout in ms (default 30_000).
  """
  @spec write_chunk(binary(), [drive_entry()], keyword()) :: write_result()
  def write_chunk(data, drives, opts) when is_binary(data) and is_list(drives) do
    min_copies = Keyword.fetch!(opts, :min_copies)
    tier = Keyword.get(opts, :tier, @default_tier)
    timeout = Keyword.get(opts, :timeout, @default_timeout_ms)
    write_opts = Keyword.take(opts, [:compression, :compression_level])
    writer_fn = Keyword.get_lazy(opts, :writer_fn, fn -> default_writer_fn(drives) end)

    results =
      drives
      |> Task.async_stream(
        fn drive -> {drive, do_write(writer_fn, drive, data, tier, write_opts)} end,
        timeout: timeout,
        max_concurrency: max(length(drives), 1),
        on_timeout: :kill_task,
        ordered: false
      )
      |> Enum.map(&normalise_stream_result/1)

    {ok_results, err_results} =
      Enum.split_with(results, fn
        {_drive, {:ok, _hash, _info}} -> true
        _ -> false
      end)

    successful = Enum.map(ok_results, fn {drive, _} -> drive.drive_id end)

    failed =
      Enum.map(err_results, fn
        {drive, {:error, reason}} -> {drive.drive_id, reason}
        {nil, {:error, reason}} -> {nil, reason}
      end)

    classify(ok_results, successful, failed, min_copies, length(drives))
  end

  ## Internals

  # `do_write` builds an `:rpc.call` envelope around the writer when
  # the drive lives on a remote node, so the default writer (which is
  # just a wrapper around `BlobStore.write_chunk/4`) doesn't need to
  # know about distribution. Test stubs ignore `drive.node` entirely.
  defp do_write(writer_fn, drive, data, tier, write_opts) do
    target = drive.node

    if target == node() or target == :nonode@nohost do
      writer_fn.(data, drive.drive_id, tier, write_opts)
    else
      case :rpc.call(target, :erlang, :apply, [
             writer_fn,
             [data, drive.drive_id, tier, write_opts]
           ]) do
        {:badrpc, reason} -> {:error, {:badrpc, reason}}
        result -> result
      end
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  defp default_writer_fn(_drives) do
    fn data, drive_id, tier, write_opts ->
      BlobStore.write_chunk(data, drive_id, tier, write_opts)
    end
  end

  defp normalise_stream_result({:ok, {drive, result}}), do: {drive, result}

  defp normalise_stream_result({:exit, reason}),
    do: {nil, {:error, {:task_exit, reason}}}

  defp classify(ok_results, successful, failed, min_copies, total) do
    cond do
      ok_results == [] ->
        emit_insufficient_telemetry(successful, failed, min_copies, total)

        {:error, :insufficient_replicas,
         %{successful: successful, failed: failed, needed: min_copies}}

      length(successful) >= min_copies ->
        {_drive, {:ok, hash, _info}} = hd(ok_results)
        emit_write_telemetry(hash, successful, failed, total)
        {:ok, hash, %{successful: successful, failed: failed}}

      true ->
        {_drive, {:ok, hash, _info}} = hd(ok_results)
        emit_insufficient_telemetry(successful, failed, min_copies, total, hash)

        {:error, :insufficient_replicas,
         %{successful: successful, failed: failed, needed: min_copies}}
    end
  end

  defp emit_write_telemetry(hash, successful, failed, total) do
    :telemetry.execute(
      [:neonfs, :volume, :chunk_replicator, :write],
      %{successful: length(successful), failed: length(failed)},
      %{hash: hash, total: total}
    )
  end

  defp emit_insufficient_telemetry(successful, failed, needed, total, hash \\ nil) do
    :telemetry.execute(
      [:neonfs, :volume, :chunk_replicator, :insufficient_replicas],
      %{successful: length(successful), failed: length(failed)},
      %{hash: hash, needed: needed, total: total}
    )
  end
end
