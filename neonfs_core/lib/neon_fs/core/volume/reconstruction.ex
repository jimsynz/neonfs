defmodule NeonFS.Core.Volume.Reconstruction do
  @moduledoc """
  Pure-logic core of the bootstrap-layer reconstruction (#838 /
  part of #788).

  Given:

  - The list of drive paths an operator wants to scan (one entry
    per drive on the local node).
  - A function that lists candidate chunk hashes on a drive.
  - A function that reads a chunk's bytes by hash.
  - The expected `cluster_id` (refuses to incorporate root segments
    from a different cluster).

  Walks each drive's identity file (`Drive.Identity`, #778), scans
  candidate chunks, decodes each one, and keeps those that parse
  as a `Volume.RootSegment` (#780). The output is the `:register_drive`
  + `:register_volume_root` Ra commands an operator's CLI (#839)
  would submit to rebuild the bootstrap layer (#779) state.

  All filesystem / blob I/O is injectable via opts so this module
  is unit-testable against in-memory fixtures without spinning up
  a real cluster.

  When two root-segment chunks are found for the same `{volume_id, shard}`,
  the one with the highest HLC wins — copy-on-write means newer
  writes produce newer chunks; the older versions linger as
  unreferenced bytes until GC picks them up. Reconstructing from
  partial GC means the operator may see both, and the HLC tiebreak
  is the correct call.

  ## Sharded roots (#1313)

  A volume's metadata root is split into `Shard.count/0` shards, each its
  own bootstrap pointer. A segment records the shard it belongs to
  (`RootSegment.shard`), stamped on the first write that diverges the
  shard from the shared empty root. So reconstruction recovers per-shard
  identity directly from disk:

  - a `shard: n` segment restores shard `n` (HLC-tiebroken per shard);
  - the provision-time `shard: nil` empty chunk — content-addressed-shared
    by every shard that has never been written — fills every shard not
    otherwise recovered.

  An empty shard needs no individual identity (it's correctly served by
  the shared empty chunk), so only diverged shards carry one.
  """

  alias NeonFS.Core.Drive.Identity
  alias NeonFS.Core.HLC
  alias NeonFS.Core.Volume.{RootSegment, Shard}

  @type drive_path :: String.t()
  @type chunk_hash :: binary()
  @type warning :: {atom(), String.t(), map()}
  @type shard :: non_neg_integer() | nil
  @type discovered_root :: %{segment: RootSegment.t(), hash: chunk_hash(), drives: [Identity.t()]}

  @type result :: %{
          drives: [Identity.t()],
          volumes: %{optional(binary()) => %{optional(shard()) => discovered_root()}},
          commands: [tuple()],
          warnings: [warning()]
        }

  @doc """
  Walk `drive_paths` and produce the Ra commands that rebuild the
  bootstrap layer.

  Required opts:

  - `:expected_cluster_id` — drives + segments whose `cluster_id`
    doesn't match are skipped with a warning.
  - `:node` — the local node identity (used for `register_drive`
    entries and for picking local replica drive_locations on
    each `register_volume_root`).

  Optional opts:

  - `:dry_run?` — populate the result struct without submitting
    any commands. The `:commands` list is built either way so
    the CLI's preview output matches what would be submitted; the
    submission gate lives in the handler (`submit_commands/2`).
  - `:chunk_lister` — `(drive_path -> [chunk_hash])`. Default
    raises with a clear error so production callers must supply
    one (the CLI / orchestrator in #839 wires up the real walker).
  - `:chunk_reader` — `(drive_path, chunk_hash -> {:ok, bytes} |
    {:error, _})`. Same default behaviour as above.
  - `:identity_reader` — `(drive_path -> {:ok, Identity.t()} |
    {:error, _})`. Defaults to `NeonFS.Core.Drive.Identity.read/1`.
  - `:shards` — the shard indices to fill from the shared empty chunk
    when a volume has un-diverged shards. Defaults to `Shard.all/0` (the
    recovering node's configured count). Injectable for tests.
  """
  @spec reconstruct([drive_path()], keyword()) :: result()
  def reconstruct(drive_paths, opts) when is_list(drive_paths) do
    expected_cluster_id = Keyword.fetch!(opts, :expected_cluster_id)
    target_node = Keyword.fetch!(opts, :node)
    all_shards = Keyword.get(opts, :shards, Shard.all())
    # `:dry_run?` is accepted for API stability but no longer changes
    # what the algorithm returns — the handler gates submission, not
    # this function. See #855.
    _ = Keyword.get(opts, :dry_run?, false)
    identity_reader = Keyword.get(opts, :identity_reader, &Identity.read/1)
    chunk_lister = Keyword.get(opts, :chunk_lister, &default_chunk_lister/1)
    chunk_reader = Keyword.get(opts, :chunk_reader, &default_chunk_reader/2)

    {drives, drive_warnings} =
      scan_drives(drive_paths, identity_reader, expected_cluster_id)

    drive_paths_by_id = build_path_index(drives, drive_paths, identity_reader)

    {volumes, segment_warnings} =
      scan_root_segments(
        drives,
        drive_paths_by_id,
        chunk_lister,
        chunk_reader,
        expected_cluster_id
      )

    # Build commands regardless of `:dry_run?` so the CLI's preview
    # output reports `commands == drives + volumes` per the runbook
    # contract. The handler's `submit_commands/2` is what actually
    # gates submission against the dry-run flag (#855).
    commands = build_commands(drives, volumes, target_node, all_shards)

    %{
      drives: drives,
      volumes: volumes,
      commands: commands,
      warnings: drive_warnings ++ segment_warnings
    }
  end

  ## Internals

  defp scan_drives(drive_paths, identity_reader, expected_cluster_id) do
    Enum.reduce(drive_paths, {[], []}, fn path, {drives, warnings} ->
      case identity_reader.(path) do
        {:ok, %Identity{cluster_id: ^expected_cluster_id} = identity} ->
          {[identity | drives], warnings}

        {:ok, %Identity{cluster_id: actual}} ->
          warning =
            {:foreign_cluster,
             "drive at #{path} belongs to cluster #{inspect(actual)}, expected " <>
               inspect(expected_cluster_id), %{path: path}}

          {drives, [warning | warnings]}

        {:error, reason} ->
          warning =
            {:identity_unreadable, "could not read drive identity at #{path}: #{inspect(reason)}",
             %{path: path}}

          {drives, [warning | warnings]}
      end
    end)
    |> then(fn {ds, ws} -> {Enum.reverse(ds), Enum.reverse(ws)} end)
  end

  # Build a `drive_id -> drive_path` lookup so we can pair a drive
  # entry with the path it lives at when we need to read chunks
  # back out of it.
  defp build_path_index(drives, drive_paths, identity_reader) do
    Enum.reduce(drive_paths, %{}, fn path, acc ->
      with {:ok, %Identity{drive_id: id}} <- identity_reader.(path),
           true <- Enum.any?(drives, &(&1.drive_id == id)) do
        Map.put(acc, id, path)
      else
        _ -> acc
      end
    end)
  end

  defp scan_root_segments(drives, drive_paths_by_id, chunk_lister, chunk_reader, expected) do
    {discovered, warnings} =
      Enum.reduce(drives, {%{}, []}, fn drive, acc ->
        path = Map.fetch!(drive_paths_by_id, drive.drive_id)
        scan_drive_chunks(drive, path, chunk_lister.(path), chunk_reader, expected, acc)
      end)

    {discovered, Enum.reverse(warnings)}
  end

  defp scan_drive_chunks(drive, path, hashes, chunk_reader, expected, acc) do
    Enum.reduce(hashes, acc, fn hash, {vols, warns} ->
      apply_candidate_result(
        scan_candidate(drive, path, hash, chunk_reader, expected),
        hash,
        {vols, warns}
      )
    end)
  end

  defp apply_candidate_result(:skip, _hash, acc), do: acc

  defp apply_candidate_result({:ok, segment, drive}, hash, {vols, warns}),
    do: {merge_segment(vols, segment, drive, hash), warns}

  defp apply_candidate_result({:warning, warning}, _hash, {vols, warns}),
    do: {vols, [warning | warns]}

  defp scan_candidate(drive, path, hash, chunk_reader, expected) do
    case chunk_reader.(path, hash) do
      {:ok, bytes} ->
        case RootSegment.decode(bytes) do
          {:ok, %RootSegment{cluster_id: ^expected} = segment} ->
            {:ok, segment, drive}

          {:ok, %RootSegment{cluster_id: actual}} ->
            {:warning,
             {:foreign_segment,
              "root segment chunk #{Base.encode16(hash, case: :lower)} on drive " <>
                "#{drive.drive_id} has cluster_id #{inspect(actual)}, expected " <>
                inspect(expected), %{drive_id: drive.drive_id, hash: hash}}}

          {:error, _} ->
            # Not a root segment chunk — most chunks won't be.
            :skip
        end

      {:error, reason} ->
        {:warning,
         {:chunk_unreadable,
          "could not read chunk #{Base.encode16(hash, case: :lower)} on " <>
            "drive #{drive.drive_id}: #{inspect(reason)}",
          %{drive_id: drive.drive_id, hash: hash, reason: reason}}}
    end
  end

  # Discovered roots are keyed `volume_id => %{shard => root}`, where
  # `shard` is the segment's recorded shard (`nil` for the shared empty
  # chunk). Each `{volume_id, shard}` HLC-tiebreaks independently.
  defp merge_segment(volumes, segment, drive, hash) do
    fresh = %{segment: segment, hash: hash, drives: [drive]}

    Map.update(volumes, segment.volume_id, %{segment.shard => fresh}, fn shard_map ->
      Map.update(shard_map, segment.shard, fresh, &merge_existing(&1, segment, drive, hash))
    end)
  end

  defp merge_existing(current, segment, drive, hash) do
    case compare_hlc(current.segment.hlc, segment.hlc) do
      :lt -> %{segment: segment, hash: hash, drives: [drive]}
      :gt -> current
      :eq -> add_replica(current, drive, hash)
    end
  end

  # Same HLC + same root chunk hash on a different drive → record
  # the drive as a replica of the same root.
  defp add_replica(current, drive, hash) do
    if hash == current.hash and drive not in current.drives do
      %{current | drives: [drive | current.drives]}
    else
      current
    end
  end

  defp compare_hlc(%HLC{} = a, %HLC{} = b) do
    cond do
      a.last_wall < b.last_wall -> :lt
      a.last_wall > b.last_wall -> :gt
      a.last_counter < b.last_counter -> :lt
      a.last_counter > b.last_counter -> :gt
      true -> :eq
    end
  end

  defp build_commands(drives, volumes, target_node, all_shards) do
    drive_commands =
      Enum.map(drives, fn drive ->
        {:register_drive,
         %{
           drive_id: drive.drive_id,
           node: target_node,
           cluster_id: drive.cluster_id,
           on_disk_format_version: drive.on_disk_format_version,
           registered_at: drive.created_at
         }}
      end)

    volume_commands =
      Enum.flat_map(volumes, fn {volume_id, shard_map} ->
        volume_shard_commands(volume_id, shard_map, target_node, all_shards)
      end)

    drive_commands ++ volume_commands
  end

  # A `shard: n` segment restores shard `n`; the `shard: nil` shared empty
  # chunk fills every shard that didn't diverge (#1313).
  defp volume_shard_commands(volume_id, shard_map, target_node, all_shards) do
    {empty_root, diverged} = Map.pop(shard_map, nil)

    diverged_commands =
      Enum.map(diverged, fn {shard, root} ->
        register_root_command(volume_id, shard, root, target_node)
      end)

    empty_commands =
      case empty_root do
        nil ->
          []

        root ->
          (all_shards -- Map.keys(diverged))
          |> Enum.map(&register_root_command(volume_id, &1, root, target_node))
      end

    diverged_commands ++ empty_commands
  end

  defp register_root_command(volume_id, shard, root, target_node) do
    %{segment: segment, hash: hash, drives: replica_drives} = root

    drive_locations =
      Enum.map(replica_drives, fn drive -> %{node: target_node, drive_id: drive.drive_id} end)

    {:register_volume_root, volume_id, shard,
     %{
       volume_id: volume_id,
       root_chunk_hash: hash,
       drive_locations: drive_locations,
       durability_cache: segment.durability,
       updated_at: DateTime.utc_now()
     }}
  end

  defp default_chunk_lister(_drive_path) do
    raise ArgumentError,
          "Reconstruction.reconstruct/2 needs a `:chunk_lister` opt; " <>
            "the production walker lives in #839 (CLI orchestrator)"
  end

  defp default_chunk_reader(_drive_path, _hash) do
    raise ArgumentError,
          "Reconstruction.reconstruct/2 needs a `:chunk_reader` opt; " <>
            "the production reader lives in #839 (CLI orchestrator)"
  end
end
