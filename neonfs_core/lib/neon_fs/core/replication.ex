defmodule NeonFS.Core.Replication do
  @moduledoc """
  Handles chunk replication across cluster nodes.

  Implements replication strategies based on volume configuration:
  - Local acknowledgement: async background replication
  - Quorum acknowledgement: wait for W of N replicas
  - All acknowledgement: wait for all replicas

  Phase 2 implements simple replication. Erasure coding comes in Phase 4.
  """

  alias NeonFS.Client.Router
  alias NeonFS.Core.{BlobStore, ChunkIndex, DriveRegistry}

  require Logger

  @type replication_result :: {:ok, [location()]} | {:error, term()}
  @type location :: %{node: atom(), drive_id: String.t(), tier: atom()}
  @type replication_target :: %{node: atom(), drive_id: String.t()}

  @doc """
  Replicates a chunk to target nodes based on volume durability configuration.

  ## Arguments
    * `chunk_hash` - The hash of the chunk to replicate
    * `chunk_data` - The raw chunk data to replicate
    * `volume` - The volume configuration
    * `opts` - Optional parameters:
      * `:tier` - Target tier for replicas (default: volume.tiering.initial_tier)
      * `:local_drive_id` - The drive already holding the primary copy
        (default: the least-used local drive for `tier`). Excluded from
        replica selection so the extra copies land on distinct drives.
      * `:exclude_drives` - `{node, drive_id}` pairs already holding the
        chunk; never selected for a new replica (default: the local
        drive). Used during repair to skip drives that already have a copy.

  ## Returns
    * `{:ok, locations}` - List of all locations where chunk is stored
    * `{:error, reason}` - Replication failed
  """
  @spec replicate_chunk(binary(), binary(), NeonFS.Core.Volume.t(), keyword()) ::
          replication_result()
  def replicate_chunk(chunk_hash, chunk_data, volume, opts \\ []) do
    start_time = System.monotonic_time()
    tier = Keyword.get(opts, :tier, volume.tiering.initial_tier)
    local_drive_id = Keyword.get(opts, :local_drive_id) || local_drive_id_for_tier(tier)
    exclude_drives = Keyword.get(opts, :exclude_drives, [{Node.self(), local_drive_id}])

    :telemetry.execute(
      [:neonfs, :replication, :start],
      %{bytes: byte_size(chunk_data)},
      %{hash: chunk_hash, volume_id: volume.id}
    )

    # Determine how many replicas we need (minus the local copy already stored)
    target_count = volume.durability.factor - 1

    {:ok, targets} = select_replication_targets(target_count, tier, exclude_drives)

    maybe_emit_under_replicated(chunk_hash, volume, target_count, length(targets))

    result =
      case targets do
        [] ->
          {:ok, [%{node: Node.self(), drive_id: local_drive_id, tier: tier}]}

        _ ->
          perform_replication(chunk_hash, chunk_data, tier, targets, volume, local_drive_id)
      end

    duration = System.monotonic_time() - start_time

    case result do
      {:ok, locations} ->
        :telemetry.execute(
          [:neonfs, :replication, :stop],
          %{duration: duration, replica_count: length(locations)},
          %{hash: chunk_hash, volume_id: volume.id}
        )

      {:error, reason} ->
        :telemetry.execute(
          [:neonfs, :replication, :exception],
          %{duration: duration},
          %{hash: chunk_hash, volume_id: volume.id, error: reason}
        )
    end

    result
  end

  @doc """
  Selects replica drives for a chunk from the active drives across the
  cluster.

  Replication is drive-keyed: a chunk's copies must land on distinct
  drives, which may sit on the same node or on different nodes. Distinct
  failure domains are preferred — nodes already holding a copy (counted
  from `exclude`) are deprioritised, so a node holding zero copies is
  filled before doubling up on the node that holds the primary. A single
  multi-drive node still satisfies a replication factor greater than one
  when no other node has spare capacity.

  `exclude` lists the `{node, drive_id}` pairs that already hold the
  chunk (the local primary copy, plus any existing replicas during
  repair) so they are never selected again.

  ## Arguments
    * `count` - Number of replica drives to select
    * `tier` - Storage tier to draw drives from
    * `exclude` - `{node, drive_id}` pairs to skip

  ## Returns
    * `{:ok, targets}` - Up to `count` replica drives. Fewer than `count`
      means the cluster cannot currently satisfy the requested factor;
      the chunk is under-replicated and left for anti-entropy.
  """
  @spec select_replication_targets(non_neg_integer(), atom(), [{node(), String.t()}]) ::
          {:ok, [replication_target()]}
  def select_replication_targets(count, tier, exclude \\ []) when count >= 0 do
    if count == 0 do
      {:ok, []}
    else
      copies_by_node = Enum.frequencies_by(exclude, fn {node, _drive_id} -> node end)

      targets =
        tier
        |> DriveRegistry.drives_for_tier()
        |> Enum.filter(&(&1.state == :active))
        |> Enum.reject(&({&1.node, &1.id} in exclude))
        |> spread_across_nodes(copies_by_node)
        |> Enum.take(count)
        |> Enum.map(&%{node: &1.node, drive_id: &1.id})

      {:ok, targets}
    end
  end

  # Private Functions

  # Round-robins drives across nodes so successive replicas prefer
  # distinct failure domains. Node buckets are ordered by how many copies
  # the node already holds (from the exclude set), ascending, then by
  # name — so a zero-copy node is filled before doubling up on the node
  # that holds the primary. Drives within a node are ordered by id.
  defp spread_across_nodes(drives, copies_by_node) do
    drives
    |> Enum.group_by(& &1.node)
    |> Enum.sort_by(fn {node, _drives} -> {Map.get(copies_by_node, node, 0), node} end)
    |> Enum.map(fn {_node, node_drives} -> Enum.sort_by(node_drives, & &1.id) end)
    |> round_robin()
  end

  defp round_robin(buckets) do
    case Enum.reject(buckets, &(&1 == [])) do
      [] ->
        []

      non_empty ->
        Enum.map(non_empty, &hd/1) ++ round_robin(Enum.map(non_empty, &tl/1))
    end
  end

  defp maybe_emit_under_replicated(_hash, _volume, requested, available)
       when available >= requested,
       do: :ok

  defp maybe_emit_under_replicated(chunk_hash, volume, requested, available) do
    :telemetry.execute(
      [:neonfs, :replication, :under_replicated],
      %{requested: requested, available: available},
      %{hash: chunk_hash, volume_id: volume.id}
    )

    Logger.warning("Insufficient drives to fully replicate chunk; flagged for anti-entropy",
      volume_id: volume.id,
      requested: requested,
      available: available
    )

    :ok
  end

  defp perform_replication(chunk_hash, chunk_data, tier, targets, volume, local_drive_id) do
    case volume.write_ack do
      :local ->
        # Background replication - spawn async and return immediately
        spawn_background_replication(chunk_hash, chunk_data, tier, targets, volume.id)
        # Return only local location for now
        {:ok, [%{node: Node.self(), drive_id: local_drive_id, tier: tier}]}

      :quorum ->
        # Quorum replication - wait for W of N
        quorum_replicate(chunk_hash, chunk_data, tier, targets, volume, local_drive_id)

      :all ->
        # Synchronous replication - wait for all
        sync_replicate(chunk_hash, chunk_data, tier, targets, volume.id, local_drive_id)
    end
  end

  defp spawn_background_replication(chunk_hash, chunk_data, tier, targets, volume_id) do
    Task.start(fn ->
      Logger.debug("Starting background replication", chunk_hash: Base.encode16(chunk_hash))

      results = replicate_to_targets(chunk_hash, chunk_data, tier, targets)

      # Update chunk metadata with successful locations
      successful_locations =
        results
        |> Enum.filter(&match?({:ok, _}, &1))
        |> Enum.map(fn {:ok, location} -> location end)

      unless Enum.empty?(successful_locations) do
        add_locations_to_chunk(volume_id, chunk_hash, successful_locations)
      end

      # Log any failures
      failures = Enum.filter(results, &match?({:error, _}, &1))

      unless Enum.empty?(failures) do
        Logger.warning("Background replication had failures", failure_count: length(failures))
      end
    end)
  end

  defp quorum_replicate(chunk_hash, chunk_data, tier, targets, volume, local_drive_id) do
    # Calculate quorum size (W of N)
    # For 3-replica volumes with min_copies=2, we need 1 additional success (already stored locally)
    min_copies = volume.durability.min_copies
    required_successes = max(1, min_copies - 1)

    results = replicate_to_targets(chunk_hash, chunk_data, tier, targets)

    successful_locations =
      results
      |> Enum.filter(&match?({:ok, _}, &1))
      |> Enum.map(fn {:ok, location} -> location end)

    # Include local location
    all_locations = [
      %{node: Node.self(), drive_id: local_drive_id, tier: tier} | successful_locations
    ]

    if length(successful_locations) >= required_successes do
      # Quorum achieved - update metadata and return
      add_locations_to_chunk(volume.id, chunk_hash, successful_locations)
      {:ok, all_locations}
    else
      # Quorum failed
      Logger.error("Quorum replication failed",
        succeeded: length(successful_locations),
        required: required_successes
      )

      {:error, :quorum_not_met}
    end
  end

  defp sync_replicate(chunk_hash, chunk_data, tier, targets, volume_id, local_drive_id) do
    results = replicate_to_targets(chunk_hash, chunk_data, tier, targets)

    # Check if all succeeded
    if Enum.all?(results, &match?({:ok, _}, &1)) do
      locations = Enum.map(results, fn {:ok, location} -> location end)

      # Include local location
      all_locations = [%{node: Node.self(), drive_id: local_drive_id, tier: tier} | locations]

      # Update metadata with all locations
      add_locations_to_chunk(volume_id, chunk_hash, locations)
      {:ok, all_locations}
    else
      # At least one failed
      failures = Enum.filter(results, &match?({:error, _}, &1))

      Logger.error("Synchronous replication failed", failure_count: length(failures))

      {:error, :replication_failed}
    end
  end

  defp replicate_to_targets(chunk_hash, chunk_data, tier, targets) do
    # Replicate to all targets in parallel
    targets
    |> Task.async_stream(
      fn target ->
        replicate_to_node(chunk_hash, chunk_data, tier, target)
      end,
      timeout: 30_000,
      max_concurrency: 10
    )
    |> Enum.map(fn
      {:ok, result} -> result
      {:exit, reason} -> {:error, {:task_exit, reason}}
    end)
  end

  defp replicate_to_node(chunk_hash, chunk_data, tier, target) do
    tier_str = Atom.to_string(tier)
    drive_id = target.drive_id

    case Router.data_call(target.node, :put_chunk,
           hash: chunk_hash,
           volume_id: drive_id,
           write_id: nil,
           tier: tier_str,
           data: chunk_data
         ) do
      :ok ->
        {:ok, %{node: target.node, drive_id: drive_id, tier: tier}}

      {:error, :already_exists} ->
        {:ok, %{node: target.node, drive_id: drive_id, tier: tier}}

      {:error, :no_data_endpoint} ->
        Logger.info("No data endpoint, falling back to distribution RPC", node: target.node)
        replicate_to_node_rpc(chunk_hash, chunk_data, tier_str, target)

      {:error, reason} ->
        Logger.warning("Failed to replicate chunk to node",
          node: target.node,
          reason: inspect(reason)
        )

        {:error, reason}
    end
  end

  defp replicate_to_node_rpc(chunk_hash, chunk_data, tier_str, target) do
    drive_id = target.drive_id

    case :rpc.call(
           target.node,
           BlobStore,
           :write_chunk,
           [chunk_data, drive_id, tier_str, []],
           10_000
         ) do
      {:ok, returned_hash, _chunk_info} ->
        if returned_hash == chunk_hash do
          {:ok, %{node: target.node, drive_id: drive_id, tier: String.to_existing_atom(tier_str)}}
        else
          Logger.error("Hash mismatch during replication",
            expected_hash: Base.encode16(chunk_hash)
          )

          {:error, :hash_mismatch}
        end

      {:error, reason} ->
        Logger.warning("Failed to replicate chunk to node",
          node: target.node,
          reason: inspect(reason)
        )

        {:error, reason}

      {:badrpc, reason} ->
        Logger.warning("RPC failed when replicating to node",
          node: target.node,
          reason: inspect(reason)
        )

        {:error, {:rpc_failed, reason}}
    end
  end

  defp local_drive_id_for_tier(tier) do
    case DriveRegistry.select_drive(tier) do
      {:ok, drive} -> drive.id
      {:error, _} -> "default"
    end
  end

  defp add_locations_to_chunk(volume_id, chunk_hash, new_locations) do
    case ChunkIndex.get(volume_id, chunk_hash) do
      {:ok, chunk_meta} ->
        # Merge new locations with existing ones (avoid duplicates)
        updated_locations = Enum.uniq(chunk_meta.locations ++ new_locations)
        ChunkIndex.update_locations(chunk_hash, updated_locations)

      {:error, :not_found} ->
        Logger.warning("Cannot add locations to chunk, chunk not found",
          chunk_hash: Base.encode16(chunk_hash)
        )

        :ok
    end
  end
end
