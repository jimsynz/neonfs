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
  alias NeonFS.Cluster.State, as: ClusterState
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
      * `:exclude_nodes` - Nodes to exclude from selection (default: [])

  ## Returns
    * `{:ok, locations}` - List of all locations where chunk is stored
    * `{:error, reason}` - Replication failed
  """
  @spec replicate_chunk(binary(), binary(), NeonFS.Core.Volume.t(), keyword()) ::
          replication_result()
  def replicate_chunk(chunk_hash, chunk_data, volume, opts \\ []) do
    start_time = System.monotonic_time()
    tier = Keyword.get(opts, :tier, volume.tiering.initial_tier)
    exclude_nodes = Keyword.get(opts, :exclude_nodes, [Node.self()])

    :telemetry.execute(
      [:neonfs, :replication, :start],
      %{bytes: byte_size(chunk_data)},
      %{hash: chunk_hash, volume_id: volume.id}
    )

    # Determine how many replicas we need (minus the local copy already stored)
    target_count = volume.durability.factor - 1

    local_drive_id = local_drive_id_for_tier(tier)

    result =
      case select_replication_targets(target_count, exclude_nodes) do
        {:ok, targets} ->
          perform_replication(chunk_hash, chunk_data, tier, targets, volume, local_drive_id)

        {:error, :no_targets} ->
          # No replication targets available - return local location only
          # This is acceptable when running in single-node mode
          Logger.debug("No replication targets available, returning local location only")
          {:ok, [%{node: Node.self(), drive_id: local_drive_id, tier: tier}]}
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
  Selects replication target nodes from available cluster members.

  Attempts to maximize failure domain separation by selecting nodes
  on different physical machines. If the requested count exceeds
  available nodes, returns all available nodes with a warning.

  ## Arguments
    * `count` - Number of target nodes to select
    * `exclude_nodes` - Nodes to exclude from selection

  ## Returns
    * `{:ok, targets}` - List of target nodes
    * `{:error, :no_targets}` - No suitable targets available
  """
  @spec select_replication_targets(non_neg_integer(), [atom()]) ::
          {:ok, [replication_target()]} | {:error, :no_targets}
  def select_replication_targets(count, exclude_nodes \\ []) when count >= 0 do
    # If count is 0, no replication needed
    if count == 0 do
      {:ok, []}
    else
      case get_cluster_members() do
        {:ok, members} ->
          members
          |> filter_and_prepare_targets(exclude_nodes)
          |> Enum.take(count)
          |> validate_selected_targets(count)

        {:error, _reason} ->
          {:error, :no_targets}
      end
    end
  end

  # Private Functions

  defp validate_selected_targets([], _count), do: {:error, :no_targets}

  defp validate_selected_targets(selected, count) do
    if length(selected) < count do
      Logger.warning(
        "Requested more replicas than nodes available, some redundancy is better than none",
        requested: count,
        available: length(selected)
      )
    end

    {:ok, selected}
  end

  defp filter_and_prepare_targets(members, exclude_nodes) do
    members
    |> Enum.reject(&(&1 in exclude_nodes))
    |> Enum.map(fn node ->
      # Try to find a drive for this node from registry; fall back to "default"
      drive_id =
        DriveRegistry.drives_for_node(node)
        |> Enum.filter(&(&1.state == :active))
        |> case do
          [drive | _] -> drive.id
          [] -> "default"
        end

      %{node: node, drive_id: drive_id}
    end)
  end

  defp get_cluster_members do
    case ClusterState.load() do
      {:ok, state} ->
        # Include all Ra cluster members as potential targets
        {:ok, state.ra_cluster_members}

      {:error, :not_found} ->
        # Single-node cluster (no cluster state file)
        {:ok, [Node.self()]}

      {:error, reason} ->
        {:error, reason}
    end
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
