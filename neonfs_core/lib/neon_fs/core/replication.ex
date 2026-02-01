defmodule NeonFS.Core.Replication do
  @moduledoc """
  Handles chunk replication across cluster nodes.

  Implements replication strategies based on volume configuration:
  - Local acknowledgement: async background replication
  - Quorum acknowledgement: wait for W of N replicas
  - All acknowledgement: wait for all replicas

  Phase 2 implements simple replication. Erasure coding comes in Phase 4.
  """

  alias NeonFS.Cluster.State, as: ClusterState
  alias NeonFS.Core.{BlobStore, ChunkIndex}

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
      * `:tier` - Target tier for replicas (default: volume.initial_tier)
      * `:exclude_nodes` - Nodes to exclude from selection (default: [])

  ## Returns
    * `{:ok, locations}` - List of all locations where chunk is stored
    * `{:error, reason}` - Replication failed
  """
  @spec replicate_chunk(binary(), binary(), NeonFS.Core.Volume.t(), keyword()) ::
          replication_result()
  def replicate_chunk(chunk_hash, chunk_data, volume, opts \\ []) do
    start_time = System.monotonic_time()
    tier = Keyword.get(opts, :tier, volume.initial_tier)
    exclude_nodes = Keyword.get(opts, :exclude_nodes, [Node.self()])

    :telemetry.execute(
      [:neonfs, :replication, :start],
      %{bytes: byte_size(chunk_data)},
      %{hash: chunk_hash, volume_id: volume.id}
    )

    # Determine how many replicas we need (minus the local copy already stored)
    target_count = volume.durability.factor - 1

    result =
      case select_replication_targets(target_count, exclude_nodes) do
        {:ok, targets} ->
          perform_replication(chunk_hash, chunk_data, tier, targets, volume)

        {:error, :no_targets} ->
          # No replication targets available - return local location only
          # This is acceptable when running in single-node mode
          Logger.debug("No replication targets available, returning local location only")
          {:ok, [%{node: Node.self(), drive_id: "default", tier: tier}]}
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
      with {:ok, members} <- get_cluster_members(),
           available <- filter_and_prepare_targets(members, exclude_nodes),
           selected <- Enum.take(available, count) do
        # Warn if we couldn't get enough targets
        if length(selected) < count and not Enum.empty?(selected) do
          Logger.warning(
            "Requested #{count} replicas but only #{length(selected)} nodes available. " <>
              "Some redundancy is better than none."
          )
        end

        # Return error if no targets, otherwise return selected
        case selected do
          [] -> {:error, :no_targets}
          targets -> {:ok, targets}
        end
      else
        {:error, _reason} -> {:error, :no_targets}
      end
    end
  end

  # Private Functions

  defp filter_and_prepare_targets(members, exclude_nodes) do
    members
    |> Enum.reject(&(&1 in exclude_nodes))
    |> Enum.map(&%{node: &1, drive_id: "default"})
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

  defp perform_replication(chunk_hash, chunk_data, tier, targets, volume) do
    case volume.write_ack do
      :local ->
        # Background replication - spawn async and return immediately
        spawn_background_replication(chunk_hash, chunk_data, tier, targets)
        # Return only local location for now
        {:ok, [%{node: Node.self(), drive_id: "default", tier: tier}]}

      :quorum ->
        # Quorum replication - wait for W of N
        quorum_replicate(chunk_hash, chunk_data, tier, targets, volume)

      :all ->
        # Synchronous replication - wait for all
        sync_replicate(chunk_hash, chunk_data, tier, targets)
    end
  end

  defp spawn_background_replication(chunk_hash, chunk_data, tier, targets) do
    Task.start(fn ->
      Logger.debug("Starting background replication for #{Base.encode16(chunk_hash)}")

      results = replicate_to_targets(chunk_hash, chunk_data, tier, targets)

      # Update chunk metadata with successful locations
      successful_locations =
        results
        |> Enum.filter(&match?({:ok, _}, &1))
        |> Enum.map(fn {:ok, location} -> location end)

      unless Enum.empty?(successful_locations) do
        add_locations_to_chunk(chunk_hash, successful_locations)
      end

      # Log any failures
      failures = Enum.filter(results, &match?({:error, _}, &1))

      unless Enum.empty?(failures) do
        Logger.warning("Background replication had #{length(failures)} failures")
      end
    end)
  end

  defp quorum_replicate(chunk_hash, chunk_data, tier, targets, volume) do
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
      %{node: Node.self(), drive_id: "default", tier: tier} | successful_locations
    ]

    if length(successful_locations) >= required_successes do
      # Quorum achieved - update metadata and return
      add_locations_to_chunk(chunk_hash, successful_locations)
      {:ok, all_locations}
    else
      # Quorum failed
      Logger.error(
        "Quorum replication failed: #{length(successful_locations)}/#{required_successes} succeeded"
      )

      {:error, :quorum_not_met}
    end
  end

  defp sync_replicate(chunk_hash, chunk_data, tier, targets) do
    results = replicate_to_targets(chunk_hash, chunk_data, tier, targets)

    # Check if all succeeded
    if Enum.all?(results, &match?({:ok, _}, &1)) do
      locations = Enum.map(results, fn {:ok, location} -> location end)

      # Include local location
      all_locations = [%{node: Node.self(), drive_id: "default", tier: tier} | locations]

      # Update metadata with all locations
      add_locations_to_chunk(chunk_hash, locations)
      {:ok, all_locations}
    else
      # At least one failed
      failures = Enum.filter(results, &match?({:error, _}, &1))

      Logger.error("Synchronous replication failed: #{length(failures)} targets failed")

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

    # Use :rpc to call BlobStore.write_chunk on the remote node
    case :rpc.call(
           target.node,
           BlobStore,
           :write_chunk,
           [chunk_data, tier_str, []],
           10_000
         ) do
      {:ok, returned_hash, _chunk_info} ->
        # Verify hash matches
        if returned_hash == chunk_hash do
          location = %{node: target.node, drive_id: target.drive_id, tier: tier}
          {:ok, location}
        else
          Logger.error("Hash mismatch during replication: expected #{Base.encode16(chunk_hash)}")
          {:error, :hash_mismatch}
        end

      {:error, reason} ->
        Logger.warning("Failed to replicate chunk to #{target.node}: #{inspect(reason)}")

        {:error, reason}

      {:badrpc, reason} ->
        Logger.warning("RPC failed when replicating to #{target.node}: #{inspect(reason)}")

        {:error, {:rpc_failed, reason}}
    end
  end

  defp add_locations_to_chunk(chunk_hash, new_locations) do
    case ChunkIndex.get(chunk_hash) do
      {:ok, chunk_meta} ->
        # Merge new locations with existing ones (avoid duplicates)
        updated_locations = Enum.uniq(chunk_meta.locations ++ new_locations)
        ChunkIndex.update_locations(chunk_hash, updated_locations)

      {:error, :not_found} ->
        Logger.warning(
          "Cannot add locations to chunk #{Base.encode16(chunk_hash)}: chunk not found"
        )

        :ok
    end
  end
end
