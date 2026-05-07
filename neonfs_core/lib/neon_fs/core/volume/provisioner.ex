defmodule NeonFS.Core.Volume.Provisioner do
  @moduledoc """
  Allocates a volume's initial root segment chunk and registers the
  bootstrap-layer entry on volume create (#805).

  The flow:

  1. Resolve `cluster_id` / `cluster_name` from `NeonFS.Cluster.State`.
  2. Read the bootstrap-layer drive registry (#779) for the cluster's
     active drives.
  3. Pick replica drives via `Volume.DriveSelector` (#803) according
     to the volume's durability.
  4. Build the initial `Volume.RootSegment` (#780). `index_roots`
     stays nil — empty trees don't need a chunk; the first metadata
     write through #785 will allocate them via copy-on-write.
  5. Encode the segment and replicate the chunk via
     `Volume.ChunkReplicator` (#804).
  6. Submit `:register_volume_root` to Ra so the bootstrap layer
     knows where the root lives.

  Each external dependency (cluster state load, Ra query / command,
  chunk replicator) is injectable via opts so unit tests can drive
  the function with deterministic stubs without spinning up a full
  cluster.
  """

  alias NeonFS.Cluster.State, as: ClusterState
  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.Core.RaSupervisor
  alias NeonFS.Core.Volume
  alias NeonFS.Core.Volume.{ChunkReplicator, DriveSelector, RootSegment}

  @type chunk_hash :: binary()
  @type provision_error ::
          {:error, {:cluster_state_unavailable, term()}}
          | {:error, {:drive_query_failed, term()}}
          | {:error, :insufficient_drives, %{available: non_neg_integer(), needed: pos_integer()}}
          | {:error, :insufficient_replicas, map()}
          | {:error, {:bootstrap_register_failed, term()}}

  @doc """
  Provisions a freshly-created volume's metadata. Returns the
  `root_chunk_hash` that the bootstrap layer now points at.

  Failure cases (in order of when they fire):

  - cluster state unavailable → `{:error, {:cluster_state_unavailable, _}}`
  - bootstrap-layer drive query failed → `{:error, {:drive_query_failed, _}}`
  - fewer drives available than the durability's minimum →
    `{:error, :insufficient_drives, %{available, needed}}`
  - chunk-write quorum failure → `{:error, :insufficient_replicas, _}`
  - Ra `:register_volume_root` rejected → `{:error, {:bootstrap_register_failed, _}}`

  The caller is responsible for rolling back the volume's
  registration in `VolumeRegistry` on any of these — leftover
  chunks become unreferenced and GC reaps them.

  Opts (all optional, all stubbable):

  - `:cluster_state_loader` — `(() -> {:ok, ClusterState.t()} | {:error, _})`
  - `:drive_lister` — `(() -> {:ok, [drive_entry]} | {:error, _})`
  - `:chunk_replicator` — module with `write_chunk/3` (default `ChunkReplicator`)
  - `:bootstrap_registrar` — `(register_command :: tuple() -> {:ok, _} | {:error, _})`
  """
  @spec provision(Volume.t(), keyword()) :: {:ok, chunk_hash()} | provision_error()
  def provision(%Volume{} = volume, opts \\ []) do
    cluster_state_loader = Keyword.get(opts, :cluster_state_loader, &default_cluster_loader/0)
    drive_lister = Keyword.get(opts, :drive_lister, &default_drive_lister/0)
    chunk_replicator = Keyword.get(opts, :chunk_replicator, ChunkReplicator)
    bootstrap_registrar = Keyword.get(opts, :bootstrap_registrar, &default_bootstrap_registrar/1)

    with {:ok, cluster_state} <- load_cluster_state(cluster_state_loader),
         {:ok, all_drives} <- list_drives(drive_lister),
         {:ok, replica_ids} <- DriveSelector.select_replicas(volume.durability, all_drives),
         replica_drives = filter_drives(all_drives, replica_ids),
         segment = build_segment(volume, cluster_state),
         encoded = RootSegment.encode(segment),
         min_copies = min_copies(volume.durability),
         {:ok, hash, _summary} <-
           replicate(chunk_replicator, encoded, replica_drives, min_copies),
         entry = build_entry(volume, hash, replica_drives),
         {:ok, _} <- register_root(bootstrap_registrar, entry) do
      {:ok, hash}
    end
  end

  ## Internals

  defp default_cluster_loader, do: ClusterState.load()

  defp default_drive_lister do
    case RaSupervisor.local_query(&MetadataStateMachine.get_drives/1) do
      {:ok, drives_map} when is_map(drives_map) -> {:ok, Map.values(drives_map)}
      other -> other
    end
  end

  # Normalises `RaSupervisor.command/1`'s `{:ok, result, leader}` reply
  # shape into the simpler `:ok | {:ok, _} | {:error, _}` contract
  # `register_root/2` matches against. Without this wrapper the
  # 3-tuple slipped through `register_root/2`'s `other -> ` clause and
  # surfaced as `{:bootstrap_register_failed, {:ok, :ok, leader}}` for
  # every successful Ra commit, which broke `create_volume/2` whenever
  # the cluster had at least one drive registered. Mirrors the same
  # wrapper in `Deprovisioner` and `MetadataWriter`.
  defp default_bootstrap_registrar(command) do
    case RaSupervisor.command(command) do
      {:ok, result, _leader} -> {:ok, result}
      {:error, _} = err -> err
      other -> {:error, other}
    end
  end

  defp load_cluster_state(loader) do
    case loader.() do
      {:ok, %ClusterState{} = state} -> {:ok, state}
      {:error, reason} -> {:error, {:cluster_state_unavailable, reason}}
      other -> {:error, {:cluster_state_unavailable, other}}
    end
  end

  defp list_drives(lister) do
    case lister.() do
      {:ok, drives} when is_list(drives) -> {:ok, drives}
      {:error, reason} -> {:error, {:drive_query_failed, reason}}
      other -> {:error, {:drive_query_failed, other}}
    end
  end

  defp filter_drives(all_drives, replica_ids) do
    by_id = Map.new(all_drives, &{&1.drive_id, &1})
    Enum.map(replica_ids, &Map.fetch!(by_id, &1))
  end

  defp build_segment(%Volume{} = volume, %ClusterState{} = cluster) do
    RootSegment.new(
      volume_id: volume.id,
      volume_name: volume.name,
      cluster_id: cluster.cluster_id,
      cluster_name: cluster.cluster_name,
      durability: volume.durability
    )
  end

  defp min_copies(%{type: :replicate, min_copies: m}), do: m
  defp min_copies(%{type: :erasure, data_chunks: d}), do: d

  defp replicate(chunk_replicator, encoded, drives, min_copies) do
    chunk_replicator.write_chunk(encoded, drives, min_copies: min_copies)
  end

  defp build_entry(%Volume{} = volume, root_chunk_hash, replica_drives) do
    %{
      volume_id: volume.id,
      root_chunk_hash: root_chunk_hash,
      drive_locations: Enum.map(replica_drives, &%{node: &1.node, drive_id: &1.drive_id}),
      durability_cache: volume.durability,
      updated_at: DateTime.utc_now()
    }
  end

  defp register_root(registrar, entry) do
    case registrar.({:register_volume_root, entry}) do
      :ok -> {:ok, :registered}
      {:ok, _} = ok -> ok
      {:error, reason} -> {:error, {:bootstrap_register_failed, reason}}
      other -> {:error, {:bootstrap_register_failed, other}}
    end
  end
end
