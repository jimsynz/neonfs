defmodule NeonFS.Core.Supervisor do
  @moduledoc """
  Top-level supervisor for NeonFS Core application.

  Supervises all core components in the correct dependency order:
  1. Persistence - Restores volume config from DETS on startup
  2. RaSupervisor - Raft consensus for cluster-wide state (Phase 2+)
  3. BlobStore - Storage layer, required by all other components
  4. MetadataStore - Metadata namespace over BlobStore (must start before indexes)
  5. ClockMonitor - Clock skew detection and quarantine
  6. ChunkIndex - Chunk metadata (quorum-backed), depends on MetadataStore
  7. FileIndex - File metadata (quorum-backed), depends on ChunkIndex
  8. StripeIndex - Stripe metadata (quorum-backed)
  9. ReadRepair - Async read repair via BackgroundWorker
  10. ResolvedLookupCache - Resolved file→chunk mappings cache
  11. VolumeRegistry - Volume configuration, depends on FileIndex
  12. ServiceRegistry - Service discovery, depends on Ra being available

  ## Test Configuration

  In test environment, set `config :neonfs_core, start_children?: false` to prevent
  automatic child startup. Tests are then responsible for starting the subsystems
  they need via `start_supervised!/1`.
  """

  use Supervisor

  require Logger

  alias NeonFS.Core.{AntiEntropy, MetadataRing, ServiceRegistry}

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    children =
      if start_children?() do
        build_children()
      else
        Logger.info("Supervisor starting with no children (start_children?: false)")
        []
      end

    # Use one_for_one strategy: if a child crashes, only that child is restarted
    # This is appropriate because components are independent after initialization
    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Returns the child specs for all core components.

  This is useful for tests that need to start specific children manually.
  """
  @spec child_specs() :: [Supervisor.child_spec()]
  def child_specs, do: build_children()

  @doc """
  Returns the child spec for a specific component by module name.
  """
  @spec child_spec_for(module()) :: Supervisor.child_spec() | nil
  def child_spec_for(module) do
    Enum.find(build_children(), fn
      %{id: ^module} -> true
      {^module, _opts} -> true
      ^module -> true
      _ -> false
    end)
  end

  @doc """
  Rebuilds the quorum metadata ring with the current set of core nodes.

  Call this after cluster membership changes (e.g. a node joins or leaves)
  to ensure metadata is routed to all core nodes. Updates the persistent_term
  for ChunkIndex, FileIndex, and StripeIndex.
  """
  @spec rebuild_quorum_ring() :: :ok
  def rebuild_quorum_ring do
    old_quorum_opts = :persistent_term.get({NeonFS.Core.ChunkIndex, :quorum_opts}, nil)
    quorum_opts = build_quorum_opts()

    for module <- [NeonFS.Core.ChunkIndex, NeonFS.Core.FileIndex, NeonFS.Core.StripeIndex] do
      :persistent_term.put({module, :quorum_opts}, quorum_opts)
    end

    ring = Keyword.fetch!(quorum_opts, :ring)
    Logger.info("Rebuilt quorum ring with #{MapSet.size(ring.node_set)} nodes")

    sync_after_ring_change(old_quorum_opts, ring)

    :ok
  end

  # Private functions

  defp start_children?, do: Application.get_env(:neonfs_core, :start_children?, true)

  defp sync_after_ring_change(nil, _new_ring), do: :ok

  defp sync_after_ring_change(old_quorum_opts, new_ring) do
    old_ring = Keyword.fetch!(old_quorum_opts, :ring)

    if old_ring.node_set != new_ring.node_set do
      local_segments =
        new_ring
        |> MetadataRing.segments()
        |> Enum.filter(fn {_seg_id, replicas} -> Node.self() in replicas end)

      Logger.info("Ring membership changed, syncing #{length(local_segments)} segments")

      Enum.each(local_segments, fn {segment_id, _replicas} ->
        AntiEntropy.sync_segment(segment_id, ring: new_ring)
      end)
    end
  end

  defp build_children do
    drives = Application.get_env(:neonfs_core, :drives, default_drives())
    prefix_depth = Application.get_env(:neonfs_core, :blob_store_prefix_depth, 2)
    meta_dir = Application.get_env(:neonfs_core, :meta_dir, "/tmp/neonfs/meta")
    snapshot_interval_ms = Application.get_env(:neonfs_core, :snapshot_interval_ms, 30_000)

    # Ra requires a named Erlang node (not :nonode@nohost) to function
    node_named = Node.self() != :nonode@nohost
    ra_config = Application.get_env(:neonfs_core, :enable_ra, false)
    enable_ra = node_named and ra_config

    # Build quorum opts for metadata indexes
    quorum_opts = build_quorum_opts()

    Logger.info(
      "Building children: node=#{inspect(Node.self())}, node_named=#{node_named}, ra_config=#{ra_config}, enable_ra=#{enable_ra}"
    )

    # Per-drive power management state machines
    base_children =
      [
        # Persistence must start first - restores volume config from DETS
        # Give it extra shutdown time to complete the final snapshot
        %{
          id: NeonFS.Core.Persistence,
          start:
            {NeonFS.Core.Persistence, :start_link,
             [[meta_dir: meta_dir, snapshot_interval_ms: snapshot_interval_ms]]},
          shutdown: 30_000
        },

        # Registry for DriveState process naming (must start before BlobStore)
        {Registry, keys: :unique, name: NeonFS.Core.DriveStateRegistry},

        # BlobStore provides storage layer for all components (multi-drive)
        {NeonFS.Core.BlobStore, drives: drives, prefix_depth: prefix_depth},

        # DriveRegistry tracks all drives and their usage/state
        {NeonFS.Core.DriveRegistry, drives: drives},

        # DynamicSupervisor for DriveState processes (managed by DriveManager)
        {DynamicSupervisor, name: NeonFS.Core.DriveStateSupervisor, strategy: :one_for_one},

        # DriveManager orchestrates runtime drive add/remove
        NeonFS.Core.DriveManager
      ] ++
        [
          # MetadataStore wraps BlobStore metadata namespace with ETS caching and HLC timestamps
          # Must start before ChunkIndex/FileIndex/StripeIndex (they need it for loading)
          NeonFS.Core.MetadataStore,

          # Event notification infrastructure (Phase 10)
          # :pg scope for cross-node event relay, Registry for node-local fan-out
          %{id: :pg_neonfs_events, start: {:pg, :start_link, [:neonfs_events]}},
          {Registry, keys: :duplicate, name: NeonFS.Events.Registry},
          NeonFS.Events.Relay,
          NeonFS.Client.PartitionRecovery,

          # ClockMonitor detects clock skew and quarantines nodes
          NeonFS.Core.ClockMonitor,

          # ChunkAccessTracker records chunk access patterns for tiering decisions
          NeonFS.Core.ChunkAccessTracker,

          # ChunkCache provides LRU caching for decompressed/decrypted chunks
          NeonFS.Core.ChunkCache,

          # Task.Supervisor for background work crash isolation
          {Task.Supervisor, name: NeonFS.Core.BackgroundTaskSupervisor},

          # BackgroundWorker provides priority queues and rate limiting
          NeonFS.Core.BackgroundWorker,

          # Task.Supervisor for job runner processes (separate from BackgroundTaskSupervisor)
          {Task.Supervisor, name: NeonFS.Core.JobTaskSupervisor},

          # JobTracker manages persistent, resumable background jobs
          NeonFS.Core.JobTracker,

          # ReadRepair submits async repair jobs for stale metadata replicas
          NeonFS.Core.ReadRepair,

          # ResolvedLookupCache caches fully resolved file→chunk mappings
          NeonFS.Core.ResolvedLookupCache,

          # ChunkIndex depends on MetadataStore (quorum-backed)
          {NeonFS.Core.ChunkIndex, quorum_opts: quorum_opts},

          # FileIndex depends on ChunkIndex (quorum-backed)
          {NeonFS.Core.FileIndex, quorum_opts: quorum_opts},

          # StripeIndex for erasure-coded stripe metadata (quorum-backed)
          {NeonFS.Core.StripeIndex, quorum_opts: quorum_opts},

          # VolumeRegistry depends on FileIndex
          NeonFS.Core.VolumeRegistry,

          # ACLManager caches volume ACLs in ETS, backed by Ra
          NeonFS.Core.ACLManager,

          # AuditLog records security-relevant events in bounded ETS
          NeonFS.Core.AuditLog,

          # Transport: HandlerSupervisor + Listener + PoolSupervisor + PoolManager for data transfer (Phase 9)
          NeonFS.Transport.HandlerSupervisor,
          NeonFS.Transport.Listener,
          NeonFS.Transport.PoolSupervisor,
          {NeonFS.Transport.PoolManager,
           service_list_fn: fn ->
             try do
               ServiceRegistry.list()
             rescue
               _ -> []
             end
           end},

          # ServiceRegistry depends on Ra being available
          ServiceRegistry,

          # TieringManager evaluates chunks for promotion/demotion
          NeonFS.Core.TieringManager,

          # AntiEntropy periodically syncs metadata segments via Merkle tree comparison
          NeonFS.Core.AntiEntropy,

          # Retention prunes old audit log files from the system volume
          NeonFS.Core.SystemVolume.Retention,

          # ReadySignal MUST be the last child — it joins :pg group {:node, :ready}
          # to signal that all preceding children have started successfully.
          # Integration tests use :pg.monitor/2 on this group for event-driven
          # readiness detection instead of polling.
          NeonFS.Core.ReadySignal
        ]

    # Conditionally add RaSupervisor for Phase 2+ distributed operation
    if enable_ra do
      List.insert_at(base_children, 1, NeonFS.Core.RaSupervisor)
    else
      base_children
    end
  end

  defp build_quorum_opts do
    # Build a MetadataRing for the current node.
    # At startup, only the local node is guaranteed to have MetadataStore running.
    # Non-core nodes (test controllers, FUSE nodes) must not be included in the ring,
    # as they lack MetadataStore and would cause quorum failures.
    #
    # In a multi-node cluster, the ring is rebuilt when cluster membership changes
    # (future: dynamic ring rebuild via ServiceRegistry events).
    core_nodes = discover_core_nodes()
    timeout = Application.get_env(:neonfs_core, :quorum_timeout_ms, 5_000)

    ring =
      MetadataRing.new(core_nodes,
        virtual_nodes_per_physical: 64,
        replicas: min(3, length(core_nodes))
      )

    [ring: ring, timeout: timeout]
  end

  defp discover_core_nodes do
    other_core =
      Node.list()
      |> Enum.filter(fn node ->
        try do
          has_metadata_store =
            case :erpc.call(node, Process, :whereis, [NeonFS.Core.MetadataStore], 2_000) do
              pid when is_pid(pid) -> true
              _ -> false
            end

          # Only include nodes that have actually joined the cluster.
          # Unjoined nodes may have MetadataStore running (from app start)
          # but lack cluster data, which would cause quorum read failures.
          is_cluster_member =
            :erpc.call(node, NeonFS.Cluster.State, :exists?, [], 2_000)

          has_metadata_store and is_cluster_member
        catch
          _, _ -> false
        end
      end)

    [Node.self() | other_core]
  end

  defp default_drives do
    base_dir = Application.get_env(:neonfs_core, :blob_store_base_dir, "/tmp/neonfs/blobs")
    [%{id: "default", path: base_dir, tier: :hot, capacity: 0}]
  end
end
