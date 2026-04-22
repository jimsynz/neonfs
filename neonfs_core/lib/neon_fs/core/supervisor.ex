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

  alias NeonFS.Cluster.{Formation, State}
  alias NeonFS.Core.{AntiEntropy, MetadataRing, MetricsSupervisor, ServiceRegistry}

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
    Logger.info("Rebuilt quorum ring", node_count: MapSet.size(ring.node_set))

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

      Logger.info("Ring membership changed, syncing segments",
        segment_count: length(local_segments)
      )

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

    # Build quorum opts for metadata indexes and set persistent_term BEFORE
    # children start. Children read from persistent_term at runtime; the opts
    # are NOT baked into child specs, so crash-restart won't revert the ring
    # to a stale value from supervisor boot time.
    quorum_opts = build_quorum_opts()

    for module <- [NeonFS.Core.ChunkIndex, NeonFS.Core.FileIndex, NeonFS.Core.StripeIndex] do
      :persistent_term.put({module, :quorum_opts}, quorum_opts)
    end

    Logger.info("Building children",
      node: Node.self(),
      node_named: node_named,
      ra_config: ra_config,
      enable_ra: enable_ra
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
        NeonFS.Core.DriveManager,

        # Registry for I/O drive worker process naming (must start before IO.Supervisor)
        {Registry, keys: :unique, name: NeonFS.IO.WorkerRegistry},

        # I/O scheduler supervision tree — producer, priority adjuster, drive workers
        # Must start after DriveRegistry so it can query drives on init
        NeonFS.IO.Supervisor
      ] ++
        [
          # MetadataStore wraps BlobStore metadata namespace with ETS caching and HLC timestamps
          # Must start before ChunkIndex/FileIndex/StripeIndex (they need it for loading)
          NeonFS.Core.MetadataStore,

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
          NeonFS.Core.ChunkIndex,

          # FileIndex depends on ChunkIndex (quorum-backed)
          NeonFS.Core.FileIndex,

          # StripeIndex for erasure-coded stripe metadata (quorum-backed)
          NeonFS.Core.StripeIndex,

          # StorageMetrics tracks per-drive usage via telemetry
          NeonFS.Core.StorageMetrics,

          # VolumeRegistry depends on FileIndex
          NeonFS.Core.VolumeRegistry,

          # PendingWriteRecovery opens the pending-write DETS log and
          # reclaims chunks orphaned by interrupted streaming writes
          # on startup (#296). Must start AFTER ChunkIndex so the
          # recovery sweep can call WriteOperation.abort_chunks/1.
          NeonFS.Core.PendingWriteRecovery,

          # AuditLog records security-relevant events in bounded ETS
          NeonFS.Core.AuditLog,

          # Transport: HandlerSupervisor + Listener for data transfer (Phase 9)
          # PoolSupervisor + PoolManager are owned by neonfs_client's supervisor
          NeonFS.Transport.HandlerSupervisor,
          NeonFS.Transport.Listener,

          # Escalation expiry ticker — Escalation itself is stateless and
          # Ra-backed; the ticker periodically reaps overdue pending entries
          # and emits pending-count metrics.
          NeonFS.Core.Escalation.Ticker,

          # EscalationWebhook forwards :raised telemetry to an HTTP endpoint
          NeonFS.Core.EscalationWebhook,

          # ServiceRegistry depends on Ra being available
          ServiceRegistry,

          # TieringManager evaluates chunks for promotion/demotion
          NeonFS.Core.TieringManager,

          # GCScheduler creates periodic GC jobs and triggers on storage pressure
          {NeonFS.Core.GCScheduler, gc_scheduler_opts()},

          # ScrubScheduler creates periodic scrub jobs per volume
          {NeonFS.Core.ScrubScheduler, scrub_scheduler_opts()},

          # AntiEntropy periodically syncs metadata segments via Merkle tree comparison
          NeonFS.Core.AntiEntropy,

          # Retention prunes old audit log files from the system volume
          NeonFS.Core.SystemVolume.Retention,

          # LockManager: Grace period coordinator for lock recovery after failover
          NeonFS.Core.LockManager.GraceCoordinator,

          # LockManager: Registry for per-file lock process naming
          {Registry, keys: :unique, name: NeonFS.Core.LockManager.Registry},

          # LockManager: DynamicSupervisor for per-file lock GenServers
          NeonFS.Core.LockManager.Supervisor
        ] ++
        maybe_metrics_child() ++
        [
          # ReadySignal MUST be the last regular child — it joins :pg group {:node, :ready}
          # to signal that all preceding children have started successfully.
          # Integration tests use :pg.monitor/2 on this group for event-driven
          # readiness detection instead of polling.
          NeonFS.Core.ReadySignal
        ] ++
        maybe_formation_child()

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
      ServiceRegistry.connected_nodes_by_type(:core)
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
            :erpc.call(node, State, :exists?, [], 2_000)

          has_metadata_store and is_cluster_member
        catch
          _, _ -> false
        end
      end)

    [Node.self() | other_core]
  end

  defp gc_scheduler_opts do
    [
      interval_ms: Application.get_env(:neonfs_core, :gc_interval_ms, 86_400_000),
      pressure_threshold: Application.get_env(:neonfs_core, :gc_pressure_threshold, 0.85),
      pressure_check_interval_ms:
        Application.get_env(:neonfs_core, :gc_pressure_check_interval_ms, 300_000)
    ]
  end

  defp scrub_scheduler_opts do
    [
      check_interval_ms: Application.get_env(:neonfs_core, :scrub_check_interval_ms, 3_600_000)
    ]
  end

  defp maybe_formation_child do
    if Application.get_env(:neonfs_core, :auto_bootstrap, false) and
         not State.exists?() do
      [
        %{
          id: Formation,
          start:
            {Formation, :start_link,
             [
               [
                 cluster_name: Application.fetch_env!(:neonfs_core, :cluster_name),
                 bootstrap_expect: Application.fetch_env!(:neonfs_core, :bootstrap_expect),
                 bootstrap_peers: Application.fetch_env!(:neonfs_core, :bootstrap_peers),
                 bootstrap_peer_ports:
                   Application.get_env(:neonfs_core, :bootstrap_peer_ports, %{}),
                 bootstrap_timeout: Application.get_env(:neonfs_core, :bootstrap_timeout, 300_000)
               ]
             ]},
          restart: :temporary,
          shutdown: 30_000
        }
      ]
    else
      []
    end
  end

  defp maybe_metrics_child do
    if MetricsSupervisor.enabled?() do
      [MetricsSupervisor]
    else
      []
    end
  end

  defp default_drives do
    base_dir = Application.get_env(:neonfs_core, :blob_store_base_dir, "/tmp/neonfs/blobs")
    [%{id: "default", path: base_dir, tier: :hot, capacity: 0}]
  end
end
