defmodule NeonFS.Core.Supervisor do
  @moduledoc """
  Top-level supervisor for NeonFS Core application.

  Supervises all core components in the correct dependency order:
  1. Persistence - Restores metadata from DETS on startup
  2. BlobStore - Storage layer, required by all other components
  3. ChunkIndex - Chunk metadata, depends on BlobStore
  4. FileIndex - File metadata, depends on ChunkIndex
  5. VolumeRegistry - Volume configuration, depends on FileIndex
  """

  use Supervisor

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    # Get configuration from application environment
    base_dir = Application.get_env(:neonfs_core, :blob_store_base_dir, "/tmp/neonfs/blobs")
    prefix_depth = Application.get_env(:neonfs_core, :blob_store_prefix_depth, 2)
    meta_dir = Application.get_env(:neonfs_core, :meta_dir, "/tmp/neonfs/meta")
    snapshot_interval_ms = Application.get_env(:neonfs_core, :snapshot_interval_ms, 30_000)

    children = [
      # Persistence must start first - restores metadata from DETS
      {NeonFS.Core.Persistence, meta_dir: meta_dir, snapshot_interval_ms: snapshot_interval_ms},

      # BlobStore provides storage layer for all components
      {NeonFS.Core.BlobStore, base_dir: base_dir, prefix_depth: prefix_depth},

      # ChunkIndex depends on BlobStore
      NeonFS.Core.ChunkIndex,

      # FileIndex depends on ChunkIndex
      NeonFS.Core.FileIndex,

      # VolumeRegistry depends on FileIndex
      NeonFS.Core.VolumeRegistry
    ]

    # Use one_for_one strategy: if a child crashes, only that child is restarted
    # This is appropriate because components are independent after initialization
    Supervisor.init(children, strategy: :one_for_one)
  end
end
