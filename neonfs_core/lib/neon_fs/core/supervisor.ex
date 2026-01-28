defmodule NeonFS.Core.Supervisor do
  @moduledoc """
  Top-level supervisor for NeonFS Core application.

  Supervises all core components in the correct dependency order:
  1. BlobStore - Storage layer, required by all other components
  2. ChunkIndex - Chunk metadata, depends on BlobStore
  3. FileIndex - File metadata, depends on ChunkIndex
  4. VolumeRegistry - Volume configuration, depends on FileIndex
  """

  use Supervisor

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    # Get blob store configuration from application environment
    base_dir = Application.get_env(:neonfs_core, :blob_store_base_dir, "/tmp/neonfs/blobs")
    prefix_depth = Application.get_env(:neonfs_core, :blob_store_prefix_depth, 2)

    children = [
      # BlobStore must start first - provides storage layer for all components
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
