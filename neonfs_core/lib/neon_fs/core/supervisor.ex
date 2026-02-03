defmodule NeonFS.Core.Supervisor do
  @moduledoc """
  Top-level supervisor for NeonFS Core application.

  Supervises all core components in the correct dependency order:
  1. Persistence - Restores metadata from DETS on startup
  2. RaSupervisor - Raft consensus for cluster-wide state (Phase 2+)
  3. BlobStore - Storage layer, required by all other components
  4. ChunkIndex - Chunk metadata, depends on BlobStore
  5. FileIndex - File metadata, depends on ChunkIndex
  6. VolumeRegistry - Volume configuration, depends on FileIndex

  ## Test Configuration

  In test environment, set `config :neonfs_core, start_children?: false` to prevent
  automatic child startup. Tests are then responsible for starting the subsystems
  they need via `start_supervised!/1`.
  """

  use Supervisor

  require Logger

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

  # Private functions

  defp start_children?, do: Application.get_env(:neonfs_core, :start_children?, true)

  defp build_children do
    base_dir = Application.get_env(:neonfs_core, :blob_store_base_dir, "/tmp/neonfs/blobs")
    prefix_depth = Application.get_env(:neonfs_core, :blob_store_prefix_depth, 2)
    meta_dir = Application.get_env(:neonfs_core, :meta_dir, "/tmp/neonfs/meta")
    snapshot_interval_ms = Application.get_env(:neonfs_core, :snapshot_interval_ms, 30_000)

    # Ra requires a named Erlang node (not :nonode@nohost) to function
    node_named = Node.self() != :nonode@nohost
    ra_config = Application.get_env(:neonfs_core, :enable_ra, false)
    enable_ra = node_named and ra_config

    Logger.info(
      "Building children: node=#{inspect(Node.self())}, node_named=#{node_named}, ra_config=#{ra_config}, enable_ra=#{enable_ra}"
    )

    base_children = [
      # Persistence must start first - restores metadata from DETS
      # Give it extra shutdown time to complete the final snapshot
      %{
        id: NeonFS.Core.Persistence,
        start:
          {NeonFS.Core.Persistence, :start_link,
           [[meta_dir: meta_dir, snapshot_interval_ms: snapshot_interval_ms]]},
        shutdown: 30_000
      },

      # BlobStore provides storage layer for all components
      {NeonFS.Core.BlobStore, base_dir: base_dir, prefix_depth: prefix_depth},

      # ChunkIndex depends on BlobStore
      NeonFS.Core.ChunkIndex,

      # FileIndex depends on ChunkIndex
      NeonFS.Core.FileIndex,

      # VolumeRegistry depends on FileIndex
      NeonFS.Core.VolumeRegistry
    ]

    # Conditionally add RaSupervisor for Phase 2+ distributed operation
    if enable_ra do
      List.insert_at(base_children, 1, NeonFS.Core.RaSupervisor)
    else
      base_children
    end
  end
end
