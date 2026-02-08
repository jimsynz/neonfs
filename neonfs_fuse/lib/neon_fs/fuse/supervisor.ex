defmodule NeonFS.FUSE.Supervisor do
  @moduledoc """
  Top-level supervisor for neonfs_fuse application.

  Supervises:
  - InodeTable: Manages inode-to-path mappings
  - MountSupervisor: DynamicSupervisor for mount handler processes
  - MountManager: Coordinates mount lifecycle and handler processes
  """

  use Supervisor

  @doc """
  Start the FUSE supervisor.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      # Client connectivity — must start before anything that needs core nodes
      {NeonFS.Client.Connection, bootstrap_nodes: bootstrap_nodes()},
      NeonFS.Client.Discovery,
      NeonFS.Client.CostFunction,
      # Inode table must start before handlers
      NeonFS.FUSE.InodeTable,
      # DynamicSupervisor for mount handlers
      NeonFS.FUSE.MountSupervisor,
      # MountManager coordinates mounts and starts handlers under MountSupervisor
      NeonFS.FUSE.MountManager
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp bootstrap_nodes do
    case Application.get_env(:neonfs_fuse, :core_node) do
      nil -> Application.get_env(:neonfs_client, :bootstrap_nodes, [])
      core_node -> [core_node]
    end
  end
end
