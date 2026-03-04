defmodule NeonFS.NFS.Supervisor do
  @moduledoc """
  Top-level supervisor for neonfs_nfs application.

  Supervises:
  - Client connectivity (Connection, Discovery, CostFunction)
  - Event notification infrastructure (:pg, Registry, Relay)
  - InodeTable: Manages inode-to-path mappings
  - ExportSupervisor: DynamicSupervisor for handler processes
  - ExportManager: Coordinates NFS server and volume export lifecycle
  """

  use Supervisor

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
      # Transport: PoolSupervisor + PoolManager for data transfer (Phase 9)
      NeonFS.Transport.PoolSupervisor,
      NeonFS.Transport.PoolManager,
      # Event notification infrastructure (Phase 10)
      %{id: :pg_neonfs_events, start: {:pg, :start_link, [:neonfs_events]}},
      {Registry, keys: :duplicate, name: NeonFS.Events.Registry},
      NeonFS.Events.Relay,
      NeonFS.Client.PartitionRecovery,
      # Inode table must start before handlers
      NeonFS.NFS.InodeTable,
      # DynamicSupervisor for handler processes
      NeonFS.NFS.ExportSupervisor,
      # ExportManager starts NFS server and manages volume exports
      NeonFS.NFS.ExportManager
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp bootstrap_nodes do
    case Application.get_env(:neonfs_nfs, :core_node) do
      nil -> Application.get_env(:neonfs_client, :bootstrap_nodes, [])
      core_node -> [core_node]
    end
  end
end
