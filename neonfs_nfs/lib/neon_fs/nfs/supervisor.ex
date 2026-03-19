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
      # Service registration (unique name — FUSE and NFS each need their own)
      {NeonFS.Client.Registrar,
       metadata: registration_metadata(), type: :nfs, name: NeonFS.Client.Registrar.NFS},
      # Inode table must start before handlers
      NeonFS.NFS.InodeTable,
      # Metadata cache for reducing RPC round-trips
      NeonFS.NFS.MetadataCache,
      # DynamicSupervisor for handler processes
      NeonFS.NFS.ExportSupervisor,
      # ExportManager starts NFS server and manages volume exports
      NeonFS.NFS.ExportManager
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp registration_metadata do
    %{
      capabilities: [:export, :read, :write],
      version: to_string(Application.spec(:neonfs_nfs, :vsn) || "0.0.0")
    }
  end
end
