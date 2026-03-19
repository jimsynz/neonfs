defmodule NeonFS.FUSE.Supervisor do
  @moduledoc """
  Top-level supervisor for neonfs_fuse application.

  Supervises:
  - InodeTable: Manages inode-to-path mappings
  - MountSupervisor: DynamicSupervisor for mount handler processes
  - MountManager: Coordinates mount lifecycle and handler processes
  """

  use Supervisor

  alias NeonFS.FUSE.MetricsSupervisor

  @doc """
  Start the FUSE supervisor.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children =
      [
        # Service registration (unique name — FUSE and NFS each need their own)
        {NeonFS.Client.Registrar,
         metadata: registration_metadata(), type: :fuse, name: NeonFS.Client.Registrar.FUSE},
        # Inode table must start before handlers
        NeonFS.FUSE.InodeTable,
        # DynamicSupervisor for mount handlers
        NeonFS.FUSE.MountSupervisor,
        # MountManager coordinates mounts and starts handlers under MountSupervisor
        NeonFS.FUSE.MountManager
      ] ++ metrics_children()

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp registration_metadata do
    %{
      capabilities: [:mount, :unmount],
      version: to_string(Application.spec(:neonfs_fuse, :vsn) || "0.0.0")
    }
  end

  defp metrics_children do
    if MetricsSupervisor.enabled?(), do: [MetricsSupervisor], else: []
  end
end
