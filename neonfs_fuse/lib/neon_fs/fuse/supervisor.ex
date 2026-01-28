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
      # Inode table must start first - used by handlers
      NeonFS.FUSE.InodeTable,
      # DynamicSupervisor for mount handlers
      NeonFS.FUSE.MountSupervisor,
      # MountManager coordinates mounts and starts handlers under MountSupervisor
      NeonFS.FUSE.MountManager
    ]

    # one_for_one: each child restarts independently
    # InodeTable failure would break all mounts, but restart quickly
    # MountSupervisor failure loses all handlers, but restart allows new mounts
    # MountManager failure loses tracking but DynamicSupervisor keeps handlers alive
    Supervisor.init(children, strategy: :one_for_one)
  end
end
