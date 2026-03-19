defmodule NeonFS.Client.Application do
  @moduledoc """
  OTP Application for neonfs_client.

  Supervises shared infrastructure used by core, fuse, and nfs nodes:
  client connectivity, event notification, transport pools, and partition
  recovery. Running these under a single application supervisor ensures
  each process is started exactly once per BEAM node — required for
  omnibus mode where all services share one node.
  """

  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    children =
      if Application.get_env(:neonfs_client, :start_children?, true) do
        build_children()
      else
        []
      end

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: NeonFS.Client.Supervisor
    )
  end

  defp build_children do
    [
      # Client connectivity — must start before anything that needs core nodes
      NeonFS.Client.Connection,
      NeonFS.Client.Discovery,
      NeonFS.Client.CostFunction,

      # Event notification infrastructure
      %{id: :pg_neonfs_events, start: {:pg, :start_link, [:neonfs_events]}},
      {Registry, keys: :duplicate, name: NeonFS.Events.Registry},
      NeonFS.Events.Relay,
      NeonFS.Client.PartitionRecovery,

      # Transport pool management
      NeonFS.Transport.PoolSupervisor,
      NeonFS.Transport.PoolManager
    ]
  end
end
