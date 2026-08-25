defmodule NeonFS.Client.Application do
  @moduledoc """
  OTP Application for neonfs_client.

  Supervises shared infrastructure used by every node type that depends
  on `neonfs_client` (core, fuse, nfs, s3, webdav, docker, iam):
  client connectivity, event notification, transport pools, node certificate
  renewal, and partition
  recovery. Running these under a single application supervisor ensures
  each process is started exactly once per BEAM node — required for
  omnibus mode where all services share one node.
  """

  use Application

  alias NeonFS.Client.HealthCheck

  @impl true
  def start(_type, _args) do
    start_children? = Application.get_env(:neonfs_client, :start_children?, true)

    children =
      if start_children? do
        build_children()
      else
        []
      end

    result =
      Supervisor.start_link(children,
        strategy: :one_for_one,
        name: NeonFS.Client.Supervisor
      )

    if start_children? do
      register_health_checks()
    end

    result
  end

  defp register_health_checks do
    HealthCheck.register(:client, health_checks())
  end

  @doc """
  Returns the keyword list of shared client health checks.

  Public so a test can assert the set that gets registered — registration
  itself only runs when `:start_children?` is true, which it is not under
  test, so a subsystem wired in here would otherwise ship unexercised.
  """
  @spec health_checks() :: keyword((-> HealthCheck.subsystem_report()))
  def health_checks do
    alias NeonFS.Client.{Connection, CostFunction, Discovery}
    alias NeonFS.Transport.{CAExpiry, CertRenewal}

    [
      connection: fn ->
        if Process.whereis(Connection) do
          case Connection.connected_core_node() do
            {:ok, node} -> %{status: :healthy, connected_node: node}
            {:error, :no_connection} -> %{status: :degraded, reason: :no_connection}
          end
        else
          %{status: :unhealthy, reason: :not_running}
        end
      end,
      discovery: fn ->
        if Process.whereis(Discovery) do
          case Discovery.get_core_nodes() do
            [_ | _] = nodes -> %{status: :healthy, core_node_count: length(nodes)}
            [] -> %{status: :degraded, reason: :no_core_nodes}
          end
        else
          %{status: :unhealthy, reason: :not_running}
        end
      end,
      cost_function: fn ->
        if Process.whereis(CostFunction) do
          %{status: :healthy}
        else
          %{status: :unhealthy, reason: :not_running}
        end
      end,
      cert_expiry: &CertRenewal.health_check/0,
      ca_expiry: &CAExpiry.health_check/0
    ]
  end

  defp build_children do
    [
      # Client connectivity — must start before anything that needs core nodes
      NeonFS.Client.Connection,
      NeonFS.Client.Discovery,
      NeonFS.Client.CostFunction,
      # Per-volume root-placement cache for metadata-write routing;
      # resolves via Router, so it follows Discovery/CostFunction.
      NeonFS.Client.RootPlacement,

      # Event notification infrastructure
      %{id: :pg_neonfs_events, start: {:pg, :start_link, [:neonfs_events]}},
      {Registry, keys: :duplicate, name: NeonFS.Events.Registry},
      NeonFS.Events.Relay,
      NeonFS.Client.PartitionRecovery,

      # Transport pool management
      NeonFS.Transport.PoolSupervisor,
      NeonFS.Transport.PoolManager,

      # Node certificates are issued for a year; without this every node in
      # every cluster has an unattended one-year fuse.
      NeonFS.Transport.CertRenewal,

      # Per-node whole-chunk read cache for the data-plane read path
      NeonFS.Client.ChunkCache
    ]
  end
end
