defmodule NeonFS.CLI.Handler.Node do
  @moduledoc """
  CLI command handlers for node-level views: per-node health status and
  the cluster-wide node listing (combining `ServiceRegistry` entries
  with Ra membership to label leader/follower roles).

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates its `handle_node_status`/`handle_node_list` RPC entry points
  here, so the CLI wire contract is unchanged.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Client.HealthCheck, as: ClientHealthCheck
  alias NeonFS.Core.{RaSupervisor, ServiceRegistry}

  @doc """
  Returns this node's health report from the HealthCheck subsystem.
  """
  @spec handle_node_status() :: {:ok, map()}
  def handle_node_status do
    set_cli_metadata()
    ClientHealthCheck.handle_node_status()
  end

  @doc """
  Lists all cluster nodes with role (leader/follower for core nodes,
  service type otherwise) and uptime, sorted by node name.
  """
  @spec handle_node_list() :: {:ok, [map()]}
  def handle_node_list do
    set_cli_metadata()

    with :ok <- require_cluster() do
      {ra_members, leader} = get_ra_membership()

      nodes =
        ServiceRegistry.list()
        |> Enum.map(&service_to_node_info(&1, ra_members, leader))
        |> Enum.sort_by(& &1.node)

      {:ok, nodes}
    end
  end

  # Private

  defp service_to_node_info(service, ra_members, leader) do
    server_id = {RaSupervisor.cluster_name(), service.node}
    is_leader = server_id == leader

    role =
      cond do
        service.type != :core -> Atom.to_string(service.type)
        is_leader -> "leader"
        server_id in ra_members -> "follower"
        true -> Atom.to_string(service.type)
      end

    {status, uptime_seconds} =
      case get_remote_uptime(service.node) do
        {:ok, uptime} -> {Atom.to_string(service.status), uptime}
        :unreachable -> {"offline", 0}
      end

    %{
      node: Atom.to_string(service.node),
      type: Atom.to_string(service.type),
      role: role,
      status: status,
      uptime_seconds: uptime_seconds
    }
  end

  defp get_ra_membership do
    case :ra.members(RaSupervisor.server_id(), 1_000) do
      {:ok, members, leader} -> {members, leader}
      _ -> {[], nil}
    end
  end

  defp get_remote_uptime(node) when node == node() do
    {:ok, get_uptime()}
  end

  defp get_remote_uptime(node) do
    # `{:badrpc, reason}` is itself a 2-tuple, so it must be matched before
    # the wall-clock result or it binds as `uptime_ms` and `div/2` raises
    # `:badarith` — crashing `node list` exactly when a node is down.
    case :rpc.call(node, :erlang, :statistics, [:wall_clock], 2_000) do
      {:badrpc, _reason} -> :unreachable
      {uptime_ms, _} -> {:ok, div(uptime_ms, 1000)}
    end
  end
end
