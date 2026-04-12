defmodule NeonFS.WebDAV.HealthCheck do
  @moduledoc """
  WebDAV-specific health checks registered with the universal framework.
  """

  alias NeonFS.Client.Discovery
  alias NeonFS.Client.HealthCheck

  @doc "Registers WebDAV health checks with the universal framework."
  @spec register_checks() :: :ok
  def register_checks do
    HealthCheck.register(:webdav,
      registrar: &check_registrar/0
    )
  end

  @doc "Returns the current cluster status for the health endpoint."
  @spec cluster_status() :: %{
          status: :ok | :degraded | :unavailable,
          writable: boolean(),
          readable: boolean(),
          reason: String.t() | nil,
          quorum_reachable: boolean()
        }
  def cluster_status do
    cluster_status(core_nodes_fn())
  end

  @doc "Returns the current cluster status using the given core node lookup function."
  @spec cluster_status((-> [node()])) :: %{
          status: :ok | :degraded | :unavailable,
          writable: boolean(),
          readable: boolean(),
          reason: String.t() | nil,
          quorum_reachable: boolean()
        }
  def cluster_status(core_nodes_fn) when is_function(core_nodes_fn, 0) do
    case core_nodes_fn.() do
      nodes when is_list(nodes) and nodes != [] ->
        %{
          status: :ok,
          writable: true,
          readable: true,
          reason: nil,
          quorum_reachable: true
        }

      _ ->
        %{
          status: :unavailable,
          writable: false,
          readable: false,
          reason: "no-core-nodes",
          quorum_reachable: false
        }
    end
  end

  defp core_nodes_fn do
    case Application.get_env(:neonfs_webdav, :core_nodes_fn) do
      nil -> &Discovery.get_core_nodes/0
      fun when is_function(fun, 0) -> fun
    end
  end

  defp check_registrar do
    case Process.whereis(NeonFS.Client.Registrar.WebDAV) do
      nil ->
        %{status: :unhealthy, reason: :not_running}

      pid ->
        state = :sys.get_state(pid)

        if state.registered? do
          %{status: :healthy}
        else
          %{status: :degraded, reason: :not_registered}
        end
    end
  rescue
    _ -> %{status: :unhealthy, reason: :not_available}
  end
end
