defmodule NeonFS.Containerd.HealthCheck do
  @moduledoc """
  Containerd-plugin-specific health checks registered with the
  universal `NeonFS.Client.HealthCheck` framework.

  Two checks register at boot:

    * `:cluster` — unhealthy when no core nodes are discoverable
      (quorum unreachable from this plugin's perspective). Mirrors
      the pattern in `NeonFS.S3.HealthCheck.cluster_status/0` and
      `NeonFS.Docker.HealthCheck.check_cluster/0`.
    * `:registrar` — unhealthy when the plugin's `Registrar` isn't
      registered with the cluster's service registry yet.

  The aggregate is exposed via `aggregate/0` so the gRPC Health
  server (`NeonFS.Containerd.HealthServer`) can map onto the three
  `grpc.health.v1` `ServingStatus` values.
  """

  alias NeonFS.Client.{Discovery, HealthCheck}

  @doc """
  Registers containerd-plugin health checks with the universal
  framework. Idempotent — safe to call from supervisor `init/1`.
  """
  @spec register_checks() :: :ok
  def register_checks do
    HealthCheck.register(:containerd,
      cluster: &check_cluster/0,
      registrar: &check_registrar/0
    )
  end

  @doc """
  Aggregate health: `:healthy`, `:degraded`, or `:unhealthy`.

  Used by the gRPC Health server to decide between `SERVING` and
  `NOT_SERVING`. Treats `:degraded` as `NOT_SERVING` because
  containerd routes around plugins reporting anything other than
  `SERVING`.
  """
  @spec aggregate() :: :healthy | :degraded | :unhealthy
  def aggregate do
    HealthCheck.check() |> Map.get(:status, :unhealthy)
  end

  defp check_cluster do
    case core_nodes_fn().() do
      nodes when is_list(nodes) and nodes != [] ->
        %{status: :healthy}

      _ ->
        %{status: :unhealthy, reason: :no_core_nodes}
    end
  end

  defp check_registrar do
    case Process.whereis(NeonFS.Client.Registrar.Containerd) do
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

  defp core_nodes_fn do
    case Application.get_env(:neonfs_containerd, :core_nodes_fn) do
      nil -> &Discovery.get_core_nodes/0
      fun when is_function(fun, 0) -> fun
    end
  end
end
