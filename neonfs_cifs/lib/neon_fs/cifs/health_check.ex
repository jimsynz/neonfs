defmodule NeonFS.CIFS.HealthCheck do
  @moduledoc """
  CIFS-bridge-specific health checks registered with the universal
  `NeonFS.Client.HealthCheck` framework.

  Two checks register at boot:

    * `:cluster` — unhealthy when no core nodes are discoverable
      (quorum unreachable from this bridge's perspective). Mirrors
      the pattern in `NeonFS.Containerd.HealthCheck.check_cluster/0`.
    * `:registrar` — unhealthy when the bridge's `Registrar` isn't
      registered with the cluster's service registry yet.

  The aggregate is surfaced through the universal framework's
  `neonfs node status --json` RPC, which is what the CIFS container
  and systemd health probes query — there is no HTTP endpoint (the
  bridge is a Unix-socket ETF service, matching `neonfs_nfs` and
  `neonfs_containerd`).
  """

  alias NeonFS.Client.{Discovery, HealthCheck}

  @doc """
  Registers CIFS-bridge health checks with the universal framework.
  Idempotent — safe to call from `Application.start/2`.
  """
  @spec register_checks() :: :ok
  def register_checks do
    HealthCheck.register(:cifs,
      cluster: &check_cluster/0,
      registrar: &check_registrar/0
    )
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
    case Process.whereis(NeonFS.Client.Registrar.CIFS) do
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
    case Application.get_env(:neonfs_cifs, :core_nodes_fn) do
      nil -> &Discovery.get_core_nodes/0
      fun when is_function(fun, 0) -> fun
    end
  end
end
