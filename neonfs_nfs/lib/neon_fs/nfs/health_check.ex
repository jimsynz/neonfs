defmodule NeonFS.NFS.HealthCheck do
  @moduledoc """
  NFS-specific health checks registered with the universal framework.
  """

  alias NeonFS.Client.HealthCheck
  alias NeonFS.NFS.{ExportManager, MetadataCache}

  @doc """
  Registers NFS health checks with the universal framework.
  """
  @spec register_checks() :: :ok
  def register_checks do
    HealthCheck.register(:nfs,
      registrar: &check_registrar/0,
      exports: &check_exports/0,
      metadata_cache: &check_metadata_cache/0
    )
  end

  defp check_registrar do
    case Process.whereis(NeonFS.Client.Registrar.NFS) do
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

  defp check_exports do
    if Process.whereis(ExportManager) do
      exports = ExportManager.list_exports()
      %{status: :healthy, export_count: length(exports)}
    else
      %{status: :unhealthy, reason: :not_running}
    end
  rescue
    _ -> %{status: :unhealthy, reason: :not_available}
  end

  defp check_metadata_cache do
    if Process.whereis(MetadataCache) do
      %{status: :healthy}
    else
      %{status: :unhealthy, reason: :not_running}
    end
  end
end
