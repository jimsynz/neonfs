defmodule NeonFS.Docker.HealthCheck do
  @moduledoc """
  Docker plugin-specific health checks registered with the universal
  `NeonFS.Client.HealthCheck` framework.

  Each check returns a map with `:status` (`:healthy | :degraded |
  :unhealthy`) plus optional metadata. The framework aggregates the
  worst status across every registered check.
  """

  alias NeonFS.Client.HealthCheck
  alias NeonFS.Docker.{MountTracker, VolumeStore}

  @doc """
  Registers Docker-plugin health checks with the universal framework.
  """
  @spec register_checks() :: :ok
  def register_checks do
    HealthCheck.register(:docker,
      registrar: &check_registrar/0,
      volume_store: &check_volume_store/0,
      mount_tracker: &check_mount_tracker/0
    )
  end

  defp check_registrar do
    case Process.whereis(NeonFS.Client.Registrar.Docker) do
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

  defp check_volume_store do
    if Process.whereis(VolumeStore) do
      volumes = VolumeStore.list()
      %{status: :healthy, volume_count: length(volumes)}
    else
      %{status: :unhealthy, reason: :not_running}
    end
  rescue
    _ -> %{status: :unhealthy, reason: :not_available}
  end

  defp check_mount_tracker do
    if Process.whereis(MountTracker) do
      mounts = MountTracker.list()
      %{status: :healthy, mount_count: length(mounts)}
    else
      %{status: :unhealthy, reason: :not_running}
    end
  rescue
    _ -> %{status: :unhealthy, reason: :not_available}
  end
end
