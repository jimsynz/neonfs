defmodule NeonFS.FUSE.HealthCheck do
  @moduledoc """
  FUSE-specific health checks registered with the universal framework.
  """

  alias NeonFS.Client.HealthCheck
  alias NeonFS.FUSE.{InodeTable, MountManager}

  @doc """
  Registers FUSE health checks with the universal framework.
  """
  @spec register_checks() :: :ok
  def register_checks do
    HealthCheck.register(:fuse,
      registrar: &check_registrar/0,
      mounts: &check_mounts/0,
      inode_table: &check_inode_table/0
    )
  end

  defp check_registrar do
    case Process.whereis(NeonFS.Client.Registrar.FUSE) do
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

  defp check_mounts do
    if Process.whereis(MountManager) do
      mounts = MountManager.list_mounts()
      %{status: :healthy, mount_count: length(mounts)}
    else
      %{status: :unhealthy, reason: :not_running}
    end
  rescue
    _ -> %{status: :unhealthy, reason: :not_available}
  end

  defp check_inode_table do
    if Process.whereis(InodeTable) do
      %{status: :healthy}
    else
      %{status: :unhealthy, reason: :not_running}
    end
  end
end
