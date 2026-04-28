defmodule NeonFS.NFS.Application do
  @moduledoc """
  OTP Application for neonfs_nfs.

  Starts the NFS supervision tree and handles graceful shutdown.

  The NFSv3 stack is the native-BEAM `NFSServer.NFSv3.Handler` bound
  to `NeonFS.NFS.NFSv3Backend` plus `NFSServer.Mount.Handler` bound
  to `NeonFS.NFS.MountBackend`. The legacy `nfs3_server` NIF was
  removed in sub-issue #657 of #286.
  """

  use Application
  require Logger

  alias NeonFS.NFS.{ExportManager, HealthCheck, NFSv3Backend, Supervisor}
  alias NFSServer.NFSv3.Handler, as: NFSv3Handler

  @doc """
  Build the bound NFSv3 handler module for the BEAM stack — the same
  shape `RPC.Dispatcher` expects in its program map.
  """
  @spec bound_nfsv3_handler() :: module()
  def bound_nfsv3_handler do
    NFSv3Handler.with_backend(NFSv3Backend)
  end

  @impl true
  def start(_type, _args) do
    Logger.metadata(node_name: node())

    start_supervisor? = Application.get_env(:neonfs_nfs, :start_supervisor, true)

    result =
      if start_supervisor? do
        Supervisor.start_link()
      else
        Elixir.Supervisor.start_link([], strategy: :one_for_one, name: __MODULE__)
      end

    if start_supervisor? do
      HealthCheck.register_checks()
    end

    unless Application.spec(:neonfs_omnibus) do
      NeonFS.Systemd.notify_ready()
    end

    result
  end

  @impl true
  def stop(_state) do
    # Graceful shutdown: unexport all volumes
    case Process.whereis(ExportManager) do
      nil ->
        :ok

      _pid ->
        Enum.each(ExportManager.list_exports(), fn export ->
          Logger.debug("Unexporting volume before shutdown", volume_name: export.volume_name)
          ExportManager.unexport(export.id)
        end)
    end

    :ok
  end
end
