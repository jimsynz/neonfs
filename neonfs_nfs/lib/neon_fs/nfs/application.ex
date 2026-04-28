defmodule NeonFS.NFS.Application do
  @moduledoc """
  OTP Application for neonfs_nfs.

  Starts the NFS supervision tree and handles graceful shutdown.

  ## Handler stack selection

  `:handler_stack` (default `:beam`) selects which NFSv3 implementation
  the application uses:

    * `:beam` — the native-BEAM stack: `NFSServer.NFSv3.Handler` bound
      to `NeonFS.NFS.NFSv3Backend` plus `NFSServer.Mount.Handler` bound
      to `NeonFS.NFS.MountBackend`. The default since the cutover
      (#656 of #286).
    * `:nif` — the legacy Rust `nfs3_server` NIF + `NeonFS.NFS.Handler`
      shim. Transitional fallback; slated for deletion in #657.

  Set via `config :neonfs_nfs, handler_stack: :nif` or the
  `NEONFS_NFS_HANDLER_STACK` environment variable in releases to opt
  into the legacy stack.
  """

  use Application
  require Logger

  alias NeonFS.NFS.{ExportManager, HealthCheck, NFSv3Backend, Supervisor}
  alias NFSServer.NFSv3.Handler, as: NFSv3Handler

  @doc """
  Returns the active handler stack — `:beam` (default) or `:nif`.
  """
  @spec handler_stack() :: :nif | :beam
  def handler_stack do
    case Application.get_env(:neonfs_nfs, :handler_stack, :beam) do
      :nif -> :nif
      _ -> :beam
    end
  end

  @doc """
  Build the handler module for the active stack.

  For `:beam`, returns the result of
  `NFSServer.NFSv3.Handler.with_backend(NeonFS.NFS.NFSv3Backend)` —
  the same shape `RPC.Dispatcher` expects in its program map. For
  `:nif`, raises `:not_applicable` because the NIF stack uses
  `NeonFS.NFS.Handler` directly without going through the BEAM
  dispatcher.
  """
  @spec bound_nfsv3_handler() :: module()
  def bound_nfsv3_handler do
    case handler_stack() do
      :beam -> NFSv3Handler.with_backend(NFSv3Backend)
      :nif -> raise ArgumentError, ":nif stack does not bind through NFSServer.NFSv3.Handler"
    end
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
