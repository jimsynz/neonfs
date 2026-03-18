defmodule NeonFS.Omnibus.Application do
  @moduledoc """
  Application callback for NeonFS Omnibus.

  Starts the FUSE and NFS services after the core application is already
  running (core is a runtime dependency, so OTP starts it before us).
  """

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    Logger.info("Starting NeonFS in omnibus mode")

    opts = [strategy: :one_for_one, name: NeonFS.Omnibus.Application]

    with {:ok, pid} <- Supervisor.start_link([], opts),
         {:ok, _} <- Application.ensure_all_started(:neonfs_fuse),
         :ok <- Logger.info("NeonFS FUSE started"),
         {:ok, _} <- Application.ensure_all_started(:neonfs_nfs),
         :ok <- Logger.info("NeonFS NFS started") do
      {:ok, pid}
    end
  end
end
