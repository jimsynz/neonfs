defmodule NeonFS.WebDAV.Application do
  @moduledoc """
  OTP Application for neonfs_webdav.

  Starts the WebDAV supervision tree including client infrastructure
  and the Bandit HTTP server running Davy.Plug.
  """

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    Logger.metadata(node_name: node())

    start_supervisor? = Application.get_env(:neonfs_webdav, :start_supervisor, true)

    result =
      if start_supervisor? do
        NeonFS.WebDAV.Supervisor.start_link()
      else
        Supervisor.start_link([], strategy: :one_for_one, name: __MODULE__)
      end

    unless Application.spec(:neonfs_omnibus) do
      NeonFS.Systemd.notify_ready()
    end

    result
  end
end
