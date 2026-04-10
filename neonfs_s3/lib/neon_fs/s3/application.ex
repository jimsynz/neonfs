defmodule NeonFS.S3.Application do
  @moduledoc """
  OTP Application for neonfs_s3.

  Starts the S3 supervision tree including client infrastructure
  and the Bandit HTTP server running S3Server.Plug.
  """

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    Logger.metadata(node_name: node())

    start_supervisor? = Application.get_env(:neonfs_s3, :start_supervisor, true)

    result =
      if start_supervisor? do
        NeonFS.S3.Supervisor.start_link()
      else
        Supervisor.start_link([], strategy: :one_for_one, name: __MODULE__)
      end

    unless Application.spec(:neonfs_omnibus) do
      NeonFS.Systemd.notify_ready()
    end

    result
  end
end
