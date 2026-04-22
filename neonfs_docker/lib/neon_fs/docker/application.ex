defmodule NeonFS.Docker.Application do
  @moduledoc """
  OTP application callback for `neonfs_docker`.
  """

  use Application
  require Logger

  alias NeonFS.Docker.HealthCheck

  @impl true
  def start(_type, _args) do
    Logger.metadata(node_name: node())

    start_supervisor? = Application.get_env(:neonfs_docker, :start_supervisor, true)

    result =
      if start_supervisor? do
        NeonFS.Docker.Supervisor.start_link()
      else
        Supervisor.start_link([], strategy: :one_for_one, name: __MODULE__)
      end

    if start_supervisor?, do: HealthCheck.register_checks()

    result
  end
end
