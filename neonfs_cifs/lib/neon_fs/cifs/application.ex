defmodule NeonFS.CIFS.Application do
  @moduledoc """
  OTP application callback for `neonfs_cifs`.

  Starts the supervision tree (UDS listener + service registrar)
  unless `:start_supervisor` is set to `false` in application
  environment. Tests typically disable auto-start so they can spin
  up a listener with a per-test socket path.
  """

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    Logger.metadata(node_name: node())

    if Application.get_env(:neonfs_cifs, :start_supervisor, true) do
      NeonFS.CIFS.Supervisor.start_link()
    else
      Supervisor.start_link([], strategy: :one_for_one, name: __MODULE__)
    end
  end
end
