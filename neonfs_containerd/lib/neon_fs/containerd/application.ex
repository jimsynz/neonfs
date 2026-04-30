defmodule NeonFS.Containerd.Application do
  @moduledoc """
  OTP application callback for `neonfs_containerd`.

  Boots the supervision tree (gRPC server + service registrar)
  unless `:start_supervisor` is set to `false` in the application
  environment. Tests typically disable auto-start so each test can
  spin up an isolated listener on a per-test socket path.
  """

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    Logger.metadata(node_name: node())

    if Application.get_env(:neonfs_containerd, :start_supervisor, true) do
      NeonFS.Containerd.Supervisor.start_link()
    else
      Supervisor.start_link([], strategy: :one_for_one, name: __MODULE__)
    end
  end
end
