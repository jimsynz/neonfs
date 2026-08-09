defmodule NeonFS.Block.Application do
  @moduledoc """
  OTP application callback for `neonfs_block`.

  Starts the supervision tree unless `:start_supervisor` is set to `false`,
  which tests do so they can start a listener on a per-test port rather than
  contending for the configured one.
  """

  use Application

  @impl Application
  def start(_type, _args) do
    Logger.metadata(node_name: node())

    if Application.get_env(:neonfs_block, :start_supervisor, true) do
      NeonFS.Block.Supervisor.start_link()
    else
      Supervisor.start_link([], strategy: :one_for_one, name: __MODULE__)
    end
  end
end
