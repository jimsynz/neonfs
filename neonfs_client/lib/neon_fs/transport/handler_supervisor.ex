defmodule NeonFS.Transport.HandlerSupervisor do
  @moduledoc """
  DynamicSupervisor for `NeonFS.Transport.Handler` processes.

  Each accepted TLS connection gets its own Handler process, supervised here.
  Handler failures are isolated — a crashed Handler does not affect the Listener
  or other active connections.
  """

  use DynamicSupervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    DynamicSupervisor.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Starts a new Handler process under this supervisor.
  """
  @spec start_handler(GenServer.server(), keyword()) :: DynamicSupervisor.on_start_child()
  def start_handler(supervisor \\ __MODULE__, handler_opts) do
    DynamicSupervisor.start_child(supervisor, {NeonFS.Transport.Handler, handler_opts})
  end

  @impl DynamicSupervisor
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
