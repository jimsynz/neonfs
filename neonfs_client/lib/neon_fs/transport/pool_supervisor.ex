defmodule NeonFS.Transport.PoolSupervisor do
  @moduledoc """
  DynamicSupervisor for `NeonFS.Transport.ConnPool` instances.

  Each peer node gets its own ConnPool under this supervisor. If a ConnPool
  crashes, only that pool is restarted — other peer connections are unaffected.
  """

  use DynamicSupervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    DynamicSupervisor.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Starts a new ConnPool under this supervisor.
  """
  @spec start_pool(GenServer.server(), keyword()) :: DynamicSupervisor.on_start_child()
  def start_pool(supervisor \\ __MODULE__, pool_opts) do
    child_spec = %{
      id: NeonFS.Transport.ConnPool,
      start: {NeonFS.Transport.ConnPool, :start_link, [pool_opts]},
      type: :worker,
      restart: :temporary
    }

    DynamicSupervisor.start_child(supervisor, child_spec)
  end

  @impl DynamicSupervisor
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
