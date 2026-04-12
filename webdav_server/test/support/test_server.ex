defmodule WebdavServer.Test.TestServer do
  @moduledoc """
  Starts a Bandit HTTP server serving WebdavServer.Plug for external client testing.
  """

  @spec start(keyword()) :: {:ok, port :: pos_integer()}
  def start(opts \\ []) do
    backend = Keyword.get(opts, :backend, WebdavServer.Test.MemoryBackend)

    plug_opts = [backend: backend]

    {:ok, server} =
      Bandit.start_link(
        plug: {WebdavServer.Plug, plug_opts},
        port: 0,
        ip: :loopback,
        startup_log: false
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {:ok, port}
  end
end
