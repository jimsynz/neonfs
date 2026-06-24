defmodule NeonFS.S3.ShutdownDrainTest do
  @moduledoc """
  Exercises the Bandit/ThousandIsland drain that #1382 configures via
  `thousand_island_options[:shutdown_timeout]`: on shutdown the listener
  stops accepting new connections but lets an in-flight request finish
  within the drain deadline rather than being cut.

  The drain mechanism is ThousandIsland's and is shared by every Bandit
  interface (S3, WebDAV, docker), so this behavioural test lives here once;
  the per-interface supervisor tests assert each supervisor wires the
  configured deadline into the listener.
  """
  use ExUnit.Case, async: false

  defmodule SlowPlug do
    @behaviour Plug

    import Plug.Conn

    @impl true
    def init(opts), do: opts

    @impl true
    def call(conn, opts) do
      send(opts[:test_pid], {:request_started, self()})

      receive do
        :proceed -> :ok
      after
        5_000 -> :ok
      end

      send_resp(conn, 200, "drained")
    end
  end

  test "an in-flight request completes while the listener drains on shutdown" do
    test_pid = self()

    {:ok, server} =
      Bandit.start_link(
        plug: {SlowPlug, test_pid: test_pid},
        scheme: :http,
        port: 0,
        ip: :loopback,
        startup_log: false,
        thousand_island_options: [shutdown_timeout: 5_000]
      )

    {:ok, {_addr, port}} = ThousandIsland.listener_info(server)

    request =
      Task.async(fn ->
        {:ok, sock} =
          :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])

        :ok = :gen_tcp.send(sock, "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
        recv_until_closed(sock, "")
      end)

    assert_receive {:request_started, handler_pid}, 2_000

    # Begin shutdown while the request is mid-flight. `Supervisor.stop` blocks
    # until the drain completes, so run it off the test process.
    shutdown = Task.async(fn -> Supervisor.stop(server) end)

    send(handler_pid, :proceed)

    response = Task.await(request, 10_000)
    assert response =~ "200"
    assert response =~ "drained"

    Task.await(shutdown, 10_000)
  end

  defp recv_until_closed(sock, acc) do
    case :gen_tcp.recv(sock, 0, 5_000) do
      {:ok, chunk} -> recv_until_closed(sock, acc <> chunk)
      {:error, :closed} -> acc
    end
  end
end
