defmodule NeonFS.Core.BanditLauncherTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.BanditLauncher

  defmodule EchoPlug do
    @behaviour Plug

    @impl true
    def init(opts), do: opts

    @impl true
    def call(conn, _opts), do: Plug.Conn.send_resp(conn, 200, "ok")
  end

  describe "start_link/1" do
    test "starts Bandit when the listener binds cleanly (happy path)" do
      opts = [plug: EchoPlug, scheme: :http, port: 0, ip: :loopback]

      assert {:ok, pid} = BanditLauncher.start_link(opts)
      assert Process.alive?(pid)
      assert {:ok, {_ip, port}} = ThousandIsland.listener_info(pid)
      assert is_integer(port) and port > 0

      on_exit(fn ->
        if Process.alive?(pid) do
          Process.unlink(pid)
          Process.exit(pid, :shutdown)
        end
      end)
    end

    test "propagates non-transient errors from Bandit.start_link" do
      # Claim a port, then try to bind to it — emits :eaddrinuse which is
      # not the transient `:inet_async` timeout, so the launcher should
      # surface it rather than retry.
      {:ok, sock} = :gen_tcp.listen(0, ip: {127, 0, 0, 1}, reuseaddr: true)
      {:ok, port} = :inet.port(sock)

      opts = [plug: EchoPlug, scheme: :http, port: port, ip: :loopback]

      try do
        # Bandit may report the bind failure via either an {:error, _}
        # return or a linked-process exit; either way it must not retry
        # indefinitely and must bubble out of the launcher.
        result =
          try do
            BanditLauncher.start_link(opts)
          catch
            :exit, reason -> {:caught_exit, reason}
          end

        case result do
          {:error, _reason} -> :ok
          {:caught_exit, _reason} -> :ok
          other -> flunk("expected {:error, _} or caught exit, got #{inspect(other)}")
        end
      after
        :gen_tcp.close(sock)
      end
    end
  end

  describe "transient_listener_error?/1 (via retry behaviour)" do
    # Retry behaviour is covered indirectly — directly triggering the
    # `{:inet_async, :timeout}` flake from a unit test isn't feasible
    # (it's a load-dependent condition inside ThousandIsland). The
    # production benefit is validated by observing the flake no longer
    # invalidating `:shared`-mode test modules in CI after this lands.
  end
end
