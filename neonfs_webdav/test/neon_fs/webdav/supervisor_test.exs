defmodule NeonFS.WebDAV.SupervisorTest do
  use ExUnit.Case, async: false

  alias NeonFS.WebDAV.Supervisor

  describe "listener_child_spec/0" do
    test "wires the default drain deadline into the Bandit listener" do
      Application.delete_env(:neonfs_webdav, :drain_deadline_ms)

      {Bandit, opts} = Supervisor.listener_child_spec()

      assert get_in(opts, [:thousand_island_options, :shutdown_timeout]) == 25_000
    end

    test "respects a configured drain deadline" do
      Application.put_env(:neonfs_webdav, :drain_deadline_ms, 12_000)
      on_exit(fn -> Application.delete_env(:neonfs_webdav, :drain_deadline_ms) end)

      {Bandit, opts} = Supervisor.listener_child_spec()

      assert get_in(opts, [:thousand_island_options, :shutdown_timeout]) == 12_000
    end
  end
end
