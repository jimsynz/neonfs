defmodule NeonFS.Client.RegistrarTest do
  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Client.{Connection, CostFunction, Discovery, Registrar, Router}

  setup do
    start_supervised!({Connection, bootstrap_nodes: []})
    start_supervised!(Discovery)
    start_supervised!(CostFunction)
    :ok
  end

  test "stores the configured interval" do
    pid = start_supervised!({Registrar, interval_ms: 12_345, metadata: %{}, type: :nfs})
    state = :sys.get_state(pid)

    assert state.interval_ms == 12_345
  end

  test "keeps retrying registration when no core node is reachable" do
    pid = start_supervised!({Registrar, interval_ms: 60_000, metadata: %{}, type: :nfs})

    send(pid, :register)
    state = :sys.get_state(pid)

    assert Process.alive?(pid)
    refute state.registered?
  end

  test "traps exits so terminate/2 runs on supervisor shutdown (#1386)" do
    pid = start_supervised!({Registrar, interval_ms: 60_000, metadata: %{}, type: :nfs})

    assert {:trap_exit, true} = Process.info(pid, :trap_exit)
  end

  describe "deregister on shutdown (#1386)" do
    setup :set_mimic_global

    test "deregisters the service when the supervisor stops it" do
      test_pid = self()

      stub(Router, :call, fn
        NeonFS.Core.ServiceRegistry, :register, _args ->
          :ok

        NeonFS.Core.ServiceRegistry, :deregister, _args ->
          send(test_pid, :deregistered)
          :ok

        _module, _function, _args ->
          {:error, :stubbed}
      end)

      pid = start_supervised!({Registrar, interval_ms: 60_000, metadata: %{}, type: :nfs})
      assert :sys.get_state(pid).registered?

      :ok = stop_supervised(Registrar)

      assert_receive :deregistered, 1_000
    end
  end
end
