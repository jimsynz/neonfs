defmodule NeonFS.Client.RegistrarTest do
  use ExUnit.Case, async: false

  alias NeonFS.Client.{Connection, CostFunction, Discovery, Registrar}

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
end
