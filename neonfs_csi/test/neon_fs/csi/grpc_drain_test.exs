defmodule NeonFS.CSI.GRPCDrainTest do
  @moduledoc """
  Asserts the CSI gRPC endpoint drains an in-flight call on graceful
  shutdown rather than cutting it (#1384).

  grpc (0.11, over ranch) already grants in-flight calls up to ranch's
  connection-drain window (~5s) when the listener terminates: ranch's
  `terminate/2` gracefully shuts down active connections. This test
  pins that behaviour end to end against the real `Endpoint` over a
  Unix-socket connection — a `CreateVolume` call held inside its
  handler keeps `Supervisor.stop/1` of the gRPC supervisor blocked
  until the call returns, then both complete cleanly.

  `NeonFS.CSI.Supervisor` wraps this same `GRPC.Server.Supervisor` and
  orders the `Registrar` after it so it deregisters first; the drain
  semantics are identical whether started here directly or as that
  supervisor's child.
  """
  use ExUnit.Case, async: false

  alias Csi.V1.{
    CapacityRange,
    Controller,
    CreateVolumeRequest,
    CreateVolumeResponse,
    VolumeCapability
  }

  alias NeonFS.Core.Volume

  setup do
    socket_path =
      Path.join(
        System.tmp_dir!(),
        "neonfs_csi_drain_#{System.unique_integer([:positive])}.sock"
      )

    start_supervised!({GRPC.Client.Supervisor, []})

    on_exit(fn ->
      Application.delete_env(:neonfs_csi, :core_call_fn)
      File.rm(socket_path)
    end)

    %{socket_path: socket_path}
  end

  test "an in-flight CreateVolume call drains before the listener shuts down", %{
    socket_path: socket_path
  } do
    test = self()

    # Block the CreateVolume handler inside core_call so the call is
    # genuinely in-flight when shutdown begins, signalling the test
    # from inside the handler so synchronisation is event-driven.
    Application.put_env(:neonfs_csi, :core_call_fn, fn NeonFS.Core,
                                                       :create_volume,
                                                       [name, _opts] ->
      send(test, {:in_flight, self()})

      receive do
        {:proceed} -> {:ok, sample_volume(name)}
      end
    end)

    File.rm(socket_path)

    {:ok, sup} =
      GRPC.Server.Supervisor.start_link(
        endpoint: NeonFS.CSI.Endpoint,
        port: 0,
        start_server: true,
        adapter_opts: [ip: {:local, socket_path}]
      )

    on_exit(fn -> if Process.alive?(sup), do: Supervisor.stop(sup) end)

    {:ok, channel} = GRPC.Stub.connect("unix:" <> socket_path)

    req = %CreateVolumeRequest{
      name: "pvc-drain",
      capacity_range: %CapacityRange{required_bytes: 1024, limit_bytes: 0},
      volume_capabilities: [
        %VolumeCapability{access_mode: %VolumeCapability.AccessMode{mode: :SINGLE_NODE_WRITER}}
      ]
    }

    caller = Task.async(fn -> Controller.Stub.create_volume(channel, req) end)

    assert_receive {:in_flight, handler}, 5_000

    # Begin graceful shutdown while the call is held. `stop/1` blocks
    # until the endpoint terminates, which ranch defers until the
    # in-flight connection drains.
    stopper = Task.async(fn -> Supervisor.stop(sup) end)

    # The drain must be waiting on the in-flight call: shutdown cannot
    # have finished while the handler is still blocked. A non-draining
    # (brutal) teardown would complete near-instantly and this would
    # observe `:ok`.
    refute Task.yield(stopper, 300),
           "shutdown completed without waiting for the in-flight gRPC call to drain"

    send(handler, {:proceed})

    assert {:ok, %CreateVolumeResponse{}} = Task.await(caller, 5_000)
    assert :ok = Task.await(stopper, 5_000)
  end

  defp sample_volume(name) do
    %Volume{
      id: "vol-id-" <> name,
      name: name,
      logical_size: 0,
      physical_size: 0,
      chunk_count: 0,
      created_at: DateTime.from_unix!(0),
      updated_at: DateTime.from_unix!(0)
    }
  end
end
