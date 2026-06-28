defmodule NeonFS.Containerd.GRPCDrainTest do
  @moduledoc """
  Asserts the containerd content-store gRPC endpoint drains an
  in-flight call on graceful shutdown rather than cutting it (#1384).

  grpc (0.11, over ranch) already grants in-flight calls up to ranch's
  connection-drain window (~5s) when the listener terminates: ranch's
  `terminate/2` gracefully shuts down active connections. This test
  pins that behaviour end to end against the real `Endpoint` over a
  Unix-socket connection — a call held inside its handler keeps
  `Supervisor.stop/1` of the gRPC supervisor blocked until the call
  returns, then both complete cleanly.

  `NeonFS.Containerd.Supervisor` wraps this same `GRPC.Server.Supervisor`
  and orders the `Registrar` after it so it deregisters first; the
  drain semantics are identical whether started here directly or as
  that supervisor's child.
  """
  use ExUnit.Case, async: false

  alias Containerd.Services.Content.V1.{Content, InfoRequest, InfoResponse}

  @valid_digest "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  @valid_path "sha256/01/23/456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

  setup do
    socket_path =
      Path.join(
        System.tmp_dir!(),
        "neonfs_containerd_drain_#{System.unique_integer([:positive])}.sock"
      )

    start_supervised!({GRPC.Client.Supervisor, []})

    Application.put_env(:neonfs_containerd, :listener, :socket)
    Application.put_env(:neonfs_containerd, :socket_path, socket_path)
    Application.put_env(:neonfs_containerd, :register_service, false)
    Application.put_env(:neonfs_containerd, :volume, "containerd-test")

    on_exit(fn ->
      for k <- [:listener, :socket_path, :register_service, :volume, :core_call_fn] do
        Application.delete_env(:neonfs_containerd, k)
      end

      File.rm(socket_path)
    end)

    %{socket_path: socket_path}
  end

  test "an in-flight Info call drains before the listener shuts down", %{
    socket_path: socket_path
  } do
    test = self()

    # Block the Info handler inside core_call so the call is genuinely
    # in-flight when shutdown begins, signalling the test from inside
    # the handler so synchronisation is event-driven, never timed.
    Application.put_env(:neonfs_containerd, :core_call_fn, fn :get_file_meta, _args ->
      send(test, {:in_flight, self()})

      receive do
        {:proceed, meta} -> {:ok, meta}
      end
    end)

    File.rm(socket_path)

    {:ok, sup} =
      GRPC.Server.Supervisor.start_link(
        endpoint: NeonFS.Containerd.Endpoint,
        port: 0,
        start_server: true,
        adapter_opts: [ip: {:local, socket_path}]
      )

    on_exit(fn -> if Process.alive?(sup), do: Supervisor.stop(sup) end)

    {:ok, channel} = GRPC.Stub.connect("unix:" <> socket_path)

    caller = Task.async(fn -> Content.Stub.info(channel, %InfoRequest{digest: @valid_digest}) end)

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

    send(handler, {:proceed, file_meta()})

    assert {:ok, %InfoResponse{}} = Task.await(caller, 5_000)
    assert :ok = Task.await(stopper, 5_000)
  end

  defp file_meta do
    %{
      path: @valid_path,
      size: 42,
      created_at: DateTime.from_unix!(1_700_000_000, :second),
      updated_at: DateTime.from_unix!(1_700_000_500, :second),
      xattrs: %{"containerd.io/foo" => "bar"}
    }
  end
end
