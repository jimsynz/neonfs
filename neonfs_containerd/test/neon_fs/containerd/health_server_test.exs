defmodule NeonFS.Containerd.HealthServerTest do
  @moduledoc """
  Smoke tests for the gRPC Health Checking Protocol server. Calls
  the handler directly — covering the gRPC-layer wire-up belongs in
  an integration test (out of scope for the scaffold slice).
  """

  use ExUnit.Case, async: false

  alias Grpc.Health.V1.{HealthCheckRequest, HealthCheckResponse}
  alias NeonFS.Client.HealthCheck
  alias NeonFS.Containerd.HealthServer

  @service_name "containerd.services.content.v1.Content"

  setup do
    HealthCheck.reset()
    NeonFS.Containerd.HealthCheck.register_checks()

    on_exit(fn ->
      HealthCheck.reset()
      Application.delete_env(:neonfs_containerd, :core_nodes_fn)
    end)

    :ok
  end

  describe "Check" do
    test "reports SERVING when every registered check is healthy" do
      Application.put_env(:neonfs_containerd, :core_nodes_fn, fn -> [Node.self()] end)
      stub_registrar_running()

      assert %HealthCheckResponse{status: :SERVING} =
               HealthServer.check(%HealthCheckRequest{service: ""}, nil)

      assert %HealthCheckResponse{status: :SERVING} =
               HealthServer.check(%HealthCheckRequest{service: @service_name}, nil)
    end

    test "reports NOT_SERVING when no core nodes are discoverable" do
      Application.put_env(:neonfs_containerd, :core_nodes_fn, fn -> [] end)
      stub_registrar_running()

      assert %HealthCheckResponse{status: :NOT_SERVING} =
               HealthServer.check(%HealthCheckRequest{service: ""}, nil)
    end

    test "reports NOT_SERVING when the registrar isn't running" do
      Application.put_env(:neonfs_containerd, :core_nodes_fn, fn -> [Node.self()] end)
      # No registrar stub — the check returns :unhealthy.

      assert %HealthCheckResponse{status: :NOT_SERVING} =
               HealthServer.check(%HealthCheckRequest{service: ""}, nil)
    end

    test "reports SERVICE_UNKNOWN for an unrelated service name" do
      Application.put_env(:neonfs_containerd, :core_nodes_fn, fn -> [Node.self()] end)
      stub_registrar_running()

      assert %HealthCheckResponse{status: :SERVICE_UNKNOWN} =
               HealthServer.check(%HealthCheckRequest{service: "some.other.Service"}, nil)
    end
  end

  defp stub_registrar_running do
    {:ok, agent} = Agent.start_link(fn -> %{registered?: true} end)

    Process.register(agent, NeonFS.Client.Registrar.Containerd)

    on_exit(&cleanup_stub_registrar/0)
  end

  # Race: the agent process can exit between `Process.whereis` and
  # `Process.unregister`, freeing the name. `unregister` then
  # raises ArgumentError. Tolerate that explicitly.
  defp cleanup_stub_registrar do
    pid = Process.whereis(NeonFS.Client.Registrar.Containerd)
    if pid, do: safe_unregister_and_stop(pid)
    :ok
  end

  defp safe_unregister_and_stop(pid) do
    try do
      Process.unregister(NeonFS.Client.Registrar.Containerd)
    rescue
      ArgumentError -> :ok
    end

    if Process.alive?(pid), do: Process.exit(pid, :normal)
  end
end
