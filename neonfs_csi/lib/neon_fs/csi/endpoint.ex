defmodule NeonFS.CSI.Endpoint do
  @moduledoc """
  gRPC endpoint that exposes the CSI services.

  Declares Identity, Controller, and Node. Only
  the services appropriate for the current `:mode` actually serve
  requests at runtime — the endpoint registers all of them and the
  gRPC layer ignores the irrelevant ones for the active mode.
  """

  use GRPC.Endpoint

  intercept(GRPC.Server.Interceptors.Logger)

  run(NeonFS.CSI.IdentityServer)
  run(NeonFS.CSI.ControllerServer)
  run(NeonFS.CSI.NodeServer)
end
