defmodule NeonFS.CSI.Endpoint do
  @moduledoc """
  gRPC endpoint that exposes the CSI services.

  Declares Identity (#313) and Controller (#314); the Node service
  (#315) gets added when that slice lands. Only the services
  appropriate for the current `:mode` actually serve requests at
  runtime — the endpoint registers all of them and the gRPC layer
  ignores the irrelevant ones for the active mode.
  """

  use GRPC.Endpoint

  intercept(GRPC.Server.Interceptors.Logger)

  run(NeonFS.CSI.IdentityServer)
  run(NeonFS.CSI.ControllerServer)
end
