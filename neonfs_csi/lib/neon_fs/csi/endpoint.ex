defmodule NeonFS.CSI.Endpoint do
  @moduledoc """
  gRPC endpoint that exposes the CSI services.

  This first slice (#313) only exposes the Identity service. The
  Controller (#314) and Node (#315) services are added in their own
  sub-issues; the endpoint declaration here grows to include them
  when those slices land.
  """

  use GRPC.Endpoint

  intercept(GRPC.Server.Interceptors.Logger)

  run(NeonFS.CSI.IdentityServer)
end
