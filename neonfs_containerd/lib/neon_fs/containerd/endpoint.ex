defmodule NeonFS.Containerd.Endpoint do
  @moduledoc """
  gRPC endpoint that exposes the containerd content store services.

  Two services are registered:

    * `containerd.services.content.v1.Content` — image-layer content
      store. The streaming Read / Write RPCs land in #549 / #550;
      metadata RPCs in #551. This scaffold (#548) wires every RPC to
      a skeleton handler — most return `UNIMPLEMENTED`, with `Status`
      / `ListStatuses` returning empty in-progress lists.
    * `grpc.health.v1.Health` — standard gRPC Health Checking
      Protocol so `grpc_health_probe` can verify the plugin is
      reachable and the underlying NeonFS cluster has quorum.
  """

  use GRPC.Endpoint

  intercept(GRPC.Server.Interceptors.Logger)

  run(NeonFS.Containerd.ContentServer)
  run(NeonFS.Containerd.HealthServer)
end
