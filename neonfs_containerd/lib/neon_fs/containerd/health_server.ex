defmodule NeonFS.Containerd.HealthServer do
  @moduledoc """
  Standard gRPC Health Checking Protocol server.

  Maps `NeonFS.Containerd.HealthCheck` aggregate status onto the
  three states the protocol defines:

    * `:SERVING` — every registered check is healthy.
    * `:NOT_SERVING` — at least one check is unhealthy. The most
      common cause is `:cluster` being unreachable (no core node
      discoverable, i.e. quorum lost from this plugin's perspective).
    * `:SERVICE_UNKNOWN` — the caller asked about a service this
      plugin doesn't host. The protocol allows plugins to report on
      siblings, but `neonfs_containerd` only knows about itself.

  `Watch` is supported and re-emits whenever the aggregate status
  flips. The check polls every 5s — enough to track quorum loss
  without burning CPU on a healthy cluster.
  """

  use GRPC.Server, service: Grpc.Health.V1.Health.Service

  alias Grpc.Health.V1.HealthCheckRequest
  alias Grpc.Health.V1.HealthCheckResponse
  alias Grpc.Health.V1.HealthListRequest
  alias Grpc.Health.V1.HealthListResponse

  alias NeonFS.Containerd.HealthCheck

  @watch_poll_ms 5_000

  @service_name "containerd.services.content.v1.Content"

  @doc """
  `grpc.health.v1.Health.Check` RPC. Returns the current serving
  status of the named service — `""` and the Content service share
  the plugin's aggregate health; anything else returns
  `SERVICE_UNKNOWN`.
  """
  @spec check(HealthCheckRequest.t(), GRPC.Server.Stream.t()) :: HealthCheckResponse.t()
  def check(%HealthCheckRequest{service: service}, _stream) do
    %HealthCheckResponse{status: status_for(service)}
  end

  @doc """
  `grpc.health.v1.Health.List` RPC. Reports every service this
  plugin knows about — currently `""` (overall) and the Content
  service. They share aggregate health.
  """
  @spec list(HealthListRequest.t(), GRPC.Server.Stream.t()) :: HealthListResponse.t()
  def list(_request, _stream) do
    %HealthListResponse{
      statuses: %{
        "" => %HealthCheckResponse{status: status_for("")},
        @service_name => %HealthCheckResponse{status: status_for(@service_name)}
      }
    }
  end

  @doc """
  `grpc.health.v1.Health.Watch` server-streaming RPC. Sends an
  initial status, then re-sends only when the aggregate flips.
  Polls `HealthCheck.aggregate/0` every 5s — fast enough to track
  quorum loss without burning CPU on a healthy cluster.
  """
  @spec watch(HealthCheckRequest.t(), GRPC.Server.Stream.t()) :: no_return()
  def watch(%HealthCheckRequest{service: service}, stream) do
    send_status(stream, status_for(service))
    watch_loop(stream, service, status_for(service))
  end

  defp watch_loop(stream, service, last_status) do
    Process.sleep(@watch_poll_ms)
    current = status_for(service)

    if current == last_status do
      watch_loop(stream, service, last_status)
    else
      send_status(stream, current)
      watch_loop(stream, service, current)
    end
  end

  defp send_status(stream, status) do
    GRPC.Server.send_reply(stream, %HealthCheckResponse{status: status})
  end

  # An empty service string queries the overall server health, per
  # the gRPC Health protocol spec. The Content service shares the
  # same health as the server because it's the only thing this
  # plugin hosts. Anything else is unknown.
  defp status_for(""), do: aggregate_status()
  defp status_for(@service_name), do: aggregate_status()
  defp status_for(_other), do: :SERVICE_UNKNOWN

  defp aggregate_status do
    case HealthCheck.aggregate() do
      :healthy -> :SERVING
      _ -> :NOT_SERVING
    end
  end
end
