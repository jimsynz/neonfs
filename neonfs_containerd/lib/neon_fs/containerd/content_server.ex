defmodule NeonFS.Containerd.ContentServer do
  @moduledoc """
  Skeleton gRPC handler for `containerd.services.content.v1.Content`.

  This is the package-scaffold slice (#548). Most RPCs return
  `GRPC.RPCError{status: :unimplemented}` with a pointer to the
  sub-issue that will land them. Two RPCs return real (empty)
  responses so containerd's plugin-probing handshake succeeds:

    * `Status` — the in-progress write tracker (#552). Returning
      `{:ok, empty}` on an unknown ref is the same shape containerd
      sees when no upload is active for that ref.
    * `ListStatuses` — the in-progress listing (#552). Empty list is
      what containerd expects when there are no active writes.

  The remaining RPCs raise the standard gRPC `UNIMPLEMENTED` status
  so containerd's error handling kicks in cleanly rather than seeing
  malformed replies.
  """

  use GRPC.Server, service: Containerd.Services.Content.V1.Content.Service

  alias Containerd.Services.Content.V1.{
    AbortRequest,
    DeleteContentRequest,
    InfoRequest,
    ListContentRequest,
    ListStatusesRequest,
    ListStatusesResponse,
    ReadContentRequest,
    StatusRequest,
    StatusResponse,
    UpdateRequest
  }

  alias GRPC.RPCError

  # ─── Status / ListStatuses (real impls — return empty) ─────────────

  @doc """
  In-progress write status. Returns the empty `StatusResponse`
  shape (`status: nil`) — once #552 lands this looks up the actual
  write in the in-progress tracker.
  """
  @spec status(StatusRequest.t(), GRPC.Server.Stream.t()) :: StatusResponse.t()
  def status(_request, _stream) do
    %StatusResponse{status: nil}
  end

  @doc """
  Lists currently in-progress writes. Empty until #552 lands.
  """
  @spec list_statuses(ListStatusesRequest.t(), GRPC.Server.Stream.t()) ::
          ListStatusesResponse.t()
  def list_statuses(_request, _stream) do
    %ListStatusesResponse{statuses: []}
  end

  # ─── Skeleton RPCs (return UNIMPLEMENTED) ──────────────────────────

  @doc "Stub for `Info` RPC — lands in #551."
  @spec info(InfoRequest.t(), GRPC.Server.Stream.t()) :: no_return()
  def info(_request, _stream), do: raise_unimplemented("Info", 551)

  @doc "Stub for `Update` RPC — lands in #551."
  @spec update(UpdateRequest.t(), GRPC.Server.Stream.t()) :: no_return()
  def update(_request, _stream), do: raise_unimplemented("Update", 551)

  @doc "Stub for `List` RPC — lands in #551."
  @spec list(ListContentRequest.t(), GRPC.Server.Stream.t()) :: no_return()
  def list(_request, _stream), do: raise_unimplemented("List", 551)

  @doc "Stub for `Delete` RPC — lands in #551."
  @spec delete(DeleteContentRequest.t(), GRPC.Server.Stream.t()) :: no_return()
  def delete(_request, _stream), do: raise_unimplemented("Delete", 551)

  @doc "Stub for `Read` server-streaming RPC — lands in #549."
  @spec read(ReadContentRequest.t(), GRPC.Server.Stream.t()) :: no_return()
  def read(_request, _stream), do: raise_unimplemented("Read", 549)

  @doc "Stub for `Write` bidi-streaming RPC — lands in #550."
  @spec write(Enumerable.t(), GRPC.Server.Stream.t()) :: no_return()
  def write(_request_stream, _stream), do: raise_unimplemented("Write", 550)

  @doc "Stub for `Abort` RPC — lands in #552."
  @spec abort(AbortRequest.t(), GRPC.Server.Stream.t()) :: no_return()
  def abort(_request, _stream), do: raise_unimplemented("Abort", 552)

  defp raise_unimplemented(rpc_name, issue) do
    raise RPCError,
      status: :unimplemented,
      message: "#{rpc_name} not implemented in scaffold; lands in ##{issue}"
  end
end
