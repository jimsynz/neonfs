defmodule NeonFS.Containerd.ContentServerTest do
  @moduledoc """
  Smoke tests for the scaffold ContentServer's still-stub RPCs.
  `Info` / `Update` / `List` / `Delete` land in #551 (covered in
  `ContentServerMetadataTest`); `Read` / `Write` land in #549 / #550
  (covered separately). `Abort` is left for #552.
  """

  use ExUnit.Case, async: true

  alias Containerd.Services.Content.V1.{
    AbortRequest,
    ListStatusesRequest,
    ListStatusesResponse,
    StatusRequest,
    StatusResponse
  }

  alias NeonFS.Containerd.ContentServer

  describe "Status (real impl)" do
    test "returns an empty StatusResponse" do
      assert %StatusResponse{status: nil} =
               ContentServer.status(%StatusRequest{ref: "anything"}, nil)
    end
  end

  describe "ListStatuses (real impl)" do
    test "returns an empty list" do
      assert %ListStatusesResponse{statuses: []} =
               ContentServer.list_statuses(%ListStatusesRequest{filters: []}, nil)
    end
  end

  describe "skeleton RPCs raise UNIMPLEMENTED" do
    test "Abort" do
      assert_raise GRPC.RPCError, ~r/Abort not implemented/, fn ->
        ContentServer.abort(%AbortRequest{ref: ""}, nil)
      end
    end
  end
end
