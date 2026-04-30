defmodule NeonFS.Containerd.ContentServerTest do
  @moduledoc """
  Smoke tests for the scaffold ContentServer. Most RPCs raise
  `GRPC.RPCError{status: :unimplemented}` and will be replaced as the
  dependent sub-issues land. Status / ListStatuses return real (empty)
  shapes so the containerd plugin handshake completes.
  """

  use ExUnit.Case, async: true

  alias Containerd.Services.Content.V1.{
    AbortRequest,
    DeleteContentRequest,
    InfoRequest,
    ListContentRequest,
    ListStatusesRequest,
    ListStatusesResponse,
    StatusRequest,
    StatusResponse,
    UpdateRequest
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
    test "Info" do
      assert_raise GRPC.RPCError, ~r/Info not implemented/, fn ->
        ContentServer.info(%InfoRequest{digest: ""}, nil)
      end
    end

    test "Update" do
      assert_raise GRPC.RPCError, ~r/Update not implemented/, fn ->
        ContentServer.update(%UpdateRequest{}, nil)
      end
    end

    test "List" do
      assert_raise GRPC.RPCError, ~r/List not implemented/, fn ->
        ContentServer.list(%ListContentRequest{filters: []}, nil)
      end
    end

    test "Delete" do
      assert_raise GRPC.RPCError, ~r/Delete not implemented/, fn ->
        ContentServer.delete(%DeleteContentRequest{digest: ""}, nil)
      end
    end

    test "Abort" do
      assert_raise GRPC.RPCError, ~r/Abort not implemented/, fn ->
        ContentServer.abort(%AbortRequest{ref: ""}, nil)
      end
    end
  end
end
