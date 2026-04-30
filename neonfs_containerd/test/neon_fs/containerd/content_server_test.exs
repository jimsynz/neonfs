defmodule NeonFS.Containerd.ContentServerTest do
  @moduledoc """
  Smoke checks for `Status` / `ListStatuses` against an empty
  registry — every other RPC has its own dedicated test module
  (`ContentServerReadTest`, `ContentServerMetadataTest`,
  `WriteSessionTest`, `ContentServerStatusTest`).
  """

  use ExUnit.Case, async: false

  alias Containerd.Services.Content.V1.{
    ListStatusesRequest,
    ListStatusesResponse,
    StatusRequest
  }

  alias GRPC.RPCError
  alias NeonFS.Containerd.{ContentServer, WriteRegistry}

  setup do
    case Process.whereis(WriteRegistry) do
      nil -> {:ok, _} = Registry.start_link(keys: :unique, name: WriteRegistry)
      _ -> :ok
    end

    :ok
  end

  describe "Status" do
    test "returns NOT_FOUND when no in-progress write exists for the ref" do
      assert_raise RPCError, ~r/no in-progress write/, fn ->
        ContentServer.status(%StatusRequest{ref: "no-such-ref"}, nil)
      end
    end
  end

  describe "ListStatuses" do
    test "returns an empty list when no writes are in progress" do
      assert %ListStatusesResponse{statuses: []} =
               ContentServer.list_statuses(%ListStatusesRequest{filters: []}, nil)
    end
  end
end
