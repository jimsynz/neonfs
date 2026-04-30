defmodule NeonFS.Containerd.ContentServerStatusTest do
  @moduledoc """
  Tests for the `Status` / `ListStatuses` / `Abort` RPCs (#552)
  driven against live `WriteSession` GenServers. Stubs the
  ChunkWriter via `StubChunkWriter` and the core RPC plane via
  `:core_call_fn` so the tests don't need a running cluster.
  """

  use ExUnit.Case, async: false

  alias Containerd.Services.Content.V1.{
    AbortRequest,
    ListStatusesRequest,
    ListStatusesResponse,
    Status,
    StatusRequest,
    StatusResponse
  }

  alias GRPC.RPCError

  alias NeonFS.Containerd.{
    ContentServer,
    StubChunkWriter,
    WriteSession,
    WriteSupervisor
  }

  setup do
    StubChunkWriter.start({:ok, []})
    Application.put_env(:neonfs_containerd, :volume, "containerd-test")

    on_exit(fn ->
      StubChunkWriter.stop()
      Application.delete_env(:neonfs_containerd, :volume)
    end)

    :ok
  end

  defp start_session(ref, opts \\ []) do
    opts = Keyword.merge([ref: ref, chunk_writer_module: StubChunkWriter], opts)
    WriteSupervisor.start_session(ref, opts)
  end

  describe "Status" do
    test "returns offset / total / timestamps for a known ref" do
      {:ok, pid} = start_session("status-known", total: 1024)
      WriteSession.feed(pid, "hello")

      assert %StatusResponse{status: %Status{} = status} =
               ContentServer.status(%StatusRequest{ref: "status-known"}, nil)

      assert status.ref == "status-known"
      assert status.offset == 5
      assert status.total == 1024
      assert %Google.Protobuf.Timestamp{} = status.started_at
      assert %Google.Protobuf.Timestamp{} = status.updated_at

      _ = WriteSession.abort(pid)
    end

    test "raises NOT_FOUND for an unknown ref" do
      assert_raise RPCError, ~r/no in-progress write/, fn ->
        ContentServer.status(%StatusRequest{ref: "missing"}, nil)
      end
    end
  end

  describe "ListStatuses" do
    test "lists every active session" do
      {:ok, p1} = start_session("ls-a")
      {:ok, p2} = start_session("ls-b")
      {:ok, p3} = start_session("ls-c")
      WriteSession.feed(p1, "a")
      WriteSession.feed(p2, "bb")
      WriteSession.feed(p3, "ccc")

      assert %ListStatusesResponse{statuses: statuses} =
               ContentServer.list_statuses(%ListStatusesRequest{filters: []}, nil)

      refs = Enum.map(statuses, & &1.ref)
      assert "ls-a" in refs
      assert "ls-b" in refs
      assert "ls-c" in refs

      Enum.each([p1, p2, p3], fn pid -> _ = WriteSession.abort(pid) end)
    end

    test "applies a `ref==<prefix>` filter" do
      {:ok, p1} = start_session("prefixed-a")
      {:ok, p2} = start_session("prefixed-b")
      {:ok, p3} = start_session("other-c")

      assert %ListStatusesResponse{statuses: statuses} =
               ContentServer.list_statuses(
                 %ListStatusesRequest{filters: ["ref==prefixed-"]},
                 nil
               )

      refs = Enum.map(statuses, & &1.ref) |> Enum.sort()
      assert refs == ["prefixed-a", "prefixed-b"]

      Enum.each([p1, p2, p3], fn pid -> _ = WriteSession.abort(pid) end)
    end
  end

  describe "Abort" do
    test "aborts an active session and returns Empty" do
      {:ok, pid} = start_session("abort-me")
      WriteSession.feed(pid, "data")

      assert %Google.Protobuf.Empty{} =
               ContentServer.abort(%AbortRequest{ref: "abort-me"}, nil)

      refute Process.alive?(pid)

      assert_raise RPCError, fn ->
        ContentServer.status(%StatusRequest{ref: "abort-me"}, nil)
      end
    end

    test "raises NOT_FOUND for an unknown ref" do
      assert_raise RPCError, ~r/no in-progress write/, fn ->
        ContentServer.abort(%AbortRequest{ref: "missing"}, nil)
      end
    end
  end

  describe "stale-partial sweep" do
    test "abort_stale aborts sessions whose updated_at is older than the cutoff" do
      {:ok, pid_old} = start_session("stale-old")
      {:ok, pid_new} = start_session("stale-new")
      WriteSession.feed(pid_old, "x")
      WriteSession.feed(pid_new, "y")

      # Hand-roll an "ancient" updated_at so the sweep targets one
      # session deterministically without sleeping a real day.
      :sys.replace_state(pid_old, fn s ->
        %{s | updated_at: DateTime.add(DateTime.utc_now(), -100_000, :second)}
      end)

      assert ["stale-old"] = WriteSession.abort_stale(86_400)

      refute Process.alive?(pid_old)
      assert Process.alive?(pid_new)

      _ = WriteSession.abort(pid_new)
    end
  end
end
