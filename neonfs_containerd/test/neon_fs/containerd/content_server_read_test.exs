defmodule NeonFS.Containerd.ContentServerReadTest do
  @moduledoc """
  Tests for the `Read` server-streaming RPC (#549). Exercises the
  digest → path resolution, the stub-driven chunk stream, the
  per-frame size cap, and the gRPC error mapping. Goes through the
  refactored `stream_blob/4` entry point so the tests don't need a
  real gRPC stream.
  """

  use ExUnit.Case, async: false

  alias Containerd.Services.Content.V1.{ReadContentRequest, ReadContentResponse}
  alias GRPC.RPCError
  alias NeonFS.Containerd.{ContentServer, Digest, StubChunkReader}

  setup do
    Application.put_env(:neonfs_containerd, :chunk_reader, StubChunkReader)
    Application.put_env(:neonfs_containerd, :volume, "containerd-test")

    on_exit(fn ->
      Application.delete_env(:neonfs_containerd, :chunk_reader)
      Application.delete_env(:neonfs_containerd, :volume)
    end)

    :ok
  end

  @valid_digest "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  @expected_path "sha256/01/23/456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

  describe "Read happy path" do
    test "streams every chunk through ReadContentResponse with cumulative offsets" do
      StubChunkReader.set_response({:ok, %{stream: ["alpha", "beta", "gamma"], file_size: 13}})

      replies = capture_replies(@valid_digest, 0, 0)

      assert [
               %ReadContentResponse{offset: 0, data: "alpha"},
               %ReadContentResponse{offset: 5, data: "beta"},
               %ReadContentResponse{offset: 9, data: "gamma"}
             ] = replies
    end

    test "splits chunks larger than the 64 KiB per-frame cap" do
      big = :binary.copy(<<0xAB>>, 100 * 1024)
      StubChunkReader.set_response({:ok, %{stream: [big], file_size: byte_size(big)}})

      replies = capture_replies(@valid_digest, 0, 0)

      assert [
               %ReadContentResponse{offset: 0, data: head},
               %ReadContentResponse{offset: 65_536, data: tail}
             ] =
               replies

      assert byte_size(head) == 65_536
      assert byte_size(tail) == 100 * 1024 - 65_536
      assert head <> tail == big
    end

    test "passes offset/size through to ChunkReader as :offset/:length" do
      StubChunkReader.set_response({:ok, %{stream: ["x"], file_size: 1}})

      capture_replies(@valid_digest, 256, 1024)

      assert ["containerd-test", @expected_path, opts] = StubChunkReader.last_args()
      assert opts[:offset] == 256
      assert opts[:length] == 1024
    end

    test "drops zero/missing offset and size from the read opts" do
      StubChunkReader.set_response({:ok, %{stream: ["x"], file_size: 1}})

      capture_replies(@valid_digest, 0, 0)

      assert ["containerd-test", @expected_path, []] = StubChunkReader.last_args()
    end
  end

  describe "Read error mapping" do
    test "unknown digest → NOT_FOUND" do
      StubChunkReader.set_response({:error, :not_found})

      assert_grpc_status(:not_found, fn -> capture_replies(@valid_digest, 0, 0) end)
    end

    test "forbidden → PERMISSION_DENIED" do
      StubChunkReader.set_response({:error, :forbidden})

      assert_grpc_status(:permission_denied, fn -> capture_replies(@valid_digest, 0, 0) end)
    end

    test "any other error → INTERNAL" do
      StubChunkReader.set_response({:error, :weird})

      assert_grpc_status(:internal, fn -> capture_replies(@valid_digest, 0, 0) end)
    end

    test "unsupported digest algorithm → INVALID_ARGUMENT" do
      assert_grpc_status(:invalid_argument, fn ->
        ContentServer.read(%ReadContentRequest{digest: "sha512:abc", offset: 0, size: 0}, nil)
      end)
    end

    test "malformed digest → INVALID_ARGUMENT" do
      assert_grpc_status(:invalid_argument, fn ->
        ContentServer.read(
          %ReadContentRequest{digest: "sha256:tooshort", offset: 0, size: 0},
          nil
        )
      end)
    end
  end

  describe "streaming invariant" do
    test "peak in-flight buffer is bounded by chunk size, not blob size" do
      # 100 MiB simulated in 1 MiB chunks. Each chunk gets split into
      # 64 KiB frames; the test asserts no single frame exceeds 64 KiB
      # so the gRPC framing layer never sees a multi-MiB iolist.
      chunks =
        Stream.repeatedly(fn -> :binary.copy(<<0>>, 1024 * 1024) end)
        |> Stream.take(100)

      StubChunkReader.set_response({:ok, %{stream: chunks, file_size: 100 * 1024 * 1024}})

      max_frame =
        capture_replies(@valid_digest, 0, 0)
        |> Stream.map(fn %ReadContentResponse{data: d} -> byte_size(d) end)
        |> Enum.max()

      assert max_frame == 65_536
    end
  end

  defp capture_replies(digest, offset, size) do
    parent = self()
    ref = make_ref()

    send_fn = fn reply -> send(parent, {ref, reply}) end

    {:ok, path} = Digest.to_path(digest)
    ContentServer.stream_blob(path, positive_or_nil(offset), positive_or_nil(size), send_fn)

    drain_replies(ref, [])
  end

  defp positive_or_nil(0), do: 0
  defp positive_or_nil(n), do: n

  defp drain_replies(ref, acc) do
    receive do
      {^ref, reply} -> drain_replies(ref, [reply | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp assert_grpc_status(expected_status, fun) do
    fun.()

    flunk(
      "expected GRPC.RPCError with status #{inspect(expected_status)}, but no error was raised"
    )
  rescue
    e in RPCError ->
      assert e.status == expected_code(expected_status),
             "expected status #{inspect(expected_status)}, got #{inspect(e.status)}"
  end

  # GRPC.RPCError.status holds the numeric code, even though new/2
  # accepts the atom form — see the `grpc` package's `RPCError.new/2`.
  defp expected_code(:not_found), do: GRPC.Status.not_found()
  defp expected_code(:permission_denied), do: GRPC.Status.permission_denied()
  defp expected_code(:invalid_argument), do: GRPC.Status.invalid_argument()
  defp expected_code(:internal), do: GRPC.Status.internal()
end
