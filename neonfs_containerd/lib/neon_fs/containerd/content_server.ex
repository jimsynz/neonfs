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
    ReadContentResponse,
    StatusRequest,
    StatusResponse,
    UpdateRequest
  }

  alias Containerd.Services.Content.V1.WriteContentRequest
  alias Containerd.Services.Content.V1.WriteContentResponse
  alias GRPC.RPCError
  alias NeonFS.Containerd.{Digest, WriteSession, WriteSupervisor}

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

  @doc """
  `Read` server-streaming RPC. Resolves the digest to a path under
  the configured content-store volume (sharded `sha256/<ab>/<cd>/<rest>`
  per the layout decision in #547), opens a chunk stream via
  `NeonFS.Client.ChunkReader.read_file_stream/3`, and emits
  `ReadContentResponse` frames carrying ≤ 64 KiB of `data` each.

  Honours the request's `offset` (skip prefix) and `size` (cap total
  bytes) window. The `offset` field on each response is the
  cumulative bytes streamed from the *start of the read window*, not
  the absolute offset in the blob — matching containerd's local
  store and what the client expects.

  Errors map to gRPC status codes:

    * `{:error, :not_found}` → `NOT_FOUND` (digest unknown).
    * `{:error, :forbidden}` → `PERMISSION_DENIED`.
    * `{:error, :invalid_digest}` / `:unsupported_algorithm` →
      `INVALID_ARGUMENT`.
    * Anything else → `INTERNAL`.

  Stream cancellation by the client is honoured implicitly — the
  gRPC server raises `GRPC.RPCError` from inside `send_reply/2` when
  the underlying transport closes, which propagates out and tears
  down the chunk stream.
  """
  @spec read(ReadContentRequest.t(), GRPC.Server.Stream.t()) :: any()
  def read(%ReadContentRequest{digest: digest, offset: offset, size: size}, stream) do
    case Digest.to_path(digest) do
      {:ok, path} ->
        stream_blob(path, offset, size, &GRPC.Server.send_reply(stream, &1))

      {:error, reason} ->
        raise_invalid_digest(reason)
    end
  end

  @max_response_bytes 64 * 1024

  @doc false
  # Public for testing — `send_fn` is the per-frame emitter so tests
  # can capture replies without spinning up a gRPC server. Production
  # passes a closure over `GRPC.Server.send_reply/2`.
  @spec stream_blob(String.t(), integer() | nil, integer() | nil, (ReadContentResponse.t() ->
                                                                     any())) ::
          :ok | no_return()
  def stream_blob(path, offset, size, send_fn) when is_function(send_fn, 1) do
    volume = volume_name()
    read_opts = build_read_opts(offset, size)
    chunk_reader = chunk_reader_module()

    case chunk_reader.read_file_stream(volume, path, read_opts) do
      {:ok, %{stream: chunk_stream}} ->
        emit_chunks(chunk_stream, send_fn)

      {:error, :not_found} ->
        raise RPCError, status: :not_found, message: "blob #{inspect(path)} not found"

      {:error, :forbidden} ->
        raise RPCError, status: :permission_denied, message: "read forbidden"

      {:error, reason} ->
        raise RPCError,
          status: :internal,
          message: "blob read failed: #{inspect(reason)}"
    end
  end

  defp emit_chunks(chunk_stream, send_fn) do
    _final_offset =
      chunk_stream
      |> Stream.flat_map(&split_oversized_chunk/1)
      |> Enum.reduce(0, fn chunk, sent ->
        reply = %ReadContentResponse{offset: sent, data: chunk}
        send_fn.(reply)
        sent + byte_size(chunk)
      end)

    :ok
  end

  defp split_oversized_chunk(chunk) when byte_size(chunk) <= @max_response_bytes, do: [chunk]

  defp split_oversized_chunk(chunk) do
    <<head::binary-size(@max_response_bytes), rest::binary>> = chunk
    [head | split_oversized_chunk(rest)]
  end

  defp build_read_opts(offset, size) do
    []
    |> maybe_put_opt(:offset, positive_integer(offset))
    |> maybe_put_opt(:length, positive_integer(size))
  end

  defp maybe_put_opt(opts, _key, nil), do: opts
  defp maybe_put_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp positive_integer(n) when is_integer(n) and n > 0, do: n
  defp positive_integer(_), do: nil

  defp volume_name do
    Application.get_env(:neonfs_containerd, :volume, "containerd")
  end

  defp chunk_reader_module do
    Application.get_env(:neonfs_containerd, :chunk_reader, NeonFS.Client.ChunkReader)
  end

  defp raise_invalid_digest(:unsupported_algorithm) do
    raise RPCError,
      status: :invalid_argument,
      message: "only sha256 digests are supported"
  end

  defp raise_invalid_digest(_) do
    raise RPCError,
      status: :invalid_argument,
      message: "malformed digest"
  end

  @doc """
  `Write` bidi-streaming RPC. Each `WriteContentRequest` carries an
  action (`STAT` / `WRITE` / `COMMIT`), the in-progress write `ref`
  (opaque, caller-supplied), an optional `expected` digest, an
  optional `total` size, and (for `WRITE`) a `data` payload.

  The server tracks each `ref`'s state in a `WriteSession`
  GenServer registered via `WriteRegistry` so partial writes survive
  bidi-stream disconnects (containerd reuses the same ref to
  resume). On `COMMIT` the session verifies the running SHA-256
  hash equals `expected` and atomically lands the chunks at the
  canonical `sha256/<ab>/<cd>/<rest>` path (the layout decision in
  #547). A digest mismatch surfaces as `INVALID_ARGUMENT` and the
  partial chunks orphan to core-side GC.

  This handler does **not** support the `Abort` action — that's a
  separate `Abort` RPC under #552.
  """
  @spec write(Enumerable.t(), GRPC.Server.Stream.t()) :: any()
  def write(request_stream, stream) do
    _final_state =
      Enum.reduce(request_stream, %{}, fn request, state ->
        dispatch_write_frame(request, state, stream)
      end)

    :ok
  end

  defp dispatch_write_frame(%WriteContentRequest{action: action} = req, state, stream) do
    case action do
      :STAT ->
        handle_stat(req, state, stream)

      :WRITE ->
        handle_write_frame(req, state, stream)

      :COMMIT ->
        handle_commit_frame(req, state, stream)

      other ->
        raise RPCError, status: :invalid_argument, message: "unknown action #{inspect(other)}"
    end
  end

  defp handle_stat(%WriteContentRequest{ref: ref}, state, stream) do
    pid = ensure_session(ref)
    snapshot = WriteSession.stat(pid)

    GRPC.Server.send_reply(stream, %WriteContentResponse{
      action: :STAT,
      started_at: to_timestamp(snapshot.started_at),
      updated_at: to_timestamp(snapshot.updated_at),
      offset: snapshot.offset,
      total: snapshot.total
    })

    Map.put(state, ref, pid)
  end

  defp handle_write_frame(%WriteContentRequest{} = req, state, _stream) do
    pid = ensure_session(req.ref, req)

    if req.expected != "", do: WriteSession.set_expected(pid, req.expected)
    if req.total > 0, do: WriteSession.set_total(pid, req.total)

    case WriteSession.feed(pid, req.data, normalise_offset(req.offset)) do
      {:ok, _} ->
        Map.put(state, req.ref, pid)

      {:error, :offset_mismatch} ->
        raise RPCError,
          status: :out_of_range,
          message:
            "offset mismatch: containerd asked to resume from #{req.offset} but " <>
              "session is at #{WriteSession.stat(pid).offset}"
    end
  end

  defp handle_commit_frame(%WriteContentRequest{} = req, state, stream) do
    pid = ensure_session(req.ref, req)

    if req.expected != "", do: WriteSession.set_expected(pid, req.expected)
    if req.total > 0, do: WriteSession.set_total(pid, req.total)

    case WriteSession.commit(pid, req.expected) do
      {:ok, %{digest: digest, offset: offset, total: total}} ->
        GRPC.Server.send_reply(stream, %WriteContentResponse{
          action: :COMMIT,
          digest: digest,
          offset: offset,
          total: total,
          started_at: nil,
          updated_at: to_timestamp(DateTime.utc_now())
        })

        Map.delete(state, req.ref)

      {:error, :digest_mismatch} ->
        raise RPCError,
          status: :invalid_argument,
          message: "digest mismatch: expected #{req.expected}"

      {:error, reason} ->
        raise RPCError,
          status: :internal,
          message: "commit failed: #{inspect(reason)}"
    end
  end

  defp ensure_session(ref, req \\ nil) do
    case WriteSupervisor.start_session(ref, session_opts(req)) do
      {:ok, pid} ->
        pid

      {:error, reason} ->
        raise RPCError,
          status: :internal,
          message: "could not start write session for #{inspect(ref)}: #{inspect(reason)}"
    end
  end

  defp session_opts(nil), do: []

  defp session_opts(%WriteContentRequest{} = req) do
    []
    |> maybe_put_opt(:total, positive_integer(req.total))
    |> maybe_put_opt(:expected, non_empty_string(req.expected))
  end

  defp non_empty_string(""), do: nil
  defp non_empty_string(s) when is_binary(s), do: s
  defp non_empty_string(_), do: nil

  defp normalise_offset(0), do: nil
  defp normalise_offset(n) when is_integer(n) and n > 0, do: n
  defp normalise_offset(_), do: nil

  defp to_timestamp(%DateTime{} = dt) do
    seconds = DateTime.to_unix(dt, :second)
    nanos = (DateTime.to_unix(dt, :microsecond) - seconds * 1_000_000) * 1_000
    %Google.Protobuf.Timestamp{seconds: seconds, nanos: nanos}
  end

  @doc "Stub for `Abort` RPC — lands in #552."
  @spec abort(AbortRequest.t(), GRPC.Server.Stream.t()) :: no_return()
  def abort(_request, _stream), do: raise_unimplemented("Abort", 552)

  defp raise_unimplemented(rpc_name, issue) do
    raise RPCError,
      status: :unimplemented,
      message: "#{rpc_name} not implemented in scaffold; lands in ##{issue}"
  end
end
