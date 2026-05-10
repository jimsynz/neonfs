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

  alias Containerd.Services.Content.V1.{Info, InfoResponse, ListContentResponse, UpdateResponse}
  alias Containerd.Services.Content.V1.{Status, WriteContentRequest, WriteContentResponse}
  alias GRPC.RPCError
  alias NeonFS.Client.Router
  alias NeonFS.Containerd.{Digest, Metadata, WriteRegistry, WriteSession, WriteSupervisor}

  # ─── Status / ListStatuses (real impls — return empty) ─────────────

  @doc """
  In-progress write status. Looks up the `WriteSession` for `ref`
  in `WriteRegistry` and returns its current snapshot (cumulative
  bytes, expected total, recorded digest, last-update timestamp).
  Returns `NOT_FOUND` if no session is registered for the ref.
  """
  @spec status(StatusRequest.t(), GRPC.Server.Stream.t()) :: StatusResponse.t()
  def status(%StatusRequest{ref: ref}, _stream) do
    case WriteRegistry.lookup(ref) do
      {:ok, pid} ->
        try do
          %StatusResponse{status: build_status(ref, pid)}
        catch
          :exit, _ ->
            raise RPCError,
              status: :not_found,
              message: "no in-progress write for #{inspect(ref)}"
        end

      :error ->
        raise RPCError, status: :not_found, message: "no in-progress write for #{inspect(ref)}"
    end
  end

  @doc """
  Lists every active in-progress write. Containerd's filter syntax
  on `ListStatusesRequest.filters` is rich; this slice supports a
  single `"ref==<prefix>"` filter for ref-prefix matching, which is
  what `ctr content active` uses. Anything else goes through as an
  unfiltered list.
  """
  @spec list_statuses(ListStatusesRequest.t(), GRPC.Server.Stream.t()) ::
          ListStatusesResponse.t()
  def list_statuses(%ListStatusesRequest{filters: filters}, _stream) do
    prefix = ref_prefix_from_filters(filters)

    statuses =
      WriteRegistry.list_all()
      |> Enum.filter(fn {ref, _pid} -> matches_prefix?(ref, prefix) end)
      |> Enum.flat_map(fn {ref, pid} ->
        try do
          [build_status(ref, pid)]
        catch
          :exit, _ -> []
        end
      end)

    %ListStatusesResponse{statuses: statuses}
  end

  defp build_status(ref, pid) do
    snapshot = WriteSession.stat(pid)

    %Status{
      ref: ref,
      offset: snapshot.offset,
      total: snapshot.total,
      started_at: to_timestamp(snapshot.started_at),
      updated_at: to_timestamp(snapshot.updated_at),
      expected: ""
    }
  end

  defp ref_prefix_from_filters([]), do: nil
  defp ref_prefix_from_filters(nil), do: nil

  defp ref_prefix_from_filters(filters) when is_list(filters) do
    Enum.find_value(filters, fn
      "ref==" <> prefix -> prefix
      _ -> nil
    end)
  end

  defp matches_prefix?(_ref, nil), do: true
  defp matches_prefix?(ref, prefix), do: String.starts_with?(ref, prefix)

  # ─── Skeleton RPCs (return UNIMPLEMENTED) ──────────────────────────

  @doc """
  `Info` RPC. Resolves the digest, looks up the FileMeta, and
  returns the containerd `Info` view (size, timestamps, labels
  extracted from xattrs per the design call in #547).
  """
  @spec info(InfoRequest.t(), GRPC.Server.Stream.t()) :: InfoResponse.t()
  def info(%InfoRequest{digest: digest}, _stream) do
    with {:ok, path} <- Metadata.digest_to_path(digest),
         {:ok, meta} <- core_call(:get_file_meta, [volume_name(), path]),
         {:ok, info_struct} <- Metadata.info_from_file_meta(meta) do
      %InfoResponse{info: info_struct}
    else
      {:error, :not_found} ->
        raise RPCError, status: :not_found, message: "blob #{inspect(digest)} not found"

      {:error, reason} when reason in [:invalid_digest, :unsupported_algorithm] ->
        raise_invalid_digest(reason)

      :error ->
        raise RPCError, status: :internal, message: "blob path doesn't reverse-map to a digest"

      {:error, reason} ->
        raise RPCError, status: :internal, message: "info lookup failed: #{inspect(reason)}"
    end
  end

  @doc """
  `Update` RPC. Honours the `Google.Protobuf.FieldMask` semantics
  containerd's content store expects:

    * empty mask or `"labels"` → replace the entire label map.
    * `"labels.<key>"` → set or clear (empty value) one label.
    * Other mask paths are ignored.

  Reads the current xattrs, applies the mask, writes back via
  `update_file_meta/3` with the merged xattrs map.
  """
  @spec update(UpdateRequest.t(), GRPC.Server.Stream.t()) :: UpdateResponse.t()
  def update(%UpdateRequest{info: nil}, _stream) do
    raise RPCError, status: :invalid_argument, message: "Update requires an Info payload"
  end

  def update(
        %UpdateRequest{info: %Info{digest: digest, labels: req_labels}, update_mask: mask},
        _stream
      ) do
    mask_paths = mask_paths(mask)

    with {:ok, path} <- Metadata.digest_to_path(digest),
         {:ok, meta} <- core_call(:get_file_meta, [volume_name(), path]),
         current_labels = Metadata.extract_labels(Map.get(meta, :xattrs, %{})),
         new_labels = Metadata.apply_label_mask(current_labels, req_labels, mask_paths),
         new_xattrs = Metadata.merge_labels_into_xattrs(Map.get(meta, :xattrs, %{}), new_labels),
         {:ok, updated} <-
           core_call(:update_file_meta, [volume_name(), path, [xattrs: new_xattrs]]),
         {:ok, info_struct} <- Metadata.info_from_file_meta(updated) do
      %UpdateResponse{info: info_struct}
    else
      {:error, :not_found} ->
        raise RPCError, status: :not_found, message: "blob #{inspect(digest)} not found"

      {:error, reason} when reason in [:invalid_digest, :unsupported_algorithm] ->
        raise_invalid_digest(reason)

      :error ->
        raise RPCError, status: :internal, message: "blob path doesn't reverse-map to a digest"

      {:error, reason} ->
        raise RPCError, status: :internal, message: "update failed: #{inspect(reason)}"
    end
  end

  @doc """
  `List` server-streaming RPC. Walks every blob under `sha256/` in
  the configured volume and emits one `ListContentResponse` per
  batch.

  Containerd's filter syntax is rich (a list of strings like
  `labels."key"==value`); this slice supports the no-filter case.
  Filtered listing is a follow-up if it turns out to be load-bearing
  for any real client — `ctr` and BuildKit don't issue label
  filters against the content store in normal flows.
  """
  @spec list(ListContentRequest.t(), GRPC.Server.Stream.t()) :: any()
  def list(%ListContentRequest{filters: filters}, stream) do
    if filters not in [nil, []] do
      raise RPCError,
        status: :unimplemented,
        message: "label filters not yet supported on List"
    end

    case core_call(:list_files_recursive, [volume_name(), "sha256/"]) do
      {:ok, files} ->
        infos =
          files
          |> Enum.map(&Metadata.info_from_file_meta/1)
          |> Enum.flat_map(fn
            {:ok, info} -> [info]
            :error -> []
          end)

        # One response per batch keeps the framing simple. Real
        # containerd accepts either pattern; per-info or batched.
        GRPC.Server.send_reply(stream, %ListContentResponse{info: infos})

      {:error, reason} ->
        raise RPCError, status: :internal, message: "list failed: #{inspect(reason)}"
    end
  end

  @doc """
  `Delete` RPC. Resolves the digest and deletes the file via
  core's `delete_file/2`. Returns `Google.Protobuf.Empty` on
  success.
  """
  @spec delete(DeleteContentRequest.t(), GRPC.Server.Stream.t()) :: Google.Protobuf.Empty.t()
  def delete(%DeleteContentRequest{digest: digest}, _stream) do
    with {:ok, path} <- Metadata.digest_to_path(digest),
         :ok <- core_call(:delete_file, [volume_name(), path]) do
      %Google.Protobuf.Empty{}
    else
      {:ok, _} ->
        # Some core paths return `{:ok, _}` instead of `:ok`. Either
        # way, the file was removed.
        %Google.Protobuf.Empty{}

      {:error, :not_found} ->
        raise RPCError, status: :not_found, message: "blob #{inspect(digest)} not found"

      {:error, reason} when reason in [:invalid_digest, :unsupported_algorithm] ->
        raise_invalid_digest(reason)

      {:error, reason} ->
        raise RPCError, status: :internal, message: "delete failed: #{inspect(reason)}"
    end
  end

  defp mask_paths(nil), do: []
  defp mask_paths(%Google.Protobuf.FieldMask{paths: paths}) when is_list(paths), do: paths
  defp mask_paths(_), do: []

  defp core_call(function, args) do
    case Application.get_env(:neonfs_containerd, :core_call_fn) do
      nil -> Router.call(NeonFS.Core, function, args)
      fun when is_function(fun, 2) -> fun.(function, args)
    end
  end

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
    process_write_stream(request_stream, &GRPC.Server.send_reply(stream, &1))
  end

  @doc """
  Test entry point that mirrors `write/2` but takes a `send_fn`
  instead of a `GRPC.Server.Stream`. Used by
  `ContentServerWriteTest` to exercise the dispatch state machine
  without a real gRPC stream.
  """
  @spec process_write_stream(Enumerable.t(), (WriteContentResponse.t() -> any())) :: :ok
  def process_write_stream(request_stream, send_fn) when is_function(send_fn, 1) do
    _final_state =
      Enum.reduce(request_stream, %{bound_ref: nil}, fn request, state ->
        {bound_request, state} = bind_ref(request, state)
        dispatch_write_frame(bound_request, state, send_fn)
      end)

    :ok
  end

  # Containerd's `WriteContentRequest` proto explicitly states that
  # "once a write stream has started, it may only write to a single
  # ref, thus once a stream is started, the ref may be omitted on
  # subsequent writes". The client (`core/content/proxy/content_writer.go`)
  # populates `Ref` only on the opening STAT frame from
  # `proxyContentStore.negotiate`; every subsequent WRITE / COMMIT /
  # STAT (from `cw.Status()`) is sent with `ref == ""`. The server
  # is responsible for binding the stream to the ref established on
  # the first frame and applying it to every subsequent frame.
  #
  # Without this the WRITE frame's empty ref would route to a
  # different (unbound) session, which would either crash on the
  # `_writes/` path or get torn down with `failed_precondition` —
  # exactly the failure mode #741 was reporting.
  defp bind_ref(%WriteContentRequest{ref: ref} = req, %{bound_ref: nil} = state)
       when ref != "" do
    {req, %{state | bound_ref: ref}}
  end

  defp bind_ref(%WriteContentRequest{ref: ""}, %{bound_ref: nil}) do
    raise RPCError,
      status: :invalid_argument,
      message: "first Write frame must carry a non-empty ref (per containerd proto)"
  end

  defp bind_ref(%WriteContentRequest{ref: ""} = req, %{bound_ref: bound} = state) do
    {%{req | ref: bound}, state}
  end

  defp bind_ref(%WriteContentRequest{ref: ref} = req, %{bound_ref: ref} = state) do
    {req, state}
  end

  defp bind_ref(%WriteContentRequest{ref: ref}, %{bound_ref: bound}) do
    raise RPCError,
      status: :failed_precondition,
      message:
        "ref switched mid-stream: bound to #{inspect(bound)}, frame carried #{inspect(ref)}"
  end

  defp dispatch_write_frame(%WriteContentRequest{action: action} = req, state, send_fn) do
    case action do
      :STAT ->
        handle_stat(req, state, send_fn)

      :WRITE ->
        handle_write_frame(req, state, send_fn)

      :COMMIT ->
        handle_commit_frame(req, state, send_fn)

      other ->
        raise RPCError, status: :invalid_argument, message: "unknown action #{inspect(other)}"
    end
  end

  defp handle_stat(%WriteContentRequest{ref: ref} = req, state, send_fn) do
    pid = ensure_session(ref, req)

    snapshot =
      call_session(pid, ref, fn pid ->
        # Containerd's `proxyContentStore.negotiate` sends the very
        # first STAT with `Total` and `Expected` populated; subsequent
        # in-stream STATs (from `Fetch` / `content.Copy`) keep them
        # populated too. Apply each so the session's `Status` RPC
        # response and the snapshot we hand back here both reflect
        # what the client declared.
        if req.expected != "", do: WriteSession.set_expected(pid, req.expected)
        if req.total > 0, do: WriteSession.set_total(pid, req.total)
        WriteSession.stat(pid)
      end)

    response = %WriteContentResponse{
      action: :STAT,
      started_at: to_timestamp(snapshot.started_at),
      updated_at: to_timestamp(snapshot.updated_at),
      offset: snapshot.offset,
      total: snapshot.total
    }

    emit_frame_telemetry(req, response)
    send_fn.(response)

    Map.put(state, ref, pid)
  end

  defp handle_write_frame(%WriteContentRequest{} = req, state, send_fn) do
    pid = ensure_session(req.ref, req)

    feed_result =
      call_session(pid, req.ref, fn pid ->
        if req.expected != "", do: WriteSession.set_expected(pid, req.expected)
        if req.total > 0, do: WriteSession.set_total(pid, req.total)

        WriteSession.feed(pid, req.data, normalise_offset(req.offset))
      end)

    case feed_result do
      {:ok, _} ->
        snapshot = call_session(pid, req.ref, &WriteSession.stat/1)

        response = %WriteContentResponse{
          action: :WRITE,
          offset: snapshot.offset,
          total: snapshot.total,
          started_at: to_timestamp(snapshot.started_at),
          updated_at: to_timestamp(snapshot.updated_at)
        }

        emit_frame_telemetry(req, response)
        send_fn.(response)

        Map.put(state, req.ref, pid)

      {:error, :offset_mismatch} ->
        snapshot = call_session(pid, req.ref, &WriteSession.stat/1)
        emit_frame_telemetry(req, {:error, :offset_mismatch, snapshot})

        raise RPCError,
          status: :out_of_range,
          message:
            "offset mismatch: containerd asked to resume from #{req.offset} but " <>
              "session is at #{snapshot.offset}"
    end
  end

  defp handle_commit_frame(%WriteContentRequest{} = req, state, send_fn) do
    pid = ensure_session(req.ref, req)

    commit_result =
      call_session(pid, req.ref, fn pid ->
        if req.expected != "", do: WriteSession.set_expected(pid, req.expected)
        if req.total > 0, do: WriteSession.set_total(pid, req.total)

        WriteSession.commit(pid, req.expected)
      end)

    case commit_result do
      {:ok, %{digest: digest, offset: offset, total: total}} ->
        response = %WriteContentResponse{
          action: :COMMIT,
          digest: digest,
          offset: offset,
          total: total,
          started_at: nil,
          updated_at: to_timestamp(DateTime.utc_now())
        }

        emit_frame_telemetry(req, response)
        send_fn.(response)

        Map.delete(state, req.ref)

      {:error, :digest_mismatch} ->
        emit_frame_telemetry(req, {:error, :digest_mismatch})

        raise RPCError,
          status: :invalid_argument,
          message: "digest mismatch: expected #{req.expected}"

      {:error, reason} ->
        emit_frame_telemetry(req, {:error, reason})

        raise RPCError,
          status: :internal,
          message: "commit failed: #{inspect(reason)}"
    end
  end

  defp ensure_session(ref, %WriteContentRequest{} = req) do
    case WriteSupervisor.start_session(ref, session_opts(req)) do
      {:ok, pid} ->
        pid

      {:error, reason} ->
        raise RPCError,
          status: :internal,
          message: "could not start write session for #{inspect(ref)}: #{inspect(reason)}"
    end
  end

  # Translates a `WriteSession` `:exit` from a freshly-terminated
  # session pid (typical race after `commit/2` while the
  # registry's `:DOWN` hasn't propagated yet) into a clean gRPC
  # `failed_precondition` error. Containerd retries the whole
  # `Write` stream when it sees this, which spins up a fresh
  # session — semantically equivalent to "the writer is gone,
  # start over" without us guessing what state to recreate.
  defp call_session(pid, ref, fun) do
    fun.(pid)
  catch
    :exit, {reason, _mfa} when reason in [:normal, :noproc, :shutdown] ->
      raise RPCError,
        status: :failed_precondition,
        message: "write session for #{inspect(ref)} terminated; containerd should retry"
  end

  defp session_opts(%WriteContentRequest{} = req) do
    base_session_opts()
    |> maybe_put_opt(:total, positive_integer(req.total))
    |> maybe_put_opt(:expected, non_empty_string(req.expected))
  end

  defp base_session_opts do
    case Application.get_env(:neonfs_containerd, :chunk_writer_module) do
      nil -> []
      module -> [chunk_writer_module: module]
    end
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

  # Per-frame telemetry for the bidi `Write` stream — captures the
  # request action / ref / declared offset / data size and the
  # response action / offset (or error). Surfaces the offset-leak
  # pattern from #741 (response offset advancing by more than the
  # request data size, or stale state surviving across refs).
  #
  # Event metadata is intentionally rich — debugging containerd
  # protocol behaviour benefits from knowing exactly what each frame
  # carried. Strings are kept short; bytes are not included.
  defp emit_frame_telemetry(%WriteContentRequest{} = req, %WriteContentResponse{} = resp) do
    :telemetry.execute(
      [:neonfs, :containerd, :write_frame],
      %{
        req_offset: req.offset,
        req_data_size: byte_size(req.data || <<>>),
        req_total: req.total,
        resp_offset: resp.offset,
        resp_total: resp.total
      },
      %{
        ref: req.ref,
        action: req.action,
        expected: req.expected,
        result: :ok,
        digest: resp.digest
      }
    )
  end

  defp emit_frame_telemetry(%WriteContentRequest{} = req, {:error, reason}) do
    :telemetry.execute(
      [:neonfs, :containerd, :write_frame],
      %{
        req_offset: req.offset,
        req_data_size: byte_size(req.data || <<>>),
        req_total: req.total
      },
      %{
        ref: req.ref,
        action: req.action,
        expected: req.expected,
        result: :error,
        reason: reason
      }
    )
  end

  defp emit_frame_telemetry(%WriteContentRequest{} = req, {:error, reason, snapshot}) do
    :telemetry.execute(
      [:neonfs, :containerd, :write_frame],
      %{
        req_offset: req.offset,
        req_data_size: byte_size(req.data || <<>>),
        req_total: req.total,
        session_offset: snapshot.offset,
        session_total: snapshot.total
      },
      %{
        ref: req.ref,
        action: req.action,
        expected: req.expected,
        result: :error,
        reason: reason
      }
    )
  end

  @doc """
  `Abort` RPC. Same semantics as `WriteContentRequest{action: ABORT}`
  from #550, exposed as a separate RPC so a client that has already
  closed its bidi-stream can still cancel. Returns `Empty` on
  success, `NOT_FOUND` if no session is registered for the ref.
  """
  @spec abort(AbortRequest.t(), GRPC.Server.Stream.t()) :: Google.Protobuf.Empty.t()
  def abort(%AbortRequest{ref: ref}, _stream) do
    case WriteRegistry.lookup(ref) do
      {:ok, pid} ->
        :ok = WriteSession.abort(pid)
        %Google.Protobuf.Empty{}

      :error ->
        raise RPCError, status: :not_found, message: "no in-progress write for #{inspect(ref)}"
    end
  end
end
