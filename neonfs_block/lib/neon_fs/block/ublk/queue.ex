defmodule NeonFS.Block.Ublk.Queue do
  @moduledoc """
  One ublk queue's IO loop.

  Reads a request frame from the helper's socket, answers it through
  `NeonFS.Block.Frontend`, writes a reply frame. One request in flight at a
  time on this socket, which is what makes the helper's tag match trivially
  correct — concurrency across a device is queues, not pipelining within
  one.

  A read's payload is streamed: the frame's length is announced from the
  request, and each chunk goes to the socket as it arrives, so the largest
  request an export advertises costs one chunk of memory rather than the
  range. That is the same shape `NeonFS.Block.ConnectionHandler` uses for
  NBD, and for the same reason.

  The loop ends when the socket does. A queue that cannot read is a queue
  that cannot answer, and its `Target` takes the device down rather than
  leaving it half-serving.
  """

  alias NeonFS.Block.Frontend
  alias NeonFS.Block.Ublk.Protocol

  require Logger

  @accept_timeout_ms 30_000

  @doc """
  Accepts the helper's connection for `queue` and serves it until it closes.
  """
  @spec serve(map(), map(), non_neg_integer(), :gen_tcp.socket()) :: :ok
  def serve(device, info, queue, listener) do
    case :gen_tcp.accept(listener, @accept_timeout_ms) do
      {:ok, socket} ->
        loop(%{device: device, info: info}, socket)

      {:error, reason} ->
        exit({:ublk_accept_failed, queue, reason})
    end
  end

  defp loop(device, socket) do
    case read_frame(socket) do
      {:ok, frame} ->
        answer(device, socket, frame)
        loop(device, socket)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        exit({:ublk_recv_failed, reason})
    end
  end

  # The prefix is read on its own and then exactly as many bytes as it names,
  # so a frame is never partly consumed — `{packet, 4}` would do this, but it
  # also insists a reply leave in one `send`, which a streamed read cannot.
  defp read_frame(socket) do
    with {:ok, <<length::32>>} <-
           :gen_tcp.recv(socket, Protocol.length_prefix_bytes(), :infinity) do
      :gen_tcp.recv(socket, length, :infinity)
    end
  end

  defp answer(device, socket, frame) do
    case Protocol.decode_request(frame) do
      {:ok, request} ->
        serve_request(device, socket, request)

      # A frame this node cannot read is not one to guess at: the offsets
      # would be interpreted with the wrong layout and the write would land
      # somewhere else on the device. There is no tag to answer either, so
      # the helper's own read will fail and take the queue with it.
      {:error, reason} ->
        Logger.error("ublk request refused", reason: inspect(reason))
        send_reply(socket, Protocol.header_error(0, reason))
    end
  end

  defp serve_request(%{device: device}, socket, %{op: :read} = request) do
    start_time = System.monotonic_time()

    case Frontend.impl().read_stream(device, request.offset, request.length) do
      {:ok, stream} ->
        bytes = stream_reply(socket, request, stream)
        Frontend.impl().measure_read(device, bytes, start_time, :ok)

      {:error, reason} ->
        Frontend.impl().measure_read(device, 0, start_time, :error)
        send_reply(socket, Protocol.header_error(request.tag, reason))
    end
  end

  # A read-only export is enforced here rather than only advertised. ublk has
  # no negotiation in which a guest agrees to the restriction, so a write that
  # arrives on one is a write the kernel would otherwise land.
  defp serve_request(%{info: %{read_only: true}}, socket, %{op: op} = request)
       when op in [:write, :discard, :write_zeroes] do
    send_reply(socket, Protocol.header_error(request.tag, %{class: :forbidden}))
  end

  defp serve_request(%{device: device}, socket, %{op: :write} = request) do
    reply(socket, request, Frontend.impl().write(device, request.offset, request.data))
  end

  defp serve_request(%{device: device}, socket, %{op: :flush} = request) do
    reply(socket, request, Frontend.impl().flush(device))
  end

  defp serve_request(%{device: device}, socket, %{op: op} = request)
       when op in [:discard, :write_zeroes] do
    reply(socket, request, Frontend.impl().write_zeroes(device, request.offset, request.length))
  end

  defp reply(socket, request, :ok), do: send_reply(socket, Protocol.header_ok(request.tag))

  defp reply(socket, request, {:error, reason}),
    do: send_reply(socket, Protocol.header_error(request.tag, reason))

  defp send_reply(socket, header), do: :ok = :gen_tcp.send(socket, Protocol.frame(header))

  # The declared length is the request's, not the stream's, because it has to
  # go out before any of it is fetched. A stream that then yields a different
  # number of bytes desynchronises the socket and takes the device down —
  # loud, and the same assumption the NBD path already relies on.
  defp stream_reply(socket, request, stream) do
    :ok = :gen_tcp.send(socket, Protocol.prefix(request.length))
    :ok = :gen_tcp.send(socket, Protocol.header_ok(request.tag, request.length))

    Enum.reduce(stream, 0, fn chunk, sent ->
      :ok = :gen_tcp.send(socket, chunk)
      sent + byte_size(chunk)
    end)
  end
end
