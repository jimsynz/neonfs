defmodule NeonFS.Block.Ublk.Queue do
  @moduledoc """
  One ublk queue's IO loop.

  Reads a request frame from the helper's socket, answers it through
  `NeonFS.Block.Frontend`, writes a reply frame. One request in flight at a
  time on this socket, which is what makes the helper's tag match trivially
  correct — concurrency across a device is queues, not pipelining within
  one.

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
    case :gen_tcp.recv(socket, 0, :infinity) do
      {:ok, frame} ->
        :ok = :gen_tcp.send(socket, answer(device, frame))
        loop(device, socket)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        exit({:ublk_recv_failed, reason})
    end
  end

  defp answer(device, frame) do
    case Protocol.decode_request(frame) do
      {:ok, request} ->
        serve_request(device, request)

      # A frame this node cannot read is not one to guess at: the offsets
      # would be interpreted with the wrong layout and the write would land
      # somewhere else on the device. There is no tag to answer either, so
      # the helper's own read will fail and take the queue with it.
      {:error, reason} ->
        Logger.error("ublk request refused", reason: inspect(reason))
        Protocol.encode_error(0, reason)
    end
  end

  # The payload is materialised because the frame is length-prefixed and the
  # length has to go out first. It is bounded by the request, which the
  # helper bounds by its io_uring buffer size — one request's worth, never a
  # device's. That is the same bound `BlockBacking.read/4` keeps.
  defp serve_request(%{device: device}, %{op: :read} = request) do
    start_time = System.monotonic_time()

    case Frontend.impl().read_stream(device, request.offset, request.length) do
      {:ok, stream} ->
        bytes = stream |> Enum.to_list() |> IO.iodata_to_binary()
        Frontend.impl().measure_read(device, byte_size(bytes), start_time, :ok)
        Protocol.encode_ok(request.tag, bytes)

      {:error, reason} ->
        Frontend.impl().measure_read(device, 0, start_time, :error)
        Protocol.encode_error(request.tag, reason)
    end
  end

  # A read-only export is enforced here rather than only advertised. ublk has
  # no negotiation in which a guest agrees to the restriction, so a write that
  # arrives on one is a write the kernel would otherwise land.
  defp serve_request(%{info: %{read_only: true}}, %{op: op} = request)
       when op in [:write, :discard, :write_zeroes] do
    Protocol.encode_error(request.tag, %{class: :forbidden})
  end

  defp serve_request(%{device: device}, %{op: :write} = request) do
    reply(request, Frontend.impl().write(device, request.offset, request.data))
  end

  defp serve_request(%{device: device}, %{op: :flush} = request) do
    reply(request, Frontend.impl().flush(device))
  end

  defp serve_request(%{device: device}, %{op: op} = request)
       when op in [:discard, :write_zeroes] do
    reply(request, Frontend.impl().write_zeroes(device, request.offset, request.length))
  end

  defp reply(request, :ok), do: Protocol.encode_ok(request.tag)
  defp reply(request, {:error, reason}), do: Protocol.encode_error(request.tag, reason)
end
