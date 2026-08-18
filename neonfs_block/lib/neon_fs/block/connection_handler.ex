defmodule NeonFS.Block.ConnectionHandler do
  @moduledoc """
  One NBD connection, from greeting to disconnect.

  `ThousandIsland.Handler`, so each connection is its own process and a client
  that misbehaves takes only itself down. A device may be attached by several
  connections at once — that is how blk-mq gets its parallelism — so nothing
  device-wide lives here; `NeonFS.Block.DeviceRegistry` owns that.

  ## Framing

  NBD is a byte stream with its own framing rather than length-prefixed
  messages, so the socket is `packet: :raw` and this buffers. Every decode
  distinguishes "the frame has not all arrived" (`:incomplete`, keep reading)
  from "the frame is wrong" (`{:error, _}`, answer or disconnect), which is
  what makes a request split across TCP segments ordinary rather than fatal.

  ## Phases

  `:handshake` covers the client flags and option haggling; `:transmission`
  is the command loop. The phase is explicit in the state because the same
  bytes mean different things in each.
  """

  use ThousandIsland.Handler

  require Logger

  alias NeonFS.Block.{DeviceRegistry, Frontend, Protocol}
  alias ThousandIsland.Socket

  # Bounds a single read or write, and is advertised to the client as
  # NBD_INFO_BLOCK_SIZE's maximum so a well-behaved one never exceeds it.
  # Matches `BlockBacking`'s own per-request ceiling.
  @max_request_bytes 32 * 1024 * 1024

  @impl ThousandIsland.Handler
  def handle_connection(socket, _state) do
    :ok = Socket.send(socket, Protocol.server_greeting())

    # ThousandIsland hands the handler its configured options as initial
    # state, so the connection's own state starts here rather than merging
    # into whatever shape that was.
    {:continue, %{phase: :client_flags, buffer: <<>>, no_zeroes: false, device: nil}}
  end

  @impl ThousandIsland.Handler
  def handle_data(data, socket, state) do
    consume(socket, %{state | buffer: state.buffer <> data})
  end

  @impl ThousandIsland.Handler
  def handle_close(_socket, state) do
    release(state)
    :ok
  end

  @impl ThousandIsland.Handler
  def handle_error(reason, _socket, state) do
    Logger.debug("NBD connection error", reason: inspect(reason))
    release(state)
    :ok
  end

  # Drains the buffer until a decode says the rest of a frame has not arrived.
  defp consume(socket, state) do
    case step(socket, state) do
      {:continue, %{buffer: buffer} = next} when buffer != state.buffer ->
        consume(socket, next)

      other ->
        other
    end
  end

  defp step(_socket, %{phase: :client_flags, buffer: buffer} = state) do
    case Protocol.decode_client_flags(buffer) do
      {:ok, flags, rest} ->
        {:continue, %{state | phase: :options, buffer: rest, no_zeroes: flags.no_zeroes}}

      :incomplete ->
        {:continue, state}

      {:error, reason} ->
        Logger.info("NBD client refused at handshake", reason: inspect(reason))
        {:close, state}
    end
  end

  defp step(socket, %{phase: :options, buffer: buffer} = state) do
    case Protocol.decode_option(buffer) do
      {:ok, option, rest} ->
        handle_option(option, socket, %{state | buffer: rest})

      :incomplete ->
        {:continue, state}

      {:error, reason} ->
        Logger.info("NBD option frame rejected", reason: inspect(reason))
        {:close, state}
    end
  end

  defp step(socket, %{phase: :transmission, buffer: buffer} = state) do
    case Protocol.decode_request(buffer) do
      {:ok, request, rest} ->
        handle_request(request, socket, %{state | buffer: rest})

      :incomplete ->
        {:continue, state}

      {:error, reason} ->
        Logger.info("NBD request frame rejected", reason: inspect(reason))
        {:close, state}
    end
  end

  # `NBD_OPT_EXPORT_NAME` has no reply header: either the export details go
  # out and transmission begins, or the connection closes. There is nowhere
  # to report an error, which is why a bad name closes rather than replies.
  defp handle_option({:export_name, name}, socket, state) do
    case attach(name, state) do
      {:ok, device} ->
        reply =
          device
          |> io_core().export_info()
          |> Protocol.encode_export_name_reply(no_zeroes: state.no_zeroes)

        :ok = Socket.send(socket, reply)
        {:continue, %{state | phase: :transmission, device: device}}

      {:error, reason} ->
        Logger.info("NBD export refused", export: name, reason: inspect(reason))
        {:close, state}
    end
  end

  defp handle_option({:go, %{name: name}}, socket, state) do
    case attach(name, state) do
      {:ok, device} ->
        export = io_core().export_info(device)

        :ok =
          Socket.send(
            socket,
            Protocol.encode_option_reply(:go, :info, Protocol.encode_info_export(export)) <>
              Protocol.encode_option_reply(
                :go,
                :info,
                Protocol.encode_info_block_size(export, @max_request_bytes)
              ) <>
              Protocol.encode_option_reply(:go, :ack)
          )

        {:continue, %{state | phase: :transmission, device: device}}

      {:error, reason} ->
        Logger.info("NBD export refused", export: name, reason: inspect(reason))
        :ok = Socket.send(socket, Protocol.encode_option_reply(:go, :err_unknown))
        {:continue, state}
    end
  end

  defp handle_option({:info, %{name: name}}, socket, state) do
    case io_core().open(name) do
      {:ok, device} ->
        export = io_core().export_info(device)

        :ok =
          Socket.send(
            socket,
            Protocol.encode_option_reply(:info, :info, Protocol.encode_info_export(export)) <>
              Protocol.encode_option_reply(
                :info,
                :info,
                Protocol.encode_info_block_size(export, @max_request_bytes)
              ) <>
              Protocol.encode_option_reply(:info, :ack)
          )

        {:continue, state}

      {:error, _reason} ->
        :ok = Socket.send(socket, Protocol.encode_option_reply(:info, :err_unknown))
        {:continue, state}
    end
  end

  defp handle_option({:abort, _payload}, socket, state) do
    :ok = Socket.send(socket, Protocol.encode_option_reply(:abort, :ack))
    {:close, state}
  end

  # An option this server does not recognise — or a recognised one whose
  # payload did not parse — arrives carrying its numeric code, because that is
  # the only thing left to address the reply to.
  defp handle_option({:unknown, %{code: code}}, socket, state) do
    :ok = Socket.send(socket, Protocol.encode_option_reply(code, :err_unsup))
    {:continue, state}
  end

  defp handle_option({option, _payload}, socket, state) do
    :ok = Socket.send(socket, Protocol.encode_option_reply(option, :err_unsup))
    {:continue, state}
  end

  defp handle_request(%{type: :disconnect}, _socket, state), do: {:close, state}

  defp handle_request(%{type: :read} = request, socket, state) do
    start_time = System.monotonic_time()

    case io_core().read_stream(state.device, request.offset, request.length) do
      {:ok, stream} ->
        # The header goes first and the payload follows chunk by chunk: a
        # simple reply carries no length, so the client reads exactly what it
        # asked for and nothing has to be held here to compute one.
        :ok = Socket.send(socket, Protocol.encode_simple_reply(:ok, request.cookie))
        bytes = stream_to_socket(stream, socket)
        io_core().measure_read(state.device, bytes, start_time, :ok)
        {:continue, state}

      {:error, reason} ->
        io_core().measure_read(state.device, 0, start_time, :error)
        reply_error(socket, request, reason, state)
    end
  end

  defp handle_request(%{type: :write} = request, socket, state) do
    case io_core().write(state.device, request.offset, request.data) do
      :ok -> ack(socket, request, state)
      {:error, reason} -> reply_error(socket, request, reason, state)
    end
  end

  # FUA on a write means the write must be durable before it is acknowledged,
  # which is the same barrier `flush` asks for.
  defp handle_request(%{type: :flush} = request, socket, state) do
    case io_core().flush(state.device) do
      :ok -> ack(socket, request, state)
      {:error, reason} -> reply_error(socket, request, reason, state)
    end
  end

  defp handle_request(%{type: type} = request, socket, state)
       when type in [:trim, :write_zeroes] do
    case io_core().write_zeroes(state.device, request.offset, request.length) do
      :ok -> ack(socket, request, state)
      {:error, reason} -> reply_error(socket, request, reason, state)
    end
  end

  # A write carrying FUA is acknowledged only after the flush it implies.
  defp ack(socket, %{type: :write, flags: flags} = request, state) do
    if :fua in flags do
      case io_core().flush(state.device) do
        :ok -> send_ok(socket, request, state)
        {:error, reason} -> reply_error(socket, request, reason, state)
      end
    else
      send_ok(socket, request, state)
    end
  end

  defp ack(socket, request, state), do: send_ok(socket, request, state)

  defp send_ok(socket, request, state) do
    :ok = Socket.send(socket, Protocol.encode_simple_reply(:ok, request.cookie))
    {:continue, state}
  end

  defp reply_error(socket, request, reason, state) do
    Logger.debug("NBD command failed",
      command: request.type,
      offset: request.offset,
      reason: inspect(reason)
    )

    :ok = Socket.send(socket, Protocol.encode_simple_reply(error_code(reason), request.cookie))
    {:continue, state}
  end

  defp stream_to_socket(stream, socket) do
    Enum.reduce(stream, 0, fn chunk, sent ->
      :ok = Socket.send(socket, chunk)
      sent + byte_size(chunk)
    end)
  end

  # An out-of-range or unaligned request is the client's error and is reported
  # as one; anything else is the server failing to serve a valid request.
  defp error_code({:out_of_range, _offset, _length, _size}), do: :einval
  defp error_code({:unaligned_request, _offset, _length}), do: :einval
  defp error_code({:invalid_device_size, _size}), do: :einval

  # A write that exhausted its retry budget against a genuinely contended
  # span. Nothing was lost, so `EIO` — which is what an unmapped reason gets
  # — would fail a write that never failed, and a guest ext4 typically
  # remounts read-only over one.
  #
  # NBD defines no "retry" status, and the Linux client's handling of an
  # error outside its known set is not a guarantee that the block layer will
  # reissue the request — so the retry happens here, in `retrying_stale/3`,
  # before any of this is reached. A reply that gets this far has exhausted
  # that budget too, and says what happened rather than claiming a retry:
  # after the span-scoped commit compare, only two writers to the *same*
  # span can reach here, which a guest filesystem should not produce.
  # A core-side authorisation refusal. Not reachable while NBD passes no
  # identity and core reads an absent uid as 0 — but that is a property of
  # the caller, not of this mapping, and an unmapped denial falls through to
  # `EIO` below, which reports a permission problem as a device fault.
  defp error_code(%{class: :forbidden}), do: :eperm

  defp error_code(:stale_chunks), do: :eagain
  defp error_code(_reason), do: :eio

  # Every device operation goes through the behaviour rather than naming the
  # core directly: this module is the NBD frontend, and a second frontend
  # answers the same callbacks against the same core.
  defp io_core, do: Frontend.impl()

  defp attach(name, state) do
    DeviceRegistry.attach(name, connection_key(state))
  end

  defp release(%{device: nil}), do: :ok

  defp release(%{device: device} = state) do
    DeviceRegistry.detach(device.export, connection_key(state))
  end

  defp release(_state), do: :ok

  defp connection_key(_state), do: self()
end
