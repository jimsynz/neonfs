defmodule NeonFS.NFS.RPC.Transport do
  @moduledoc """
  TCP transport for ONC RPC with record marking per RFC 5531 Section 11.

  Each RPC message is preceded by a 4-byte record marking header:
  - Bit 31: last fragment flag (1 = last fragment)
  - Bits 0-30: fragment length

  This module handles fragment reassembly and dispatches complete RPC
  messages to a configured handler module.
  """

  use GenServer

  require Logger

  alias NeonFS.NFS.RPC.Message

  @type handler :: module()

  @record_mark_size 4
  @max_fragment_size 1_048_576

  defmodule State do
    @moduledoc false

    @type t :: %__MODULE__{
            socket: :gen_tcp.socket() | nil,
            handler: module() | nil,
            handler_state: term(),
            buffer: binary(),
            fragments: [binary()]
          }

    defstruct [
      :socket,
      :handler,
      :handler_state,
      buffer: <<>>,
      fragments: []
    ]
  end

  @doc """
  Starts a transport process for an accepted TCP connection.

  Options:
    - `:socket` — the accepted `:gen_tcp` socket
    - `:handler` — module implementing the RPC handler behaviour
    - `:handler_opts` — options passed to `handler.init/1`
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Sends an RPC reply over the transport, wrapped in record marking.
  """
  @spec send_reply(pid(), binary()) :: :ok | {:error, term()}
  def send_reply(transport, reply_data) do
    GenServer.call(transport, {:send_reply, reply_data})
  end

  @impl true
  def init(opts) do
    socket = Keyword.fetch!(opts, :socket)
    handler = Keyword.fetch!(opts, :handler)
    handler_opts = Keyword.get(opts, :handler_opts, [])

    handler_state = handler.init(handler_opts)

    :inet.setopts(socket, active: :once, packet: :raw, nodelay: true)

    {:ok,
     %State{
       socket: socket,
       handler: handler,
       handler_state: handler_state
     }}
  end

  @impl true
  def handle_call({:send_reply, reply_data}, _from, state) do
    frame = encode_record_mark(reply_data)

    case :gen_tcp.send(state.socket, frame) do
      :ok -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:tcp, socket, data}, %State{socket: socket} = state) do
    state = %{state | buffer: state.buffer <> data}
    state = process_buffer(state)

    :inet.setopts(socket, active: :once)
    {:noreply, state}
  end

  def handle_info({:tcp_closed, socket}, %State{socket: socket} = state) do
    Logger.debug("RPC transport: connection closed")
    state.handler.terminate(:closed, state.handler_state)
    {:stop, :normal, state}
  end

  def handle_info({:tcp_error, socket, reason}, %State{socket: socket} = state) do
    Logger.warning("RPC transport: TCP error", reason: inspect(reason))
    state.handler.terminate({:tcp_error, reason}, state.handler_state)
    {:stop, reason, state}
  end

  ## Private

  defp process_buffer(%State{buffer: buffer} = state) when byte_size(buffer) < @record_mark_size,
    do: state

  defp process_buffer(
         %State{
           buffer: <<last_flag::1, frag_len::big-unsigned-31, rest::binary>>
         } = state
       ) do
    cond do
      frag_len > @max_fragment_size ->
        Logger.warning("RPC transport: fragment too large", size: frag_len)
        :gen_tcp.close(state.socket)
        state

      byte_size(rest) < frag_len ->
        state

      true ->
        <<fragment::binary-size(frag_len), remaining::binary>> = rest
        fragments = state.fragments ++ [fragment]

        if last_flag == 1 do
          message = IO.iodata_to_binary(fragments)
          handler_state = dispatch_message(message, state)

          process_buffer(%{
            state
            | buffer: remaining,
              fragments: [],
              handler_state: handler_state
          })
        else
          process_buffer(%{state | buffer: remaining, fragments: fragments})
        end
    end
  end

  defp dispatch_message(data, state) do
    case Message.decode_call(data) do
      {:ok, call} ->
        {reply, handler_state} = state.handler.handle_call(call, state.handler_state)

        if reply do
          frame = encode_record_mark(reply)
          :gen_tcp.send(state.socket, frame)
        end

        handler_state

      {:error, reason} ->
        Logger.debug("RPC transport: failed to decode call", reason: inspect(reason))
        state.handler_state
    end
  end

  defp encode_record_mark(data) do
    len = byte_size(data)
    <<1::1, len::big-unsigned-31, data::binary>>
  end
end
