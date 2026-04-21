defmodule NFSServer.RPC.Server do
  @moduledoc """
  ONC RPC v2 TCP listener.

  Spawns a `gen_tcp` accept loop that creates one process per
  connection (via `Task.Supervisor`). Each connection process reads
  bytes off the socket, reassembles them with
  `NFSServer.RPC.RecordMarking`, decodes the call envelope with
  `NFSServer.RPC.Message`, dispatches via `NFSServer.RPC.Dispatcher`
  using the configured program registry, and writes the encoded
  reply back.

  ## Options

    * `:port` (default `2049`) — TCP port to bind. Pass `0` to bind
      an ephemeral port (used by tests). The actual bound port is
      available via `port/1`.
    * `:bind` (default `"0.0.0.0"`) — IP address to bind.
    * `:programs` (required) — see `NFSServer.RPC.Dispatcher.programs/0`.
    * `:portmap_mappings` (default `%{}`) — `{prog, vers, proto}` →
      port map for the portmapper handler. The server will merge
      `{100003, 3, 6}` (NFSv3 over TCP) on top of this with the
      actual bound port.
    * `:name` (default `__MODULE__`) — registered name for the
      listener process.

  ## Backpressure

  One Erlang process per connection means each client's calls are
  naturally serialised by its mailbox. Slow handlers don't impact
  other clients.
  """

  use GenServer
  require Logger

  alias NFSServer.RPC.{Dispatcher, Message, Portmapper, RecordMarking}

  @default_port 2049
  @default_bind "0.0.0.0"

  @typedoc "Per-connection state."
  @type conn_state :: %{
          socket: :gen_tcp.socket(),
          buffer: binary(),
          programs: Dispatcher.programs(),
          portmap_mappings: %{
            required({non_neg_integer(), non_neg_integer(), non_neg_integer()}) =>
              non_neg_integer()
          }
        }

  # ——— Client API ———————————————————————————————————————————————

  @doc "Start the listener under the given supervision tree."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Return the actual TCP port the listener is bound on."
  @spec port(GenServer.server()) :: non_neg_integer()
  def port(server \\ __MODULE__), do: GenServer.call(server, :port)

  # ——— Server callbacks ——————————————————————————————————————————

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    port = Keyword.get(opts, :port, @default_port)
    bind = Keyword.get(opts, :bind, @default_bind)
    programs = Keyword.fetch!(opts, :programs)
    portmap_mappings = Keyword.get(opts, :portmap_mappings, %{})

    case open_listen_socket(bind, port) do
      {:ok, listen_socket, actual_port} ->
        # Always register the portmapper so `mount -t nfs` can find us.
        programs =
          Map.update(
            programs,
            Portmapper.program(),
            %{Portmapper.version() => Portmapper},
            &Map.put_new(&1, Portmapper.version(), Portmapper)
          )

        # If an NFSv3 handler is registered, advertise its port via
        # portmap automatically.
        portmap_mappings =
          case Map.get(programs, 100_003) do
            %{3 => _} -> Map.put_new(portmap_mappings, {100_003, 3, 6}, actual_port)
            _ -> portmap_mappings
          end

        {:ok, accept_supervisor} = Task.Supervisor.start_link()

        Task.Supervisor.start_child(accept_supervisor, fn ->
          accept_loop(listen_socket, programs, portmap_mappings, accept_supervisor)
        end)

        Logger.info("NFSServer RPC listener started", port: actual_port, bind: bind)

        {:ok,
         %{
           listen_socket: listen_socket,
           port: actual_port,
           bind: bind,
           programs: programs,
           portmap_mappings: portmap_mappings,
           accept_supervisor: accept_supervisor
         }}

      {:error, reason} ->
        {:stop, {:listen_failed, reason}}
    end
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  @impl true
  def terminate(_reason, %{listen_socket: listen_socket}) do
    :gen_tcp.close(listen_socket)
    :ok
  rescue
    _ -> :ok
  end

  # ——— Accept + connection loop ———————————————————————————————————

  defp open_listen_socket(bind, port) do
    {:ok, ip} = :inet.parse_address(to_charlist(bind))
    opts = [:binary, ip: ip, packet: :raw, active: false, reuseaddr: true]

    with {:ok, socket} <- :gen_tcp.listen(port, opts),
         {:ok, {_addr, actual_port}} <- :inet.sockname(socket) do
      {:ok, socket, actual_port}
    end
  end

  defp accept_loop(listen_socket, programs, portmap_mappings, supervisor) do
    case :gen_tcp.accept(listen_socket) do
      {:ok, client} ->
        Task.Supervisor.start_child(supervisor, fn ->
          serve_connection(client, programs, portmap_mappings)
        end)

        accept_loop(listen_socket, programs, portmap_mappings, supervisor)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        Logger.warning("RPC accept failed", reason: inspect(reason))
        :ok
    end
  end

  defp serve_connection(socket, programs, portmap_mappings) do
    state = %{
      socket: socket,
      buffer: <<>>,
      programs: programs,
      portmap_mappings: portmap_mappings
    }

    read_loop(state)
  rescue
    e ->
      Logger.warning("RPC connection crashed", reason: inspect(e))
      :gen_tcp.close(socket)
  catch
    :exit, _ -> :gen_tcp.close(socket)
  end

  defp read_loop(state) do
    case :gen_tcp.recv(state.socket, 0) do
      {:ok, data} ->
        state
        |> Map.update!(:buffer, &(&1 <> data))
        |> drain_messages()
        |> read_loop()

      {:error, :closed} ->
        :ok

      {:error, _reason} ->
        :gen_tcp.close(state.socket)
    end
  end

  # Pull as many complete messages off the buffer as possible,
  # dispatch each one, and write its reply back.
  defp drain_messages(state) do
    case RecordMarking.decode_message(state.buffer) do
      {:ok, body, rest} ->
        process_message(body, state)
        drain_messages(%{state | buffer: rest})

      :incomplete ->
        state
    end
  end

  defp process_message(body, state) do
    case Message.decode_call(body) do
      {:ok, call} ->
        ctx_extras = %{portmap_mappings: state.portmap_mappings}
        reply = Dispatcher.dispatch(call, state.programs)
        # The portmapper handler uses `ctx.portmap_mappings`; pass it
        # through by stashing it on the call before dispatch. We do
        # this by piggy-backing on the dispatcher's existing ctx
        # plumbing — see Handler @callback signature.
        send_reply(state.socket, maybe_redispatch_portmapper(call, reply, state, ctx_extras))

      {:error, reason} ->
        Logger.warning("RPC call decode failed", reason: inspect(reason))

        send_reply(
          state.socket,
          %Message.AcceptedReply{
            xid: 0,
            verf: %NFSServer.RPC.Auth.None{},
            stat: :garbage_args,
            body: <<>>
          }
        )
    end
  end

  # The dispatcher's stateless `dispatch/2` doesn't carry the
  # portmap mapping; if a portmap call lands we re-invoke the
  # portmapper handler with the extra context.
  defp maybe_redispatch_portmapper(
         %Message.Call{prog: 100_000} = call,
         _reply,
         _state,
         ctx_extras
       ) do
    ctx = Map.merge(%{call: call}, ctx_extras)

    case Portmapper.handle_call(call.proc, call.args, call.cred, ctx) do
      {:ok, body} ->
        %Message.AcceptedReply{
          xid: call.xid,
          verf: %NFSServer.RPC.Auth.None{},
          stat: :success,
          body: body
        }

      :proc_unavail ->
        %Message.AcceptedReply{
          xid: call.xid,
          verf: %NFSServer.RPC.Auth.None{},
          stat: :proc_unavail,
          body: <<>>
        }

      :garbage_args ->
        %Message.AcceptedReply{
          xid: call.xid,
          verf: %NFSServer.RPC.Auth.None{},
          stat: :garbage_args,
          body: <<>>
        }
    end
  end

  defp maybe_redispatch_portmapper(_call, reply, _state, _ctx_extras), do: reply

  defp send_reply(socket, reply) do
    bytes = reply |> Message.encode_reply() |> RecordMarking.encode()
    :gen_tcp.send(socket, bytes)
  end
end
