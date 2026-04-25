defmodule NeonFS.CIFS.ConnectionHandler do
  @moduledoc """
  ThousandIsland.Handler for one Samba `vfs_neonfs.so` worker
  connection.

  Owns the per-connection state that the `vfs_neonfs.so` C shim
  expects to be cookie-stable for the lifetime of the connection:

    * `:volume` — the NeonFS volume the smbd worker is bound to,
      decided by the `connect` op.
    * `:next_handle` — monotonically increasing 64-bit token used to
      mint synthetic handles for `openat` / `fdopendir`. There are
      no POSIX fds on the NeonFS side, so the C shim is handed an
      opaque `:erlang.integer/0` it can pass back into subsequent
      ops.
    * `:files` — `%{handle => {volume, path, flags}}` for open
      files. Cleared on `close` and (defensively) on `disconnect`.
    * `:dirs` — `%{handle => {volume, path, cursor}}` for open
      directories. The cursor is opaque to the C shim — the handler
      uses it to advance through `readdir` calls.

  The frame format is 4-byte big-endian length prefix + ETF — see
  `NeonFS.CIFS.Listener`. Each request decodes to a
  `{op_atom, args_map}` tuple; we dispatch to the matching
  per-op function in `NeonFS.CIFS.Handler` and write back the
  encoded reply.

  ## Reply shapes

  Every handler returns one of:

    * `{:ok, payload}` — success arm. Wire reply is `{:ok, payload}`.
    * `{:error, reason}` — failure arm. `reason` is a Samba-style
      atom (`:enoent`, `:eacces`, `:eio`, …). Wire reply is
      `{:error, reason}`.

  Handler crashes are turned into `{:error, :eio}` to keep the C
  shim from blocking forever; the supervisor will respawn the
  connection process.
  """

  use ThousandIsland.Handler

  require Logger

  alias NeonFS.CIFS.{Handler, Listener}

  @impl true
  def handle_connection(_socket, _state) do
    {:continue,
     %{
       volume: nil,
       next_handle: 1,
       files: %{},
       dirs: %{}
     }}
  end

  @impl true
  def handle_data(body, socket, state) do
    case Listener.decode(body) do
      {:ok, request} ->
        {reply, new_state} = dispatch(request, state)
        send_reply(socket, reply)
        {:continue, new_state}

      {:error, :badetf} ->
        Logger.warning("CIFS frame decode failed", reason: :badetf)
        send_reply(socket, {:error, :einval})
        {:continue, state}
    end
  end

  @impl true
  def handle_close(_socket, state) do
    # Best-effort cleanup of any handles the C shim leaked. Each
    # close path takes a `(state, handle)` and returns the new
    # state minus that handle's slot.
    Enum.reduce(Map.keys(state.files), state, &close_file/2)
    |> then(fn s -> Enum.reduce(Map.keys(s.dirs), s, &close_dir/2) end)

    :ok
  end

  defp close_file(handle, state), do: %{state | files: Map.delete(state.files, handle)}
  defp close_dir(handle, state), do: %{state | dirs: Map.delete(state.dirs, handle)}

  ## Dispatch

  defp dispatch(request, state) do
    Handler.handle(request, state)
  rescue
    e ->
      Logger.warning("CIFS handler crashed",
        reason: inspect(e),
        operation: inspect(elem(request, 0))
      )

      {{:error, :eio}, state}
  catch
    kind, reason ->
      Logger.warning("CIFS handler exited",
        reason: inspect({kind, reason}),
        operation: inspect(elem(request, 0))
      )

      {{:error, :eio}, state}
  end

  defp send_reply(socket, reply) do
    # The socket is configured with `packet: 4`, which auto-frames
    # the payload with a 4-byte big-endian length prefix on send.
    # We pass just the ETF body — adding our own prefix would
    # double-frame.
    ThousandIsland.Socket.send(socket, :erlang.term_to_binary(reply))
  end
end
