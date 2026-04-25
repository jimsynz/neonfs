defmodule NeonFS.FUSE.Session do
  @moduledoc """
  Owns a mounted `/dev/fuse` fd and runs the read-path FUSE protocol
  against `NeonFS.FUSE.Handler`.

  This is the first end-to-end layer of the native-BEAM FUSE stack
  (issue #277): a single-mount GenServer that registers for
  `enif_select` notifications, decodes incoming kernel frames via
  `FuseServer.Protocol`, dispatches to the existing Handler's business
  logic, and writes the encoded reply back to the kernel.

  ## Lifecycle

    * `init/1` receives a `/dev/fuse` fd handle (from
      `FuseServer.Fusermount.mount/2` in production, or
      `FuseServer.Native.pipe_pair/0` in tests). It also receives a
      Handler pid configured to send `{:fuse_op_complete, id, reply}`
      messages back here (via `:test_notify`). Session arms a single
      read-readiness notification and waits for the kernel's `INIT`
      request.
    * On `{:select, fd, _, :ready_input}` Session reads one frame,
      decodes it, dispatches by opcode, and re-arms.
    * `INIT` is handled inline — Session replies with the negotiated
      protocol version (pinned to 7.31) and capability flags
      (`FUSE_ASYNC_READ | FUSE_BIG_WRITES | FUSE_ATOMIC_O_TRUNC |
      FUSE_DO_READDIRPLUS`).
    * `FORGET` and `BATCH_FORGET` decrement inode refcounts via
      `NeonFS.FUSE.InodeTable` and produce no reply.
    * `STATFS` returns a hardcoded reasonable response (the underlying
      core has no per-volume statvfs yet).
    * `DESTROY` shuts the session down cleanly.
    * All other read-path opcodes (`LOOKUP`, `GETATTR`, `OPEN`,
      `RELEASE`, `READ`, `READDIR`, `READDIRPLUS`) are translated into
      Handler's string-keyed message format, dispatched via
      `{:fuse_op, internal_id, op}`, and replied to when the
      corresponding `{:fuse_op_complete, internal_id, reply}` arrives.
    * `terminate/2` stops any Handler started by Session and emits a
      telemetry event so a supervisor can call `fusermount3 -u` from
      the outside.

  ## Backpressure

  Sessions process kernel frames serially in their own mailbox. The
  Handler also serialises through its mailbox. If the Handler falls
  behind, Session keeps reading frames and queuing pending replies;
  the kernel's writer-side flow control will eventually block on the
  `/dev/fuse` ring buffer. There is no explicit queue.

  ## Telemetry

  Each opcode dispatch emits `[:neonfs, :fuse, :session, :stop]` with
  metadata `%{opcode: atom, status: :ok | :error}`. `INIT` emits
  `[:neonfs, :fuse, :session, :init]` after replying.
  `terminate/2` emits `[:neonfs, :fuse, :session, :terminate]`.
  """

  use GenServer
  require Logger

  alias FuseServer.Native, as: FNative
  alias FuseServer.Protocol
  alias FuseServer.Protocol.{Attr, Request, Response}
  alias NeonFS.FUSE.{Handler, InodeTable}

  # Pinned kernel protocol version (libfuse 3.10+, Linux 5.4+).
  @kernel_major 7
  @kernel_minor 31

  # FUSE init capability flags (see include/uapi/linux/fuse.h).
  # We start with a minimal set for read-path correctness and
  # READDIRPLUS perf. Write-path flags arrive in #278.
  @fuse_async_read 0x00000001
  @fuse_atomic_o_trunc 0x00000010
  @fuse_big_writes 0x00000020
  @fuse_do_readdirplus 0x00002000

  @init_flags Bitwise.bor(
                @fuse_async_read,
                Bitwise.bor(
                  @fuse_atomic_o_trunc,
                  Bitwise.bor(@fuse_big_writes, @fuse_do_readdirplus)
                )
              )

  # FUSE max_write — kernel sends WRITE requests up to
  # `sizeof(fuse_in_header) + sizeof(fuse_write_in) + max_write`
  # bytes in one frame. `FuseServer.Native.read_frame/1` uses a
  # 128 KiB buffer, so the cap here must leave room for the 80-byte
  # request prefix. 64 KiB is the conservative libfuse-3.0 default
  # and avoids the EINVAL-on-INIT-reply that 128 KiB triggers when
  # the buffer can't hold a maximum-sized WRITE.
  @max_write 64 * 1024

  # POSIX file-type bits.
  @s_ifreg 0o100000
  @s_ifdir 0o040000

  # DT_* values for fuse_dirent.type (5th field of struct dirent).
  @dt_unknown 0
  @dt_dir 4
  @dt_reg 8

  defmodule State do
    @moduledoc false
    defstruct [
      :fd,
      :handler,
      :handler_started_by_session?,
      :volume,
      :pending,
      :next_id,
      :init_done?
    ]

    @type pending_kind ::
            :lookup
            | :getattr
            | :open
            | :release
            | :read
            | :readdir
            | {:readdirplus_collect, list()}

    @type t :: %__MODULE__{
            fd: reference(),
            handler: pid(),
            handler_started_by_session?: boolean(),
            volume: String.t(),
            pending: %{non_neg_integer() => {non_neg_integer(), pending_kind()}},
            next_id: non_neg_integer(),
            init_done?: boolean()
          }
  end

  ## Client API

  @doc """
  Start a Session GenServer.

  Required options:

    * `:fd` — a `/dev/fuse` (or pipe-pair-read) handle from
      `FuseServer.Native`. The handle's fd is closed when this process
      terminates and the resource refcount drops to zero.
    * `:volume` — volume id used by the Handler.

  Optional:

    * `:handler` — pid of an existing Handler. The Handler MUST be
      started with `test_notify: self()` (or be reconfigured to send
      replies to the Session pid). When omitted, Session starts a
      Handler internally with `volume`, `volume_name` (defaulting to
      `volume`), and `test_notify: self()`, and stops it on
      termination.
    * `:volume_name` — passed through to the Handler. Defaults to the
      `:volume` value.
    * `:name` — registered name for the Session GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    case Keyword.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  ## GenServer callbacks

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    fd = Keyword.fetch!(opts, :fd)
    volume = Keyword.fetch!(opts, :volume)

    {handler, started?} =
      case Keyword.get(opts, :handler) do
        nil ->
          {:ok, pid} =
            Handler.start_link(
              volume: volume,
              volume_name: Keyword.get(opts, :volume_name, volume),
              test_notify: self()
            )

          Process.link(pid)
          {pid, true}

        pid when is_pid(pid) ->
          Process.link(pid)
          {pid, false}
      end

    state = %State{
      fd: fd,
      handler: handler,
      handler_started_by_session?: started?,
      volume: volume,
      pending: %{},
      next_id: 1,
      init_done?: false
    }

    case FNative.select_read(fd) do
      :ok ->
        {:ok, state}

      {:error, reason} ->
        {:stop, {:select_failed, reason}}
    end
  end

  @impl true
  def handle_info({:select, fd, _ref, :ready_input}, %State{fd: fd} = state) do
    case FNative.read_frame(fd) do
      {:ok, frame} ->
        {:noreply, dispatch_frame(frame, state) |> rearm_or_stop()}

      {:error, :eagain} ->
        {:noreply, rearm_or_stop(state)}

      {:error, :enodev} ->
        # Kernel unmounted us — clean shutdown.
        {:stop, :normal, state}

      {:error, reason} ->
        Logger.error("FUSE session read failed",
          reason: inspect(reason),
          volume: state.volume
        )

        {:stop, {:read_failed, reason}, state}
    end
  end

  def handle_info({:fuse_op_complete, internal_id, reply}, state) do
    case Map.pop(state.pending, internal_id) do
      {nil, _pending} ->
        Logger.warning("Unexpected fuse_op_complete for unknown id",
          internal_id: internal_id,
          volume: state.volume
        )

        {:noreply, state}

      {{kernel_unique, kind}, pending} ->
        new_state = %{state | pending: pending}
        {:noreply, handle_handler_reply(kind, kernel_unique, reply, new_state)}
    end
  end

  def handle_info({:EXIT, pid, reason}, %State{handler: pid} = state) do
    Logger.error("FUSE session handler exited",
      reason: inspect(reason),
      volume: state.volume
    )

    {:stop, {:handler_exit, reason}, %{state | handler: nil}}
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  def handle_info(:session_destroy, state), do: {:stop, :normal, state}

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(reason, state) do
    :telemetry.execute(
      [:neonfs, :fuse, :session, :terminate],
      %{},
      %{volume: state.volume, reason: reason}
    )

    if state.handler_started_by_session? and is_pid(state.handler) and
         Process.alive?(state.handler) do
      try do
        GenServer.stop(state.handler, :shutdown, 1_000)
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  ## Frame dispatch

  defp dispatch_frame(frame, state) do
    case Protocol.decode_request(frame) do
      {:ok, opcode, header, request} ->
        handle_opcode(opcode, header, request, state)

      {:error, {:unknown_opcode, n}} ->
        Logger.debug("FUSE unknown opcode", opcode: n, volume: state.volume)
        # Reply with -ENOSYS so the kernel falls back gracefully.
        kernel_unique = unique_from_frame(frame)
        write_frame(state.fd, Protocol.encode_error(kernel_unique, -38))
        state

      {:error, reason} ->
        Logger.warning("FUSE frame decode error",
          reason: inspect(reason),
          volume: state.volume
        )

        state
    end
  end

  # Pull `unique` out of a 40-byte InHeader without re-decoding the
  # entire frame. Used so unknown-opcode error replies still carry the
  # right `unique`.
  defp unique_from_frame(<<_len::little-32, _opcode::little-32, unique::little-64, _::binary>>),
    do: unique

  defp unique_from_frame(_), do: 0

  ## Opcode handlers

  # INIT — handled inline. Reply with the kernel's major version and
  # negotiated minor (clamped down to 7.31), the negotiated readahead,
  # the supported capability flags (intersected with what the kernel
  # advertised), and our max_write.
  defp handle_opcode(:init, header, %Request.Init{} = req, state) do
    minor = min(req.minor, @kernel_minor)
    flags = Bitwise.band(req.flags, @init_flags)
    max_readahead = req.max_readahead

    reply = %Response.Init{
      major: @kernel_major,
      minor: minor,
      max_readahead: max_readahead,
      flags: flags,
      max_background: 16,
      congestion_threshold: 12,
      max_write: @max_write,
      time_gran: 1,
      max_pages: 0,
      map_alignment: 0
    }

    write_reply(state.fd, header.unique, reply, 0)

    :telemetry.execute(
      [:neonfs, :fuse, :session, :init],
      %{},
      %{volume: state.volume, kernel_minor: req.minor, negotiated_minor: minor, flags: flags}
    )

    %{state | init_done?: true}
  end

  # DESTROY — stop cleanly. The kernel doesn't expect a reply.
  defp handle_opcode(:destroy, _header, %Request.Destroy{}, state) do
    :telemetry.execute(
      [:neonfs, :fuse, :session, :destroy],
      %{},
      %{volume: state.volume}
    )

    Process.send_after(self(), :session_destroy, 0)
    state
  end

  # FORGET — drop one reference to a nodeid. No reply.
  defp handle_opcode(:forget, header, %Request.Forget{nlookup: _n}, state) do
    _ = InodeTable.release_inode(header.nodeid)
    state
  end

  # BATCH_FORGET — drop refs for many nodeids in one shot. No reply.
  defp handle_opcode(:batch_forget, _header, %Request.BatchForget{items: items}, state) do
    Enum.each(items, fn {nodeid, _nlookup} -> InodeTable.release_inode(nodeid) end)
    state
  end

  # STATFS — return a reasonable hardcoded response. Per-volume
  # statvfs is a follow-up; this keeps `df` and friends from erroring.
  defp handle_opcode(:statfs, header, %Request.Statfs{}, state) do
    reply = %Response.Statfs{
      blocks: 0,
      bfree: 0,
      bavail: 0,
      files: 0,
      ffree: 0,
      bsize: 4096,
      namelen: 255,
      frsize: 4096
    }

    write_reply(state.fd, header.unique, reply, 0)
    state
  end

  defp handle_opcode(:lookup, header, %Request.Lookup{name: name}, state) do
    op = {"lookup", %{"parent" => header.nodeid, "name" => name}}
    enqueue(:lookup, header.unique, op, state)
  end

  defp handle_opcode(:getattr, header, %Request.GetAttr{}, state) do
    op = {"getattr", %{"ino" => header.nodeid}}
    enqueue(:getattr, header.unique, op, state)
  end

  defp handle_opcode(:open, header, %Request.Open{}, state) do
    op = {"open", %{"ino" => header.nodeid}}
    enqueue(:open, header.unique, op, state)
  end

  # OPENDIR uses the same wire layout as OPEN and Handler treats the
  # two identically — return the inode as the directory handle.
  defp handle_opcode(:opendir, header, %Request.Open{}, state) do
    op = {"open", %{"ino" => header.nodeid}}
    enqueue(:open, header.unique, op, state)
  end

  defp handle_opcode(:release, header, %Request.Release{} = _r, state) do
    op = {"release", %{"ino" => header.nodeid}}
    enqueue(:release, header.unique, op, state)
  end

  defp handle_opcode(:releasedir, header, %Request.Release{} = _r, state) do
    op = {"release", %{"ino" => header.nodeid}}
    enqueue(:release, header.unique, op, state)
  end

  defp handle_opcode(:read, header, %Request.Read{} = r, state) do
    op = {"read", %{"ino" => header.nodeid, "offset" => r.offset, "size" => r.size}}
    enqueue(:read, header.unique, op, state)
  end

  defp handle_opcode(:readdir, header, %Request.Readdir{} = r, state) do
    op = {"readdir", %{"ino" => header.nodeid}}
    enqueue({:readdir, r.offset, r.size}, header.unique, op, state)
  end

  defp handle_opcode(:readdirplus, header, %Request.ReaddirPlus{} = r, state) do
    op = {"readdir", %{"ino" => header.nodeid}}
    enqueue({:readdirplus, r.offset, r.size}, header.unique, op, state)
  end

  # Catch-all for opcodes we accept in the codec but don't route here
  # (write-path, xattrs, etc. — out of scope for #277).
  defp handle_opcode(_other, header, _req, state) do
    write_frame(state.fd, Protocol.encode_error(header.unique, -38))
    state
  end

  ## Handler reply translation

  defp handle_handler_reply(:lookup, kernel_unique, {"lookup_ok", payload}, state) do
    reply = build_entry(payload)
    write_reply(state.fd, kernel_unique, reply, 0)
    emit_opcode_telemetry(:lookup, :ok, state)
    state
  end

  defp handle_handler_reply(:getattr, kernel_unique, {"attr_ok", payload}, state) do
    reply = %Response.AttrReply{
      attr_valid: 1,
      attr_valid_nsec: 0,
      attr: build_attr(payload)
    }

    write_reply(state.fd, kernel_unique, reply, 0)
    emit_opcode_telemetry(:getattr, :ok, state)
    state
  end

  defp handle_handler_reply(:open, kernel_unique, {"open_ok", payload}, state) do
    reply = %Response.Open{fh: payload["fh"] || 0, open_flags: 0}
    write_reply(state.fd, kernel_unique, reply, 0)
    emit_opcode_telemetry(:open, :ok, state)
    state
  end

  defp handle_handler_reply(:release, kernel_unique, {"ok", _}, state) do
    write_reply(state.fd, kernel_unique, %Response.Empty{}, 0)
    emit_opcode_telemetry(:release, :ok, state)
    state
  end

  defp handle_handler_reply(:read, kernel_unique, {"read_ok", %{"data" => data}}, state) do
    reply = %Response.Read{data: data}
    write_reply(state.fd, kernel_unique, reply, 0)
    emit_opcode_telemetry(:read, :ok, state)
    state
  end

  defp handle_handler_reply(
         {:readdir, offset, size},
         kernel_unique,
         {"readdir_ok", %{"entries" => entries}},
         state
       ) do
    reply = %Response.Readdir{entries: build_dirents(entries, offset, size)}
    write_reply(state.fd, kernel_unique, reply, 0)
    emit_opcode_telemetry(:readdir, :ok, state)
    state
  end

  defp handle_handler_reply(
         {:readdirplus, offset, size},
         kernel_unique,
         {"readdir_ok", %{"entries" => entries}},
         state
       ) do
    reply = %Response.ReaddirPlus{entries: build_direntpluses(entries, offset, size)}
    write_reply(state.fd, kernel_unique, reply, 0)
    emit_opcode_telemetry(:readdirplus, :ok, state)
    state
  end

  defp handle_handler_reply(kind, kernel_unique, {"error", %{"errno" => errno}}, state) do
    write_frame(state.fd, Protocol.encode_error(kernel_unique, -errno))
    emit_opcode_telemetry(opcode_for_kind(kind), :error, state)
    state
  end

  defp handle_handler_reply(kind, kernel_unique, other, state) do
    Logger.warning("Unexpected handler reply",
      kind: inspect(kind),
      reply: inspect(other),
      volume: state.volume
    )

    write_frame(state.fd, Protocol.encode_error(kernel_unique, -5))
    emit_opcode_telemetry(opcode_for_kind(kind), :error, state)
    state
  end

  defp opcode_for_kind({:readdir, _, _}), do: :readdir
  defp opcode_for_kind({:readdirplus, _, _}), do: :readdirplus
  defp opcode_for_kind(atom) when is_atom(atom), do: atom

  defp emit_opcode_telemetry(opcode, status, state) do
    :telemetry.execute(
      [:neonfs, :fuse, :session, :stop],
      %{},
      %{opcode: opcode, status: status, volume: state.volume}
    )
  end

  ## Helpers

  defp enqueue(kind, kernel_unique, op, %State{} = state) do
    internal_id = state.next_id
    send(state.handler, {:fuse_op, internal_id, op})

    %{
      state
      | next_id: internal_id + 1,
        pending: Map.put(state.pending, internal_id, {kernel_unique, kind})
    }
  end

  @doc """
  Build a `fuse_entry_out` reply struct from a Handler-style payload
  (string-keyed map with `"ino"`, `"size"`, `"kind"`, optional time
  fields). Public so tests can exercise it directly.
  """
  @spec build_entry(map()) :: Response.Entry.t()
  def build_entry(payload) do
    %Response.Entry{
      nodeid: payload["ino"] || 0,
      generation: 0,
      entry_valid: 1,
      attr_valid: 1,
      entry_valid_nsec: 0,
      attr_valid_nsec: 0,
      attr: build_attr(payload)
    }
  end

  @doc """
  Build a `fuse_attr` from a Handler-style payload. Public so tests
  can exercise it directly.
  """
  @spec build_attr(map()) :: Attr.t()
  def build_attr(payload) do
    ino = payload["ino"] || 0
    size = payload["size"] || 0

    %Attr{
      ino: ino,
      size: size,
      blocks: max(div(size + 511, 512), 0),
      atime: payload["atime"] || 0,
      mtime: payload["mtime"] || 0,
      ctime: payload["ctime"] || 0,
      atimensec: 0,
      mtimensec: 0,
      ctimensec: 0,
      mode: mode_from_kind(payload["kind"]),
      nlink: 1,
      uid: 0,
      gid: 0,
      rdev: 0,
      blksize: 4096
    }
  end

  @doc "POSIX mode bits for a Handler-style `\"kind\"` string."
  @spec mode_from_kind(String.t() | nil) :: non_neg_integer()
  def mode_from_kind("directory"), do: Bitwise.bor(@s_ifdir, 0o755)
  def mode_from_kind("file"), do: Bitwise.bor(@s_ifreg, 0o644)
  def mode_from_kind(_), do: Bitwise.bor(@s_ifreg, 0o644)

  @doc "DT_* dirent type for a Handler-style `\"kind\"` string."
  @spec dt_from_kind(String.t() | nil) :: non_neg_integer()
  def dt_from_kind("directory"), do: @dt_dir
  def dt_from_kind("file"), do: @dt_reg
  def dt_from_kind(_), do: @dt_unknown

  @doc """
  Build a list of `Dirent` records from Handler-style entries,
  starting after `offset` (the cookie from the previous READDIR call,
  zero on the first one) and capped at `size` total bytes so the
  kernel's READDIR reply buffer isn't exceeded. The per-entry `off`
  is the 1-based index into the directory's stable order — the
  cookie the kernel should send next time to resume after this entry.
  Public so tests can exercise it directly.
  """
  @spec build_dirents([map()], non_neg_integer(), non_neg_integer()) :: [Response.Dirent.t()]
  def build_dirents(entries, offset, size) do
    entries
    |> Enum.with_index(1)
    |> Enum.drop(offset)
    |> Enum.reduce_while({[], 0}, fn {entry, idx}, {acc, used} ->
      d = %Response.Dirent{
        ino: entry["ino"] || 0,
        off: idx,
        type: dt_from_kind(entry["kind"]),
        name: entry["name"] || ""
      }

      need = dirent_size(byte_size(d.name))

      if used + need > size do
        {:halt, {acc, used}}
      else
        {:cont, {[d | acc], used + need}}
      end
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  @doc """
  Build a list of `DirentPlus` records — same shape as
  `build_dirents/3` but each entry carries inline attributes for the
  kernel's dentry+inode cache. Public so tests can exercise it
  directly.
  """
  @spec build_direntpluses([map()], non_neg_integer(), non_neg_integer()) :: [
          Response.DirentPlus.t()
        ]
  def build_direntpluses(entries, offset, size) do
    entries
    |> Enum.with_index(1)
    |> Enum.drop(offset)
    |> Enum.reduce_while({[], 0}, fn {entry, idx}, {acc, used} ->
      attr = build_attr(entry)

      dp = %Response.DirentPlus{
        entry: %Response.Entry{
          nodeid: entry["ino"] || 0,
          generation: 0,
          entry_valid: 1,
          attr_valid: 1,
          entry_valid_nsec: 0,
          attr_valid_nsec: 0,
          attr: attr
        },
        dirent: %Response.Dirent{
          ino: entry["ino"] || 0,
          off: idx,
          type: dt_from_kind(entry["kind"]),
          name: entry["name"] || ""
        }
      }

      need = 128 + dirent_size(byte_size(dp.dirent.name))

      if used + need > size do
        {:halt, {acc, used}}
      else
        {:cont, {[dp | acc], used + need}}
      end
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  defp dirent_size(namelen) do
    pad = rem(8 - rem(24 + namelen, 8), 8)
    24 + namelen + pad
  end

  defp write_reply(fd, unique, reply, error) do
    bytes =
      Protocol.encode_response(unique, reply, error)
      |> :erlang.iolist_to_binary()

    write_frame(fd, bytes)
  end

  defp write_frame(fd, bytes) when is_binary(bytes) do
    case FNative.write_frame(fd, bytes) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("FUSE write_frame failed",
          reason: inspect(reason),
          bytes: byte_size(bytes)
        )

        {:error, reason}
    end
  end

  defp rearm_or_stop(%State{fd: fd} = state) do
    case FNative.select_read(fd) do
      :ok ->
        state

      {:error, :select_already_closed} ->
        # fd has been finalized — terminate.
        send(self(), :session_destroy)
        state

      {:error, reason} ->
        Logger.error("FUSE select_read re-arm failed",
          reason: inspect(reason),
          volume: state.volume
        )

        send(self(), :session_destroy)
        state
    end
  end
end
