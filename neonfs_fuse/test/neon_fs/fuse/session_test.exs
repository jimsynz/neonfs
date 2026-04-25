defmodule NeonFS.FUSE.SessionTest do
  use ExUnit.Case, async: false

  alias FuseServer.Native, as: FNative
  alias FuseServer.Protocol
  alias FuseServer.Protocol.{Attr, InHeader}
  alias NeonFS.FUSE.{InodeTable, Session}
  alias NeonFS.FUSE.SessionTest.StubHandler

  defmodule StubHandler do
    @moduledoc false
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts)
    end

    def set_replies(pid, replies) do
      GenServer.call(pid, {:set_replies, replies})
    end

    def received_ops(pid) do
      GenServer.call(pid, :received_ops)
    end

    @impl true
    def init(_opts) do
      Process.flag(:trap_exit, true)
      {:ok, %{replies: %{}, received: [], session_pid: nil}}
    end

    @impl true
    def handle_call({:set_replies, replies}, _from, state),
      do: {:reply, :ok, %{state | replies: replies}}

    def handle_call(:received_ops, _from, state),
      do: {:reply, Enum.reverse(state.received), state}

    @impl true
    def handle_info({:fuse_op, id, op}, state) do
      reply =
        case Map.get(state.replies, op_kind(op)) do
          nil -> {"error", %{"errno" => 5}}
          fun when is_function(fun, 1) -> fun.(op)
          canned -> canned
        end

      session = caller_pid(state)

      if session do
        send(session, {:fuse_op_complete, id, reply})
      end

      {:noreply, %{state | received: [op | state.received]}}
    end

    def handle_info({:set_session, pid}, state),
      do: {:noreply, %{state | session_pid: pid}}

    def handle_info(_, state), do: {:noreply, state}

    defp op_kind({op, _params}), do: op

    defp caller_pid(%{session_pid: nil}) do
      # Best-effort: the only sender of {:fuse_op, ...} we care about
      # is the Session that owns us. We pin it after the first message.
      nil
    end

    defp caller_pid(%{session_pid: pid}), do: pid
  end

  ## Pure helpers

  describe "build_attr/1" do
    test "directory kind returns 0o755 with S_IFDIR set" do
      attr = Session.build_attr(%{"ino" => 42, "size" => 0, "kind" => "directory"})

      assert attr.ino == 42
      assert attr.size == 0
      # S_IFDIR | 0o755 = 0o040755
      assert attr.mode == 0o040755
      assert attr.nlink == 1
      assert attr.blksize == 4096
    end

    test "file kind returns 0o644 with S_IFREG set" do
      attr = Session.build_attr(%{"ino" => 7, "size" => 100, "kind" => "file"})
      # S_IFREG | 0o644 = 0o100644
      assert attr.mode == 0o100644
      assert attr.size == 100
      # 100 bytes rounds up to 1 block of 512 bytes
      assert attr.blocks == 1
    end

    test "missing fields default to zero" do
      attr = Session.build_attr(%{})
      assert attr.ino == 0
      assert attr.size == 0
      assert attr.atime == 0
      assert attr.mtime == 0
      assert attr.ctime == 0
    end
  end

  describe "build_entry/1" do
    test "wraps build_attr and sets nodeid + valid windows" do
      entry = Session.build_entry(%{"ino" => 99, "size" => 1024, "kind" => "file"})
      assert entry.nodeid == 99
      assert entry.entry_valid == 1
      assert entry.attr_valid == 1
      assert entry.attr.ino == 99
      assert entry.attr.size == 1024
    end
  end

  describe "build_dirents/3" do
    test "honours offset (skips entries already returned)" do
      entries = [
        %{"ino" => 10, "name" => "a", "kind" => "file"},
        %{"ino" => 11, "name" => "b", "kind" => "file"},
        %{"ino" => 12, "name" => "c", "kind" => "file"}
      ]

      dirents = Session.build_dirents(entries, 1, 4096)
      assert length(dirents) == 2
      assert Enum.map(dirents, & &1.name) == ["b", "c"]
      assert Enum.map(dirents, & &1.off) == [2, 3]
    end

    test "stops when adding the next entry would exceed the size cap" do
      entries =
        for i <- 1..50, do: %{"ino" => i, "name" => "name#{i}", "kind" => "file"}

      dirents = Session.build_dirents(entries, 0, 100)
      total = Enum.reduce(dirents, 0, fn d, acc -> acc + 24 + dirent_pad(byte_size(d.name)) end)
      assert total <= 100
      assert length(dirents) < 50
    end
  end

  describe "build_direntpluses/3" do
    test "every record carries inline attrs from the same payload" do
      entries = [
        %{"ino" => 1, "name" => "x", "size" => 4, "kind" => "file"},
        %{"ino" => 2, "name" => "y", "size" => 0, "kind" => "directory"}
      ]

      [dp_x, dp_y] = Session.build_direntpluses(entries, 0, 4096)
      assert dp_x.entry.attr.ino == 1
      assert dp_x.entry.attr.size == 4
      # S_IFREG | 0o644
      assert dp_x.entry.attr.mode == 0o100644
      assert dp_y.entry.attr.ino == 2
      assert dp_y.entry.attr.size == 0
      # S_IFDIR | 0o755
      assert dp_y.entry.attr.mode == 0o040755
    end
  end

  describe "mode_from_kind/1 and dt_from_kind/1" do
    test "directory" do
      assert Session.mode_from_kind("directory") == 0o040755
      # DT_DIR
      assert Session.dt_from_kind("directory") == 4
    end

    test "file" do
      assert Session.mode_from_kind("file") == 0o100644
      # DT_REG
      assert Session.dt_from_kind("file") == 8
    end

    test "unknown / nil" do
      assert Session.mode_from_kind(nil) == 0o100644
      assert Session.dt_from_kind(nil) == 0
    end
  end

  ## Wire round-trip — Session ↔ synthetic kernel via socketpair

  describe "INIT handshake" do
    setup do
      ctx = setup_session()
      on_exit(fn -> teardown_session(ctx) end)
      ctx
    end

    test "negotiates 7.31 and replies with our supported flags", ctx do
      kernel_unique = 0xCAFE_F00D
      send_init(ctx, kernel_unique, kernel_minor: 36)

      assert {:ok, header, body} = receive_response(ctx.kernel_fd)
      assert header.error == 0
      assert header.unique == kernel_unique

      assert <<
               7::little-32,
               31::little-32,
               _max_readahead::little-32,
               flags::little-32,
               _max_background::little-16,
               _congestion::little-16,
               max_write::little-32,
               _time_gran::little-32,
               _max_pages::little-16,
               _map_alignment::little-16,
               _unused::binary-32
             >> = body

      # Negotiated minor capped at 31 even though kernel asked 36.
      # Flags: at least FUSE_DO_READDIRPLUS (0x2000) is set when the
      # kernel advertises support — we ANDed with our supported set.
      assert max_write == 64 * 1024
      # Flags only include what we support; in this test kernel
      # advertises 0 so the negotiated set is also 0.
      assert flags == 0
    end

    test "intersects supported flags with the kernel's advertised flags", ctx do
      # Kernel advertises FUSE_ASYNC_READ (0x1) | FUSE_DO_READDIRPLUS (0x2000)
      kernel_flags = Bitwise.bor(0x1, 0x2000)
      send_init(ctx, 1, kernel_minor: 31, kernel_flags: kernel_flags)

      assert {:ok, _header, body} = receive_response(ctx.kernel_fd)

      <<
        7::little-32,
        31::little-32,
        _max_readahead::little-32,
        flags::little-32,
        _rest::binary
      >> = body

      assert Bitwise.band(flags, 0x1) != 0
      assert Bitwise.band(flags, 0x2000) != 0
    end
  end

  describe "STATFS" do
    setup do
      ctx = setup_session()
      _ = handshake!(ctx)
      on_exit(fn -> teardown_session(ctx) end)
      ctx
    end

    test "replies with sane defaults", ctx do
      header_bytes = build_in_header(opcode: opcode(:statfs), len: 40, unique: 7)
      :ok = FNative.write_frame(ctx.kernel_fd, header_bytes)

      assert {:ok, header, body} = receive_response(ctx.kernel_fd)
      assert header.error == 0
      assert header.unique == 7
      assert byte_size(body) == 80
    end
  end

  describe "FORGET" do
    setup do
      start_supervised!(InodeTable)
      ctx = setup_session()
      _ = handshake!(ctx)
      on_exit(fn -> teardown_session(ctx) end)
      ctx
    end

    test "produces no reply (kernel doesn't expect one)", ctx do
      header = build_in_header(opcode: opcode(:forget), len: 40 + 8, unique: 99, nodeid: 42)
      body = <<1::little-64>>
      :ok = FNative.write_frame(ctx.kernel_fd, header <> body)

      # Give the session a tick to process; assert nothing came back.
      refute_receive_response(ctx.kernel_fd, 200)
    end
  end

  describe "LOOKUP" do
    setup do
      ctx = setup_session_with_handler()
      _ = handshake!(ctx)

      :ok =
        StubHandler.set_replies(ctx.handler, %{
          "lookup" => fn {"lookup", %{"name" => "foo"}} ->
            {"lookup_ok", %{"ino" => 42, "size" => 1024, "kind" => "file"}}
          end
        })

      send(ctx.handler, {:set_session, ctx.session})

      on_exit(fn -> teardown_session(ctx) end)
      ctx
    end

    test "translates a handler success into fuse_entry_out", ctx do
      header =
        build_in_header(
          opcode: opcode(:lookup),
          len: 40 + 4,
          unique: 5,
          nodeid: 1
        )

      body = <<"foo", 0>>
      :ok = FNative.write_frame(ctx.kernel_fd, header <> body)

      assert {:ok, out_header, body} = receive_response(ctx.kernel_fd)
      assert out_header.error == 0
      assert out_header.unique == 5
      # 128 bytes of fuse_entry_out: 40 header + 88 attr.
      assert byte_size(body) == 128

      <<
        nodeid::little-64,
        _gen::little-64,
        _ev::little-64,
        _av::little-64,
        _evn::little-32,
        _avn::little-32,
        attr::binary-88
      >> = body

      assert nodeid == 42
      assert {:ok, %Attr{ino: 42, size: 1024, mode: 0o100644}, <<>>} = Attr.decode(attr)
    end
  end

  describe "READ" do
    setup do
      ctx = setup_session_with_handler()
      _ = handshake!(ctx)

      :ok =
        StubHandler.set_replies(ctx.handler, %{
          "read" => fn {"read", %{"size" => size}} ->
            {"read_ok", %{"data" => :binary.copy("x", size)}}
          end
        })

      send(ctx.handler, {:set_session, ctx.session})
      on_exit(fn -> teardown_session(ctx) end)
      ctx
    end

    test "echoes data verbatim into the response body", ctx do
      header = build_in_header(opcode: opcode(:read), len: 40 + 40, unique: 11, nodeid: 7)

      body =
        <<
          0::little-64,
          0::little-64,
          16::little-32,
          0::little-32,
          0::little-64,
          0::little-32,
          0::little-32
        >>

      :ok = FNative.write_frame(ctx.kernel_fd, header <> body)

      assert {:ok, out_header, data} = receive_response(ctx.kernel_fd)
      assert out_header.error == 0
      assert out_header.unique == 11
      assert data == :binary.copy("x", 16)
    end
  end

  # ——— Mutation metadata opcodes (#575) ——————————————————————————

  describe "SETATTR" do
    setup do
      ctx = setup_session_with_handler()
      _ = handshake!(ctx)
      send(ctx.handler, {:set_session, ctx.session})

      :ok =
        StubHandler.set_replies(ctx.handler, %{
          "setattr" => fn {"setattr", params} ->
            send(self(), {:setattr_called, params})

            {"attr_ok",
             %{
               "ino" => params["ino"],
               "size" => params["size"] || 12,
               "kind" => "file"
             }}
          end
        })

      on_exit(fn -> teardown_session(ctx) end)
      ctx
    end

    test "translates `valid` bitmask into Handler params (mode, size only)", ctx do
      header = build_in_header(opcode: opcode(:setattr), len: 40 + 88, unique: 51, nodeid: 7)
      body = encode_setattr(valid: 0x09, mode: 0o755, size: 1024)

      :ok = FNative.write_frame(ctx.kernel_fd, header <> body)

      assert {:ok, out_header, _} = receive_response(ctx.kernel_fd)
      assert out_header.error == 0
      assert out_header.unique == 51

      [{"setattr", params}] = StubHandler.received_ops(ctx.handler)
      assert params["ino"] == 7
      assert params["mode"] == 0o755
      assert params["size"] == 1024
      # FATTR_UID / FATTR_GID / FATTR_ATIME / FATTR_MTIME bits not set →
      # those entries decode to nil so Handler skips them.
      assert is_nil(params["uid"])
      assert is_nil(params["atime"])
    end

    test "FATTR_ATIME_NOW substitutes server time", ctx do
      header = build_in_header(opcode: opcode(:setattr), len: 40 + 88, unique: 52, nodeid: 7)
      # Bit 0x80 = FATTR_ATIME_NOW. atime field itself is ignored.
      body = encode_setattr(valid: 0x80, atime: 0, atimensec: 0)

      :ok = FNative.write_frame(ctx.kernel_fd, header <> body)
      assert {:ok, _out, _} = receive_response(ctx.kernel_fd)

      [{"setattr", params}] = StubHandler.received_ops(ctx.handler)
      assert {sec, nsec} = params["atime"]
      assert sec > 0
      assert is_integer(nsec)
    end

    test "ATTR reply on a valid-bitmask of zero (no-op setattr)", ctx do
      header = build_in_header(opcode: opcode(:setattr), len: 40 + 88, unique: 53, nodeid: 7)
      body = encode_setattr(valid: 0)

      :ok = FNative.write_frame(ctx.kernel_fd, header <> body)
      assert {:ok, out, attr_body} = receive_response(ctx.kernel_fd)
      assert out.error == 0
      # `fuse_attr_out`: 8 (attr_valid) + 4 (attr_valid_nsec) + 4 (dummy) + 88 (fuse_attr).
      assert byte_size(attr_body) == 104
    end
  end

  describe "MKDIR" do
    setup do
      ctx = setup_session_with_handler()
      _ = handshake!(ctx)
      send(ctx.handler, {:set_session, ctx.session})

      :ok =
        StubHandler.set_replies(ctx.handler, %{
          "mkdir" => fn {"mkdir", params} ->
            {"entry_ok",
             %{
               "ino" => 99,
               "size" => 0,
               "kind" => "directory",
               "fh" => 0,
               "_received_mode" => params["mode"]
             }}
          end
        })

      on_exit(fn -> teardown_session(ctx) end)
      ctx
    end

    test "applies umask before forwarding to Handler", ctx do
      # mode 0o777 & ~0o022 = 0o755
      header = build_in_header(opcode: opcode(:mkdir), len: 40 + 12, unique: 60, nodeid: 1)
      body = <<0o777::little-32, 0o022::little-32, "sub", 0>>

      :ok = FNative.write_frame(ctx.kernel_fd, header <> body)

      assert {:ok, out, entry_body} = receive_response(ctx.kernel_fd)
      assert out.error == 0
      # 128 bytes of fuse_entry_out: 40 header + 88 attr.
      assert byte_size(entry_body) == 128

      [{"mkdir", params}] = StubHandler.received_ops(ctx.handler)
      assert params["parent"] == 1
      assert params["name"] == "sub"
      assert params["mode"] == 0o755
    end
  end

  describe "UNLINK / RMDIR" do
    setup do
      ctx = setup_session_with_handler()
      _ = handshake!(ctx)
      send(ctx.handler, {:set_session, ctx.session})

      :ok =
        StubHandler.set_replies(ctx.handler, %{
          "unlink" => {"ok", %{}},
          "rmdir" => {"ok", %{}}
        })

      on_exit(fn -> teardown_session(ctx) end)
      ctx
    end

    test "UNLINK replies with empty success", ctx do
      header = build_in_header(opcode: opcode(:unlink), len: 40 + 4, unique: 70, nodeid: 1)
      :ok = FNative.write_frame(ctx.kernel_fd, header <> <<"old", 0>>)

      assert {:ok, out, <<>>} = receive_response(ctx.kernel_fd)
      assert out.error == 0
      assert out.unique == 70

      [{"unlink", %{"parent" => 1, "name" => "old"}}] = StubHandler.received_ops(ctx.handler)
    end

    test "RMDIR replies with empty success", ctx do
      header = build_in_header(opcode: opcode(:rmdir), len: 40 + 4, unique: 71, nodeid: 1)
      :ok = FNative.write_frame(ctx.kernel_fd, header <> <<"sub", 0>>)

      assert {:ok, out, <<>>} = receive_response(ctx.kernel_fd)
      assert out.error == 0
      [{"rmdir", %{"parent" => 1, "name" => "sub"}}] = StubHandler.received_ops(ctx.handler)
    end
  end

  describe "RENAME / RENAME2" do
    setup do
      ctx = setup_session_with_handler()
      _ = handshake!(ctx)
      send(ctx.handler, {:set_session, ctx.session})

      :ok =
        StubHandler.set_replies(ctx.handler, %{
          "rename" => {"ok", %{}}
        })

      on_exit(fn -> teardown_session(ctx) end)
      ctx
    end

    test "RENAME translates oldname/newname/newdir into Handler params", ctx do
      header = build_in_header(opcode: opcode(:rename), len: 40 + 16, unique: 80, nodeid: 1)
      body = <<2::little-64, "old", 0, "new", 0>>

      :ok = FNative.write_frame(ctx.kernel_fd, header <> body)

      assert {:ok, out, <<>>} = receive_response(ctx.kernel_fd)
      assert out.error == 0

      [
        {"rename",
         %{"old_parent" => 1, "new_parent" => 2, "old_name" => "old", "new_name" => "new"}}
      ] = StubHandler.received_ops(ctx.handler)
    end

    test "RENAME2 with flags=0 dispatches as a regular rename", ctx do
      header = build_in_header(opcode: opcode(:rename2), len: 40 + 24, unique: 81, nodeid: 1)
      body = <<2::little-64, 0::little-32, 0::little-32, "old", 0, "new", 0>>

      :ok = FNative.write_frame(ctx.kernel_fd, header <> body)

      assert {:ok, out, <<>>} = receive_response(ctx.kernel_fd)
      assert out.error == 0
      assert [{"rename", _}] = StubHandler.received_ops(ctx.handler)
    end

    test "RENAME2 with RENAME_EXCHANGE returns -EINVAL without invoking Handler", ctx do
      # 0x02 = RENAME_EXCHANGE
      header = build_in_header(opcode: opcode(:rename2), len: 40 + 24, unique: 82, nodeid: 1)
      body = <<2::little-64, 0x02::little-32, 0::little-32, "old", 0, "new", 0>>

      :ok = FNative.write_frame(ctx.kernel_fd, header <> body)

      assert {:ok, out, <<>>} = receive_response(ctx.kernel_fd)
      assert out.error == -22
      assert StubHandler.received_ops(ctx.handler) == []
    end
  end

  # ——— End mutation metadata opcodes —————————————————————————————

  # ——— Data-path opcodes (#576) ————————————————————————————————————

  describe "WRITE" do
    setup do
      ctx = setup_session_with_handler()
      _ = handshake!(ctx)
      send(ctx.handler, {:set_session, ctx.session})

      :ok =
        StubHandler.set_replies(ctx.handler, %{
          "write" => fn {"write", params} ->
            {"write_ok", %{"size" => byte_size(params["data"])}}
          end
        })

      on_exit(fn -> teardown_session(ctx) end)
      ctx
    end

    test "translates a single frame's payload into Handler params verbatim", ctx do
      payload = :binary.copy("Z", 8)
      header = build_in_header(opcode: opcode(:write), len: 40 + 40 + 8, unique: 91, nodeid: 7)

      body = <<
        # fh
        0::little-64,
        # offset
        128::little-64,
        # size
        8::little-32,
        # write_flags
        0::little-32,
        # lock_owner
        0::little-64,
        # flags
        0::little-32,
        # pad
        0::little-32,
        payload::binary
      >>

      :ok = FNative.write_frame(ctx.kernel_fd, header <> body)

      assert {:ok, out, reply} = receive_response(ctx.kernel_fd)
      assert out.error == 0
      assert out.unique == 91
      # `fuse_write_out` is 8 bytes (size + padding).
      assert byte_size(reply) == 8
      assert <<8::little-32, 0::little-32>> = reply

      [{"write", params}] = StubHandler.received_ops(ctx.handler)
      assert params["ino"] == 7
      assert params["offset"] == 128
      assert params["data"] == payload
    end

    test "passes data straight through without buffering across frames", ctx do
      # The streaming invariant the issue asks us to protect: a
      # single frame is bounded by `max_write` (negotiated 64 KiB).
      # If the session ever materialised the *whole file* in one
      # buffer it would have to span multiple frames — so all we can
      # check at this layer is that two sequential frames each only
      # carry their own bytes through, never accumulated.
      payload_a = :binary.copy("A", 32)
      payload_b = :binary.copy("B", 32)

      send_write = fn unique, offset, payload ->
        header =
          build_in_header(
            opcode: opcode(:write),
            len: 40 + 40 + byte_size(payload),
            unique: unique,
            nodeid: 7
          )

        body = <<
          0::little-64,
          offset::little-64,
          byte_size(payload)::little-32,
          0::little-32,
          0::little-64,
          0::little-32,
          0::little-32,
          payload::binary
        >>

        :ok = FNative.write_frame(ctx.kernel_fd, header <> body)
      end

      send_write.(101, 0, payload_a)
      assert {:ok, _, _} = receive_response(ctx.kernel_fd)

      send_write.(102, 32, payload_b)
      assert {:ok, _, _} = receive_response(ctx.kernel_fd)

      [{"write", first}, {"write", second}] = StubHandler.received_ops(ctx.handler)
      assert first["data"] == payload_a
      assert second["data"] == payload_b
    end
  end

  describe "CREATE" do
    setup do
      ctx = setup_session_with_handler()
      _ = handshake!(ctx)
      send(ctx.handler, {:set_session, ctx.session})

      :ok =
        StubHandler.set_replies(ctx.handler, %{
          "create" => fn {"create", params} ->
            {"entry_ok",
             %{
               "ino" => 1234,
               "size" => 0,
               "kind" => "file",
               "fh" => 1234,
               "_received_mode" => params["mode"]
             }}
          end
        })

      on_exit(fn -> teardown_session(ctx) end)
      ctx
    end

    test "applies umask and emits a combined fuse_create_out body", ctx do
      header = build_in_header(opcode: opcode(:create), len: 40 + 20, unique: 121, nodeid: 1)

      body = <<
        # flags (O_CREAT | O_RDWR)
        0o102::little-32,
        # mode 0o777
        0o777::little-32,
        # umask 0o022
        0o022::little-32,
        # pad
        0::little-32,
        "new",
        0
      >>

      :ok = FNative.write_frame(ctx.kernel_fd, header <> body)

      assert {:ok, out, reply} = receive_response(ctx.kernel_fd)
      assert out.error == 0
      assert out.unique == 121
      # `fuse_entry_out` (128) + `fuse_open_out` (16) = 144 bytes.
      assert byte_size(reply) == 144

      [{"create", params}] = StubHandler.received_ops(ctx.handler)
      assert params["parent"] == 1
      assert params["name"] == "new"
      # 0o777 & ~0o022 = 0o755
      assert params["mode"] == 0o755
    end

    test "embedded fh in fuse_open_out matches the handler's fh", ctx do
      header = build_in_header(opcode: opcode(:create), len: 40 + 20, unique: 122, nodeid: 1)

      body =
        <<0::little-32, 0o644::little-32, 0::little-32, 0::little-32, "new", 0>>

      :ok = FNative.write_frame(ctx.kernel_fd, header <> body)
      assert {:ok, _, reply} = receive_response(ctx.kernel_fd)

      <<_entry::binary-128, fh::little-64, _open_flags::little-32, _padding::little-32>> = reply
      # StubHandler returns 1234 as the fh.
      assert fh == 1234
    end
  end

  # ——— End data-path opcodes ——————————————————————————————————————

  describe "error mapping" do
    setup do
      ctx = setup_session_with_handler()
      _ = handshake!(ctx)

      :ok =
        StubHandler.set_replies(ctx.handler, %{
          "lookup" => {"error", %{"errno" => 2}}
        })

      send(ctx.handler, {:set_session, ctx.session})
      on_exit(fn -> teardown_session(ctx) end)
      ctx
    end

    test "ENOENT from handler becomes -2 in fuse_out_header", ctx do
      header =
        build_in_header(opcode: opcode(:lookup), len: 40 + 8, unique: 21, nodeid: 1)

      body = <<"missing", 0>>
      :ok = FNative.write_frame(ctx.kernel_fd, header <> body)

      assert {:ok, out_header, <<>>} = receive_response(ctx.kernel_fd)
      assert out_header.unique == 21
      assert out_header.error == -2
    end
  end

  ## Test fixtures

  defp setup_session do
    {:ok, {kernel_fd, server_fd}} = FNative.socketpair_stream()

    {:ok, handler} = StubHandler.start_link([])

    {:ok, session} =
      Session.start_link(fd: server_fd, volume: "test", handler: handler)

    %{session: session, handler: handler, kernel_fd: kernel_fd, server_fd: server_fd}
  end

  defp setup_session_with_handler, do: setup_session()

  defp teardown_session(ctx) do
    if Process.alive?(ctx.session), do: GenServer.stop(ctx.session, :normal, 1_000)
    if Process.alive?(ctx.handler), do: GenServer.stop(ctx.handler, :normal, 1_000)
  catch
    :exit, _ -> :ok
  end

  defp send_init(ctx, kernel_unique, opts) do
    minor = Keyword.get(opts, :kernel_minor, 31)
    flags = Keyword.get(opts, :kernel_flags, 0)
    body = <<7::little-32, minor::little-32, 32_768::little-32, flags::little-32>>

    header =
      build_in_header(opcode: opcode(:init), len: 40 + byte_size(body), unique: kernel_unique)

    :ok = FNative.write_frame(ctx.kernel_fd, header <> body)
  end

  defp handshake!(ctx) do
    send_init(ctx, 0, kernel_minor: 31, kernel_flags: 0x1)
    {:ok, _header, _body} = receive_response(ctx.kernel_fd)
  end

  defp build_in_header(opts) do
    %InHeader{
      len: Keyword.fetch!(opts, :len),
      opcode: Keyword.fetch!(opts, :opcode),
      unique: Keyword.get(opts, :unique, 1),
      nodeid: Keyword.get(opts, :nodeid, 0),
      uid: 0,
      gid: 0,
      pid: 0
    }
    |> InHeader.encode()
  end

  # Read a `fuse_out_header` (16 bytes) plus body from the test side
  # of the socketpair. Blocks via select_read up to 5 s.
  defp receive_response(fd) do
    case FNative.read_frame(fd) do
      {:ok, frame} -> decode_out(frame)
      {:error, :eagain} -> wait_and_read(fd, 5_000)
      err -> err
    end
  end

  defp wait_and_read(fd, timeout) do
    :ok = FNative.select_read(fd)

    receive do
      {:select, ^fd, :undefined, :ready_input} ->
        case FNative.read_frame(fd) do
          {:ok, frame} -> decode_out(frame)
          err -> err
        end
    after
      timeout -> {:error, :timeout}
    end
  end

  defp refute_receive_response(fd, timeout) do
    :ok = FNative.select_read(fd)

    receive do
      {:select, ^fd, :undefined, :ready_input} ->
        flunk("Did not expect a response, but got #{inspect(FNative.read_frame(fd))}")
    after
      timeout -> :ok
    end
  end

  defp decode_out(<<len::little-32, error::little-signed-32, unique::little-64, body::binary>>) do
    {:ok, %{len: len, error: error, unique: unique}, body}
  end

  defp opcode(atom), do: Protocol.atom_to_opcode(atom)

  # Encode an 88-byte `fuse_setattr_in` for tests. All optional kw
  # fields default to zero so callers only set what the `valid` bits
  # they care about reference.
  defp encode_setattr(opts) do
    valid = Keyword.fetch!(opts, :valid)
    fh = Keyword.get(opts, :fh, 0)
    size = Keyword.get(opts, :size, 0)
    lock_owner = Keyword.get(opts, :lock_owner, 0)
    atime = Keyword.get(opts, :atime, 0)
    mtime = Keyword.get(opts, :mtime, 0)
    ctime = Keyword.get(opts, :ctime, 0)
    atimensec = Keyword.get(opts, :atimensec, 0)
    mtimensec = Keyword.get(opts, :mtimensec, 0)
    ctimensec = Keyword.get(opts, :ctimensec, 0)
    mode = Keyword.get(opts, :mode, 0)
    uid = Keyword.get(opts, :uid, 0)
    gid = Keyword.get(opts, :gid, 0)

    <<
      valid::little-32,
      0::little-32,
      fh::little-64,
      size::little-64,
      lock_owner::little-64,
      atime::little-64,
      mtime::little-64,
      ctime::little-64,
      atimensec::little-32,
      mtimensec::little-32,
      ctimensec::little-32,
      mode::little-32,
      0::little-32,
      uid::little-32,
      gid::little-32,
      0::little-32
    >>
  end

  defp dirent_pad(namelen) do
    rem(8 - rem(24 + namelen, 8), 8) + namelen
  end
end
