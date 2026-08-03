defmodule NeonFS.FUSE.HandlerTest do
  use ExUnit.Case, async: false
  use Mimic

  import Bitwise, only: [|||: 2]

  alias NeonFS.Client.ChunkReader
  alias NeonFS.Error.{AlreadyExists, Conflict}
  alias NeonFS.FUSE.{Handler, InodeTable}

  setup :verify_on_exit!

  describe "read operation — data plane" do
    setup do
      start_supervised!(InodeTable)
      {:ok, inode} = InodeTable.allocate_inode("vol", "/file.txt")
      handler = start_supervised!({Handler, volume: "vol", test_notify: self()})
      Mimic.allow(ChunkReader, self(), handler)

      {:ok, handler: handler, inode: inode}
    end

    test "dispatches reads through NeonFS.Client.ChunkReader", %{
      handler: handler,
      inode: inode
    } do
      test_pid = self()

      expect(ChunkReader, :read_file, fn "vol", "/file.txt", opts ->
        send(test_pid, {:chunk_reader_called, opts})
        {:ok, "hello"}
      end)

      send(handler, {:fuse_op, 1, {"read", %{"ino" => inode, "offset" => 0, "size" => 100}}})

      assert_receive {:fuse_op_complete, 1, {"read_ok", %{"data" => "hello"}}}, 5_000
      assert_receive {:chunk_reader_called, opts}, 1_000
      assert Keyword.get(opts, :offset) == 0
      assert Keyword.get(opts, :length) == 100
    end

    test "forwards non-zero offsets and lengths to ChunkReader", %{
      handler: handler,
      inode: inode
    } do
      expect(ChunkReader, :read_file, fn "vol", "/file.txt", opts ->
        assert Keyword.get(opts, :offset) == 4096
        assert Keyword.get(opts, :length) == 512
        {:ok, :binary.copy("x", 512)}
      end)

      send(handler, {:fuse_op, 1, {"read", %{"ino" => inode, "offset" => 4096, "size" => 512}}})

      assert_receive {:fuse_op_complete, 1, {"read_ok", %{"data" => data}}}, 5_000
      assert byte_size(data) == 512
    end

    test "maps ChunkReader not_found errors to ENOENT", %{handler: handler, inode: inode} do
      expect(ChunkReader, :read_file, fn _, _, _ -> {:error, :not_found} end)

      send(handler, {:fuse_op, 1, {"read", %{"ino" => inode, "offset" => 0, "size" => 100}}})

      assert_receive {:fuse_op_complete, 1, {"error", %{"errno" => 2}}}, 5_000
    end

    test "maps other ChunkReader errors to EIO", %{handler: handler, inode: inode} do
      expect(ChunkReader, :read_file, fn _, _, _ -> {:error, :no_available_locations} end)

      send(handler, {:fuse_op, 1, {"read", %{"ino" => inode, "offset" => 0, "size" => 100}}})

      assert_receive {:fuse_op_complete, 1, {"error", %{"errno" => 5}}}, 5_000
    end
  end

  describe "relatime_stale?/3" do
    test "stale when accessed_at is older than modified_at" do
      accessed_at = ~U[2026-01-01 10:00:00Z]
      modified_at = ~U[2026-01-01 12:00:00Z]
      now = ~U[2026-01-01 12:30:00Z]

      assert Handler.relatime_stale?(accessed_at, modified_at, now)
    end

    test "not stale when accessed_at is newer than modified_at and less than 24h old" do
      accessed_at = ~U[2026-01-01 14:00:00Z]
      modified_at = ~U[2026-01-01 12:00:00Z]
      now = ~U[2026-01-01 15:00:00Z]

      refute Handler.relatime_stale?(accessed_at, modified_at, now)
    end

    test "stale when accessed_at is newer than modified_at but more than 24h old" do
      accessed_at = ~U[2026-01-01 10:00:00Z]
      modified_at = ~U[2026-01-01 08:00:00Z]
      now = ~U[2026-01-02 11:00:00Z]

      assert Handler.relatime_stale?(accessed_at, modified_at, now)
    end

    test "not stale when accessed_at equals modified_at and less than 24h old" do
      accessed_at = ~U[2026-01-01 12:00:00Z]
      modified_at = ~U[2026-01-01 12:00:00Z]
      now = ~U[2026-01-01 13:00:00Z]

      refute Handler.relatime_stale?(accessed_at, modified_at, now)
    end

    test "stale when accessed_at equals modified_at but more than 24h old" do
      accessed_at = ~U[2026-01-01 12:00:00Z]
      modified_at = ~U[2026-01-01 12:00:00Z]
      now = ~U[2026-01-02 13:00:00Z]

      assert Handler.relatime_stale?(accessed_at, modified_at, now)
    end
  end

  # `create` opcode atomicity for `O_EXCL | O_CREAT`. The Rust shim
  # now plumbs the `open(2)` flags through to
  # Elixir; the handler routes `O_EXCL` writes through
  # `WriteOperation`'s `create_only: true`. `{:error, :exists}` from
  # core round-trips back to the FUSE caller as `EEXIST` (errno 17).
  describe "create opcode — O_EXCL atomicity" do
    # Linux open(2) flag values. POSIX-portable across glibc / musl /
    # kernel headers; same constants the Rust shim reads.
    @o_creat 0x40
    @o_excl 0x80

    setup do
      start_supervised!(InodeTable)

      # Allocate the parent dir's inode (root) so the create handler
      # can resolve it via `resolve_inode/2`.
      {:ok, parent_inode} = InodeTable.allocate_inode("vol", "/")

      handler = start_supervised!({Handler, volume: "vol", test_notify: self()})
      Mimic.allow(NeonFS.Client, self(), handler)

      {:ok, handler: handler, parent_inode: parent_inode}
    end

    test "without O_EXCL → write_file_at runs without create_only", %{
      handler: handler,
      parent_inode: parent_inode
    } do
      test_pid = self()

      stub(NeonFS.Client, :core_call, &create_test_core_call(&1, &2, &3, test_pid, "file-1"))

      stub(
        NeonFS.Client,
        :write_call_by_id,
        &create_test_write_call(&1, &2, &3, &4, test_pid, "file-1")
      )

      send(
        handler,
        {:fuse_op, 1,
         {"create",
          %{
            "parent" => parent_inode,
            "name" => "without-excl.txt",
            "mode" => 0o644,
            "flags" => @o_creat
          }}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", %{"kind" => "file"}}}, 5_000
      assert_receive {:write_opts, opts}, 1_000
      refute Keyword.get(opts, :create_only)
    end

    test "O_EXCL | O_CREAT → write_file_at carries create_only: true", %{
      handler: handler,
      parent_inode: parent_inode
    } do
      test_pid = self()

      stub(NeonFS.Client, :core_call, &create_test_core_call(&1, &2, &3, test_pid, "file-2"))

      stub(
        NeonFS.Client,
        :write_call_by_id,
        &create_test_write_call(&1, &2, &3, &4, test_pid, "file-2")
      )

      send(
        handler,
        {:fuse_op, 2,
         {"create",
          %{
            "parent" => parent_inode,
            "name" => "fresh-excl.txt",
            "mode" => 0o644,
            "flags" => @o_creat ||| @o_excl
          }}}
      )

      assert_receive {:fuse_op_complete, 2, {"entry_ok", %{"kind" => "file"}}}, 5_000
      assert_receive {:write_opts, opts}, 1_000
      assert Keyword.get(opts, :create_only) == true
    end

    test "O_EXCL → :exists from core maps to EEXIST", %{
      handler: handler,
      parent_inode: parent_inode
    } do
      # `:exists` short-circuits before the pin runs, so a single
      # `expect` on the write path (routed via write_call_by_id)
      # still suffices.
      expect(NeonFS.Client, :write_call_by_id, fn _volume_id,
                                                  NeonFS.Core.WriteOperation,
                                                  :write_file_at,
                                                  [_v, _path, 0, <<>>, opts] ->
        assert Keyword.get(opts, :create_only) == true
        {:error, AlreadyExists.from_reason(:exists)}
      end)

      send(
        handler,
        {:fuse_op, 3,
         {"create",
          %{
            "parent" => parent_inode,
            "name" => "racy.txt",
            "mode" => 0o644,
            "flags" => @o_creat ||| @o_excl
          }}}
      )

      # errno(:eexist) == 17.
      assert_receive {:fuse_op_complete, 3, {"error", %{"errno" => 17}}}, 5_000
    end

    # Same rule as `open`: a handle whose pin was refused claims
    # unlink-while-open semantics it does not have.
    test "a create whose pin is refused fails with EIO", %{
      handler: handler,
      parent_inode: parent_inode
    } do
      test_pid = self()

      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core, :pin_file, [_, _, _] -> {:error, :coordinator_unavailable}
      end)

      stub(
        NeonFS.Client,
        :write_call_by_id,
        &create_test_write_call(&1, &2, &3, &4, test_pid, "file-unpinnable")
      )

      send(
        handler,
        {:fuse_op, 4,
         {"create",
          %{
            "parent" => parent_inode,
            "name" => "unpinnable.txt",
            "mode" => 0o644,
            "flags" => @o_creat
          }}}
      )

      assert_receive {:fuse_op_complete, 4, {"error", %{"errno" => 5}}}, 5_000

      assert :sys.get_state(handler).fh_table == %{},
             "a refused create must not leave a handle behind"
    end
  end

  # POSIX unlink-while-open pin lifecycle.
  # The Handler GenServer claims a `:pinned` namespace claim on
  # `open` / `create` and releases it on `release`, so the
  # coordinator's holder-DOWN handler covers FUSE-peer crashes and
  # `read` / `write` can route via `Core.read_file_by_id` /
  # `write_file_at_by_id` against the cached `file_id` even after
  # another peer detaches the path.
  # `mkdir` publishes through the same `write_file_at` call as `create`,
  # so it can be handed the same errors — but its `else` block carried
  # only the two `:forbidden` clauses and collapsed the rest to `EIO`. A
  # shell doing `mkdir existing` on a NeonFS mount saw "Input/output
  # error" where `mkdir(2)` specifies `EEXIST`.
  describe "mkdir opcode — error mapping" do
    setup do
      start_supervised!(InodeTable)
      {:ok, parent_inode} = InodeTable.allocate_inode("vol", "/")
      handler = start_supervised!({Handler, volume: "vol", test_notify: self()})
      Mimic.allow(NeonFS.Client, self(), handler)

      {:ok, handler: handler, parent_inode: parent_inode}
    end

    test "a taken name maps to EEXIST", ctx do
      expect_mkdir_write(fn -> {:error, AlreadyExists.from_reason(:already_exists, "docs")} end)

      mkdir(ctx, 1, "docs")

      assert_receive {:fuse_op_complete, 1, {"error", %{"errno" => 17}}}, 5_000
    end

    test "a conflicting claim maps to EAGAIN rather than a fault", ctx do
      expect_mkdir_write(fn -> {:error, Conflict.from_reason(:conflict, %{})} end)

      mkdir(ctx, 2, "contended")

      assert_receive {:fuse_op_complete, 2, {"error", %{"errno" => 11}}}, 5_000
    end

    test "a missing parent maps to ENOENT", ctx do
      mkdir(ctx, 3, "orphan", parent: 999_999)

      assert_receive {:fuse_op_complete, 3, {"error", %{"errno" => 2}}}, 5_000
    end

    # Everything the client cannot act on still collapses to EIO — the
    # point is that the mapped conditions no longer join it.
    test "an unrecognised failure still maps to EIO", ctx do
      expect_mkdir_write(fn -> {:error, :something_unmapped} end)

      mkdir(ctx, 4, "broken")

      assert_receive {:fuse_op_complete, 4, {"error", %{"errno" => 5}}}, 5_000
    end

    defp expect_mkdir_write(result) do
      expect(NeonFS.Client, :write_call_by_id, fn _volume_id,
                                                  NeonFS.Core.WriteOperation,
                                                  :write_file_at,
                                                  [_v, _path, 0, <<>>, _opts] ->
        result.()
      end)
    end

    defp mkdir(ctx, request_id, name, opts \\ []) do
      parent = Keyword.get(opts, :parent, ctx.parent_inode)

      send(
        ctx.handler,
        {:fuse_op, request_id, {"mkdir", %{"parent" => parent, "name" => name, "mode" => 0o755}}}
      )
    end
  end

  describe "open / release pin lifecycle" do
    setup do
      start_supervised!(InodeTable)
      {:ok, parent_inode} = InodeTable.allocate_inode("vol", "/")
      {:ok, file_inode} = InodeTable.allocate_inode("vol", "/handle.txt")

      handler = start_supervised!({Handler, volume: "vol", test_notify: self()})
      Mimic.allow(NeonFS.Client, self(), handler)

      {:ok, handler: handler, parent_inode: parent_inode, file_inode: file_inode}
    end

    test "open claims a :pinned namespace claim and stores fh state",
         %{handler: handler, file_inode: file_inode} do
      test_pid = self()

      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core.FileIndex, :get_by_path, ["vol", "/handle.txt"] ->
          {:ok, %{id: "file-handle-id", mode: 0o100644}}

        NeonFS.Core, :pin_file, [volume_name, file_path, holder] ->
          send(test_pid, {:pin_call, volume_name, file_path, holder})
          {:ok, %{file_id: "file-handle-id", claim_id: "ns-claim-open", file: %{}}}
      end)

      send(handler, {:fuse_op, 10, {"open", %{"ino" => file_inode}}})

      assert_receive {:fuse_op_complete, 10, {"open_ok", %{"fh" => fh}}}, 5_000
      assert is_integer(fh) and fh >= 1

      assert_receive {:pin_call, "vol", "/handle.txt", ^handler}, 1_000
    end

    test "release drops the fh entry and releases the pin",
         %{handler: handler, file_inode: file_inode} do
      test_pid = self()

      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core.FileIndex, :get_by_path, ["vol", "/handle.txt"] ->
          {:ok, %{id: "file-handle-id", mode: 0o100644}}

        NeonFS.Core, :pin_file, [_volume_name, _path, _holder] ->
          {:ok, %{file_id: "file-handle-id", claim_id: "ns-claim-release", file: %{}}}

        NeonFS.Core, :unpin_file, ["ns-claim-release"] ->
          send(test_pid, {:released, "ns-claim-release"})
          :ok
      end)

      send(handler, {:fuse_op, 11, {"open", %{"ino" => file_inode}}})
      assert_receive {:fuse_op_complete, 11, {"open_ok", %{"fh" => fh}}}, 5_000

      send(handler, {:fuse_op, 12, {"release", %{"fh" => fh}}})
      assert_receive {:fuse_op_complete, 12, {"ok", %{}}}, 5_000

      assert_receive {:released, "ns-claim-release"}, 1_000
    end

    test "release on an unknown fh is a no-op",
         %{handler: handler} do
      send(handler, {:fuse_op, 13, {"release", %{"fh" => 99_999}}})
      assert_receive {:fuse_op_complete, 13, {"ok", %{}}}, 5_000
    end

    test "directory open returns fh=0 without a pin claim",
         %{handler: handler, parent_inode: parent_inode} do
      # Root dir resolution doesn't go through FileIndex
      # (handler synthesises the root metadata); confirm no
      # `claim_pinned_for` is called for directory opens.
      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core, :pin_file, [_, _, _] ->
          flunk("directory open must not claim a pin")

        _, _, _ ->
          {:error, :unexpected_in_test}
      end)

      send(handler, {:fuse_op, 14, {"open", %{"ino" => parent_inode}}})
      assert_receive {:fuse_op_complete, 14, {"open_ok", %{"fh" => 0}}}, 5_000
    end

    # An unpinned handle is indistinguishable from a pinned one downstream,
    # but the delete side reads the pin set and acts on it as authoritative
    # — so handing one back means a live fd whose file can be hard-deleted
    # under it. Failing the open keeps the two halves in agreement.
    test "open fails with EIO if the file cannot be pinned",
         %{handler: handler, file_inode: file_inode} do
      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core.FileIndex, :get_by_path, ["vol", "/handle.txt"] ->
          {:ok, %{id: "file-coord-down", mode: 0o100644}}

        NeonFS.Core, :pin_file, [_volume_name, _path, _holder] ->
          {:error, :coordinator_unavailable}
      end)

      send(handler, {:fuse_op, 15, {"open", %{"ino" => file_inode}}})

      assert_receive {:fuse_op_complete, 15, {"error", %{"errno" => errno}}}, 5_000
      assert errno == 5, "expected EIO"
    end

    test "a failed open allocates no file handle", %{handler: handler, file_inode: file_inode} do
      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core.FileIndex, :get_by_path, ["vol", "/handle.txt"] ->
          {:ok, %{id: "file-coord-down", mode: 0o100644}}

        NeonFS.Core, :pin_file, [_volume_name, _path, _holder] ->
          {:error, :coordinator_unavailable}
      end)

      send(handler, {:fuse_op, 17, {"open", %{"ino" => file_inode}}})
      assert_receive {:fuse_op_complete, 17, {"error", %{}}}, 5_000

      assert :sys.get_state(handler).fh_table == %{},
             "a refused open must not leave an entry behind for a later release to drop"
    end
  end

  describe "read / write via cached file_id" do
    setup do
      start_supervised!(InodeTable)
      {:ok, file_inode} = InodeTable.allocate_inode("vol", "/data.txt")

      handler =
        start_supervised!({Handler, volume: "vol", volume_name: "vol-name", test_notify: self()})

      Mimic.allow(NeonFS.Client, self(), handler)
      # Reads on an open handle go through `ChunkReader`, which runs in the
      # handler process, so the mock has to be reachable from there too.
      Mimic.allow(ChunkReader, self(), handler)

      {:ok, handler: handler, file_inode: file_inode}
    end

    # An open handle reads over the TLS data plane, not through a core
    # RPC: `ChunkReader` builds the chunk list locally and fetches each
    # chunk directly, so the read does not pay for a core round trip.
    test "read uses ChunkReader.read_file_by_id when fh is registered",
         %{handler: handler, file_inode: file_inode} do
      test_pid = self()

      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core.FileIndex, :get_by_path, ["vol", "/data.txt"] ->
          {:ok, %{id: "data-file-id", mode: 0o100644}}

        NeonFS.Core, :pin_file, [_, _, _] ->
          {:ok, %{file_id: "data-file-id", claim_id: "ns-claim-read", file: %{}}}
      end)

      expect(ChunkReader, :read_file_by_id, fn "vol-name", "data-file-id", _opts ->
        send(test_pid, :read_by_id_called)
        {:ok, "payload"}
      end)

      send(handler, {:fuse_op, 20, {"open", %{"ino" => file_inode}}})
      assert_receive {:fuse_op_complete, 20, {"open_ok", %{"fh" => fh}}}, 5_000

      send(
        handler,
        {:fuse_op, 21, {"read", %{"ino" => file_inode, "offset" => 0, "size" => 100, "fh" => fh}}}
      )

      assert_receive {:fuse_op_complete, 21, {"read_ok", %{"data" => "payload"}}}, 5_000
      assert_receive :read_by_id_called, 1_000
    end

    test "write uses Core.write_file_at_by_id when fh is registered",
         %{handler: handler, file_inode: file_inode} do
      test_pid = self()

      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core.FileIndex, :get_by_path, ["vol", "/data.txt"] ->
          {:ok, %{id: "data-file-id", mode: 0o100644}}

        NeonFS.Core, :pin_file, [_, _, _] ->
          {:ok, %{file_id: "data-file-id", claim_id: "ns-claim-write", file: %{}}}

        NeonFS.Core, :write_file_at_by_id, ["vol-name", "data-file-id", 0, "bytes"] ->
          send(test_pid, :write_by_id_called)
          {:ok, %{id: "data-file-id"}}
      end)

      send(handler, {:fuse_op, 22, {"open", %{"ino" => file_inode}}})
      assert_receive {:fuse_op_complete, 22, {"open_ok", %{"fh" => fh}}}, 5_000

      send(
        handler,
        {:fuse_op, 23,
         {"write", %{"ino" => file_inode, "offset" => 0, "data" => "bytes", "fh" => fh}}}
      )

      assert_receive {:fuse_op_complete, 23, {"write_ok", %{"size" => 5}}}, 5_000
      assert_receive :write_by_id_called, 1_000
    end

    test "a frozen cluster maps a write to EAGAIN",
         %{handler: handler, file_inode: file_inode} do
      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core.FileIndex, :get_by_path, ["vol", "/data.txt"] ->
          {:ok, %{id: "data-file-id", mode: 0o100644}}

        NeonFS.Core, :pin_file, [_, _, _] ->
          {:ok, %{file_id: "data-file-id", claim_id: "ns-claim-frozen", file: %{}}}

        NeonFS.Core, :write_file_at_by_id, ["vol-name", "data-file-id", 0, "bytes"] ->
          {:error, :cluster_frozen}
      end)

      send(handler, {:fuse_op, 24, {"open", %{"ino" => file_inode}}})
      assert_receive {:fuse_op_complete, 24, {"open_ok", %{"fh" => fh}}}, 5_000

      send(
        handler,
        {:fuse_op, 25,
         {"write", %{"ino" => file_inode, "offset" => 0, "data" => "bytes", "fh" => fh}}}
      )

      # errno(:eagain) == 11
      assert_receive {:fuse_op_complete, 25, {"error", %{"errno" => 11}}}, 5_000
    end
  end

  describe "fsync barrier" do
    setup do
      handler = start_supervised!({Handler, volume: "vol", test_notify: self()})
      Mimic.allow(NeonFS.Client, self(), handler)
      {:ok, handler: handler}
    end

    test "an untracked fh (fsyncdir / directory, fh=0) replies ok without the barrier",
         %{handler: handler} do
      reject(&NeonFS.Client.sync_file_by_id/2)

      send(handler, {:fuse_op, 30, {"fsync", %{"fh" => 0}}})
      assert_receive {:fuse_op_complete, 30, {"ok", %{}}}, 5_000
    end

    test "a tracked fh drives Client.sync_file_by_id for the fd's file_id",
         %{handler: handler} do
      :sys.replace_state(handler, fn state ->
        %{
          state
          | fh_table:
              Map.put(state.fh_table, 7, %{file_id: "file-1", claim_id: nil, path: "/f.txt"})
        }
      end)

      expect(NeonFS.Client, :sync_file_by_id, fn "vol", "file-1" -> :ok end)

      send(handler, {:fuse_op, 31, {"fsync", %{"fh" => 7}}})
      assert_receive {:fuse_op_complete, 31, {"ok", %{}}}, 5_000
    end

    test "a barrier failure maps to EIO", %{handler: handler} do
      :sys.replace_state(handler, fn state ->
        %{
          state
          | fh_table:
              Map.put(state.fh_table, 8, %{file_id: "file-2", claim_id: nil, path: "/g.txt"})
        }
      end)

      expect(NeonFS.Client, :sync_file_by_id, fn "vol", "file-2" ->
        {:error, {:under_replicated, 1, 2}}
      end)

      send(handler, {:fuse_op, 32, {"fsync", %{"fh" => 8}}})
      # errno(:eio) == 5
      assert_receive {:fuse_op_complete, 32, {"error", %{"errno" => 5}}}, 5_000
    end
  end

  # Helper for the `O_EXCL` create tests: dispatches the two
  # `core_call/3` invocations the pin-on-create path makes — the
  # `WriteOperation.write_file_at` that creates the file, and the
  # `Core.pin_file` that pins it by identity. Forwards
  # the write opts to the test process so the caller can assert on
  # `:create_only`.
  # Only `pin_file` reaches `core_call/3` now — `write_file_at` routes
  # through `write_call_by_id/4` (see `create_test_write_call/6`).
  defp create_test_core_call(NeonFS.Core, :pin_file, [_, _, _], _test_pid, file_id) do
    {:ok, %{file_id: file_id, claim_id: "ns-claim-stub", file: %{}}}
  end

  defp create_test_write_call(
         _volume_id,
         NeonFS.Core.WriteOperation,
         :write_file_at,
         [
           _v,
           _path,
           0,
           <<>>,
           opts
         ],
         test_pid,
         file_id
       ) do
    send(test_pid, {:write_opts, opts})
    {:ok, %{id: file_id, size: 0}}
  end
end
