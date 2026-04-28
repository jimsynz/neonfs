defmodule NeonFS.FUSE.HandlerTest do
  use ExUnit.Case, async: false
  use Mimic

  import Bitwise, only: [|||: 2]

  alias NeonFS.Client.ChunkReader
  alias NeonFS.FUSE.{Handler, InodeTable}

  setup :verify_on_exit!

  describe "read operation — data plane" do
    setup do
      start_supervised!(InodeTable)
      {:ok, inode} = InodeTable.allocate_inode("vol", "/file.txt")
      {:ok, handler} = Handler.start_link(volume: "vol", test_notify: self())
      Mimic.allow(ChunkReader, self(), handler)

      on_exit(fn ->
        if Process.alive?(handler), do: GenServer.stop(handler)
      end)

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

  # `create` opcode atomicity for `O_EXCL | O_CREAT` (sub-issue #594
  # of #303). The Rust shim now plumbs the `open(2)` flags through to
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

      {:ok, handler} = Handler.start_link(volume: "vol", test_notify: self())
      Mimic.allow(NeonFS.Client, self(), handler)

      on_exit(fn ->
        if Process.alive?(handler), do: GenServer.stop(handler)
      end)

      {:ok, handler: handler, parent_inode: parent_inode}
    end

    test "without O_EXCL → write_file_at runs without create_only", %{
      handler: handler,
      parent_inode: parent_inode
    } do
      test_pid = self()

      stub(NeonFS.Client, :core_call, &create_test_core_call(&1, &2, &3, test_pid, "file-1"))

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
      # `:exists` short-circuits before claim_pinned_for_path runs,
      # so a single `expect` for the write call still suffices.
      expect(NeonFS.Client, :core_call, fn NeonFS.Core.WriteOperation,
                                           :write_file_at,
                                           [_volume_id, _path, 0, <<>>, opts] ->
        assert Keyword.get(opts, :create_only) == true
        {:error, :exists}
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
  end

  # POSIX unlink-while-open pin lifecycle (sub-issue #651 of #639).
  # The Handler GenServer claims a `:pinned` namespace claim on
  # `open` / `create` and releases it on `release`, so the
  # coordinator's holder-DOWN handler covers FUSE-peer crashes and
  # `read` / `write` can route via `Core.read_file_by_id` /
  # `write_file_at_by_id` against the cached `file_id` even after
  # another peer detaches the path.
  describe "open / release pin lifecycle" do
    setup do
      start_supervised!(InodeTable)
      {:ok, parent_inode} = InodeTable.allocate_inode("vol", "/")
      {:ok, file_inode} = InodeTable.allocate_inode("vol", "/handle.txt")

      {:ok, handler} = Handler.start_link(volume: "vol", test_notify: self())
      Mimic.allow(NeonFS.Client, self(), handler)

      on_exit(fn ->
        if Process.alive?(handler), do: GenServer.stop(handler)
      end)

      {:ok, handler: handler, parent_inode: parent_inode, file_inode: file_inode}
    end

    test "open claims a :pinned namespace claim and stores fh state",
         %{handler: handler, file_inode: file_inode} do
      test_pid = self()

      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core.FileIndex, :get_by_path, ["vol", "/handle.txt"] ->
          {:ok, %{id: "file-handle-id", mode: 0o100644}}

        _coord, :claim_pinned_for, [_, key, holder] ->
          send(test_pid, {:pin_call, key, holder})
          {:ok, "ns-claim-open"}
      end)

      send(handler, {:fuse_op, 10, {"open", %{"ino" => file_inode}}})

      assert_receive {:fuse_op_complete, 10, {"open_ok", %{"fh" => fh}}}, 5_000
      assert is_integer(fh) and fh >= 1

      assert_receive {:pin_call, "vol:vol:/handle.txt", ^handler}, 1_000
    end

    test "release drops the fh entry and releases the pin",
         %{handler: handler, file_inode: file_inode} do
      test_pid = self()

      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core.FileIndex, :get_by_path, ["vol", "/handle.txt"] ->
          {:ok, %{id: "file-handle-id", mode: 0o100644}}

        _coord, :claim_pinned_for, [_, _key, _holder] ->
          {:ok, "ns-claim-release"}

        _coord, :release, [_, "ns-claim-release"] ->
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
        _coord, :claim_pinned_for, [_, _, _] ->
          flunk("directory open must not claim a pin")

        _, _, _ ->
          {:error, :unexpected_in_test}
      end)

      send(handler, {:fuse_op, 14, {"open", %{"ino" => parent_inode}}})
      assert_receive {:fuse_op_complete, 14, {"open_ok", %{"fh" => 0}}}, 5_000
    end

    test "open succeeds without a pin if the coordinator is unreachable",
         %{handler: handler, file_inode: file_inode} do
      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core.FileIndex, :get_by_path, ["vol", "/handle.txt"] ->
          {:ok, %{id: "file-coord-down", mode: 0o100644}}

        _coord, :claim_pinned_for, [_, _key, _holder] ->
          {:error, :coordinator_unavailable}
      end)

      send(handler, {:fuse_op, 15, {"open", %{"ino" => file_inode}}})

      # Open still returns OK — pinning is best-effort. The
      # unlink-while-open guarantee is absent for this fd, but
      # everything else works.
      assert_receive {:fuse_op_complete, 15, {"open_ok", %{"fh" => fh}}}, 5_000
      assert is_integer(fh)

      # The `release` for an open-without-pin doesn't try to
      # release a nil claim.
      send(handler, {:fuse_op, 16, {"release", %{"fh" => fh}}})
      assert_receive {:fuse_op_complete, 16, {"ok", %{}}}, 5_000
    end
  end

  describe "read / write via cached file_id" do
    setup do
      start_supervised!(InodeTable)
      {:ok, file_inode} = InodeTable.allocate_inode("vol", "/data.txt")

      {:ok, handler} =
        Handler.start_link(volume: "vol", volume_name: "vol-name", test_notify: self())

      Mimic.allow(NeonFS.Client, self(), handler)

      on_exit(fn ->
        if Process.alive?(handler), do: GenServer.stop(handler)
      end)

      {:ok, handler: handler, file_inode: file_inode}
    end

    test "read uses Core.read_file_by_id when fh is registered",
         %{handler: handler, file_inode: file_inode} do
      test_pid = self()

      stub(NeonFS.Client, :core_call, fn
        NeonFS.Core.FileIndex, :get_by_path, ["vol", "/data.txt"] ->
          {:ok, %{id: "data-file-id", mode: 0o100644}}

        _coord, :claim_pinned_for, [_, _, _] ->
          {:ok, "ns-claim-read"}

        NeonFS.Core, :read_file_by_id, ["vol-name", "data-file-id", _opts] ->
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

        _coord, :claim_pinned_for, [_, _, _] ->
          {:ok, "ns-claim-write"}

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
  end

  # Helper for the `O_EXCL` create tests: dispatches the two
  # `core_call/3` invocations the new pin-on-create path makes —
  # the `WriteOperation.write_file_at` that creates the file, and
  # the `NamespaceCoordinator.claim_pinned_for` that pins it
  # (#651). Forwards the write opts to the test process so the
  # caller can assert on `:create_only`.
  defp create_test_core_call(
         NeonFS.Core.WriteOperation,
         :write_file_at,
         [
           _volume_id,
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

  defp create_test_core_call(_coordinator, :claim_pinned_for, [_, _, _], _test_pid, _file_id) do
    {:ok, "ns-claim-stub"}
  end
end
