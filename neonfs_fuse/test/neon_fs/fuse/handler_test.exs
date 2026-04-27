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

      expect(NeonFS.Client, :core_call, fn NeonFS.Core.WriteOperation,
                                           :write_file_at,
                                           [_volume_id, _path, 0, <<>>, opts] ->
        send(test_pid, {:write_opts, opts})
        {:ok, %{id: "file-1"}}
      end)

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

      expect(NeonFS.Client, :core_call, fn NeonFS.Core.WriteOperation,
                                           :write_file_at,
                                           [_volume_id, _path, 0, <<>>, opts] ->
        send(test_pid, {:write_opts, opts})
        {:ok, %{id: "file-2"}}
      end)

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
end
