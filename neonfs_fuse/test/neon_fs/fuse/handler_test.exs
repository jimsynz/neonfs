defmodule NeonFS.FUSE.HandlerTest do
  use ExUnit.Case, async: false
  use Mimic

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
end
