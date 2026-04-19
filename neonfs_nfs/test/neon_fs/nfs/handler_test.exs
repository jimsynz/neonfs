defmodule NeonFS.NFS.HandlerTest do
  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Client.ChunkReader
  alias NeonFS.NFS.{Handler, InodeTable, MockCore}

  setup :set_mimic_global
  setup :verify_on_exit!

  @volume "testvol"
  @null_volume_id <<0::128>>

  # errno constants
  @enoent 2
  @eio 5
  @eexist 17
  @exdev 18
  @enosys 38
  @estale 70

  setup do
    start_supervised!(InodeTable)
    {:ok, handler: nil, mock: nil}
  end

  defp start_handler_without_core(ctx) do
    {:ok, handler} = start_supervised({Handler, test_notify: self()})
    %{ctx | handler: handler}
  end

  defp start_handler_with_mock(ctx, opts \\ []) do
    volumes = Keyword.get(opts, :volumes, [@volume])
    mock = MockCore.start(volumes: volumes)
    on_exit(fn -> if :ets.info(mock.table) != :undefined, do: MockCore.stop(mock) end)

    stub(ChunkReader, :read_file, fn volume_name, path, opts ->
      mock.core_call_fn.(NeonFS.Core.ReadOperation, :read_file, [volume_name, path, opts])
    end)

    {:ok, handler} =
      start_supervised({Handler, core_call_fn: mock.core_call_fn, test_notify: self()})

    %{ctx | handler: handler, mock: mock}
  end

  defp send_op(handler, id, op, params) do
    send(handler, {:nfs_op, id, {op, params}})
  end

  defp assert_ok(id) do
    assert_receive {:nfs_op_complete, ^id, {:ok, reply}}, 1_000
    reply
  end

  defp assert_error(id, errno) do
    assert_receive {:nfs_op_complete, ^id, {:error, ^errno}}, 1_000
  end

  defp register_volume(handler) do
    lookup_volume_root(handler, 0, @volume)
  end

  defp lookup_volume_root(handler, op_id, volume_name) do
    send_op(handler, op_id, "lookup", %{
      "parent_inode" => 1,
      "parent_volume_id" => @null_volume_id,
      "name" => volume_name
    })

    reply = assert_ok(op_id)
    {:crypto.hash(:md5, volume_name), reply["file_id"]}
  end

  defp create_file(handler, vol_hash, parent_inode, name) do
    send_op(handler, :erlang.unique_integer([:positive]), "create", %{
      "parent_inode" => parent_inode,
      "parent_volume_id" => vol_hash,
      "name" => name,
      "mode" => 0o644
    })

    assert_receive {:nfs_op_complete, _, {:ok, reply}}, 1_000
    reply
  end

  ## Virtual Root Tests

  describe "getattr on virtual root" do
    test "returns directory attributes for root inode", ctx do
      %{handler: handler} = start_handler_without_core(ctx)

      send_op(handler, 1, "getattr", %{"inode" => 1, "volume_id" => @null_volume_id})
      reply = assert_ok(1)

      assert reply["type"] == "attrs"
      assert reply["file_id"] == 1
      assert reply["kind"] == "directory"
      assert reply["mode"] == 0o755
    end
  end

  describe "lookup on virtual root" do
    test "returns error when core is unavailable", ctx do
      %{handler: handler} = start_handler_without_core(ctx)

      send_op(handler, 1, "lookup", %{
        "parent_inode" => 1,
        "parent_volume_id" => @null_volume_id,
        "name" => "nonexistent"
      })

      assert_error(1, @eio)
    end

    test "resolves existing volume", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)

      send_op(handler, 1, "lookup", %{
        "parent_inode" => 1,
        "parent_volume_id" => @null_volume_id,
        "name" => @volume
      })

      reply = assert_ok(1)
      assert reply["type"] == "lookup"
      assert reply["kind"] == "directory"
      assert is_integer(reply["file_id"])
      assert reply["volume_id"] == :crypto.hash(:md5, @volume)
    end

    test "returns ENOENT for nonexistent volume", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)

      send_op(handler, 1, "lookup", %{
        "parent_inode" => 1,
        "parent_volume_id" => @null_volume_id,
        "name" => "no_such_vol"
      })

      assert_error(1, @enoent)
    end
  end

  describe "readdirplus on virtual root" do
    test "lists registered volumes", ctx do
      %{handler: handler} = start_handler_with_mock(ctx, volumes: ["vol_a", "vol_b"])

      send_op(handler, 1, "readdirplus", %{
        "inode" => 1,
        "volume_id" => @null_volume_id,
        "cookie" => 0
      })

      reply = assert_ok(1)
      assert reply["type"] == "dir_entries"
      names = Enum.map(reply["entries"], & &1["name"])
      assert "vol_a" in names
      assert "vol_b" in names
    end

    test "includes . and .. entries pointing to root inode", ctx do
      %{handler: handler} = start_handler_with_mock(ctx, volumes: ["vol_a"])

      send_op(handler, 1, "readdirplus", %{
        "inode" => 1,
        "volume_id" => @null_volume_id,
        "cookie" => 0
      })

      reply = assert_ok(1)
      names = Enum.map(reply["entries"], & &1["name"])
      assert "." in names
      assert ".." in names

      dot = Enum.find(reply["entries"], &(&1["name"] == "."))
      dotdot = Enum.find(reply["entries"], &(&1["name"] == ".."))
      assert dot["file_id"] == 1
      assert dotdot["file_id"] == 1
      assert dot["kind"] == "directory"
      assert dotdot["kind"] == "directory"
    end
  end

  ## Unregistered Volume Tests

  describe "operations with unregistered volume" do
    test "getattr returns ESTALE", ctx do
      %{handler: handler} = start_handler_without_core(ctx)

      send_op(handler, 1, "getattr", %{
        "inode" => 2,
        "volume_id" => :crypto.hash(:md5, "unknown_volume")
      })

      assert_error(1, @estale)
    end

    test "create returns ESTALE", ctx do
      %{handler: handler} = start_handler_without_core(ctx)

      send_op(handler, 1, "create", %{
        "parent_inode" => 2,
        "parent_volume_id" => :crypto.hash(:md5, "unknown_volume"),
        "name" => "file.txt",
        "mode" => 0o644
      })

      assert_error(1, @estale)
    end
  end

  ## Volume Operation Tests

  describe "getattr on volume" do
    test "returns directory attributes for volume root", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)

      send_op(handler, 1, "getattr", %{"inode" => root_inode, "volume_id" => vol_hash})
      reply = assert_ok(1)

      assert reply["type"] == "attrs"
      assert reply["kind"] == "directory"
    end

    test "returns attributes for a file", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)

      created = create_file(handler, vol_hash, root_inode, "test.txt")
      file_inode = created["file_id"]

      send_op(handler, 1, "getattr", %{"inode" => file_inode, "volume_id" => vol_hash})
      reply = assert_ok(1)

      assert reply["type"] == "attrs"
      assert reply["kind"] == "file"
      assert reply["file_id"] == file_inode
    end
  end

  describe "lookup on volume" do
    test "resolves existing file", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      create_file(handler, vol_hash, root_inode, "found.txt")

      send_op(handler, 1, "lookup", %{
        "parent_inode" => root_inode,
        "parent_volume_id" => vol_hash,
        "name" => "found.txt"
      })

      reply = assert_ok(1)
      assert reply["type"] == "lookup"
      assert reply["kind"] == "file"
    end

    test "returns ENOENT for nonexistent file", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)

      send_op(handler, 1, "lookup", %{
        "parent_inode" => root_inode,
        "parent_volume_id" => vol_hash,
        "name" => "missing.txt"
      })

      assert_error(1, @enoent)
    end
  end

  describe "create" do
    test "creates a new file", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)

      send_op(handler, 1, "create", %{
        "parent_inode" => root_inode,
        "parent_volume_id" => vol_hash,
        "name" => "new.txt",
        "mode" => 0o644
      })

      reply = assert_ok(1)
      assert reply["type"] == "create"
      assert reply["kind"] == "file"
      assert is_integer(reply["file_id"])
    end

    test "created file has non-epoch timestamps", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      reply = create_file(handler, vol_hash, root_inode, "stamped.txt")

      assert reply["mtime_secs"] > 0,
             "mtime should not be epoch (0), got: #{reply["mtime_secs"]}"

      assert reply["atime_secs"] > 0,
             "atime should not be epoch (0), got: #{reply["atime_secs"]}"

      assert reply["ctime_secs"] > 0,
             "ctime should not be epoch (0), got: #{reply["ctime_secs"]}"
    end

    test "getattr returns non-epoch timestamps", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      created = create_file(handler, vol_hash, root_inode, "getattr_ts.txt")
      file_inode = created["file_id"]

      send_op(handler, 1, "getattr", %{"inode" => file_inode, "volume_id" => vol_hash})
      reply = assert_ok(1)

      assert reply["mtime_secs"] > 0,
             "mtime should not be epoch (0), got: #{reply["mtime_secs"]}"

      assert reply["atime_secs"] > 0,
             "atime should not be epoch (0), got: #{reply["atime_secs"]}"

      assert reply["ctime_secs"] > 0,
             "ctime should not be epoch (0), got: #{reply["ctime_secs"]}"
    end

    test "created file is visible via lookup", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      create_file(handler, vol_hash, root_inode, "visible.txt")

      send_op(handler, 1, "lookup", %{
        "parent_inode" => root_inode,
        "parent_volume_id" => vol_hash,
        "name" => "visible.txt"
      })

      assert_ok(1)
    end
  end

  describe "create_exclusive" do
    test "creates a new file", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)

      send_op(handler, 1, "create_exclusive", %{
        "parent_inode" => root_inode,
        "parent_volume_id" => vol_hash,
        "name" => "exclusive.txt"
      })

      reply = assert_ok(1)
      assert reply["type"] == "create"
    end

    test "returns EEXIST when file already exists", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      create_file(handler, vol_hash, root_inode, "dup.txt")

      send_op(handler, 1, "create_exclusive", %{
        "parent_inode" => root_inode,
        "parent_volume_id" => vol_hash,
        "name" => "dup.txt"
      })

      assert_error(1, @eexist)
    end
  end

  describe "mkdir" do
    test "creates a new directory", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)

      send_op(handler, 1, "mkdir", %{
        "parent_inode" => root_inode,
        "parent_volume_id" => vol_hash,
        "name" => "subdir"
      })

      reply = assert_ok(1)
      assert reply["type"] == "create"
      assert reply["kind"] == "directory"
    end

    test "created directory is visible via lookup", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)

      send_op(handler, 1, "mkdir", %{
        "parent_inode" => root_inode,
        "parent_volume_id" => vol_hash,
        "name" => "mydir"
      })

      assert_ok(1)

      send_op(handler, 2, "lookup", %{
        "parent_inode" => root_inode,
        "parent_volume_id" => vol_hash,
        "name" => "mydir"
      })

      reply = assert_ok(2)
      assert reply["kind"] == "directory"
    end
  end

  describe "write and read" do
    test "write + read round-trip", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      created = create_file(handler, vol_hash, root_inode, "data.txt")
      file_inode = created["file_id"]

      send_op(handler, 1, "write", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "data" => "hello world"
      })

      write_reply = assert_ok(1)
      assert write_reply["count"] == 11

      send_op(handler, 2, "read", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "count" => 1024
      })

      read_reply = assert_ok(2)
      assert read_reply["type"] == "read"
      assert read_reply["data"] == "hello world"
      assert read_reply["eof"] == true
    end

    test "write at non-zero offset", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      created = create_file(handler, vol_hash, root_inode, "offset.txt")
      file_inode = created["file_id"]

      send_op(handler, 1, "write", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "data" => "Hello"
      })

      assert_ok(1)

      send_op(handler, 2, "write", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 5,
        "data" => " World"
      })

      assert_ok(2)

      send_op(handler, 3, "read", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "count" => 1024
      })

      reply = assert_ok(3)
      assert reply["data"] == "Hello World"
    end

    test "partial write in middle preserves trailing data", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      created = create_file(handler, vol_hash, root_inode, "mid.txt")
      file_inode = created["file_id"]

      send_op(handler, 1, "write", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "data" => "hello world"
      })

      assert_ok(1)

      send_op(handler, 2, "write", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 5,
        "data" => "-"
      })

      assert_ok(2)

      send_op(handler, 3, "read", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "count" => 1024
      })

      reply = assert_ok(3)
      assert reply["data"] == "hello-world"
    end

    test "read from nonexistent file returns ENOENT", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, _root_inode} = register_volume(handler)

      # Allocate an inode for a path that has no file in the mock
      {:ok, fake_inode} = InodeTable.allocate_inode(@volume, "/ghost.txt")

      send_op(handler, 1, "read", %{
        "inode" => fake_inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "count" => 1024
      })

      assert_error(1, @enoent)
    end
  end

  describe "read — data plane routing" do
    test "dispatches reads through NeonFS.Client.ChunkReader", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      created = create_file(handler, vol_hash, root_inode, "data.txt")
      file_inode = created["file_id"]

      send_op(handler, 1, "write", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "data" => "hello world"
      })

      assert_ok(1)

      test_pid = self()

      expect(ChunkReader, :read_file, fn volume_name, path, opts ->
        send(test_pid, {:chunk_reader_called, volume_name, path, opts})
        {:ok, "hello world"}
      end)

      send_op(handler, 2, "read", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "count" => 1024
      })

      reply = assert_ok(2)
      assert reply["data"] == "hello world"

      assert_receive {:chunk_reader_called, volume_name, path, opts}, 1_000
      assert volume_name == @volume
      assert path == "/data.txt"
      assert Keyword.get(opts, :offset) == 0
      assert Keyword.get(opts, :length) == 1024
    end

    test "forwards non-zero offsets and lengths to ChunkReader", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      created = create_file(handler, vol_hash, root_inode, "range.bin")
      file_inode = created["file_id"]

      send_op(handler, 1, "write", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "data" => :binary.copy("x", 8192)
      })

      assert_ok(1)

      expect(ChunkReader, :read_file, fn @volume, "/range.bin", opts ->
        assert Keyword.get(opts, :offset) == 4096
        assert Keyword.get(opts, :length) == 512
        {:ok, :binary.copy("x", 512)}
      end)

      send_op(handler, 2, "read", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 4096,
        "count" => 512
      })

      reply = assert_ok(2)
      assert byte_size(reply["data"]) == 512
      assert reply["eof"] == false
    end

    test "maps ChunkReader :not_found to ENOENT", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, _root_inode} = register_volume(handler)
      {:ok, inode} = InodeTable.allocate_inode(@volume, "/missing.txt")

      expect(ChunkReader, :read_file, fn _, _, _ -> {:error, :not_found} end)

      send_op(handler, 1, "read", %{
        "inode" => inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "count" => 1024
      })

      assert_error(1, @enoent)
    end

    test "maps other ChunkReader errors to EIO", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, _root_inode} = register_volume(handler)
      {:ok, inode} = InodeTable.allocate_inode(@volume, "/broken.txt")

      expect(ChunkReader, :read_file, fn _, _, _ -> {:error, :no_available_locations} end)

      send_op(handler, 1, "read", %{
        "inode" => inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "count" => 1024
      })

      assert_error(1, @eio)
    end
  end

  describe "setattr" do
    test "changes file mode", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      created = create_file(handler, vol_hash, root_inode, "chmod.txt")
      file_inode = created["file_id"]

      send_op(handler, 1, "setattr", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "mode" => 0o600,
        "uid" => nil,
        "gid" => nil,
        "size" => nil,
        "atime" => nil,
        "mtime" => nil
      })

      reply = assert_ok(1)
      assert reply["type"] == "attrs"
      assert reply["mode"] == 0o600
    end

    test "truncates file", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      created = create_file(handler, vol_hash, root_inode, "trunc.txt")
      file_inode = created["file_id"]

      # Write data first
      send_op(handler, 1, "write", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "data" => "Hello, World!"
      })

      assert_ok(1)

      # Truncate to 5 bytes
      send_op(handler, 2, "setattr", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "mode" => nil,
        "uid" => nil,
        "gid" => nil,
        "size" => 5,
        "atime" => nil,
        "mtime" => nil
      })

      reply = assert_ok(2)
      assert reply["size"] == 5

      # Read back
      send_op(handler, 3, "read", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "count" => 1024
      })

      read_reply = assert_ok(3)
      assert read_reply["data"] == "Hello"
    end
  end

  describe "remove" do
    test "deletes a file", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      create_file(handler, vol_hash, root_inode, "doomed.txt")

      send_op(handler, 1, "remove", %{
        "parent_inode" => root_inode,
        "parent_volume_id" => vol_hash,
        "name" => "doomed.txt"
      })

      reply = assert_ok(1)
      assert reply["type"] == "empty"

      # Verify gone
      send_op(handler, 2, "lookup", %{
        "parent_inode" => root_inode,
        "parent_volume_id" => vol_hash,
        "name" => "doomed.txt"
      })

      assert_error(2, @enoent)
    end

    test "returns ENOENT for nonexistent file", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)

      send_op(handler, 1, "remove", %{
        "parent_inode" => root_inode,
        "parent_volume_id" => vol_hash,
        "name" => "nope.txt"
      })

      assert_error(1, @enoent)
    end
  end

  describe "rename" do
    test "moves a file", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      create_file(handler, vol_hash, root_inode, "old.txt")

      send_op(handler, 1, "rename", %{
        "from_parent_inode" => root_inode,
        "from_parent_volume_id" => vol_hash,
        "from_name" => "old.txt",
        "to_parent_inode" => root_inode,
        "to_parent_volume_id" => vol_hash,
        "to_name" => "new.txt"
      })

      reply = assert_ok(1)
      assert reply["type"] == "empty"

      # New name resolves
      send_op(handler, 2, "lookup", %{
        "parent_inode" => root_inode,
        "parent_volume_id" => vol_hash,
        "name" => "new.txt"
      })

      assert_ok(2)

      # Old name is gone
      send_op(handler, 3, "lookup", %{
        "parent_inode" => root_inode,
        "parent_volume_id" => vol_hash,
        "name" => "old.txt"
      })

      assert_error(3, @enoent)
    end

    test "rejects cross-volume rename with EXDEV", ctx do
      %{handler: handler} = start_handler_with_mock(ctx, volumes: ["vol-a", "vol-b"])

      {vol_a, root_a} = lookup_volume_root(handler, 100, "vol-a")
      {vol_b, root_b} = lookup_volume_root(handler, 101, "vol-b")

      create_file(handler, vol_a, root_a, "src.txt")

      send_op(handler, 1, "rename", %{
        "from_parent_inode" => root_a,
        "from_parent_volume_id" => vol_a,
        "from_name" => "src.txt",
        "to_parent_inode" => root_b,
        "to_parent_volume_id" => vol_b,
        "to_name" => "dst.txt"
      })

      assert_error(1, @exdev)

      # Source is untouched
      send_op(handler, 2, "lookup", %{
        "parent_inode" => root_a,
        "parent_volume_id" => vol_a,
        "name" => "src.txt"
      })

      assert_ok(2)
    end
  end

  describe "symlink and readlink" do
    test "creates symlink and reads target", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)

      send_op(handler, 1, "symlink", %{
        "parent_inode" => root_inode,
        "parent_volume_id" => vol_hash,
        "name" => "link",
        "target" => "/some/target"
      })

      reply = assert_ok(1)
      assert reply["type"] == "create"
      assert reply["kind"] == "symlink"
      link_inode = reply["file_id"]

      send_op(handler, 2, "readlink", %{
        "inode" => link_inode,
        "volume_id" => vol_hash
      })

      readlink_reply = assert_ok(2)
      assert readlink_reply["type"] == "readlink"
      assert readlink_reply["target"] == "/some/target"
    end
  end

  describe "readdirplus on volume" do
    test "empty volume still includes . and .. entries", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)

      send_op(handler, 1, "readdirplus", %{
        "inode" => root_inode,
        "volume_id" => vol_hash,
        "cookie" => 0
      })

      reply = assert_ok(1)
      assert reply["type"] == "dir_entries"
      names = Enum.map(reply["entries"], & &1["name"])
      assert names == [".", ".."]
    end

    test ". points to directory itself and .. points to virtual root for volume root", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)

      send_op(handler, 1, "readdirplus", %{
        "inode" => root_inode,
        "volume_id" => vol_hash,
        "cookie" => 0
      })

      reply = assert_ok(1)
      dot = Enum.find(reply["entries"], &(&1["name"] == "."))
      dotdot = Enum.find(reply["entries"], &(&1["name"] == ".."))

      assert dot["file_id"] == root_inode
      assert dotdot["file_id"] == 1
      assert dot["kind"] == "directory"
      assert dotdot["kind"] == "directory"
    end

    test "lists created files alongside . and ..", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      create_file(handler, vol_hash, root_inode, "alpha.txt")
      create_file(handler, vol_hash, root_inode, "beta.txt")

      send_op(handler, 1, "readdirplus", %{
        "inode" => root_inode,
        "volume_id" => vol_hash,
        "cookie" => 0
      })

      reply = assert_ok(1)
      names = Enum.map(reply["entries"], & &1["name"])
      assert "." in names
      assert ".." in names
      assert "alpha.txt" in names
      assert "beta.txt" in names
    end

    test "directory entries include non-epoch timestamps", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      create_file(handler, vol_hash, root_inode, "timestamped.txt")

      send_op(handler, 1, "readdirplus", %{
        "inode" => root_inode,
        "volume_id" => vol_hash,
        "cookie" => 0
      })

      reply = assert_ok(1)

      entry =
        Enum.find(reply["entries"], fn e -> e["name"] == "timestamped.txt" end)

      assert entry["mtime_secs"] > 0,
             "mtime should not be epoch (0), got: #{entry["mtime_secs"]}"

      assert entry["atime_secs"] > 0,
             "atime should not be epoch (0), got: #{entry["atime_secs"]}"

      assert entry["ctime_secs"] > 0,
             "ctime should not be epoch (0), got: #{entry["ctime_secs"]}"
    end
  end

  describe "cache invalidation" do
    test "readdirplus shows files created after initial empty listing", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      cache_table = :ets.new(:test_cache, [:set, :public, read_concurrency: true])
      :sys.replace_state(handler, fn state -> %{state | cache_table: cache_table} end)
      {vol_hash, root_inode} = register_volume(handler)

      send_op(handler, 1, "readdirplus", %{
        "inode" => root_inode,
        "volume_id" => vol_hash,
        "cookie" => 0
      })

      reply = assert_ok(1)
      names = Enum.map(reply["entries"], & &1["name"])
      assert names == [".", ".."]

      create_file(handler, vol_hash, root_inode, "after.txt")

      send_op(handler, 2, "readdirplus", %{
        "inode" => root_inode,
        "volume_id" => vol_hash,
        "cookie" => 0
      })

      reply = assert_ok(2)
      names = Enum.map(reply["entries"], & &1["name"])
      assert "after.txt" in names
    end

    test "getattr reflects updated size after write", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      cache_table = :ets.new(:test_cache2, [:set, :public, read_concurrency: true])
      :sys.replace_state(handler, fn state -> %{state | cache_table: cache_table} end)
      {vol_hash, root_inode} = register_volume(handler)
      created = create_file(handler, vol_hash, root_inode, "sized.txt")
      file_inode = created["file_id"]

      send_op(handler, 1, "getattr", %{"inode" => file_inode, "volume_id" => vol_hash})
      reply = assert_ok(1)
      assert reply["size"] == 0

      send_op(handler, 2, "write", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "data" => "hello"
      })

      assert_ok(2)

      send_op(handler, 3, "getattr", %{"inode" => file_inode, "volume_id" => vol_hash})
      reply = assert_ok(3)
      assert reply["size"] == 5
    end
  end

  ## Edge Cases

  describe "unknown operations" do
    test "returns ENOSYS", ctx do
      %{handler: handler} = start_handler_without_core(ctx)

      send_op(handler, 1, "nonexistent_op", %{})
      assert_error(1, @enosys)
    end
  end

  describe "write throttle" do
    @ejukebox 10_008

    alias NeonFS.NFS.WriteThrottle

    test "returns JUKEBOX when write throttle is saturated", ctx do
      start_supervised!(WriteThrottle)

      Application.put_env(:neonfs_nfs, :max_in_flight_writes, 1)
      Application.put_env(:neonfs_nfs, :write_acquire_timeout, 50)

      on_exit(fn ->
        Application.delete_env(:neonfs_nfs, :max_in_flight_writes)
        Application.delete_env(:neonfs_nfs, :write_acquire_timeout)
      end)

      # Hold a permit so the handler can't acquire one
      {:ok, _permit} = WriteThrottle.acquire(100)

      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      created = create_file(handler, vol_hash, root_inode, "throttled.txt")
      file_inode = created["file_id"]

      send_op(handler, 1, "write", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "data" => "data"
      })

      assert_error(1, @ejukebox)
    end

    test "allows write when throttle has capacity", ctx do
      start_supervised!(WriteThrottle)

      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)
      created = create_file(handler, vol_hash, root_inode, "ok.txt")
      file_inode = created["file_id"]

      send_op(handler, 1, "write", %{
        "inode" => file_inode,
        "volume_id" => vol_hash,
        "offset" => 0,
        "data" => "hello"
      })

      reply = assert_ok(1)
      assert reply["count"] == 5
    end
  end

  describe "telemetry" do
    test "emits request telemetry", ctx do
      %{handler: handler} = start_handler_without_core(ctx)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :nfs, :request, :stop]
        ])

      send_op(handler, 1, "getattr", %{"inode" => 1, "volume_id" => @null_volume_id})
      assert_ok(1)

      assert_receive {[:neonfs, :nfs, :request, :stop], ^ref, %{duration: _},
                      %{operation: "getattr", result: :ok}},
                     1_000
    end
  end
end
