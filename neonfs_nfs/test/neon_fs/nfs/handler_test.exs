defmodule NeonFS.NFS.HandlerTest do
  use ExUnit.Case, async: false

  alias NeonFS.NFS.{Handler, InodeTable, MockCore}

  @volume "testvol"
  @null_volume_id <<0::128>>

  # errno constants
  @enoent 2
  @eio 5
  @eexist 17
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
    send_op(handler, 0, "lookup", %{
      "parent_inode" => 1,
      "parent_volume_id" => @null_volume_id,
      "name" => @volume
    })

    reply = assert_ok(0)
    vol_hash = :crypto.hash(:md5, @volume)
    root_inode = reply["file_id"]
    {vol_hash, root_inode}
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
    test "returns empty list for empty volume", ctx do
      %{handler: handler} = start_handler_with_mock(ctx)
      {vol_hash, root_inode} = register_volume(handler)

      send_op(handler, 1, "readdirplus", %{
        "inode" => root_inode,
        "volume_id" => vol_hash,
        "cookie" => 0
      })

      reply = assert_ok(1)
      assert reply["type"] == "dir_entries"
      assert reply["entries"] == []
    end

    test "lists created files", ctx do
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
      assert "alpha.txt" in names
      assert "beta.txt" in names
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
      assert reply["entries"] == []

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
