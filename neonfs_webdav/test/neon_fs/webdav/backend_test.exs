defmodule NeonFS.WebDAV.BackendTest do
  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Client.ChunkReader
  alias NeonFS.WebDAV.Backend
  alias NeonFS.WebDAV.LockStore
  alias NeonFS.WebDAV.Test.MockCore

  @auth %{user: "anonymous"}

  setup :set_mimic_global
  setup :verify_on_exit!

  setup do
    MockCore.setup()

    Application.put_env(:neonfs_webdav, :core_call_fn, fn function, args ->
      apply(MockCore, function, args)
    end)

    stub(ChunkReader, :read_file, fn volume_name, path, opts ->
      MockCore.read_file(volume_name, path, opts)
    end)

    stub(ChunkReader, :read_file_stream, fn volume_name, path, opts ->
      MockCore.read_file_stream(volume_name, path, opts)
    end)

    on_exit(fn ->
      Application.delete_env(:neonfs_webdav, :core_call_fn)
    end)

    :ok
  end

  # Authentication

  describe "authenticate/1" do
    test "accepts all connections" do
      conn = Plug.Test.conn(:get, "/")
      assert {:ok, %{user: "anonymous"}} = Backend.authenticate(conn)
    end
  end

  # Resource resolution

  describe "resolve/2" do
    test "resolves root path to root collection" do
      assert {:ok, resource} = Backend.resolve(@auth, [])
      assert resource.type == :collection
      assert resource.path == []
      assert resource.display_name == "NeonFS"
    end

    test "resolves volume path to volume collection" do
      MockCore.create_volume("docs")

      assert {:ok, resource} = Backend.resolve(@auth, ["docs"])
      assert resource.type == :collection
      assert resource.path == ["docs"]
      assert resource.display_name == "docs"
    end

    test "returns not_found for unknown volume" do
      assert {:error, %WebdavServer.Error{code: :not_found}} =
               Backend.resolve(@auth, ["nonexistent"])
    end

    test "resolves file path within volume" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/readme.txt", "hello")

      assert {:ok, resource} = Backend.resolve(@auth, ["docs", "readme.txt"])
      assert resource.type == :file
      assert resource.path == ["docs", "readme.txt"]
      assert resource.content_length == 5
      assert resource.content_type == "text/plain"
      assert resource.display_name == "readme.txt"
    end

    test "resolves directory path within volume" do
      MockCore.create_volume("docs")
      MockCore.mkdir("docs", "/subdir")

      assert {:ok, resource} = Backend.resolve(@auth, ["docs", "subdir"])
      assert resource.type == :collection
      assert resource.path == ["docs", "subdir"]
    end

    test "returns not_found for missing file" do
      MockCore.create_volume("docs")

      assert {:error, %WebdavServer.Error{code: :not_found}} =
               Backend.resolve(@auth, ["docs", "missing.txt"])
    end
  end

  # Properties

  describe "get_properties/3" do
    test "returns standard DAV properties for a file" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/test.txt", "content")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "test.txt"])

      props = [
        {"DAV:", "resourcetype"},
        {"DAV:", "getcontentlength"},
        {"DAV:", "getcontenttype"},
        {"DAV:", "getetag"},
        {"DAV:", "displayname"}
      ]

      results = Backend.get_properties(@auth, resource, props)

      assert {{"DAV:", "resourcetype"}, {:ok, nil}} in results
      assert {{"DAV:", "getcontentlength"}, {:ok, "7"}} in results
      assert {{"DAV:", "getcontenttype"}, {:ok, "text/plain"}} in results
      assert {{"DAV:", "displayname"}, {:ok, "test.txt"}} in results

      etag_result = Enum.find(results, fn {prop, _} -> prop == {"DAV:", "getetag"} end)
      assert {_, {:ok, etag}} = etag_result
      assert is_binary(etag)
    end

    test "returns collection resourcetype for directories" do
      MockCore.create_volume("docs")
      MockCore.mkdir("docs", "/subdir")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "subdir"])

      results = Backend.get_properties(@auth, resource, [{"DAV:", "resourcetype"}])
      assert {{"DAV:", "resourcetype"}, {:ok, :collection}} in results
    end

    test "returns not_found for unknown properties" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/test.txt", "x")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "test.txt"])

      results = Backend.get_properties(@auth, resource, [{"custom:", "prop"}])
      assert {{"custom:", "prop"}, {:error, :not_found}} in results
    end

    test "returns dead properties stored in metadata" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/props.txt", "data")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "props.txt"])

      :ok = Backend.set_properties(@auth, resource, [{:set, {"custom:", "colour"}, "blue"}])

      {:ok, updated_resource} = Backend.resolve(@auth, ["docs", "props.txt"])
      results = Backend.get_properties(@auth, updated_resource, [{"custom:", "colour"}])
      assert {{"custom:", "colour"}, {:ok, "blue"}} in results
    end
  end

  describe "set_properties/3" do
    test "sets a single dead property" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/test.txt", "content")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "test.txt"])

      assert :ok =
               Backend.set_properties(@auth, resource, [{:set, {"custom:", "author"}, "James"}])

      {:ok, meta} = MockCore.get_file_meta("docs", "/test.txt")
      assert meta.metadata["custom:author"] == "James"
    end

    test "sets multiple dead properties" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/multi.txt", "content")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "multi.txt"])

      operations = [
        {:set, {"custom:", "author"}, "James"},
        {:set, {"custom:", "priority"}, "high"}
      ]

      assert :ok = Backend.set_properties(@auth, resource, operations)

      {:ok, meta} = MockCore.get_file_meta("docs", "/multi.txt")
      assert meta.metadata["custom:author"] == "James"
      assert meta.metadata["custom:priority"] == "high"
    end

    test "removes a dead property" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/remove.txt", "content")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "remove.txt"])

      :ok = Backend.set_properties(@auth, resource, [{:set, {"custom:", "temp"}, "value"}])

      {:ok, updated_resource} = Backend.resolve(@auth, ["docs", "remove.txt"])
      :ok = Backend.set_properties(@auth, updated_resource, [{:remove, {"custom:", "temp"}}])

      {:ok, meta} = MockCore.get_file_meta("docs", "/remove.txt")
      refute Map.has_key?(meta.metadata, "custom:temp")
    end

    test "set and remove in the same request" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/mixed.txt", "content")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "mixed.txt"])

      :ok = Backend.set_properties(@auth, resource, [{:set, {"custom:", "old"}, "stale"}])

      {:ok, updated_resource} = Backend.resolve(@auth, ["docs", "mixed.txt"])

      operations = [
        {:set, {"custom:", "new"}, "fresh"},
        {:remove, {"custom:", "old"}}
      ]

      assert :ok = Backend.set_properties(@auth, updated_resource, operations)

      {:ok, meta} = MockCore.get_file_meta("docs", "/mixed.txt")
      assert meta.metadata["custom:new"] == "fresh"
      refute Map.has_key?(meta.metadata, "custom:old")
    end

    test "overwriting an existing property" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/overwrite.txt", "content")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "overwrite.txt"])

      :ok = Backend.set_properties(@auth, resource, [{:set, {"custom:", "val"}, "first"}])

      {:ok, updated_resource} = Backend.resolve(@auth, ["docs", "overwrite.txt"])

      :ok =
        Backend.set_properties(@auth, updated_resource, [{:set, {"custom:", "val"}, "second"}])

      {:ok, meta} = MockCore.get_file_meta("docs", "/overwrite.txt")
      assert meta.metadata["custom:val"] == "second"
    end

    test "removing a non-existent property succeeds" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/noop.txt", "content")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "noop.txt"])

      assert :ok =
               Backend.set_properties(@auth, resource, [{:remove, {"custom:", "nonexistent"}}])
    end

    test "forbids setting properties on root" do
      {:ok, root} = Backend.resolve(@auth, [])

      assert {:error, %WebdavServer.Error{code: :forbidden}} =
               Backend.set_properties(@auth, root, [{:set, {"custom:", "x"}, "y"}])
    end

    test "forbids setting properties on volumes" do
      MockCore.create_volume("docs")
      {:ok, volume} = Backend.resolve(@auth, ["docs"])

      assert {:error, %WebdavServer.Error{code: :forbidden}} =
               Backend.set_properties(@auth, volume, [{:set, {"custom:", "x"}, "y"}])
    end

    test "preserves properties with different namespaces" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/ns.txt", "content")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "ns.txt"])

      operations = [
        {:set, {"urn:custom:", "tag"}, "alpha"},
        {:set, {"urn:other:", "tag"}, "beta"}
      ]

      :ok = Backend.set_properties(@auth, resource, operations)

      {:ok, updated} = Backend.resolve(@auth, ["docs", "ns.txt"])

      results =
        Backend.get_properties(@auth, updated, [{"urn:custom:", "tag"}, {"urn:other:", "tag"}])

      assert {{"urn:custom:", "tag"}, {:ok, "alpha"}} in results
      assert {{"urn:other:", "tag"}, {:ok, "beta"}} in results
    end
  end

  # File operations

  describe "get_content/3" do
    test "reads file content as a stream" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/hello.txt", "Hello, world!")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "hello.txt"])

      assert {:ok, stream} = Backend.get_content(@auth, resource, %{})
      assert Enum.into(stream, <<>>) == "Hello, world!"
    end

    test "returns not_found for missing file" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/exists.txt", "x")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "exists.txt"])

      # Delete after resolving
      MockCore.delete_file("docs", "/exists.txt")

      assert {:error, %WebdavServer.Error{code: :not_found}} =
               Backend.get_content(@auth, resource, %{})
    end

    test "returns partial content for range request" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/range.txt", "0123456789ABCDEF")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "range.txt"])

      assert {:ok, stream} = Backend.get_content(@auth, resource, %{range: {5, 9}})
      assert Enum.into(stream, <<>>) == "56789"
    end

    test "returns content from offset to end for open-ended range" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/open.txt", "0123456789")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "open.txt"])

      assert {:ok, stream} = Backend.get_content(@auth, resource, %{range: {6, nil}})
      assert Enum.into(stream, <<>>) == "6789"
    end

    test "returns full content when no range specified" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/full.txt", "complete content")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "full.txt"])

      assert {:ok, stream} = Backend.get_content(@auth, resource, %{})
      assert Enum.into(stream, <<>>) == "complete content"
    end
  end

  describe "get_content/3 — streaming routing" do
    test "dispatches GET through NeonFS.Client.ChunkReader.read_file_stream" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/dp.txt", "over data plane")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "dp.txt"])

      test_pid = self()

      expect(ChunkReader, :read_file_stream, fn volume_name, path, opts ->
        send(test_pid, {:chunk_reader_stream_called, volume_name, path, opts})
        MockCore.read_file_stream(volume_name, path, opts)
      end)

      assert {:ok, stream} = Backend.get_content(@auth, resource, %{})
      assert Enum.into(stream, <<>>) == "over data plane"

      assert_receive {:chunk_reader_stream_called, "docs", "/dp.txt", opts}, 1_000
      refute Keyword.has_key?(opts, :offset)
      refute Keyword.has_key?(opts, :length)
    end

    test "forwards range requests as :offset/:length through ChunkReader" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/range.bin", "0123456789ABCDEF")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "range.bin"])

      expect(ChunkReader, :read_file_stream, fn "docs", "/range.bin", opts ->
        assert Keyword.get(opts, :offset) == 5
        assert Keyword.get(opts, :length) == 5
        MockCore.read_file_stream("docs", "/range.bin", opts)
      end)

      assert {:ok, stream} = Backend.get_content(@auth, resource, %{range: {5, 9}})
      assert Enum.into(stream, <<>>) == "56789"
    end

    test "forwards open-ended range requests as :offset only" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/open.bin", "0123456789")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "open.bin"])

      expect(ChunkReader, :read_file_stream, fn "docs", "/open.bin", opts ->
        assert Keyword.get(opts, :offset) == 6
        refute Keyword.has_key?(opts, :length)
        MockCore.read_file_stream("docs", "/open.bin", opts)
      end)

      assert {:ok, stream} = Backend.get_content(@auth, resource, %{range: {6, nil}})
      assert Enum.into(stream, <<>>) == "6789"
    end

    test "maps ChunkReader :not_found to WebDAV not_found" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/exists.txt", "hi")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "exists.txt"])

      expect(ChunkReader, :read_file_stream, fn _, _, _ -> {:error, :not_found} end)

      assert {:error, %WebdavServer.Error{code: :not_found}} =
               Backend.get_content(@auth, resource, %{})
    end

    test "maps other ChunkReader errors to bad_request" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/exists.txt", "hi")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "exists.txt"])

      expect(ChunkReader, :read_file_stream, fn _, _, _ ->
        {:error, :no_available_locations}
      end)

      assert {:error, %WebdavServer.Error{code: :bad_request}} =
               Backend.get_content(@auth, resource, %{})
    end

    test "core_stream_fn override replaces ChunkReader streaming" do
      Application.put_env(:neonfs_webdav, :core_stream_fn, fn volume, path, opts ->
        MockCore.read_file_stream(volume, path, opts)
      end)

      on_exit(fn -> Application.delete_env(:neonfs_webdav, :core_stream_fn) end)

      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/stream.txt", "streaming")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "stream.txt"])

      reject(&ChunkReader.read_file_stream/3)

      assert {:ok, stream} = Backend.get_content(@auth, resource, %{})
      assert Enum.into(stream, <<>>) == "streaming"
    end
  end

  describe "put_content/4" do
    test "creates a new file" do
      MockCore.create_volume("docs")

      assert {:ok, resource} = Backend.put_content(@auth, ["docs", "new.txt"], "data", %{})
      assert resource.type == :file
      assert resource.content_length == 4
    end

    test "overwrites existing file" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/existing.txt", "old")

      assert {:ok, resource} =
               Backend.put_content(@auth, ["docs", "existing.txt"], "new content", %{})

      assert resource.content_length == 11
    end

    test "returns error for unknown volume" do
      assert {:error, %WebdavServer.Error{code: :conflict}} =
               Backend.put_content(@auth, ["nonexistent", "file.txt"], "data", %{})
    end

    test "handles iodata body" do
      MockCore.create_volume("docs")

      assert {:ok, resource} =
               Backend.put_content(@auth, ["docs", "io.txt"], ["hello", " ", "world"], %{})

      assert resource.content_length == 11
    end

    test "creates file in nested path" do
      MockCore.create_volume("docs")

      assert {:ok, resource} =
               Backend.put_content(@auth, ["docs", "sub", "dir", "file.txt"], "nested", %{})

      assert resource.path == ["docs", "sub", "dir", "file.txt"]
    end

    test "auto-detects content type from file extension" do
      MockCore.create_volume("docs")

      assert {:ok, resource} =
               Backend.put_content(@auth, ["docs", "image.png"], "PNG data", %{})

      assert resource.content_type == "image/png"
    end

    test "honours client-provided content type" do
      MockCore.create_volume("docs")

      assert {:ok, resource} =
               Backend.put_content(
                 @auth,
                 ["docs", "data.bin"],
                 "csv,data",
                 %{content_type: "text/csv"}
               )

      assert resource.content_type == "text/csv"
    end

    test "falls back to extension detection when client sends octet-stream" do
      MockCore.create_volume("docs")

      assert {:ok, resource} =
               Backend.put_content(
                 @auth,
                 ["docs", "doc.html"],
                 "<html></html>",
                 %{content_type: "application/octet-stream"}
               )

      assert resource.content_type == "text/html"
    end
  end

  describe "put_content_stream/4" do
    test "writes a streamed file end-to-end" do
      MockCore.create_volume("docs")
      stream = Stream.map(["hello ", "streamed ", "world"], & &1)

      assert {:ok, resource} =
               Backend.put_content_stream(@auth, ["docs", "stream.txt"], stream, %{})

      assert resource.content_length == 20
      assert {:ok, "hello streamed world"} = MockCore.read_file("docs", "/stream.txt")
    end

    test "honours client-provided content type" do
      MockCore.create_volume("docs")

      assert {:ok, resource} =
               Backend.put_content_stream(
                 @auth,
                 ["docs", "data.bin"],
                 ["x"],
                 %{content_type: "text/csv"}
               )

      assert resource.content_type == "text/csv"
    end

    test "returns conflict for unknown volume" do
      assert {:error, %WebdavServer.Error{code: :conflict}} =
               Backend.put_content_stream(@auth, ["nope", "f.txt"], ["data"], %{})
    end

    test "rejects writes at root" do
      assert {:error, %WebdavServer.Error{code: :forbidden}} =
               Backend.put_content_stream(@auth, [], ["data"], %{})
    end
  end

  describe "delete/2" do
    test "deletes a file" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/delete-me.txt", "bye")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "delete-me.txt"])

      assert :ok = Backend.delete(@auth, resource)

      assert {:error, %WebdavServer.Error{code: :not_found}} =
               Backend.resolve(@auth, ["docs", "delete-me.txt"])
    end

    test "succeeds silently for already deleted file" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/temp.txt", "x")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "temp.txt"])
      MockCore.delete_file("docs", "/temp.txt")

      assert :ok = Backend.delete(@auth, resource)
    end

    test "forbids deleting root" do
      {:ok, root} = Backend.resolve(@auth, [])

      assert {:error, %WebdavServer.Error{code: :forbidden}} = Backend.delete(@auth, root)
    end

    test "forbids deleting volumes" do
      MockCore.create_volume("protected")
      {:ok, volume} = Backend.resolve(@auth, ["protected"])

      assert {:error, %WebdavServer.Error{code: :forbidden}} = Backend.delete(@auth, volume)
    end
  end

  # Copy and move

  describe "copy/4" do
    test "copies a file within the same volume" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/original.txt", "content")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "original.txt"])

      assert {:ok, :created} = Backend.copy(@auth, resource, ["docs", "copy.txt"], true)

      {:ok, "content"} = MockCore.read_file("docs", "/copy.txt")
    end

    test "preserves content type on copy" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/data.bin", "csv,data", content_type: "text/csv")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "data.bin"])

      assert {:ok, :created} = Backend.copy(@auth, resource, ["docs", "copy.bin"], true)

      {:ok, copy_meta} = MockCore.get_file_meta("docs", "/copy.bin")
      assert copy_meta.content_type == "text/csv"
    end

    test "returns no_content when overwriting" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/src.txt", "source")
      MockCore.write_file("docs", "/dst.txt", "old")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "src.txt"])

      assert {:ok, :no_content} = Backend.copy(@auth, resource, ["docs", "dst.txt"], true)
    end

    test "respects overwrite=false" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/src.txt", "source")
      MockCore.write_file("docs", "/dst.txt", "old")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "src.txt"])

      assert {:error, %WebdavServer.Error{code: :precondition_failed}} =
               Backend.copy(@auth, resource, ["docs", "dst.txt"], false)
    end

    test "preserves dead properties on copy within same volume" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/original.txt", "content")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "original.txt"])

      :ok =
        Backend.set_properties(@auth, resource, [
          {:set, {"custom:", "author"}, "James"},
          {:set, {"custom:", "priority"}, "high"}
        ])

      {:ok, updated_resource} = Backend.resolve(@auth, ["docs", "original.txt"])
      assert {:ok, :created} = Backend.copy(@auth, updated_resource, ["docs", "copy.txt"], true)

      {:ok, copy_meta} = MockCore.get_file_meta("docs", "/copy.txt")
      assert copy_meta.metadata["custom:author"] == "James"
      assert copy_meta.metadata["custom:priority"] == "high"
    end

    test "rejects cross-volume copy with bad_gateway" do
      MockCore.create_volume("vol-a")
      MockCore.create_volume("vol-b")
      MockCore.write_file("vol-a", "/source.txt", "content")
      {:ok, resource} = Backend.resolve(@auth, ["vol-a", "source.txt"])

      assert {:error, %WebdavServer.Error{code: :bad_gateway}} =
               Backend.copy(@auth, resource, ["vol-b", "source.txt"], true)

      assert {:error, :not_found} = MockCore.read_file("vol-b", "/source.txt")
    end
  end

  describe "move/4" do
    test "moves a file within the same volume" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/old-name.txt", "content")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "old-name.txt"])

      assert {:ok, :created} = Backend.move(@auth, resource, ["docs", "new-name.txt"], true)

      assert {:error, :not_found} = MockCore.read_file("docs", "/old-name.txt")
      assert {:ok, "content"} = MockCore.read_file("docs", "/new-name.txt")
    end

    test "returns no_content when overwriting" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/src.txt", "source")
      MockCore.write_file("docs", "/dst.txt", "old")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "src.txt"])

      assert {:ok, :no_content} = Backend.move(@auth, resource, ["docs", "dst.txt"], true)
    end

    test "rejects cross-volume move with bad_gateway" do
      MockCore.create_volume("vol-a")
      MockCore.create_volume("vol-b")
      MockCore.write_file("vol-a", "/file.txt", "cross-volume")
      {:ok, resource} = Backend.resolve(@auth, ["vol-a", "file.txt"])

      assert {:error, %WebdavServer.Error{code: :bad_gateway}} =
               Backend.move(@auth, resource, ["vol-b", "file.txt"], true)

      assert {:ok, "cross-volume"} = MockCore.read_file("vol-a", "/file.txt")
      assert {:error, :not_found} = MockCore.read_file("vol-b", "/file.txt")
    end
  end

  # Collection operations

  describe "create_collection/2" do
    test "creates a directory" do
      MockCore.create_volume("docs")

      assert :ok = Backend.create_collection(@auth, ["docs", "new-dir"])

      {:ok, resource} = Backend.resolve(@auth, ["docs", "new-dir"])
      assert resource.type == :collection
    end

    test "returns error for duplicate directory" do
      MockCore.create_volume("docs")
      MockCore.mkdir("docs", "/existing")

      assert {:error, %WebdavServer.Error{code: :method_not_allowed}} =
               Backend.create_collection(@auth, ["docs", "existing"])
    end

    test "returns conflict for unknown volume" do
      assert {:error, %WebdavServer.Error{code: :conflict}} =
               Backend.create_collection(@auth, ["nonexistent", "dir"])
    end

    test "forbids creating root collection" do
      assert {:error, %WebdavServer.Error{code: :forbidden}} =
               Backend.create_collection(@auth, [])
    end
  end

  describe "lock-null resources" do
    setup do
      LockStore.reset()

      Application.put_env(:neonfs_webdav, :lock_manager_call_fn, fn function, _args ->
        case function do
          :lock -> :ok
          :unlock -> :ok
          :renew -> :ok
        end
      end)

      on_exit(fn ->
        Application.delete_env(:neonfs_webdav, :lock_manager_call_fn)
        LockStore.reset()
      end)

      :ok
    end

    test "resolve returns lock-null resource when path has active lock-null" do
      MockCore.create_volume("docs")
      path = ["docs", "new-file.txt"]

      {:ok, _token} = LockStore.lock(path, :exclusive, :write, 0, "user-a", 300)

      assert {:ok, resource} = Backend.resolve(@auth, path)
      assert resource.type == :file
      assert resource.content_length == 0
      assert resource.content_type == "application/octet-stream"
      assert resource.display_name == "new-file.txt"
      assert resource.backend_data.lock_null == true
    end

    test "resolve returns not_found when no lock-null exists" do
      MockCore.create_volume("docs")

      assert {:error, %WebdavServer.Error{code: :not_found}} =
               Backend.resolve(@auth, ["docs", "missing.txt"])
    end

    test "get_content returns not_found for lock-null resource" do
      MockCore.create_volume("docs")
      path = ["docs", "locked-new.txt"]

      {:ok, _token} = LockStore.lock(path, :exclusive, :write, 0, "user-a", 300)
      {:ok, resource} = Backend.resolve(@auth, path)

      assert {:error, %WebdavServer.Error{code: :not_found}} =
               Backend.get_content(@auth, resource, %{})
    end

    test "put_content promotes lock-null to real resource" do
      MockCore.create_volume("docs")
      path = ["docs", "promoted.txt"]

      {:ok, _token} = LockStore.lock(path, :exclusive, :write, 0, "user-a", 300)
      assert LockStore.lock_null?(path)

      {:ok, resource} = Backend.put_content(@auth, path, "real content", %{})
      assert resource.type == :file
      assert resource.content_length == 12

      refute LockStore.lock_null?(path)
      assert [lock] = LockStore.get_locks(path)
      assert lock.scope == :exclusive
    end

    test "get_members includes lock-null resources in volume listing" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/existing.txt", "hello")

      lock_null_path = ["docs", "pending.txt"]

      {:ok, _token} =
        LockStore.lock(lock_null_path, :exclusive, :write, 0, "user-a", 300)

      {:ok, volume} = Backend.resolve(@auth, ["docs"])
      {:ok, members} = Backend.get_members(@auth, volume)

      names = Enum.map(members, & &1.display_name)
      assert "existing.txt" in names
      assert "pending.txt" in names
    end

    test "get_members includes lock-null resources in subdirectory listing" do
      MockCore.create_volume("docs")
      MockCore.mkdir("docs", "/sub")
      MockCore.write_file("docs", "/sub/real.txt", "data")

      lock_null_path = ["docs", "sub", "draft.txt"]

      {:ok, _token} =
        LockStore.lock(lock_null_path, :exclusive, :write, 0, "user-a", 300)

      {:ok, dir} = Backend.resolve(@auth, ["docs", "sub"])
      {:ok, members} = Backend.get_members(@auth, dir)

      names = Enum.map(members, & &1.display_name)
      assert "real.txt" in names
      assert "draft.txt" in names
    end

    test "delete cleans up lock-null resource" do
      MockCore.create_volume("docs")
      path = ["docs", "to-delete.txt"]

      {:ok, _token} = LockStore.lock(path, :exclusive, :write, 0, "user-a", 300)
      {:ok, resource} = Backend.resolve(@auth, path)

      assert :ok = Backend.delete(@auth, resource)
      refute LockStore.lock_null?(path)
      assert LockStore.get_locks(path) == []
    end

    test "resolve returns real resource after lock-null is promoted" do
      MockCore.create_volume("docs")
      path = ["docs", "upgrade.txt"]

      {:ok, _token} = LockStore.lock(path, :exclusive, :write, 0, "user-a", 300)
      assert LockStore.lock_null?(path)

      {:ok, _resource} = Backend.put_content(@auth, path, "now real", %{})

      {:ok, resource} = Backend.resolve(@auth, path)
      assert resource.backend_data.type == :file
      refute Map.get(resource.backend_data, :lock_null)
      assert resource.content_length == 8
    end
  end

  describe "get_members/2" do
    test "lists volumes at root" do
      MockCore.create_volume("alpha")
      MockCore.create_volume("beta")

      {:ok, root} = Backend.resolve(@auth, [])
      assert {:ok, members} = Backend.get_members(@auth, root)

      names = Enum.map(members, & &1.display_name)
      assert "alpha" in names
      assert "beta" in names
      assert length(members) == 2
    end

    test "lists files in volume root" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/a.txt", "aaa")
      MockCore.write_file("docs", "/b.txt", "bbb")
      MockCore.mkdir("docs", "/subdir")

      {:ok, volume} = Backend.resolve(@auth, ["docs"])
      assert {:ok, members} = Backend.get_members(@auth, volume)

      names = Enum.map(members, & &1.display_name)
      assert "a.txt" in names
      assert "b.txt" in names
      assert "subdir" in names
      assert length(members) == 3
    end

    test "lists files in subdirectory" do
      MockCore.create_volume("docs")
      MockCore.mkdir("docs", "/sub")
      MockCore.write_file("docs", "/sub/child.txt", "nested")

      {:ok, dir} = Backend.resolve(@auth, ["docs", "sub"])
      assert {:ok, members} = Backend.get_members(@auth, dir)

      assert length(members) == 1
      assert hd(members).display_name == "child.txt"
    end

    test "returns empty list for empty directory" do
      MockCore.create_volume("docs")
      MockCore.mkdir("docs", "/empty")

      {:ok, dir} = Backend.resolve(@auth, ["docs", "empty"])
      assert {:ok, []} = Backend.get_members(@auth, dir)
    end
  end
end
