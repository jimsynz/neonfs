defmodule NeonFS.WebDAV.BackendTest do
  use ExUnit.Case, async: false

  alias NeonFS.WebDAV.Backend
  alias NeonFS.WebDAV.Test.MockCore

  @auth %{user: "anonymous"}

  setup do
    MockCore.setup()

    Application.put_env(:neonfs_webdav, :core_call_fn, fn function, args ->
      apply(MockCore, function, args)
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
    test "reads file content" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/hello.txt", "Hello, world!")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "hello.txt"])

      assert {:ok, "Hello, world!"} = Backend.get_content(@auth, resource, %{})
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

      assert {:ok, "56789"} = Backend.get_content(@auth, resource, %{range: {5, 9}})
    end

    test "returns content from offset to end for open-ended range" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/open.txt", "0123456789")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "open.txt"])

      assert {:ok, "6789"} = Backend.get_content(@auth, resource, %{range: {6, nil}})
    end

    test "returns full content when no range specified" do
      MockCore.create_volume("docs")
      MockCore.write_file("docs", "/full.txt", "complete content")
      {:ok, resource} = Backend.resolve(@auth, ["docs", "full.txt"])

      assert {:ok, "complete content"} = Backend.get_content(@auth, resource, %{})
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

    test "preserves dead properties on copy across volumes" do
      MockCore.create_volume("vol-a")
      MockCore.create_volume("vol-b")
      MockCore.write_file("vol-a", "/source.txt", "content")
      {:ok, resource} = Backend.resolve(@auth, ["vol-a", "source.txt"])

      :ok =
        Backend.set_properties(@auth, resource, [{:set, {"urn:test:", "tag"}, "cross-vol"}])

      {:ok, updated_resource} = Backend.resolve(@auth, ["vol-a", "source.txt"])

      assert {:ok, :created} =
               Backend.copy(@auth, updated_resource, ["vol-b", "source.txt"], true)

      {:ok, copy_meta} = MockCore.get_file_meta("vol-b", "/source.txt")
      assert copy_meta.metadata["urn:test:tag"] == "cross-vol"
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

    test "moves between volumes via copy+delete" do
      MockCore.create_volume("vol-a")
      MockCore.create_volume("vol-b")
      MockCore.write_file("vol-a", "/file.txt", "cross-volume")
      {:ok, resource} = Backend.resolve(@auth, ["vol-a", "file.txt"])

      assert {:ok, :created} = Backend.move(@auth, resource, ["vol-b", "file.txt"], true)

      assert {:error, :not_found} = MockCore.read_file("vol-a", "/file.txt")
      assert {:ok, "cross-volume"} = MockCore.read_file("vol-b", "/file.txt")
    end

    test "preserves content type on cross-volume move" do
      MockCore.create_volume("vol-a")
      MockCore.create_volume("vol-b")
      MockCore.write_file("vol-a", "/data.bin", "csv,data", content_type: "text/csv")
      {:ok, resource} = Backend.resolve(@auth, ["vol-a", "data.bin"])

      assert {:ok, :created} = Backend.move(@auth, resource, ["vol-b", "data.bin"], true)

      {:ok, moved_meta} = MockCore.get_file_meta("vol-b", "/data.bin")
      assert moved_meta.content_type == "text/csv"
    end

    test "preserves dead properties on cross-volume move" do
      MockCore.create_volume("vol-a")
      MockCore.create_volume("vol-b")
      MockCore.write_file("vol-a", "/props.txt", "content")
      {:ok, resource} = Backend.resolve(@auth, ["vol-a", "props.txt"])

      :ok =
        Backend.set_properties(@auth, resource, [
          {:set, {"custom:", "author"}, "James"},
          {:set, {"urn:ns:", "rating"}, "5"}
        ])

      {:ok, updated_resource} = Backend.resolve(@auth, ["vol-a", "props.txt"])

      assert {:ok, :created} =
               Backend.move(@auth, updated_resource, ["vol-b", "props.txt"], true)

      assert {:error, :not_found} = MockCore.read_file("vol-a", "/props.txt")

      {:ok, moved_meta} = MockCore.get_file_meta("vol-b", "/props.txt")
      assert moved_meta.metadata["custom:author"] == "James"
      assert moved_meta.metadata["urn:ns:rating"] == "5"
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
