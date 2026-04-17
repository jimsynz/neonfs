defmodule NeonFS.S3.BackendTest do
  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Client.ChunkReader
  alias NeonFS.S3.Backend
  alias NeonFS.S3.MultipartStore
  alias NeonFS.S3.Test.MockCore

  @ctx %{access_key_id: "test-key", identity: %{user: "test-key"}}

  setup :set_mimic_global
  setup :verify_on_exit!

  setup do
    MockCore.setup()
    MockCore.add_credential("test-key", "test-secret")

    Application.put_env(:neonfs_s3, :core_call_fn, fn function, args ->
      apply(MockCore, function, args)
    end)

    stub(ChunkReader, :read_file, fn volume_name, path, opts ->
      MockCore.read_file(volume_name, path, opts)
    end)

    start_supervised!(MultipartStore)

    on_exit(fn ->
      Application.delete_env(:neonfs_s3, :core_call_fn)
    end)

    :ok
  end

  # Credential lookup

  describe "lookup_credential/1" do
    test "returns credential for known access key" do
      assert {:ok, cred} = Backend.lookup_credential("test-key")
      assert cred.access_key_id == "test-key"
      assert cred.secret_access_key == "test-secret"
      assert cred.identity == %{user: "test-key"}
    end

    test "returns error for unknown access key" do
      assert {:error, :not_found} = Backend.lookup_credential("unknown")
    end
  end

  # Bucket operations

  describe "create_bucket/2" do
    test "creates a new bucket" do
      assert :ok = Backend.create_bucket(@ctx, "my-bucket")
    end

    test "returns error for duplicate bucket" do
      Backend.create_bucket(@ctx, "my-bucket")

      assert {:error, %S3Server.Error{code: :bucket_already_exists}} =
               Backend.create_bucket(@ctx, "my-bucket")
    end
  end

  describe "list_buckets/1" do
    test "returns empty list when no buckets exist" do
      assert {:ok, []} = Backend.list_buckets(@ctx)
    end

    test "returns all buckets sorted by name" do
      Backend.create_bucket(@ctx, "zebra")
      Backend.create_bucket(@ctx, "alpha")

      assert {:ok, buckets} = Backend.list_buckets(@ctx)
      assert length(buckets) == 2
      assert Enum.at(buckets, 0).name == "alpha"
      assert Enum.at(buckets, 1).name == "zebra"
    end
  end

  describe "head_bucket/2" do
    test "returns ok for existing bucket" do
      Backend.create_bucket(@ctx, "my-bucket")
      assert :ok = Backend.head_bucket(@ctx, "my-bucket")
    end

    test "returns error for non-existent bucket" do
      assert {:error, %S3Server.Error{code: :no_such_bucket}} =
               Backend.head_bucket(@ctx, "missing")
    end
  end

  describe "delete_bucket/2" do
    test "deletes an empty bucket" do
      Backend.create_bucket(@ctx, "my-bucket")
      assert :ok = Backend.delete_bucket(@ctx, "my-bucket")

      assert {:error, %S3Server.Error{code: :no_such_bucket}} =
               Backend.head_bucket(@ctx, "my-bucket")
    end

    test "returns error for non-existent bucket" do
      assert {:error, %S3Server.Error{code: :no_such_bucket}} =
               Backend.delete_bucket(@ctx, "missing")
    end

    test "returns error for non-empty bucket" do
      Backend.create_bucket(@ctx, "my-bucket")
      Backend.put_object(@ctx, "my-bucket", "file.txt", "content", %S3Server.PutOpts{})

      assert {:error, %S3Server.Error{code: :bucket_not_empty}} =
               Backend.delete_bucket(@ctx, "my-bucket")
    end
  end

  describe "get_bucket_location/2" do
    test "returns region for existing bucket" do
      Backend.create_bucket(@ctx, "my-bucket")
      assert {:ok, _region} = Backend.get_bucket_location(@ctx, "my-bucket")
    end

    test "returns error for non-existent bucket" do
      assert {:error, %S3Server.Error{code: :no_such_bucket}} =
               Backend.get_bucket_location(@ctx, "missing")
    end
  end

  # Object operations

  describe "put_object/5" do
    test "stores an object and returns etag" do
      Backend.create_bucket(@ctx, "my-bucket")

      assert {:ok, etag} =
               Backend.put_object(
                 @ctx,
                 "my-bucket",
                 "hello.txt",
                 "hello world",
                 %S3Server.PutOpts{}
               )

      assert is_binary(etag)
      assert String.length(etag) == 32
    end

    test "returns error for non-existent bucket" do
      assert {:error, %S3Server.Error{code: :no_such_bucket}} =
               Backend.put_object(@ctx, "missing", "file.txt", "data", %S3Server.PutOpts{})
    end

    test "stores client-provided content type" do
      Backend.create_bucket(@ctx, "my-bucket")
      opts = %S3Server.PutOpts{content_type: "text/csv"}

      assert {:ok, _etag} = Backend.put_object(@ctx, "my-bucket", "data.bin", "a,b,c", opts)

      assert {:ok, object} =
               Backend.get_object(@ctx, "my-bucket", "data.bin", %S3Server.GetOpts{})

      assert object.content_type == "text/csv"
    end

    test "auto-detects content type from extension when client sends default" do
      Backend.create_bucket(@ctx, "my-bucket")

      assert {:ok, _etag} =
               Backend.put_object(
                 @ctx,
                 "my-bucket",
                 "page.html",
                 "<html></html>",
                 %S3Server.PutOpts{}
               )

      assert {:ok, object} =
               Backend.get_object(@ctx, "my-bucket", "page.html", %S3Server.GetOpts{})

      assert object.content_type == "text/html"
    end
  end

  describe "get_object/4" do
    test "retrieves a stored object" do
      Backend.create_bucket(@ctx, "my-bucket")
      Backend.put_object(@ctx, "my-bucket", "hello.txt", "hello world", %S3Server.PutOpts{})

      assert {:ok, object} =
               Backend.get_object(@ctx, "my-bucket", "hello.txt", %S3Server.GetOpts{})

      assert object.body == "hello world"
      assert object.content_length == 11
      assert object.content_type == "text/plain"
      assert is_binary(object.etag)
    end

    test "returns error for non-existent key" do
      Backend.create_bucket(@ctx, "my-bucket")

      assert {:error, %S3Server.Error{code: :no_such_key}} =
               Backend.get_object(@ctx, "my-bucket", "missing.txt", %S3Server.GetOpts{})
    end

    test "returns error for non-existent bucket" do
      assert {:error, %S3Server.Error{code: :no_such_bucket}} =
               Backend.get_object(@ctx, "missing", "file.txt", %S3Server.GetOpts{})
    end

    test "returns partial content for range request" do
      Backend.create_bucket(@ctx, "my-bucket")
      Backend.put_object(@ctx, "my-bucket", "data.txt", "0123456789ABCDEF", %S3Server.PutOpts{})

      opts = %S3Server.GetOpts{range: {5, 9}}

      assert {:ok, object} = Backend.get_object(@ctx, "my-bucket", "data.txt", opts)
      assert object.body == "56789"
      assert object.content_length == 5
      assert object.total_size == 16
    end

    test "range request clamps to file size" do
      Backend.create_bucket(@ctx, "my-bucket")
      Backend.put_object(@ctx, "my-bucket", "short.txt", "hello", %S3Server.PutOpts{})

      opts = %S3Server.GetOpts{range: {2, 100}}

      assert {:ok, object} = Backend.get_object(@ctx, "my-bucket", "short.txt", opts)
      assert object.body == "llo"
      assert object.content_length == 3
      assert object.total_size == 5
    end

    test "full request without range returns complete content" do
      Backend.create_bucket(@ctx, "my-bucket")
      Backend.put_object(@ctx, "my-bucket", "full.txt", "all content", %S3Server.PutOpts{})

      opts = %S3Server.GetOpts{range: nil}

      assert {:ok, object} = Backend.get_object(@ctx, "my-bucket", "full.txt", opts)
      assert object.body == "all content"
      assert object.content_length == 11
      assert object.total_size == 11
    end
  end

  describe "head_object/3" do
    test "returns metadata without body" do
      Backend.create_bucket(@ctx, "my-bucket")
      Backend.put_object(@ctx, "my-bucket", "hello.txt", "hello world", %S3Server.PutOpts{})

      assert {:ok, meta} = Backend.head_object(@ctx, "my-bucket", "hello.txt")
      assert meta.size == 11
      assert is_binary(meta.etag)
      assert %DateTime{} = meta.last_modified
    end

    test "returns error for non-existent key" do
      Backend.create_bucket(@ctx, "my-bucket")

      assert {:error, %S3Server.Error{code: :no_such_key}} =
               Backend.head_object(@ctx, "my-bucket", "missing.txt")
    end
  end

  describe "delete_object/3" do
    test "deletes an existing object" do
      Backend.create_bucket(@ctx, "my-bucket")
      Backend.put_object(@ctx, "my-bucket", "hello.txt", "hello world", %S3Server.PutOpts{})

      assert :ok = Backend.delete_object(@ctx, "my-bucket", "hello.txt")

      assert {:error, %S3Server.Error{code: :no_such_key}} =
               Backend.get_object(@ctx, "my-bucket", "hello.txt", %S3Server.GetOpts{})
    end

    test "succeeds silently for non-existent key" do
      Backend.create_bucket(@ctx, "my-bucket")
      assert :ok = Backend.delete_object(@ctx, "my-bucket", "nonexistent.txt")
    end
  end

  describe "delete_objects/3" do
    test "deletes multiple objects" do
      Backend.create_bucket(@ctx, "my-bucket")
      Backend.put_object(@ctx, "my-bucket", "a.txt", "a", %S3Server.PutOpts{})
      Backend.put_object(@ctx, "my-bucket", "b.txt", "b", %S3Server.PutOpts{})

      assert {:ok, result} = Backend.delete_objects(@ctx, "my-bucket", ["a.txt", "b.txt"])
      assert length(result.deleted) == 2
      assert result.errors == []
    end
  end

  describe "copy_object/5" do
    test "copies an object within the same bucket" do
      Backend.create_bucket(@ctx, "my-bucket")
      Backend.put_object(@ctx, "my-bucket", "original.txt", "hello", %S3Server.PutOpts{})

      assert {:ok, result} =
               Backend.copy_object(@ctx, "my-bucket", "copy.txt", "my-bucket", "original.txt")

      assert is_binary(result.etag)
      assert %DateTime{} = result.last_modified

      assert {:ok, object} =
               Backend.get_object(@ctx, "my-bucket", "copy.txt", %S3Server.GetOpts{})

      assert object.body == "hello"
    end

    test "copies an object across buckets" do
      Backend.create_bucket(@ctx, "source")
      Backend.create_bucket(@ctx, "dest")
      Backend.put_object(@ctx, "source", "file.txt", "data", %S3Server.PutOpts{})

      assert {:ok, _result} =
               Backend.copy_object(@ctx, "dest", "file.txt", "source", "file.txt")

      assert {:ok, object} = Backend.get_object(@ctx, "dest", "file.txt", %S3Server.GetOpts{})
      assert object.body == "data"
    end

    test "preserves content type on copy" do
      Backend.create_bucket(@ctx, "my-bucket")

      Backend.put_object(@ctx, "my-bucket", "original.bin", "csv,data", %S3Server.PutOpts{
        content_type: "text/csv"
      })

      assert {:ok, _result} =
               Backend.copy_object(
                 @ctx,
                 "my-bucket",
                 "copy.bin",
                 "my-bucket",
                 "original.bin"
               )

      assert {:ok, object} =
               Backend.get_object(@ctx, "my-bucket", "copy.bin", %S3Server.GetOpts{})

      assert object.content_type == "text/csv"
    end

    test "preserves content type on cross-bucket copy" do
      Backend.create_bucket(@ctx, "source")
      Backend.create_bucket(@ctx, "dest")

      Backend.put_object(@ctx, "source", "data.bin", "json", %S3Server.PutOpts{
        content_type: "application/json"
      })

      assert {:ok, _result} =
               Backend.copy_object(@ctx, "dest", "data.bin", "source", "data.bin")

      assert {:ok, object} =
               Backend.get_object(@ctx, "dest", "data.bin", %S3Server.GetOpts{})

      assert object.content_type == "application/json"
    end

    test "returns error for non-existent source" do
      Backend.create_bucket(@ctx, "my-bucket")

      assert {:error, %S3Server.Error{code: :no_such_key}} =
               Backend.copy_object(@ctx, "my-bucket", "copy.txt", "my-bucket", "missing.txt")
    end
  end

  describe "list_objects_v2/3" do
    test "lists objects in a bucket" do
      Backend.create_bucket(@ctx, "my-bucket")
      Backend.put_object(@ctx, "my-bucket", "a.txt", "a", %S3Server.PutOpts{})
      Backend.put_object(@ctx, "my-bucket", "b.txt", "b", %S3Server.PutOpts{})

      assert {:ok, result} = Backend.list_objects_v2(@ctx, "my-bucket", %S3Server.ListOpts{})
      assert result.name == "my-bucket"
      assert length(result.contents) == 2
    end

    test "returns empty list for empty bucket" do
      Backend.create_bucket(@ctx, "my-bucket")

      assert {:ok, result} = Backend.list_objects_v2(@ctx, "my-bucket", %S3Server.ListOpts{})
      assert result.contents == []
    end

    test "returns error for non-existent bucket" do
      assert {:error, %S3Server.Error{code: :no_such_bucket}} =
               Backend.list_objects_v2(@ctx, "missing", %S3Server.ListOpts{})
    end
  end

  describe "get_object/4 streaming" do
    setup do
      Application.put_env(:neonfs_s3, :core_stream_fn, fn volume, path, opts ->
        MockCore.read_file_stream(volume, path, opts)
      end)

      on_exit(fn ->
        Application.delete_env(:neonfs_s3, :core_stream_fn)
      end)

      :ok
    end

    test "returns stream body when streaming is available" do
      Backend.create_bucket(@ctx, "my-bucket")
      Backend.put_object(@ctx, "my-bucket", "stream.txt", "streamed content", %S3Server.PutOpts{})

      assert {:ok, object} =
               Backend.get_object(@ctx, "my-bucket", "stream.txt", %S3Server.GetOpts{})

      assert not is_binary(object.body)
      assert Enum.into(object.body, <<>>) == "streamed content"
      assert object.content_length == 16
      assert object.total_size == 16
    end

    test "streams partial content for range request" do
      Backend.create_bucket(@ctx, "my-bucket")

      Backend.put_object(
        @ctx,
        "my-bucket",
        "range-stream.txt",
        "0123456789ABCDEF",
        %S3Server.PutOpts{}
      )

      opts = %S3Server.GetOpts{range: {5, 9}}

      assert {:ok, object} = Backend.get_object(@ctx, "my-bucket", "range-stream.txt", opts)
      assert not is_binary(object.body)
      assert Enum.into(object.body, <<>>) == "56789"
      assert object.content_length == 5
      assert object.total_size == 16
    end

    test "preserves etag and metadata with streaming" do
      Backend.create_bucket(@ctx, "my-bucket")

      Backend.put_object(@ctx, "my-bucket", "meta.txt", "test data", %S3Server.PutOpts{
        content_type: "text/csv"
      })

      assert {:ok, object} =
               Backend.get_object(@ctx, "my-bucket", "meta.txt", %S3Server.GetOpts{})

      assert is_binary(object.etag)
      assert object.content_type == "text/csv"
      assert %DateTime{} = object.last_modified
    end
  end

  # Multipart upload operations

  describe "multipart upload lifecycle" do
    test "create, upload parts, complete" do
      Backend.create_bucket(@ctx, "my-bucket")

      assert {:ok, upload_id} =
               Backend.create_multipart_upload(@ctx, "my-bucket", "big-file.bin", %{})

      assert is_binary(upload_id)

      assert {:ok, etag1} =
               Backend.upload_part(@ctx, "my-bucket", "big-file.bin", upload_id, 1, "part-one-")

      assert {:ok, etag2} =
               Backend.upload_part(@ctx, "my-bucket", "big-file.bin", upload_id, 2, "part-two")

      assert is_binary(etag1)
      assert is_binary(etag2)

      parts = [{1, etag1}, {2, etag2}]

      assert {:ok, result} =
               Backend.complete_multipart_upload(
                 @ctx,
                 "my-bucket",
                 "big-file.bin",
                 upload_id,
                 parts
               )

      assert result.bucket == "my-bucket"
      assert result.key == "big-file.bin"
      assert is_binary(result.etag)

      assert {:ok, object} =
               Backend.get_object(@ctx, "my-bucket", "big-file.bin", %S3Server.GetOpts{})

      assert object.body == "part-one-part-two"
    end

    test "abort multipart upload" do
      Backend.create_bucket(@ctx, "my-bucket")

      {:ok, upload_id} =
        Backend.create_multipart_upload(@ctx, "my-bucket", "aborted.bin", %{})

      Backend.upload_part(@ctx, "my-bucket", "aborted.bin", upload_id, 1, "data")
      assert :ok = Backend.abort_multipart_upload(@ctx, "my-bucket", "aborted.bin", upload_id)

      assert {:error, %S3Server.Error{code: :no_such_upload}} =
               Backend.upload_part(@ctx, "my-bucket", "aborted.bin", upload_id, 2, "more")
    end

    test "list multipart uploads" do
      Backend.create_bucket(@ctx, "my-bucket")
      {:ok, _id1} = Backend.create_multipart_upload(@ctx, "my-bucket", "file1.bin", %{})
      {:ok, _id2} = Backend.create_multipart_upload(@ctx, "my-bucket", "file2.bin", %{})

      assert {:ok, result} = Backend.list_multipart_uploads(@ctx, "my-bucket", %{})
      assert result.bucket == "my-bucket"
      assert length(result.uploads) == 2
    end

    test "list parts of an upload" do
      Backend.create_bucket(@ctx, "my-bucket")
      {:ok, upload_id} = Backend.create_multipart_upload(@ctx, "my-bucket", "file.bin", %{})
      Backend.upload_part(@ctx, "my-bucket", "file.bin", upload_id, 1, "part-one")
      Backend.upload_part(@ctx, "my-bucket", "file.bin", upload_id, 2, "part-two")

      assert {:ok, result} = Backend.list_parts(@ctx, "my-bucket", "file.bin", upload_id, %{})
      assert length(result.parts) == 2
      assert Enum.at(result.parts, 0).part_number == 1
      assert Enum.at(result.parts, 1).part_number == 2
    end

    test "upload_part returns error for non-existent upload" do
      assert {:error, %S3Server.Error{code: :no_such_upload}} =
               Backend.upload_part(@ctx, "bucket", "key", "bad-id", 1, "data")
    end

    test "complete returns error for non-existent upload" do
      assert {:error, %S3Server.Error{code: :no_such_upload}} =
               Backend.complete_multipart_upload(@ctx, "bucket", "key", "bad-id", [])
    end

    test "abort returns error for non-existent upload" do
      assert {:error, %S3Server.Error{code: :no_such_upload}} =
               Backend.abort_multipart_upload(@ctx, "bucket", "key", "bad-id")
    end
  end

  describe "get_object/4 — data plane routing" do
    test "dispatches GET fallback through NeonFS.Client.ChunkReader" do
      Backend.create_bucket(@ctx, "my-bucket")
      Backend.put_object(@ctx, "my-bucket", "dp.txt", "over data plane", %S3Server.PutOpts{})

      test_pid = self()

      expect(ChunkReader, :read_file, fn volume_name, path, opts ->
        send(test_pid, {:chunk_reader_called, volume_name, path, opts})
        {:ok, "over data plane"}
      end)

      assert {:ok, object} =
               Backend.get_object(@ctx, "my-bucket", "dp.txt", %S3Server.GetOpts{})

      assert object.body == "over data plane"

      assert_receive {:chunk_reader_called, "my-bucket", "dp.txt", opts}, 1_000
      refute Keyword.has_key?(opts, :offset)
      refute Keyword.has_key?(opts, :length)
    end

    test "forwards range requests as :offset/:length through ChunkReader" do
      Backend.create_bucket(@ctx, "my-bucket")

      Backend.put_object(
        @ctx,
        "my-bucket",
        "range.bin",
        "0123456789ABCDEF",
        %S3Server.PutOpts{}
      )

      expect(ChunkReader, :read_file, fn "my-bucket", "range.bin", opts ->
        assert Keyword.get(opts, :offset) == 5
        assert Keyword.get(opts, :length) == 5
        {:ok, "56789"}
      end)

      opts = %S3Server.GetOpts{range: {5, 9}}

      assert {:ok, object} = Backend.get_object(@ctx, "my-bucket", "range.bin", opts)
      assert object.body == "56789"
      assert object.content_length == 5
      assert object.total_size == 16
    end

    test "maps ChunkReader :not_found to no_such_key" do
      Backend.create_bucket(@ctx, "my-bucket")
      Backend.put_object(@ctx, "my-bucket", "exists.txt", "hi", %S3Server.PutOpts{})

      expect(ChunkReader, :read_file, fn _, _, _ -> {:error, :not_found} end)

      assert {:error, %S3Server.Error{code: :no_such_key}} =
               Backend.get_object(@ctx, "my-bucket", "exists.txt", %S3Server.GetOpts{})
    end

    test "maps other ChunkReader errors to internal_error" do
      Backend.create_bucket(@ctx, "my-bucket")
      Backend.put_object(@ctx, "my-bucket", "exists.txt", "hi", %S3Server.PutOpts{})

      expect(ChunkReader, :read_file, fn _, _, _ -> {:error, :no_available_locations} end)

      assert {:error, %S3Server.Error{code: :internal_error}} =
               Backend.get_object(@ctx, "my-bucket", "exists.txt", %S3Server.GetOpts{})
    end

    test "skips ChunkReader when streaming fast path is available" do
      Application.put_env(:neonfs_s3, :core_stream_fn, fn volume, path, opts ->
        MockCore.read_file_stream(volume, path, opts)
      end)

      on_exit(fn -> Application.delete_env(:neonfs_s3, :core_stream_fn) end)

      Backend.create_bucket(@ctx, "my-bucket")
      Backend.put_object(@ctx, "my-bucket", "stream.txt", "streaming", %S3Server.PutOpts{})

      reject(&ChunkReader.read_file/3)

      assert {:ok, object} =
               Backend.get_object(@ctx, "my-bucket", "stream.txt", %S3Server.GetOpts{})

      assert not is_binary(object.body)
      assert Enum.into(object.body, <<>>) == "streaming"
    end
  end
end
