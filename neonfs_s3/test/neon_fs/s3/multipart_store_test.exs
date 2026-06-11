defmodule NeonFS.S3.MultipartStoreTest do
  use ExUnit.Case, async: false

  use Mimic

  alias NeonFS.Client.KV
  alias NeonFS.S3.MultipartStore
  alias NeonFS.S3.Test.FakeKV

  setup do
    FakeKV.stub!()
    :ok
  end

  defp create!(bucket, key) do
    {:ok, upload_id} = MultipartStore.create(bucket, key)
    upload_id
  end

  test "create returns a unique upload ID" do
    id1 = create!("bucket", "key1")
    id2 = create!("bucket", "key2")
    assert is_binary(id1)
    assert is_binary(id2)
    assert id1 != id2
  end

  test "get returns the upload entry" do
    id = create!("bucket", "key")
    assert {:ok, entry} = MultipartStore.get(id)
    assert entry.bucket == "bucket"
    assert entry.key == "key"
    assert entry.parts == %{}
    assert %DateTime{} = entry.initiated
  end

  test "get returns error for non-existent upload" do
    assert {:error, :not_found} = MultipartStore.get("nonexistent")
  end

  test "get_meta returns metadata without parts" do
    id = create!("bucket", "key")
    assert {:ok, meta} = MultipartStore.get_meta(id)
    assert meta.bucket == "bucket"
    refute Map.has_key?(meta, :parts)
  end

  test "put_part adds a part to the upload" do
    id = create!("bucket", "key")

    part = %{etag: "abc123", size: 100, chunk_refs: []}
    assert :ok = MultipartStore.put_part(id, 1, part)

    assert {:ok, entry} = MultipartStore.get(id)
    assert Map.has_key?(entry.parts, 1)
    assert entry.parts[1].etag == "abc123"
  end

  test "parts are independent keys — no read-modify-write across parts" do
    id = create!("bucket", "key")

    :ok = MultipartStore.put_part(id, 1, %{etag: "e1", size: 1, chunk_refs: []})
    :ok = MultipartStore.put_part(id, 2, %{etag: "e2", size: 2, chunk_refs: []})
    :ok = MultipartStore.put_part(id, 1, %{etag: "e1-replaced", size: 1, chunk_refs: []})

    assert {:ok, entry} = MultipartStore.get(id)
    assert entry.parts[1].etag == "e1-replaced"
    assert entry.parts[2].etag == "e2"
    assert map_size(entry.parts) == 2
  end

  test "put_part returns error for non-existent upload" do
    part = %{etag: "abc123", size: 100, chunk_refs: []}
    assert {:error, :not_found} = MultipartStore.put_part("nonexistent", 1, part)
  end

  test "delete removes the upload metadata and all parts" do
    id = create!("bucket", "key")
    :ok = MultipartStore.put_part(id, 1, %{etag: "e1", size: 1, chunk_refs: []})

    assert :ok = MultipartStore.delete(id)
    assert {:error, :not_found} = MultipartStore.get(id)
    assert KV.list_prefix("s3_multipart:") == []
  end

  test "list_for_bucket returns uploads filtered by bucket" do
    create!("bucket-a", "key1")
    create!("bucket-a", "key2")
    create!("bucket-b", "key3")

    results = MultipartStore.list_for_bucket("bucket-a")
    assert length(results) == 2
    assert Enum.all?(results, fn r -> is_binary(r.upload_id) end)
  end

  test "list_for_bucket ignores part entries" do
    id = create!("bucket-a", "key1")
    :ok = MultipartStore.put_part(id, 1, %{etag: "e1", size: 1, chunk_refs: []})

    assert [%{upload_id: ^id}] = MultipartStore.list_for_bucket("bucket-a")
  end

  test "list_for_bucket returns empty list for unknown bucket" do
    assert [] = MultipartStore.list_for_bucket("unknown")
  end
end
