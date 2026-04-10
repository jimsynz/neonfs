defmodule NeonFS.S3.MultipartStoreTest do
  use ExUnit.Case, async: false

  alias NeonFS.S3.MultipartStore

  setup do
    start_supervised!(MultipartStore)
    :ok
  end

  test "create returns a unique upload ID" do
    id1 = MultipartStore.create("bucket", "key1")
    id2 = MultipartStore.create("bucket", "key2")
    assert is_binary(id1)
    assert is_binary(id2)
    assert id1 != id2
  end

  test "get returns the upload entry" do
    id = MultipartStore.create("bucket", "key")
    assert {:ok, entry} = MultipartStore.get(id)
    assert entry.bucket == "bucket"
    assert entry.key == "key"
    assert entry.parts == %{}
    assert %DateTime{} = entry.initiated
  end

  test "get returns error for non-existent upload" do
    assert {:error, :not_found} = MultipartStore.get("nonexistent")
  end

  test "put_part adds a part to the upload" do
    id = MultipartStore.create("bucket", "key")

    part = %{etag: "abc123", size: 100, path: "/staging/part-1"}
    assert :ok = MultipartStore.put_part(id, 1, part)

    assert {:ok, entry} = MultipartStore.get(id)
    assert Map.has_key?(entry.parts, 1)
    assert entry.parts[1].etag == "abc123"
  end

  test "put_part returns error for non-existent upload" do
    part = %{etag: "abc123", size: 100, path: "/staging/part-1"}
    assert {:error, :not_found} = MultipartStore.put_part("nonexistent", 1, part)
  end

  test "delete removes the upload" do
    id = MultipartStore.create("bucket", "key")
    assert :ok = MultipartStore.delete(id)
    assert {:error, :not_found} = MultipartStore.get(id)
  end

  test "list_for_bucket returns uploads filtered by bucket" do
    MultipartStore.create("bucket-a", "key1")
    MultipartStore.create("bucket-a", "key2")
    MultipartStore.create("bucket-b", "key3")

    results = MultipartStore.list_for_bucket("bucket-a")
    assert length(results) == 2
    assert Enum.all?(results, fn r -> is_binary(r.upload_id) end)
  end

  test "list_for_bucket returns empty list for unknown bucket" do
    assert [] = MultipartStore.list_for_bucket("unknown")
  end
end
