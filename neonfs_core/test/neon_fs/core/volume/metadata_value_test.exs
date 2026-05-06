defmodule NeonFS.Core.Volume.MetadataValueTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Volume.MetadataValue

  describe "encode/decode round-trip" do
    test "preserves a flat map" do
      term = %{name: "alice", age: 42}
      assert {:ok, ^term} = term |> MetadataValue.encode() |> MetadataValue.decode()
    end

    test "preserves nested structures" do
      term = %{
        path: "/a/b/c",
        size: 1024,
        chunks: [{<<1, 2, 3>>, 256}, {<<4, 5, 6>>, 768}],
        meta: %{owner_uid: 1000, mode: 0o644}
      }

      assert {:ok, ^term} = term |> MetadataValue.encode() |> MetadataValue.decode()
    end

    test "preserves DateTime" do
      term = %{updated_at: DateTime.utc_now()}
      assert {:ok, ^term} = term |> MetadataValue.encode() |> MetadataValue.decode()
    end

    test "preserves a struct" do
      term = %URI{
        scheme: "https",
        host: "example.com",
        path: "/foo",
        port: 443
      }

      assert {:ok, ^term} = term |> MetadataValue.encode() |> MetadataValue.decode()
    end
  end

  describe "decode/1" do
    test "rejects garbage bytes with {:error, {:malformed_value, _}}" do
      assert {:error, {:malformed_value, _}} = MetadataValue.decode(<<0, 1, 2, 3>>)
    end

    test "is :safe — refuses to introduce unknown atoms" do
      # Encode a term referencing an atom that the test runtime knows.
      bytes = MetadataValue.encode(:erlang)

      assert {:ok, :erlang} = MetadataValue.decode(bytes)

      # Hand-craft bytes that would introduce a fresh atom. ETF for
      # "small atom utf8" with a name very unlikely to be loaded.
      novel_atom_name = "metadata_value_novel_atom_#{System.unique_integer([:positive])}"
      novel_bytes = <<131, 119, byte_size(novel_atom_name)::8, novel_atom_name::binary>>

      assert {:error, {:malformed_value, _}} = MetadataValue.decode(novel_bytes)
    end
  end

  describe "encode/1" do
    test "is deterministic for the same input" do
      term = %{a: 1, b: [2, 3]}
      assert MetadataValue.encode(term) == MetadataValue.encode(term)
    end
  end
end
