defmodule NeonFS.Client.ChunkReaderTest do
  @moduledoc """
  Unit tests for `NeonFS.Client.ChunkReader`.

  These tests stub `NeonFS.Client.Router` so we can exercise the chunk
  assembly logic without a real TLS data plane.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Client.{ChunkReader, Router}

  setup :verify_on_exit!

  defp fake_hash(seed) do
    :crypto.hash(:sha256, "fake-hash-#{seed}")
  end

  defp ref(opts) do
    %{
      hash: fake_hash(Keyword.fetch!(opts, :seed)),
      original_size: Keyword.fetch!(opts, :original_size),
      stored_size: Keyword.get(opts, :stored_size, Keyword.fetch!(opts, :original_size)),
      chunk_offset: Keyword.fetch!(opts, :chunk_offset),
      read_start: Keyword.fetch!(opts, :read_start),
      read_length: Keyword.fetch!(opts, :read_length),
      compression: Keyword.get(opts, :compression, :none),
      encrypted: Keyword.get(opts, :encrypted, false),
      locations: Keyword.get(opts, :locations, [%{node: :node1@host, drive_id: "d1", tier: :hot}])
    }
  end

  describe "read_file/3 — happy path" do
    test "assembles bytes from a single chunk over the data plane" do
      bytes = "hello, neonfs data plane!"

      refs = [
        ref(
          seed: 1,
          original_size: byte_size(bytes),
          chunk_offset: 0,
          read_start: 0,
          read_length: byte_size(bytes)
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, ["vol", "/a.txt", []] ->
        {:ok, %{file_size: byte_size(bytes), chunks: refs}}
      end)

      expect(Router, :data_call, fn :node1@host, :get_chunk, args, _opts ->
        assert args[:hash] == Enum.at(refs, 0).hash
        assert args[:volume_id] == "d1"
        assert args[:tier] == "hot"
        {:ok, bytes}
      end)

      assert {:ok, ^bytes} = ChunkReader.read_file("vol", "/a.txt")
    end

    test "slices chunks by read_start and read_length" do
      chunk_bytes = "0123456789abcdef"

      refs = [
        ref(seed: 1, original_size: 16, chunk_offset: 0, read_start: 4, read_length: 8)
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 16, chunks: refs}}
      end)

      expect(Router, :data_call, fn _, :get_chunk, _args, _opts ->
        {:ok, chunk_bytes}
      end)

      assert {:ok, "456789ab"} = ChunkReader.read_file("vol", "/slice.txt")
    end

    test "concatenates multiple chunks in order" do
      chunk_a = String.duplicate("A", 10)
      chunk_b = String.duplicate("B", 10)
      chunk_c = String.duplicate("C", 10)

      refs = [
        ref(seed: :a, original_size: 10, chunk_offset: 0, read_start: 5, read_length: 5),
        ref(seed: :b, original_size: 10, chunk_offset: 10, read_start: 0, read_length: 10),
        ref(seed: :c, original_size: 10, chunk_offset: 20, read_start: 0, read_length: 3)
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 30, chunks: refs}}
      end)

      # The data plane returns chunks matched by hash, not call order.
      stub(Router, :data_call, fn _node, :get_chunk, args, _opts ->
        cond do
          args[:hash] == fake_hash(:a) -> {:ok, chunk_a}
          args[:hash] == fake_hash(:b) -> {:ok, chunk_b}
          args[:hash] == fake_hash(:c) -> {:ok, chunk_c}
        end
      end)

      assert {:ok, "AAAAABBBBBBBBBBCCC"} = ChunkReader.read_file("vol", "/multi.txt")
    end

    test "forwards offset and length to read_file_refs" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, [_, _, opts] ->
        assert opts[:offset] == 100
        assert opts[:length] == 50
        {:ok, %{file_size: 200, chunks: []}}
      end)

      assert {:ok, ""} = ChunkReader.read_file("vol", "/x", offset: 100, length: 50)
    end
  end

  describe "read_file/3 — location selection" do
    test "prefers the local node when it holds the chunk" do
      local_node = Node.self()
      remote_node = :remote@elsewhere

      refs = [
        ref(
          seed: 1,
          original_size: 4,
          chunk_offset: 0,
          read_start: 0,
          read_length: 4,
          locations: [
            %{node: remote_node, drive_id: "d2", tier: :hot},
            %{node: local_node, drive_id: "d1", tier: :hot}
          ]
        )
      ]

      expect(Router, :call, fn _, _, _ -> {:ok, %{file_size: 4, chunks: refs}} end)

      expect(Router, :data_call, fn ^local_node, :get_chunk, _args, _opts ->
        {:ok, "abcd"}
      end)

      assert {:ok, "abcd"} = ChunkReader.read_file("vol", "/local.txt")
    end

    test "skips nodes listed in :exclude_nodes" do
      good = :good@host
      bad = :bad@host

      refs = [
        ref(
          seed: 1,
          original_size: 4,
          chunk_offset: 0,
          read_start: 0,
          read_length: 4,
          locations: [
            %{node: bad, drive_id: "d1", tier: :hot},
            %{node: good, drive_id: "d2", tier: :hot}
          ]
        )
      ]

      expect(Router, :call, fn _, _, _ -> {:ok, %{file_size: 4, chunks: refs}} end)

      expect(Router, :data_call, fn ^good, :get_chunk, _args, _opts -> {:ok, "okok"} end)

      assert {:ok, "okok"} = ChunkReader.read_file("vol", "/x", exclude_nodes: [bad])
    end

    test "falls through to the next location after a data-plane failure" do
      n1 = :n1@host
      n2 = :n2@host

      refs = [
        ref(
          seed: 1,
          original_size: 4,
          chunk_offset: 0,
          read_start: 0,
          read_length: 4,
          locations: [
            %{node: n1, drive_id: "d1", tier: :hot},
            %{node: n2, drive_id: "d2", tier: :hot}
          ]
        )
      ]

      expect(Router, :call, fn _, _, _ -> {:ok, %{file_size: 4, chunks: refs}} end)

      stub(Router, :data_call, fn
        ^n1, :get_chunk, _args, _opts -> {:error, :no_data_endpoint}
        ^n2, :get_chunk, _args, _opts -> {:ok, "ok!!"}
      end)

      assert {:ok, "ok!!"} = ChunkReader.read_file("vol", "/x")
    end

    test "returns an error when every location fails" do
      refs = [
        ref(
          seed: 1,
          original_size: 4,
          chunk_offset: 0,
          read_start: 0,
          read_length: 4,
          locations: [
            %{node: :a@host, drive_id: "d1", tier: :hot},
            %{node: :b@host, drive_id: "d2", tier: :hot}
          ]
        )
      ]

      expect(Router, :call, fn _, _, _ -> {:ok, %{file_size: 4, chunks: refs}} end)

      stub(Router, :data_call, fn _node, :get_chunk, _args, _opts ->
        {:error, :connection_refused}
      end)

      assert {:error, :connection_refused} = ChunkReader.read_file("vol", "/x")
    end

    test "returns no_available_locations when every location is excluded" do
      refs = [
        ref(
          seed: 1,
          original_size: 4,
          chunk_offset: 0,
          read_start: 0,
          read_length: 4,
          locations: [%{node: :only@host, drive_id: "d1", tier: :hot}]
        )
      ]

      expect(Router, :call, fn _, _, _ -> {:ok, %{file_size: 4, chunks: refs}} end)

      assert {:error, :no_available_locations} =
               ChunkReader.read_file("vol", "/x", exclude_nodes: [:only@host])
    end
  end

  describe "read_file/3 — fallback behaviour" do
    test "falls back to read_file when any chunk is compressed" do
      refs = [
        ref(seed: 1, original_size: 10, chunk_offset: 0, read_start: 0, read_length: 10),
        ref(
          seed: 2,
          original_size: 10,
          chunk_offset: 10,
          read_start: 0,
          read_length: 10,
          compression: :zstd
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 20, chunks: refs}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, ["vol", "/c.txt", opts] ->
        assert Keyword.get(opts, :offset, 0) == 0
        {:ok, "compressed-output..."}
      end)

      assert {:ok, "compressed-output..."} = ChunkReader.read_file("vol", "/c.txt")
    end

    test "falls back to read_file when any chunk is encrypted" do
      refs = [
        ref(
          seed: 1,
          original_size: 10,
          chunk_offset: 0,
          read_start: 0,
          read_length: 10,
          encrypted: true
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 10, chunks: refs}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, _ ->
        {:ok, "decrypted"}
      end)

      assert {:ok, "decrypted"} = ChunkReader.read_file("vol", "/e.txt")
    end

    test "falls back to read_file on :stripe_refs_unsupported" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, :stripe_refs_unsupported}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, _ ->
        {:ok, "ec-file-bytes"}
      end)

      assert {:ok, "ec-file-bytes"} = ChunkReader.read_file("vol", "/ec.bin")
    end

    test "fallback forwards offset and length" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, :stripe_refs_unsupported}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, [_, _, opts] ->
        assert opts[:offset] == 42
        assert opts[:length] == 99
        {:ok, "partial"}
      end)

      assert {:ok, "partial"} = ChunkReader.read_file("vol", "/x", offset: 42, length: 99)
    end
  end

  describe "read_file/3 — error pass-through" do
    test "returns metadata errors unchanged" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, :not_found}
      end)

      assert {:error, :not_found} = ChunkReader.read_file("vol", "/missing.txt")
    end

    test "returns empty bytes for empty file" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 0, chunks: []}}
      end)

      assert {:ok, ""} = ChunkReader.read_file("vol", "/empty.txt")
    end

    test "returns empty bytes when read range does not overlap any chunk" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 100, chunks: []}}
      end)

      assert {:ok, ""} =
               ChunkReader.read_file("vol", "/x", offset: 1_000, length: 50)
    end
  end
end
