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

  # Real content hash for data-plane tests, which now verify fetched bytes
  # against the chunk id.
  defp content_hash(content), do: :crypto.hash(:sha256, content)

  defp ref(opts) do
    hash =
      case Keyword.fetch(opts, :content) do
        {:ok, content} -> content_hash(content)
        :error -> fake_hash(Keyword.fetch!(opts, :seed))
      end

    %{
      hash: hash,
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

  # The primitive a caller that resolved its own references uses — a block
  # device, whose extents each are a chunk and whose map core resolves.
  describe "fetch_chunk/3" do
    test "hands back the whole chunk, verified against its hash" do
      content = :binary.copy(<<0x42>>, 64)

      expect(Router, :data_call, fn :node1@host, :get_chunk, args, _opts ->
        assert args[:hash] == content_hash(content)
        {:ok, content}
      end)

      assert {:ok, ^content} =
               ChunkReader.fetch_chunk(
                 "vol",
                 ref(
                   content: content,
                   original_size: 64,
                   chunk_offset: 0,
                   read_start: 0,
                   read_length: 64
                 )
               )
    end

    # Slicing is the caller's job precisely because the hash is only
    # checkable against the whole thing.
    test "fails over to the next location when a replica's bytes do not hash" do
      content = :binary.copy(<<0x43>>, 32)

      locations = [
        %{node: :bad@host, drive_id: "d1", tier: :hot},
        %{node: :good@host, drive_id: "d1", tier: :hot}
      ]

      stub(Router, :data_call, fn
        :bad@host, :get_chunk, _args, _opts -> {:ok, :binary.copy(<<0>>, 32)}
        :good@host, :get_chunk, _args, _opts -> {:ok, content}
      end)

      assert {:ok, ^content} =
               ChunkReader.fetch_chunk(
                 "vol",
                 ref(
                   content: content,
                   original_size: 32,
                   chunk_offset: 0,
                   read_start: 0,
                   read_length: 32,
                   locations: locations
                 )
               )
    end

    test "a chunk with no location to try is an error, not empty bytes" do
      assert {:error, :no_available_locations} =
               ChunkReader.fetch_chunk(
                 "vol",
                 ref(
                   seed: 1,
                   original_size: 8,
                   chunk_offset: 0,
                   read_start: 0,
                   read_length: 8,
                   locations: []
                 )
               )
    end
  end

  describe "chunk_readable?/1" do
    test "a plain chunk is servable by the data plane" do
      assert ChunkReader.chunk_readable?(%{compression: :none, encrypted: false})
    end

    # Neither hashes to its id as stored, and only core holds the key, so a
    # caller has to route these back through core rather than the data plane.
    test "a compressed or encrypted chunk is not" do
      refute ChunkReader.chunk_readable?(%{compression: :zstd, encrypted: false})
      refute ChunkReader.chunk_readable?(%{compression: :none, encrypted: true})
    end
  end

  describe "read_file/3 — happy path" do
    test "assembles bytes from a single chunk over the data plane" do
      bytes = "hello, neonfs data plane!"

      refs = [
        ref(
          content: bytes,
          original_size: byte_size(bytes),
          chunk_offset: 0,
          read_start: 0,
          read_length: byte_size(bytes)
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, ["vol", "/a.txt", []] ->
        {:ok, %{file_size: byte_size(bytes), chunks: refs, hole_bytes: 0}}
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
        ref(
          content: chunk_bytes,
          original_size: 16,
          chunk_offset: 0,
          read_start: 4,
          read_length: 8
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 16, chunks: refs, hole_bytes: 0}}
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
        ref(content: chunk_a, original_size: 10, chunk_offset: 0, read_start: 5, read_length: 5),
        ref(
          content: chunk_b,
          original_size: 10,
          chunk_offset: 10,
          read_start: 0,
          read_length: 10
        ),
        ref(content: chunk_c, original_size: 10, chunk_offset: 20, read_start: 0, read_length: 3)
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 30, chunks: refs, hole_bytes: 0}}
      end)

      # The data plane returns chunks matched by hash, not call order.
      stub(Router, :data_call, fn _node, :get_chunk, args, _opts ->
        cond do
          args[:hash] == content_hash(chunk_a) -> {:ok, chunk_a}
          args[:hash] == content_hash(chunk_b) -> {:ok, chunk_b}
          args[:hash] == content_hash(chunk_c) -> {:ok, chunk_c}
        end
      end)

      assert {:ok, "AAAAABBBBBBBBBBCCC"} = ChunkReader.read_file("vol", "/multi.txt")
    end

    test "forwards offset and length to read_file_refs" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, [_, _, opts] ->
        assert opts[:offset] == 100
        assert opts[:length] == 50
        {:ok, %{file_size: 200, chunks: [], hole_bytes: 0}}
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
          content: "abcd",
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

      expect(Router, :call, fn _, _, _ -> {:ok, %{file_size: 4, chunks: refs, hole_bytes: 0}} end)

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
          content: "okok",
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

      expect(Router, :call, fn _, _, _ -> {:ok, %{file_size: 4, chunks: refs, hole_bytes: 0}} end)

      expect(Router, :data_call, fn ^good, :get_chunk, _args, _opts -> {:ok, "okok"} end)

      assert {:ok, "okok"} = ChunkReader.read_file("vol", "/x", exclude_nodes: [bad])
    end

    test "falls through to the next location after a data-plane failure" do
      n1 = :n1@host
      n2 = :n2@host

      refs = [
        ref(
          content: "ok!!",
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

      expect(Router, :call, fn _, _, _ -> {:ok, %{file_size: 4, chunks: refs, hole_bytes: 0}} end)

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

      expect(Router, :call, fn _, _, _ -> {:ok, %{file_size: 4, chunks: refs, hole_bytes: 0}} end)

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

      expect(Router, :call, fn _, _, _ -> {:ok, %{file_size: 4, chunks: refs, hole_bytes: 0}} end)

      assert {:error, :no_available_locations} =
               ChunkReader.read_file("vol", "/x", exclude_nodes: [:only@host])
    end
  end

  describe "read_file/3 — content-hash verification" do
    test "fails over to the next location when a replica serves corrupt bytes" do
      good = "good"
      n1 = :corrupt@host
      n2 = :good@host

      refs = [
        ref(
          content: good,
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

      expect(Router, :call, fn _, _, _ -> {:ok, %{file_size: 4, chunks: refs, hole_bytes: 0}} end)

      stub(Router, :data_call, fn
        ^n1, :get_chunk, _args, _opts -> {:ok, "evil"}
        ^n2, :get_chunk, _args, _opts -> {:ok, good}
      end)

      assert {:ok, ^good} = ChunkReader.read_file("vol", "/x")
    end

    test "errors and emits telemetry when every replica is corrupt" do
      ref_a =
        ref(content: "good", original_size: 4, chunk_offset: 0, read_start: 0, read_length: 4)

      ref_hash = ref_a.hash

      refs = [ref_a]

      expect(Router, :call, fn _, _, _ -> {:ok, %{file_size: 4, chunks: refs, hole_bytes: 0}} end)
      stub(Router, :data_call, fn _node, :get_chunk, _args, _opts -> {:ok, "evil"} end)

      ref_tel =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :client, :chunk_reader, :verify_failed]
        ])

      assert {:error, {:chunk_verify_failed, ^ref_hash}} = ChunkReader.read_file("vol", "/x")

      assert_received {[:neonfs, :client, :chunk_reader, :verify_failed], ^ref_tel, %{size: 4},
                       %{hash: ^ref_hash}}
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
        {:ok, %{file_size: 20, chunks: refs, hole_bytes: 0}}
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
        {:ok, %{file_size: 10, chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, _ ->
        {:ok, "decrypted"}
      end)

      assert {:ok, "decrypted"} = ChunkReader.read_file("vol", "/e.txt")
    end

    test "falls back to read_file on :stripe_refs_unsupported" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, {:stripe_refs_unsupported, nil}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, _ ->
        {:ok, "ec-file-bytes"}
      end)

      assert {:ok, "ec-file-bytes"} = ChunkReader.read_file("vol", "/ec.bin")
    end

    test "falls back to read_file when every location lacks a data-plane pool" do
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

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 4, chunks: refs, hole_bytes: 0}}
      end)

      stub(Router, :data_call, fn _node, :get_chunk, _args, _opts ->
        {:error, :no_data_endpoint}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, _ ->
        {:ok, "abcd"}
      end)

      assert {:ok, "abcd"} = ChunkReader.read_file("vol", "/nopool.txt")
    end

    test "fallback forwards offset and length" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, {:stripe_refs_unsupported, nil}}
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
        {:ok, %{file_size: 0, chunks: [], hole_bytes: 0}}
      end)

      assert {:ok, ""} = ChunkReader.read_file("vol", "/empty.txt")
    end

    test "returns empty bytes when read range does not overlap any chunk" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 100, chunks: [], hole_bytes: 0}}
      end)

      assert {:ok, ""} =
               ChunkReader.read_file("vol", "/x", offset: 1_000, length: 50)
    end
  end

  describe "read_file_stream/3 — happy path" do
    test "returns a stream that assembles chunks lazily via the data plane" do
      chunk_a = String.duplicate("A", 10)
      chunk_b = String.duplicate("B", 10)

      refs = [
        ref(content: chunk_a, original_size: 10, chunk_offset: 0, read_start: 0, read_length: 10),
        ref(content: chunk_b, original_size: 10, chunk_offset: 10, read_start: 0, read_length: 10)
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, ["vol", "/f.txt", []] ->
        {:ok, %{file_size: 20, chunks: refs, hole_bytes: 0}}
      end)

      stub(Router, :data_call, fn _node, :get_chunk, args, _opts ->
        cond do
          args[:hash] == content_hash(chunk_a) -> {:ok, chunk_a}
          args[:hash] == content_hash(chunk_b) -> {:ok, chunk_b}
        end
      end)

      assert {:ok, %{stream: stream, file_size: 20}} =
               ChunkReader.read_file_stream("vol", "/f.txt")

      assert Enum.to_list(stream) == [chunk_a, chunk_b]
    end

    test "forwards offset and length to read_file_refs" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, [_, _, opts] ->
        assert opts[:offset] == 100
        assert opts[:length] == 50
        {:ok, %{file_size: 200, chunks: [], hole_bytes: 0}}
      end)

      assert {:ok, %{stream: stream, file_size: 200}} =
               ChunkReader.read_file_stream("vol", "/x", offset: 100, length: 50)

      assert Enum.to_list(stream) == []
    end

    test "slices each chunk by read_start and read_length" do
      chunk_bytes = "0123456789abcdef"

      refs = [
        ref(
          content: chunk_bytes,
          original_size: 16,
          chunk_offset: 0,
          read_start: 4,
          read_length: 8
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 16, chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :data_call, fn _, :get_chunk, _args, _opts ->
        {:ok, chunk_bytes}
      end)

      assert {:ok, %{stream: stream}} = ChunkReader.read_file_stream("vol", "/slice.txt")
      assert Enum.into(stream, <<>>) == "456789ab"
    end
  end

  describe "read_file_stream/3 — per-chunk fallback" do
    test "fetches compressed chunks via a range-limited read_file RPC" do
      chunk_a = String.duplicate("A", 10)

      refs = [
        ref(content: chunk_a, original_size: 10, chunk_offset: 0, read_start: 0, read_length: 10),
        ref(
          seed: :b,
          original_size: 10,
          chunk_offset: 10,
          read_start: 0,
          read_length: 10,
          compression: :zstd
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 20, chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :data_call, fn _, :get_chunk, args, _ ->
        assert args[:hash] == content_hash(chunk_a)
        {:ok, chunk_a}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, ["vol", "/mixed.txt", opts] ->
        assert opts[:offset] == 10
        assert opts[:length] == 10
        {:ok, String.duplicate("B", 10)}
      end)

      assert {:ok, %{stream: stream}} = ChunkReader.read_file_stream("vol", "/mixed.txt")
      assert Enum.into(stream, <<>>) == String.duplicate("A", 10) <> String.duplicate("B", 10)
    end

    test "fetches encrypted chunks via a range-limited read_file RPC" do
      refs = [
        ref(
          seed: :a,
          original_size: 10,
          chunk_offset: 0,
          read_start: 0,
          read_length: 10,
          encrypted: true
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 10, chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, ["vol", "/e.txt", opts] ->
        assert opts[:offset] == 0
        assert opts[:length] == 10
        {:ok, "decrypted!"}
      end)

      assert {:ok, %{stream: stream}} = ChunkReader.read_file_stream("vol", "/e.txt")
      assert Enum.into(stream, <<>>) == "decrypted!"
    end

    test "falls back to read_file for a chunk when every location lacks a data-plane pool" do
      refs = [
        ref(
          seed: :a,
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

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 4, chunks: refs, hole_bytes: 0}}
      end)

      stub(Router, :data_call, fn _node, :get_chunk, _args, _opts ->
        {:error, :no_data_endpoint}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, ["vol", "/nopool.txt", opts] ->
        assert opts[:offset] == 0
        assert opts[:length] == 4
        {:ok, "abcd"}
      end)

      assert {:ok, %{stream: stream}} = ChunkReader.read_file_stream("vol", "/nopool.txt")
      assert Enum.into(stream, <<>>) == "abcd"
    end

    test "returns a stream wrapping the buffered read on :stripe_refs_unsupported for non-EC meta" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, {:stripe_refs_unsupported, nil}}
      end)

      expect(Router, :call, fn NeonFS.Core, :get_file_meta, ["vol", "/ec.bin" | _] ->
        {:ok, %{size: 100, stripes: nil}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, ["vol", "/ec.bin", _] ->
        {:ok, "ec-file-bytes"}
      end)

      assert {:ok, %{stream: stream, file_size: 100}} =
               ChunkReader.read_file_stream("vol", "/ec.bin")

      assert Enum.into(stream, <<>>) == "ec-file-bytes"
    end

    test "propagates metadata errors" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, :not_found}
      end)

      assert {:error, :not_found} = ChunkReader.read_file_stream("vol", "/missing.txt")
    end
  end

  describe "read_file_stream/3 — erasure-coded degraded fallback" do
    test "iterates stripes one at a time bounded by stripe size" do
      stripes = [
        %{stripe_id: "s1", byte_range: {0, 100}},
        %{stripe_id: "s2", byte_range: {100, 200}},
        %{stripe_id: "s3", byte_range: {200, 250}}
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, {:stripe_refs_unsupported, nil}}
      end)

      expect(Router, :call, fn NeonFS.Core, :get_file_meta, ["vol", "/ec.bin" | _] ->
        {:ok, %{size: 250, stripes: stripes}}
      end)

      expect(Router, :call, 3, fn NeonFS.Core, :read_file, ["vol", "/ec.bin", opts] ->
        case {opts[:offset], opts[:length]} do
          {0, 100} -> {:ok, String.duplicate("A", 100)}
          {100, 100} -> {:ok, String.duplicate("B", 100)}
          {200, 50} -> {:ok, String.duplicate("C", 50)}
        end
      end)

      assert {:ok, %{stream: stream, file_size: 250}} =
               ChunkReader.read_file_stream("vol", "/ec.bin")

      chunks = Enum.to_list(stream)
      assert length(chunks) == 3
      assert Enum.at(chunks, 0) == String.duplicate("A", 100)
      assert Enum.at(chunks, 1) == String.duplicate("B", 100)
      assert Enum.at(chunks, 2) == String.duplicate("C", 50)
    end

    test "reads only stripes overlapping the requested range" do
      stripes = [
        %{stripe_id: "s1", byte_range: {0, 100}},
        %{stripe_id: "s2", byte_range: {100, 200}},
        %{stripe_id: "s3", byte_range: {200, 300}}
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, {:stripe_refs_unsupported, nil}}
      end)

      expect(Router, :call, fn NeonFS.Core, :get_file_meta, _ ->
        {:ok, %{size: 300, stripes: stripes}}
      end)

      # Only stripe 2 overlaps byte range 120..180
      expect(Router, :call, fn NeonFS.Core, :read_file, ["vol", "/ec.bin", opts] ->
        assert opts[:offset] == 120
        assert opts[:length] == 60
        {:ok, String.duplicate("B", 60)}
      end)

      assert {:ok, %{stream: stream, file_size: 300}} =
               ChunkReader.read_file_stream("vol", "/ec.bin", offset: 120, length: 60)

      assert Enum.into(stream, <<>>) == String.duplicate("B", 60)
    end

    test "clips partial-overlap stripes at both ends of the range" do
      stripes = [
        %{stripe_id: "s1", byte_range: {0, 100}},
        %{stripe_id: "s2", byte_range: {100, 200}},
        %{stripe_id: "s3", byte_range: {200, 300}}
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, {:stripe_refs_unsupported, nil}}
      end)

      expect(Router, :call, fn NeonFS.Core, :get_file_meta, _ ->
        {:ok, %{size: 300, stripes: stripes}}
      end)

      expect(Router, :call, 3, fn NeonFS.Core, :read_file, ["vol", "/ec.bin", opts] ->
        case {opts[:offset], opts[:length]} do
          {80, 20} -> {:ok, String.duplicate("A", 20)}
          {100, 100} -> {:ok, String.duplicate("B", 100)}
          {200, 10} -> {:ok, String.duplicate("C", 10)}
        end
      end)

      assert {:ok, %{stream: stream}} =
               ChunkReader.read_file_stream("vol", "/ec.bin", offset: 80, length: 130)

      assert Enum.into(stream, <<>>) ==
               String.duplicate("A", 20) <>
                 String.duplicate("B", 100) <> String.duplicate("C", 10)
    end

    test "raises StreamError if a stripe read fails mid-stream" do
      stripes = [
        %{stripe_id: "s1", byte_range: {0, 100}},
        %{stripe_id: "s2", byte_range: {100, 200}}
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, {:stripe_refs_unsupported, nil}}
      end)

      expect(Router, :call, fn NeonFS.Core, :get_file_meta, _ ->
        {:ok, %{size: 200, stripes: stripes}}
      end)

      expect(Router, :call, 2, fn NeonFS.Core, :read_file, ["vol", "/ec.bin", opts] ->
        case opts[:offset] do
          0 -> {:ok, String.duplicate("A", 100)}
          100 -> {:error, %NeonFS.Error.Unavailable{message: "Insufficient chunks"}}
        end
      end)

      assert {:ok, %{stream: stream}} = ChunkReader.read_file_stream("vol", "/ec.bin")
      assert_raise ChunkReader.StreamError, fn -> Enum.to_list(stream) end
    end

    test "range that falls past the last stripe yields an empty stream" do
      stripes = [
        %{stripe_id: "s1", byte_range: {0, 100}}
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, {:stripe_refs_unsupported, nil}}
      end)

      expect(Router, :call, fn NeonFS.Core, :get_file_meta, _ ->
        {:ok, %{size: 100, stripes: stripes}}
      end)

      assert {:ok, %{stream: stream, file_size: 100}} =
               ChunkReader.read_file_stream("vol", "/ec.bin", offset: 200, length: 10)

      assert Enum.to_list(stream) == []
    end
  end

  describe "read_file_stream/3 — mid-stream failure" do
    test "raises StreamError when a chunk fetch fails, rather than truncating silently" do
      refs = [
        ref(content: "abcd", original_size: 4, chunk_offset: 0, read_start: 0, read_length: 4),
        ref(seed: :b, original_size: 4, chunk_offset: 4, read_start: 0, read_length: 4)
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 8, chunks: refs, hole_bytes: 0}}
      end)

      stub(Router, :data_call, fn _node, :get_chunk, args, _opts ->
        cond do
          args[:hash] == content_hash("abcd") -> {:ok, "abcd"}
          args[:hash] == fake_hash(:b) -> {:error, :connection_refused}
        end
      end)

      assert {:ok, %{stream: stream}} = ChunkReader.read_file_stream("vol", "/half.txt")

      # The first chunk is delivered, then the failed fetch raises rather
      # than ending the stream — a silent halt would look like a clean EOF.
      assert ["abcd"] = Enum.take(stream, 1)
      assert_raise ChunkReader.StreamError, fn -> Enum.to_list(stream) end
    end
  end

  describe "chunk_fetched telemetry" do
    test "emits read_length, whole-chunk size, timing, and chunk hash per data-plane fetch" do
      chunk = String.duplicate("Z", 64)

      refs = [
        ref(content: chunk, original_size: 64, chunk_offset: 0, read_start: 8, read_length: 4)
      ]

      hash = Enum.at(refs, 0).hash

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 64, chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :data_call, fn :node1@host, :get_chunk, _args, _opts -> {:ok, chunk} end)

      ref_tel =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :client, :chunk_reader, :chunk_fetched]
        ])

      assert {:ok, "ZZZZ"} = ChunkReader.read_file("vol", "/amplified.txt")

      assert_received {[:neonfs, :client, :chunk_reader, :chunk_fetched], ^ref_tel, measurements,
                       metadata}

      assert measurements.read_length == 4
      assert measurements.chunk_size == 64
      assert is_integer(measurements.duration)
      assert metadata.hash == hash
      assert metadata.node == :node1@host
      assert metadata.volume == "vol"
      assert metadata.tier == "hot"
      assert metadata.source == :data_plane
    end

    # The core call hands back only the slice, but core read and
    # decompressed the whole chunk to produce it. Reporting the slice
    # would show an amplification of 1.0 for exactly the volumes whose
    # amplification is worst.
    test "a chunk served by core reports the whole-chunk bytes, not the slice" do
      chunk = String.duplicate("Q", 4096)

      refs = [
        ref(
          content: chunk,
          original_size: 4096,
          stored_size: 900,
          chunk_offset: 0,
          read_start: 0,
          read_length: 16,
          compression: :zstd
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs_by_id, _ ->
        {:ok, %{file_size: 4096, chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file_by_id, _ ->
        {:ok, binary_part(chunk, 0, 16)}
      end)

      ref_tel =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :client, :chunk_reader, :chunk_fetched]
        ])

      assert {:ok, %{stream: stream}} = ChunkReader.read_file_stream_by_id("vol", "file-1")
      assert Enum.to_list(stream) == [binary_part(chunk, 0, 16)]

      assert_received {[:neonfs, :client, :chunk_reader, :chunk_fetched], ^ref_tel, measurements,
                       metadata}

      assert measurements.read_length == 16
      assert measurements.chunk_size == 4096
      assert metadata.source == :core_rpc

      # `Router` picks the core node internally, so claiming one here
      # would be inventing it.
      assert metadata.node == nil
      assert metadata.tier == nil
    end

    test "caller metadata is merged into the event, whichever fetch served it" do
      chunk = String.duplicate("M", 32)

      refs = [
        ref(content: chunk, original_size: 32, chunk_offset: 0, read_start: 0, read_length: 32)
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 32, chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :data_call, fn :node1@host, :get_chunk, _args, _opts -> {:ok, chunk} end)

      ref_tel =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :client, :chunk_reader, :chunk_fetched]
        ])

      assert {:ok, ^chunk} =
               ChunkReader.read_file("vol", "/tagged.bin",
                 telemetry_metadata: %{export: "vol:/dev.img"}
               )

      assert_received {[:neonfs, :client, :chunk_reader, :chunk_fetched], ^ref_tel, _measurements,
                       metadata}

      assert metadata.export == "vol:/dev.img"
      assert metadata.volume == "vol"
    end

    test "a cache hit emits no chunk_fetched event" do
      start_supervised!({NeonFS.Client.ChunkCache, max_bytes: 1_000_000})
      bytes = "cached, not refetched"

      refs = [
        ref(
          content: bytes,
          original_size: byte_size(bytes),
          chunk_offset: 0,
          read_start: 0,
          read_length: byte_size(bytes)
        )
      ]

      stub(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: byte_size(bytes), chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :data_call, 1, fn _node, :get_chunk, _args, _opts -> {:ok, bytes} end)

      ref_tel =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :client, :chunk_reader, :chunk_fetched]
        ])

      assert {:ok, ^bytes} = ChunkReader.read_file("vol", "/cached.txt", [])
      assert_received {[:neonfs, :client, :chunk_reader, :chunk_fetched], ^ref_tel, _, _}

      assert {:ok, ^bytes} = ChunkReader.read_file("vol", "/cached.txt", [])
      refute_received {[:neonfs, :client, :chunk_reader, :chunk_fetched], ^ref_tel, _, _}
    end
  end

  describe "range_fetched telemetry" do
    setup do
      ref_tel =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :client, :chunk_reader, :range_fetched],
          [:neonfs, :client, :chunk_reader, :chunk_fetched]
        ])

      {:ok, ref_tel: ref_tel}
    end

    # The buffered fallback is taken precisely because the chunks need
    # decompressing or decrypting, so reporting the bytes handed back
    # would show an amplification of 1.0 on the volumes whose read
    # amplification is worst.
    test "a buffered fallback reports every chunk the one call had to process", %{
      ref_tel: ref_tel
    } do
      refs = [
        ref(
          seed: 1,
          original_size: 4096,
          chunk_offset: 0,
          read_start: 0,
          read_length: 16,
          compression: :zstd
        ),
        ref(
          seed: 2,
          original_size: 2048,
          chunk_offset: 4096,
          read_start: 0,
          read_length: 8,
          compression: :zstd
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 6144, chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, _ -> {:ok, "0123456789abcdefghij"} end)

      assert {:ok, _bytes} = ChunkReader.read_file("vol", "/z.txt")

      assert_received {[:neonfs, :client, :chunk_reader, :range_fetched], ^ref_tel, measurements,
                       metadata}

      assert measurements.read_length == 20
      assert measurements.chunk_bytes == 4096 + 2048
      assert is_integer(measurements.duration)
      assert metadata.source == :buffered
      assert metadata.volume == "vol"

      # One call, one event — not one per chunk it covered, which would
      # multiply the duration histogram by the chunk count.
      refute_received {[:neonfs, :client, :chunk_reader, :range_fetched], ^ref_tel, _, _}
      refute_received {[:neonfs, :client, :chunk_reader, :chunk_fetched], ^ref_tel, _, _}
    end

    test "a fallback taken for want of a data endpoint reports the same way", %{ref_tel: ref_tel} do
      refs = [
        ref(
          seed: 1,
          original_size: 512,
          chunk_offset: 0,
          read_start: 0,
          read_length: 4,
          locations: [%{node: :a@host, drive_id: "d1", tier: :hot}]
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 512, chunks: refs, hole_bytes: 0}}
      end)

      stub(Router, :data_call, fn _node, :get_chunk, _args, _opts ->
        {:error, :no_data_endpoint}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, _ -> {:ok, "abcd"} end)

      assert {:ok, "abcd"} = ChunkReader.read_file("vol", "/nopool.txt")

      assert_received {[:neonfs, :client, :chunk_reader, :range_fetched], ^ref_tel, measurements,
                       metadata}

      assert measurements.read_length == 4
      assert measurements.chunk_bytes == 512
      assert metadata.source == :buffered
    end

    # Core rebuilds a degraded stripe from whole chunks to serve any part of
    # it, and only core knows the geometry — so the figure comes from the
    # refusal, per stripe, not from the stripe's byte range. Each stripe here
    # spans 100 bytes but costs thousands to rebuild, which is the gap this
    # measurement exists to close: a file's final stripe can hold a few
    # hundred bytes and still cost every one of its chunks.
    test "the degraded-erasure walk reports what core sized each stripe at", %{ref_tel: ref_tel} do
      stripes = [
        %{stripe_id: "s1", byte_range: {0, 100}},
        %{stripe_id: "s2", byte_range: {100, 200}}
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, {:stripe_refs_unsupported, %{"s1" => 4096, "s2" => 8192}}}
      end)

      expect(Router, :call, fn NeonFS.Core, :get_file_meta, ["vol", "/ec.bin" | _] ->
        {:ok, %{size: 200, stripes: stripes}}
      end)

      expect(Router, :call, 2, fn NeonFS.Core, :read_file, ["vol", "/ec.bin", opts] ->
        {:ok, String.duplicate("A", opts[:length])}
      end)

      assert {:ok, %{stream: stream}} =
               ChunkReader.read_file_stream("vol", "/ec.bin", offset: 90, length: 20)

      assert Enum.into(stream, <<>>) == String.duplicate("A", 20)

      assert_received {[:neonfs, :client, :chunk_reader, :range_fetched], ^ref_tel, first, meta}
      assert meta.source == :stripe
      assert first.read_length == 10
      assert first.chunk_bytes == 4096

      assert_received {[:neonfs, :client, :chunk_reader, :range_fetched], ^ref_tel, second, _meta}
      assert second.read_length == 10
      assert second.chunk_bytes == 8192
    end

    # The breakdown comes from `read_file_refs` and the stripe list from a
    # separate `get_file_meta`, so a file changing between the two calls can
    # leave a stripe unaccounted. That is a real race with no correct number
    # available — so that stripe's measurement is omitted and its siblings
    # still report, rather than the walk going dark or claiming zero.
    test "a stripe missing from the breakdown omits only its own chunk_bytes", %{
      ref_tel: ref_tel
    } do
      stripes = [
        %{stripe_id: "s1", byte_range: {0, 100}},
        %{stripe_id: "s2", byte_range: {100, 200}}
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, {:stripe_refs_unsupported, %{"s1" => 4096}}}
      end)

      expect(Router, :call, fn NeonFS.Core, :get_file_meta, ["vol", "/ec.bin" | _] ->
        {:ok, %{size: 200, stripes: stripes}}
      end)

      expect(Router, :call, 2, fn NeonFS.Core, :read_file, ["vol", "/ec.bin", opts] ->
        {:ok, String.duplicate("A", opts[:length])}
      end)

      assert {:ok, %{stream: stream}} =
               ChunkReader.read_file_stream("vol", "/ec.bin", offset: 90, length: 20)

      assert Enum.into(stream, <<>>) == String.duplicate("A", 20)

      assert_received {[:neonfs, :client, :chunk_reader, :range_fetched], ^ref_tel, first, _meta}
      assert first.chunk_bytes == 4096

      assert_received {[:neonfs, :client, :chunk_reader, :range_fetched], ^ref_tel, second, _meta}
      assert second.read_length == 10

      refute Map.has_key?(second, :chunk_bytes),
             "a stripe with no entry must not be reported as zero"
    end

    # A walk core could not size at all reports nothing rather than zero,
    # the same convention one unsized stripe follows.
    test "an unsizable degraded walk omits chunk_bytes on every stripe", %{ref_tel: ref_tel} do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, {:stripe_refs_unsupported, nil}}
      end)

      expect(Router, :call, fn NeonFS.Core, :get_file_meta, ["vol", "/ec.bin" | _] ->
        {:ok, %{size: 100, stripes: [%{stripe_id: "s1", byte_range: {0, 100}}]}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, ["vol", "/ec.bin", opts] ->
        {:ok, String.duplicate("A", opts[:length])}
      end)

      assert {:ok, %{stream: stream}} = ChunkReader.read_file_stream("vol", "/ec.bin")
      assert Enum.into(stream, <<>>) == String.duplicate("A", 100)

      assert_received {[:neonfs, :client, :chunk_reader, :range_fetched], ^ref_tel, only, meta}
      assert meta.source == :stripe
      refute Map.has_key?(only, :chunk_bytes)
    end

    # The size rides out on the refusal, so this path needs no extra round
    # trip to report what it moved — which is the whole reason the error
    # carries a payload. The 4096 here stands for a degraded stripe's whole
    # chunks, which is what core counts; the buffered path sums the
    # per-stripe breakdown.
    test "the buffered degraded read reports the size core sent with its refusal", %{
      ref_tel: ref_tel
    } do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, {:stripe_refs_unsupported, %{"stripe-a" => 3072, "stripe-b" => 1024}}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, _ -> {:ok, "ec-file-bytes"} end)

      assert {:ok, "ec-file-bytes"} = ChunkReader.read_file("vol", "/ec.bin")

      assert_received {[:neonfs, :client, :chunk_reader, :range_fetched], ^ref_tel, measurements,
                       metadata}

      assert measurements.read_length == byte_size("ec-file-bytes")
      assert measurements.chunk_bytes == 4096
      assert metadata.source == :buffered
    end

    # Core reports `nil` when a stripe is missing from the index and no total
    # over the read is trustworthy. Zero would claim the call moved nothing,
    # which is the under-report this measurement exists to end.
    test "an unsizable degraded read omits chunk_bytes rather than claiming zero", %{
      ref_tel: ref_tel
    } do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, {:stripe_refs_unsupported, nil}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, _ -> {:ok, "ec-file-bytes"} end)

      assert {:ok, "ec-file-bytes"} = ChunkReader.read_file("vol", "/ec.bin")

      assert_received {[:neonfs, :client, :chunk_reader, :range_fetched], ^ref_tel, measurements,
                       metadata}

      assert measurements.read_length == byte_size("ec-file-bytes")
      refute Map.has_key?(measurements, :chunk_bytes)
      assert metadata.source == :buffered
    end

    # The streaming API's buffered fallback used to recompute this from the
    # file meta it had just fetched. It takes core's figure now, so the two
    # APIs cannot report different numbers for the same read.
    test "the streaming buffered fallback reports core's size too", %{ref_tel: ref_tel} do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:error, {:stripe_refs_unsupported, %{"stripe-a" => 8192}}}
      end)

      expect(Router, :call, fn NeonFS.Core, :get_file_meta, _ ->
        {:ok, %{size: 5, stripes: nil}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, _ -> {:ok, "plain"} end)

      assert {:ok, %{stream: stream}} = ChunkReader.read_file_stream("vol", "/ec.bin")
      assert Enum.into(stream, <<>>) == "plain"

      assert_received {[:neonfs, :client, :chunk_reader, :range_fetched], ^ref_tel, measurements,
                       metadata}

      assert measurements.chunk_bytes == 8192
      assert metadata.source == :buffered
    end

    test "caller metadata is merged into a range event too", %{ref_tel: ref_tel} do
      refs = [
        ref(
          seed: 1,
          original_size: 64,
          chunk_offset: 0,
          read_start: 0,
          read_length: 8,
          encrypted: true
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 64, chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file, _ -> {:ok, "12345678"} end)

      assert {:ok, "12345678"} =
               ChunkReader.read_file("vol", "/tagged.bin",
                 telemetry_metadata: %{export: "vol:/dev.img"}
               )

      assert_received {[:neonfs, :client, :chunk_reader, :range_fetched], ^ref_tel, _measurements,
                       metadata}

      assert metadata.export == "vol:/dev.img"
    end

    # A read served entirely from the data plane must not also look like a
    # range fetch, or the two series double-count each other.
    test "a per-chunk read emits no range event", %{ref_tel: ref_tel} do
      chunk = String.duplicate("D", 16)

      refs = [
        ref(content: chunk, original_size: 16, chunk_offset: 0, read_start: 0, read_length: 16)
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 16, chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :data_call, fn :node1@host, :get_chunk, _args, _opts -> {:ok, chunk} end)

      assert {:ok, ^chunk} = ChunkReader.read_file("vol", "/plain.bin")

      assert_received {[:neonfs, :client, :chunk_reader, :chunk_fetched], ^ref_tel, _, _}
      refute_received {[:neonfs, :client, :chunk_reader, :range_fetched], ^ref_tel, _, _}
    end
  end

  describe "chunk cache" do
    setup do
      start_supervised!({NeonFS.Client.ChunkCache, max_bytes: 1_000_000})
      :ok
    end

    test "a second read of the same chunk is served from cache, not re-fetched" do
      bytes = "cacheable chunk bytes"

      refs = [
        ref(
          content: bytes,
          original_size: byte_size(bytes),
          chunk_offset: 0,
          read_start: 0,
          read_length: byte_size(bytes)
        )
      ]

      # read_file_refs is metadata (per read); the chunk bytes data_call
      # must fire only on the first read — the second hits the cache.
      stub(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: byte_size(bytes), chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :data_call, 1, fn _node, :get_chunk, _args, _opts -> {:ok, bytes} end)

      assert {:ok, ^bytes} = ChunkReader.read_file("vol", "/cached.txt", [])
      assert {:ok, ^bytes} = ChunkReader.read_file("vol", "/cached.txt", [])
    end
  end

  # By-ID entry points. The pipeline is shared with the
  # path-based siblings — what these assert is that every route back to
  # core uses the by-ID call, so a handle whose path was renamed away or
  # unlinked still reads.
  describe "read_file_by_id/3" do
    test "assembles bytes over the data plane, resolving refs by file_id" do
      bytes = "bytes behind an open handle"

      refs = [
        ref(
          content: bytes,
          original_size: byte_size(bytes),
          chunk_offset: 0,
          read_start: 0,
          read_length: byte_size(bytes)
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs_by_id, ["vol", "file-1", []] ->
        {:ok, %{file_size: byte_size(bytes), chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :data_call, fn :node1@host, :get_chunk, _args, _opts -> {:ok, bytes} end)

      assert {:ok, ^bytes} = ChunkReader.read_file_by_id("vol", "file-1")
    end

    test "falls back to the by-ID core read for chunks needing server processing" do
      refs = [
        ref(
          seed: :zipped,
          original_size: 10,
          chunk_offset: 0,
          read_start: 0,
          read_length: 10,
          compression: :zstd
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs_by_id, _ ->
        {:ok, %{file_size: 10, chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file_by_id, ["vol", "file-1", opts] ->
        assert opts[:offset] == 3
        assert opts[:length] == 4
        {:ok, "abcd"}
      end)

      assert {:ok, "abcd"} =
               ChunkReader.read_file_by_id("vol", "file-1", offset: 3, length: 4)
    end

    test "falls back to the by-ID core read when no data endpoint is reachable" do
      refs = [
        ref(seed: :remote, original_size: 4, chunk_offset: 0, read_start: 0, read_length: 4)
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs_by_id, _ ->
        {:ok, %{file_size: 4, chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :data_call, fn _node, :get_chunk, _args, _opts ->
        {:error, :no_data_endpoint}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file_by_id, ["vol", "file-1", _opts] ->
        {:ok, "wxyz"}
      end)

      assert {:ok, "wxyz"} = ChunkReader.read_file_by_id("vol", "file-1")
    end

    test "propagates a refs error instead of returning an empty read" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs_by_id, _ ->
        {:error, %{class: :not_found}}
      end)

      assert {:error, %{class: :not_found}} = ChunkReader.read_file_by_id("vol", "gone-id")
    end
  end

  describe "read_file_stream_by_id/3" do
    test "pulls one chunk per element rather than the whole file up front" do
      chunks = for n <- 1..3, do: String.duplicate(<<?A + n>>, 10)

      refs =
        chunks
        |> Enum.with_index()
        |> Enum.map(fn {content, i} ->
          ref(
            content: content,
            original_size: 10,
            chunk_offset: i * 10,
            read_start: 0,
            read_length: 10
          )
        end)

      {:ok, fetches} = Agent.start_link(fn -> 0 end)

      expect(Router, :call, fn NeonFS.Core, :read_file_refs_by_id, ["vol", "file-1", []] ->
        {:ok, %{file_size: 30, chunks: refs, hole_bytes: 0}}
      end)

      stub(Router, :data_call, fn _node, :get_chunk, args, _opts ->
        Agent.update(fetches, &(&1 + 1))
        {:ok, Enum.find(chunks, &(content_hash(&1) == args[:hash]))}
      end)

      assert {:ok, %{stream: stream, file_size: 30}} =
               ChunkReader.read_file_stream_by_id("vol", "file-1")

      # Nothing fetched until the stream is consumed, then exactly one
      # chunk per element taken — the working set never spans the file.
      assert Agent.get(fetches, & &1) == 0
      assert Enum.take(stream, 1) == [Enum.at(chunks, 0)]
      assert Agent.get(fetches, & &1) == 1

      assert Enum.to_list(stream) == chunks
    end

    test "fetches server-processed chunks through the by-ID core read" do
      refs = [
        ref(
          seed: :crypted,
          original_size: 8,
          chunk_offset: 16,
          read_start: 2,
          read_length: 4,
          encrypted: true
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs_by_id, _ ->
        {:ok, %{file_size: 24, chunks: refs, hole_bytes: 0}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file_by_id, ["vol", "file-1", opts] ->
        assert opts[:offset] == 18
        assert opts[:length] == 4
        {:ok, "plai"}
      end)

      assert {:ok, %{stream: stream}} = ChunkReader.read_file_stream_by_id("vol", "file-1")
      assert Enum.into(stream, <<>>) == "plai"
    end

    test "walks stripes one at a time on a degraded erasure read" do
      stripes = [
        %{stripe_id: "s1", byte_range: {0, 100}},
        %{stripe_id: "s2", byte_range: {100, 150}}
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs_by_id, _ ->
        {:error, {:stripe_refs_unsupported, nil}}
      end)

      expect(Router, :call, fn NeonFS.Core, :get_file_meta_by_id, ["vol", "file-1" | _] ->
        {:ok, %{size: 150, stripes: stripes}}
      end)

      expect(Router, :call, 2, fn NeonFS.Core, :read_file_by_id, ["vol", "file-1", opts] ->
        case {opts[:offset], opts[:length]} do
          {0, 100} -> {:ok, String.duplicate("A", 100)}
          {100, 50} -> {:ok, String.duplicate("B", 50)}
        end
      end)

      assert {:ok, %{stream: stream, file_size: 150}} =
               ChunkReader.read_file_stream_by_id("vol", "file-1")

      assert Enum.to_list(stream) == [String.duplicate("A", 100), String.duplicate("B", 50)]
    end

    test "buffers through the by-ID core read when the file has no stripes" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs_by_id, _ ->
        {:error, {:stripe_refs_unsupported, nil}}
      end)

      expect(Router, :call, fn NeonFS.Core, :get_file_meta_by_id, _ ->
        {:ok, %{size: 5, stripes: nil}}
      end)

      expect(Router, :call, fn NeonFS.Core, :read_file_by_id, ["vol", "file-1", _opts] ->
        {:ok, "plain"}
      end)

      assert {:ok, %{stream: stream, file_size: 5}} =
               ChunkReader.read_file_stream_by_id("vol", "file-1")

      assert Enum.into(stream, <<>>) == "plain"
    end

    test "raises StreamError mid-stream rather than truncating the read" do
      refs = [
        ref(content: "abcd", original_size: 4, chunk_offset: 0, read_start: 0, read_length: 4),
        ref(seed: :bad, original_size: 4, chunk_offset: 4, read_start: 0, read_length: 4)
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs_by_id, _ ->
        {:ok, %{file_size: 8, chunks: refs, hole_bytes: 0}}
      end)

      stub(Router, :data_call, fn _node, :get_chunk, args, _opts ->
        cond do
          args[:hash] == content_hash("abcd") -> {:ok, "abcd"}
          args[:hash] == fake_hash(:bad) -> {:error, :connection_refused}
        end
      end)

      assert {:ok, %{stream: stream}} = ChunkReader.read_file_stream_by_id("vol", "file-1")

      assert ["abcd"] = Enum.take(stream, 1)
      assert_raise ChunkReader.StreamError, fn -> Enum.to_list(stream) end
    end
  end

  describe "sparse tails" do
    # A file grown by `truncate` has bytes inside its own size that no chunk
    # backs. Core reports how many; the client has to render them as zeros
    # rather than returning a short read.
    test "read_file/3 appends core's reported hole as zeros" do
      bytes = "abc"

      refs = [
        ref(
          content: bytes,
          original_size: 3,
          chunk_offset: 0,
          read_start: 0,
          read_length: 3
        )
      ]

      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 8192, chunks: refs, hole_bytes: 8189}}
      end)

      expect(Router, :data_call, fn :node1@host, :get_chunk, _args, _opts -> {:ok, bytes} end)

      assert {:ok, read} = ChunkReader.read_file("vol", "/sparse.img")
      assert byte_size(read) == 8192
      assert binary_part(read, 0, 3) == "abc"
      assert binary_part(read, 3, 8189) == :binary.copy(<<0>>, 8189)
    end

    test "read_file/3 renders an all-hole range with no chunks at all" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 8192, chunks: [], hole_bytes: 4096}}
      end)

      assert {:ok, read} = ChunkReader.read_file("vol", "/sparse.img", offset: 4096, length: 4096)
      assert read == :binary.copy(<<0>>, 4096)
    end

    test "read_file_stream/3 yields the hole in bounded blocks" do
      expect(Router, :call, fn NeonFS.Core, :read_file_refs, _ ->
        {:ok, %{file_size: 300_000, chunks: [], hole_bytes: 200_000}}
      end)

      assert {:ok, %{stream: stream}} = ChunkReader.read_file_stream("vol", "/big.img")
      blocks = Enum.to_list(stream)

      assert Enum.sum(Enum.map(blocks, &byte_size/1)) == 200_000

      assert Enum.all?(blocks, &(byte_size(&1) <= 64 * 1024)),
             "a hole must not be materialised as one binary, whatever its size"

      assert Enum.all?(blocks, &(&1 == :binary.copy(<<0>>, byte_size(&1))))
    end
  end
end
