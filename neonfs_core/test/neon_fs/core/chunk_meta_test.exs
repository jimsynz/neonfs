defmodule NeonFS.Core.ChunkMetaTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.ChunkMeta

  describe "new/5" do
    test "wraps a single volume_id into the volume_ids MapSet" do
      meta = ChunkMeta.new("vol-a", <<1::256>>, 1024, 1024)

      assert %MapSet{} = meta.volume_ids
      assert MapSet.to_list(meta.volume_ids) == ["vol-a"]
    end

    test "defaults compression to :none" do
      meta = ChunkMeta.new("vol-a", <<1::256>>, 1024, 1024)
      assert meta.compression == :none
    end

    test "respects explicit compression" do
      meta = ChunkMeta.new("vol-a", <<1::256>>, 2048, 1500, :zstd)
      assert meta.compression == :zstd
    end

    test "starts with no locations and an empty active_write_refs set" do
      meta = ChunkMeta.new("vol-a", <<1::256>>, 1024, 1024)
      assert meta.locations == []
      assert MapSet.size(meta.active_write_refs) == 0
    end
  end

  describe "add_volume/2" do
    test "adds a volume_id to the set" do
      meta =
        ChunkMeta.new("vol-a", <<1::256>>, 1024, 1024)
        |> ChunkMeta.add_volume("vol-b")

      assert MapSet.equal?(meta.volume_ids, MapSet.new(["vol-a", "vol-b"]))
    end

    test "is idempotent" do
      meta =
        ChunkMeta.new("vol-a", <<1::256>>, 1024, 1024)
        |> ChunkMeta.add_volume("vol-a")
        |> ChunkMeta.add_volume("vol-a")

      assert MapSet.size(meta.volume_ids) == 1
    end
  end

  describe "remove_volume/2" do
    test "removes a volume_id from the set" do
      meta =
        ChunkMeta.new("vol-a", <<1::256>>, 1024, 1024)
        |> ChunkMeta.add_volume("vol-b")
        |> ChunkMeta.remove_volume("vol-a")

      assert MapSet.to_list(meta.volume_ids) == ["vol-b"]
    end

    test "leaves an empty set when the last volume is removed" do
      meta =
        ChunkMeta.new("vol-a", <<1::256>>, 1024, 1024)
        |> ChunkMeta.remove_volume("vol-a")

      assert MapSet.size(meta.volume_ids) == 0
    end

    test "is a no-op for an absent volume_id" do
      meta =
        ChunkMeta.new("vol-a", <<1::256>>, 1024, 1024)
        |> ChunkMeta.remove_volume("vol-missing")

      assert MapSet.to_list(meta.volume_ids) == ["vol-a"]
    end
  end

  describe "any_volume_id/1" do
    test "returns the only element when the set is a singleton" do
      meta = ChunkMeta.new("vol-a", <<1::256>>, 1024, 1024)
      assert ChunkMeta.any_volume_id(meta) == "vol-a"
    end

    test "returns one of the elements when the set has multiple" do
      meta =
        ChunkMeta.new("vol-a", <<1::256>>, 1024, 1024)
        |> ChunkMeta.add_volume("vol-b")

      assert ChunkMeta.any_volume_id(meta) in ["vol-a", "vol-b"]
    end

    test "returns nil when the set is empty" do
      meta = %ChunkMeta{
        volume_ids: MapSet.new(),
        hash: <<1::256>>,
        original_size: 0,
        stored_size: 0,
        compression: :none,
        locations: [],
        target_replicas: 1,
        commit_state: :uncommitted,
        active_write_refs: MapSet.new(),
        stripe_id: nil,
        stripe_index: nil,
        created_at: DateTime.utc_now(),
        last_verified: nil
      }

      assert ChunkMeta.any_volume_id(meta) == nil
    end
  end

  describe "ETF round-trip" do
    test "preserves the volume_ids MapSet across :erlang.term_to_binary" do
      meta =
        ChunkMeta.new("vol-a", <<1::256>>, 1024, 1024)
        |> ChunkMeta.add_volume("vol-b")
        |> ChunkMeta.add_volume("vol-c")

      bytes = :erlang.term_to_binary(meta)
      decoded = :erlang.binary_to_term(bytes)

      assert MapSet.equal?(decoded.volume_ids, meta.volume_ids)
      assert decoded.hash == meta.hash
    end
  end

  describe "commit/1" do
    test "commits an uncommitted meta with no active write refs" do
      meta = ChunkMeta.new("vol-a", <<1::256>>, 1024, 1024)
      assert {:ok, committed} = ChunkMeta.commit(meta)
      assert committed.commit_state == :committed
    end

    test "rejects commit when there are active write refs" do
      meta =
        ChunkMeta.new("vol-a", <<1::256>>, 1024, 1024)
        |> ChunkMeta.add_write_ref("write-1")

      assert {:error, :has_active_writes} = ChunkMeta.commit(meta)
    end
  end
end
