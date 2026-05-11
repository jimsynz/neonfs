defmodule NeonFS.Core.Job.Runners.VolumeAntiEntropyTest do
  @moduledoc """
  Unit tests for the anti-entropy runner. Stubs `ChunkIndex`,
  `BlobStore`, and `ReplicaRepair` via Mimic so the tests don't
  need a running cluster. Integration coverage rides on the
  scheduler tests and the broader `mix test --include
  requires_containerd` peer-cluster suite.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Core.{BlobStore, ChunkIndex, ChunkMeta, Job, ReplicaRepair}
  alias NeonFS.Core.Job.Runners.VolumeAntiEntropy
  alias NeonFS.Core.RaSupervisor
  alias NeonFS.Core.Volume.MetadataReader

  setup :verify_on_exit!

  @volume_id "vol-ae"
  @local Node.self()

  defp loc(node, drive_id \\ "default", tier \\ :hot) do
    %{node: node, drive_id: drive_id, tier: tier}
  end

  defp chunk(hash, locations) do
    %ChunkMeta{
      hash: hash,
      original_size: 1024,
      stored_size: 1024,
      compression: :none,
      crypto: nil,
      locations: locations,
      target_replicas: length(locations),
      commit_state: :committed,
      active_write_refs: MapSet.new(),
      stripe_id: nil,
      stripe_index: nil,
      created_at: DateTime.utc_now(),
      last_verified: nil
    }
  end

  describe "label/0" do
    test "returns volume-anti-entropy" do
      assert VolumeAntiEntropy.label() == "volume-anti-entropy"
    end
  end

  describe "step/1" do
    test "empty volume completes in one step with no repairs" do
      stub(ChunkIndex, :get_chunks_for_volume, fn @volume_id -> [] end)
      reject(&ReplicaRepair.repair_chunks/2)

      job = Job.new(VolumeAntiEntropy, %{volume_id: @volume_id})

      assert {:complete, completed} = VolumeAntiEntropy.step(job)
      assert completed.state.divergent_hashes == []
      assert completed.state.checked == 0
      assert completed.progress.description == "Complete"
    end

    test "all chunks present on declared locations → no repair enqueued" do
      chunks = [
        chunk("h1", [loc(@local), loc(@local, "drive2")]),
        chunk("h2", [loc(@local), loc(@local, "drive2")])
      ]

      stub(ChunkIndex, :get_chunks_for_volume, fn @volume_id -> chunks end)

      stub(ChunkIndex, :get, fn @volume_id, hash ->
        {:ok, Enum.find(chunks, &(&1.hash == hash))}
      end)

      stub(BlobStore, :chunk_exists?, fn _hash, _drive_id -> true end)
      reject(&ReplicaRepair.repair_chunks/2)

      job = Job.new(VolumeAntiEntropy, %{volume_id: @volume_id})

      assert {:continue, mid} = VolumeAntiEntropy.step(job)
      assert {:complete, done} = VolumeAntiEntropy.step(mid)

      assert done.state.checked == 2
      assert done.state.divergent_hashes == []
    end

    test "chunk missing from declared location → enqueues repair_chunks" do
      diverged = chunk("h-bad", [loc(@local), loc(@local, "drive2")])
      ok_chunk = chunk("h-ok", [loc(@local)])

      stub(ChunkIndex, :get_chunks_for_volume, fn @volume_id -> [diverged, ok_chunk] end)

      stub(ChunkIndex, :get, fn @volume_id, hash ->
        {:ok, Enum.find([diverged, ok_chunk], &(&1.hash == hash))}
      end)

      # `h-bad` claims to live on @local/default and @local/drive2,
      # but drive2 says it doesn't have it. `h-ok` is fully present.
      stub(BlobStore, :chunk_exists?, fn
        "h-bad", "default" -> true
        "h-bad", "drive2" -> false
        "h-ok", _ -> true
      end)

      expect(ReplicaRepair, :repair_chunks, fn @volume_id, hashes ->
        assert hashes == ["h-bad"]
        {:ok, %{added: 1, removed: 0, errors: []}}
      end)

      job = Job.new(VolumeAntiEntropy, %{volume_id: @volume_id})

      assert {:continue, mid} = VolumeAntiEntropy.step(job)
      assert {:complete, done} = VolumeAntiEntropy.step(mid)

      assert done.state.divergent_hashes == ["h-bad"]
      assert done.state.repair_added == 1
    end

    test "respects custom :batch_size" do
      chunks = for n <- 1..5, do: chunk("h#{n}", [loc(@local)])

      stub(ChunkIndex, :get_chunks_for_volume, fn @volume_id -> chunks end)

      stub(ChunkIndex, :get, fn @volume_id, hash ->
        {:ok, Enum.find(chunks, &(&1.hash == hash))}
      end)

      stub(BlobStore, :chunk_exists?, fn _hash, _drive_id -> true end)
      reject(&ReplicaRepair.repair_chunks/2)

      job = Job.new(VolumeAntiEntropy, %{volume_id: @volume_id, batch_size: 2})

      # 5 chunks ÷ batch of 2 = 3 :continue steps + 1 :complete.
      assert {:continue, s1} = VolumeAntiEntropy.step(job)
      assert s1.state.checked == 2
      assert {:continue, s2} = VolumeAntiEntropy.step(s1)
      assert s2.state.checked == 4
      assert {:continue, s3} = VolumeAntiEntropy.step(s2)
      assert s3.state.checked == 5
      assert {:complete, _} = VolumeAntiEntropy.step(s3)
    end

    test "unreachable peer is skipped (no spurious repair) and telemetry emitted" do
      diverged = chunk("h1", [loc(@local), loc(:far_away_node)])

      stub(ChunkIndex, :get_chunks_for_volume, fn @volume_id -> [diverged] end)
      stub(ChunkIndex, :get, fn @volume_id, "h1" -> {:ok, diverged} end)
      stub(BlobStore, :chunk_exists?, fn "h1", "default" -> true end)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :volume_anti_entropy, :peer_unreachable]
        ])

      reject(&ReplicaRepair.repair_chunks/2)

      job = Job.new(VolumeAntiEntropy, %{volume_id: @volume_id})

      assert {:continue, mid} = VolumeAntiEntropy.step(job)
      assert {:complete, done} = VolumeAntiEntropy.step(mid)
      assert done.state.divergent_hashes == []

      assert_received {[:neonfs, :volume_anti_entropy, :peer_unreachable], ^ref, _,
                       %{peer_node: :far_away_node}}
    end
  end

  describe "step/1 — index-tree page enumeration (#955 / #999)" do
    test "tree-page hash present on every drive_location → no repair" do
      stub(ChunkIndex, :get_chunks_for_volume, fn @volume_id -> [] end)

      stub(MetadataReader, :list_referenced_chunks, fn @volume_id ->
        {:ok, ["tree-page-1", "tree-page-2"]}
      end)

      stub(RaSupervisor, :local_query, fn _fn ->
        {:ok, %{drive_locations: [loc(@local), loc(@local, "drive2")]}}
      end)

      stub(BlobStore, :chunk_exists?, fn _hash, _drive_id -> true end)
      reject(&ReplicaRepair.repair_chunks/2)

      job = Job.new(VolumeAntiEntropy, %{volume_id: @volume_id})

      assert {:continue, mid} = VolumeAntiEntropy.step(job)
      assert {:complete, done} = VolumeAntiEntropy.step(mid)

      assert done.state.checked == 2
      assert done.state.divergent_hashes == []
    end

    test "tree-page hash missing on a drive_location → enqueues repair" do
      stub(ChunkIndex, :get_chunks_for_volume, fn @volume_id -> [] end)

      stub(MetadataReader, :list_referenced_chunks, fn @volume_id ->
        {:ok, ["tree-bad"]}
      end)

      stub(RaSupervisor, :local_query, fn _fn ->
        {:ok, %{drive_locations: [loc(@local), loc(@local, "drive2")]}}
      end)

      stub(BlobStore, :chunk_exists?, fn
        "tree-bad", "default" -> true
        "tree-bad", "drive2" -> false
      end)

      expect(ReplicaRepair, :repair_chunks, fn @volume_id, hashes ->
        assert hashes == ["tree-bad"]
        {:ok, %{added: 1, removed: 0, errors: []}}
      end)

      job = Job.new(VolumeAntiEntropy, %{volume_id: @volume_id})

      assert {:continue, mid} = VolumeAntiEntropy.step(job)
      assert {:complete, done} = VolumeAntiEntropy.step(mid)

      assert done.state.divergent_hashes == ["tree-bad"]
    end

    test "data chunks and tree pages share the batch + divergence set" do
      diverged_chunk = chunk("data-bad", [loc(@local), loc(@local, "drive2")])

      stub(ChunkIndex, :get_chunks_for_volume, fn @volume_id -> [diverged_chunk] end)
      stub(ChunkIndex, :get, fn @volume_id, "data-bad" -> {:ok, diverged_chunk} end)

      stub(MetadataReader, :list_referenced_chunks, fn @volume_id ->
        {:ok, ["tree-bad"]}
      end)

      stub(RaSupervisor, :local_query, fn _fn ->
        {:ok, %{drive_locations: [loc(@local), loc(@local, "drive2")]}}
      end)

      stub(BlobStore, :chunk_exists?, fn
        "data-bad", "default" -> true
        "data-bad", "drive2" -> false
        "tree-bad", "default" -> true
        "tree-bad", "drive2" -> false
      end)

      expect(ReplicaRepair, :repair_chunks, fn @volume_id, hashes ->
        assert Enum.sort(hashes) == ["data-bad", "tree-bad"]
        {:ok, %{added: 2, removed: 0, errors: []}}
      end)

      job = Job.new(VolumeAntiEntropy, %{volume_id: @volume_id})

      assert {:continue, mid} = VolumeAntiEntropy.step(job)
      assert {:complete, done} = VolumeAntiEntropy.step(mid)

      assert Enum.sort(done.state.divergent_hashes) == ["data-bad", "tree-bad"]
    end

    test "tree-page enumeration failure is logged and the pass continues with chunks only" do
      chunk_meta = chunk("h1", [loc(@local)])

      stub(ChunkIndex, :get_chunks_for_volume, fn @volume_id -> [chunk_meta] end)
      stub(ChunkIndex, :get, fn @volume_id, "h1" -> {:ok, chunk_meta} end)

      stub(MetadataReader, :list_referenced_chunks, fn @volume_id ->
        {:error, {:bootstrap_query_failed, :ra_down}}
      end)

      stub(BlobStore, :chunk_exists?, fn _, _ -> true end)
      reject(&ReplicaRepair.repair_chunks/2)

      job = Job.new(VolumeAntiEntropy, %{volume_id: @volume_id})

      assert {:continue, mid} = VolumeAntiEntropy.step(job)
      assert {:complete, done} = VolumeAntiEntropy.step(mid)

      assert done.state.checked == 1
    end
  end
end
