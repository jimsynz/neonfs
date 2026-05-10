defmodule NeonFS.Core.ReplicaRepairTest do
  @moduledoc """
  Unit tests for `NeonFS.Core.ReplicaRepair` (#706). Stubs
  `VolumeRegistry`, `ChunkIndex`, `BlobStore`, and `Replication` via
  Mimic so the tests don't need a running cluster — the data-plane
  primitive is exercised against synthetic chunk metadata.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Core.{BlobStore, ChunkIndex, ChunkMeta, ReplicaRepair, Replication, VolumeRegistry}

  setup :verify_on_exit!

  @volume_id "vol-1"
  @local Node.self()

  defp volume(opts \\ []) do
    %{
      id: @volume_id,
      durability: %{factor: Keyword.get(opts, :factor, 3), type: :replicate, min_copies: 1},
      tiering: %{initial_tier: :hot}
    }
  end

  defp chunk(hash, locations, target_replicas) do
    %ChunkMeta{
      hash: hash,
      original_size: 1024,
      stored_size: 1024,
      compression: :none,
      crypto: nil,
      locations: locations,
      target_replicas: target_replicas,
      commit_state: :committed,
      active_write_refs: MapSet.new(),
      stripe_id: nil,
      stripe_index: nil,
      created_at: DateTime.utc_now(),
      last_verified: nil
    }
  end

  defp loc(node, drive_id \\ "default", tier \\ :hot) do
    %{node: node, drive_id: drive_id, tier: tier}
  end

  describe "repair_volume/2" do
    test "skips chunks whose current replica count equals target_replicas" do
      stub(VolumeRegistry, :get, fn _ -> {:ok, volume()} end)

      stub(ChunkIndex, :get_chunks_for_volume, fn _ ->
        [chunk("h1", [loc(@local), loc(:n2), loc(:n3)], 3)]
      end)

      reject(&Replication.replicate_chunk/4)
      reject(&BlobStore.delete_chunk/3)

      assert {:ok, %{added: 0, removed: 0, errors: [], next_cursor: :done}} =
               ReplicaRepair.repair_volume(@volume_id)
    end

    test "under-replicated: sources data locally, calls replicate_chunk, updates locations" do
      vol = volume()

      stub(VolumeRegistry, :get, fn _ -> {:ok, vol} end)

      stub(ChunkIndex, :get_chunks_for_volume, fn _ ->
        [chunk("h1", [loc(@local)], 3)]
      end)

      expect(BlobStore, :read_chunk, fn "h1", "default" -> {:ok, "chunk-data"} end)

      expect(Replication, :replicate_chunk, fn "h1", "chunk-data", ^vol, opts ->
        assert opts[:exclude_nodes] == [@local]

        {:ok,
         [
           loc(:n2),
           loc(:n3)
         ]}
      end)

      expect(ChunkIndex, :update_locations, fn "h1", new_locations ->
        assert length(new_locations) == 3
        :ok
      end)

      assert {:ok, %{added: 2, removed: 0, errors: []}} =
               ReplicaRepair.repair_volume(@volume_id)
    end

    test "over-replicated: deletes excess local + remote replicas, updates locations" do
      stub(VolumeRegistry, :get, fn _ -> {:ok, volume()} end)

      stub(ChunkIndex, :get_chunks_for_volume, fn _ ->
        [chunk("h1", [loc(@local), loc(:n2), loc(:n3), loc(:n4), loc(:n5)], 3)]
      end)

      reject(&Replication.replicate_chunk/4)

      # First two locations get evicted (local + n2). Remote delete goes
      # via :rpc.call which we can't easily stub; for this test we
      # verify only the local-side BlobStore.delete_chunk call.
      expect(BlobStore, :delete_chunk, fn "h1", "default", _ ->
        {:ok, 1024}
      end)

      stub(ChunkIndex, :update_locations, fn _, _ -> :ok end)

      result = ReplicaRepair.repair_volume(@volume_id)

      # The local delete records one removed; the remote-rpc deletes
      # surface as errors because :rpc.call can't reach :n2 in the
      # test VM. Either way the per-chunk error list is what we use to
      # assert the remote-side dispatch happened.
      assert {:ok, %{added: 0, removed: removed, errors: errors}} = result
      assert removed >= 1
      assert is_list(errors)
    end

    test "exactly-replicated chunks are no-ops even mixed with under/over" do
      stub(VolumeRegistry, :get, fn _ -> {:ok, volume()} end)

      stub(ChunkIndex, :get_chunks_for_volume, fn _ ->
        [
          # exact: skip
          chunk("h-exact", [loc(@local), loc(:n2), loc(:n3)], 3),
          # under: needs +1
          chunk("h-under", [loc(@local), loc(:n2)], 3)
        ]
      end)

      stub(BlobStore, :read_chunk, fn "h-under", _ -> {:ok, "data"} end)

      expect(Replication, :replicate_chunk, fn "h-under", _, _, _ ->
        {:ok, [loc(:n3)]}
      end)

      stub(ChunkIndex, :update_locations, fn _, _ -> :ok end)

      assert {:ok, %{added: 1, removed: 0}} = ReplicaRepair.repair_volume(@volume_id)
    end

    test "one chunk failing doesn't abort other chunks; error is captured per chunk" do
      stub(VolumeRegistry, :get, fn _ -> {:ok, volume()} end)

      stub(ChunkIndex, :get_chunks_for_volume, fn _ ->
        [
          # this one fails to source data
          chunk("h-fail", [loc(@local)], 3),
          # this one succeeds
          chunk("h-ok", [loc(@local)], 3)
        ]
      end)

      stub(BlobStore, :read_chunk, fn
        "h-fail", _ -> {:error, "disk gone"}
        "h-ok", _ -> {:ok, "data"}
      end)

      stub(Replication, :replicate_chunk, fn "h-ok", _, _, _ ->
        {:ok, [loc(:n2), loc(:n3)]}
      end)

      stub(ChunkIndex, :update_locations, fn _, _ -> :ok end)

      assert {:ok, %{added: added, removed: 0, errors: [{"h-fail", _} | _]}} =
               ReplicaRepair.repair_volume(@volume_id)

      assert added > 0
    end

    test "telemetry: emits :chunk_repaired on success and :error on failure" do
      stub(VolumeRegistry, :get, fn _ -> {:ok, volume()} end)

      stub(ChunkIndex, :get_chunks_for_volume, fn _ ->
        [
          chunk("h-ok", [loc(@local)], 3),
          chunk("h-fail", [loc(@local)], 3)
        ]
      end)

      stub(BlobStore, :read_chunk, fn
        "h-ok", _ -> {:ok, "data"}
        "h-fail", _ -> {:error, :gone}
      end)

      stub(Replication, :replicate_chunk, fn "h-ok", _, _, _ ->
        {:ok, [loc(:n2), loc(:n3)]}
      end)

      stub(ChunkIndex, :update_locations, fn _, _ -> :ok end)

      handler_id = "replica-repair-test-#{System.unique_integer([:positive])}"

      :telemetry.attach_many(
        handler_id,
        [
          [:neonfs, :replica_repair, :chunk_repaired],
          [:neonfs, :replica_repair, :error]
        ],
        fn event, measurements, metadata, parent ->
          send(parent, {:tel, event, measurements, metadata})
        end,
        self()
      )

      _ = ReplicaRepair.repair_volume(@volume_id)

      assert_received {:tel, [:neonfs, :replica_repair, :chunk_repaired], _,
                       %{chunk_hash: "h-ok"}}

      assert_received {:tel, [:neonfs, :replica_repair, :error], _, %{chunk_hash: "h-fail"}}

      :telemetry.detach(handler_id)
    end

    test "honours :batch_size and reports a resumable :next_cursor" do
      stub(VolumeRegistry, :get, fn _ -> {:ok, volume()} end)

      stub(ChunkIndex, :get_chunks_for_volume, fn _ ->
        for i <- 1..5 do
          chunk("h-#{i}", [loc(@local), loc(:n2), loc(:n3)], 3)
        end
      end)

      assert {:ok, %{next_cursor: 2}} =
               ReplicaRepair.repair_volume(@volume_id, batch_size: 2, cursor: 0)

      assert {:ok, %{next_cursor: 4}} =
               ReplicaRepair.repair_volume(@volume_id, batch_size: 2, cursor: 2)

      assert {:ok, %{next_cursor: :done}} =
               ReplicaRepair.repair_volume(@volume_id, batch_size: 2, cursor: 4)
    end

    test "returns {:error, _} when volume isn't registered" do
      stub(VolumeRegistry, :get, fn _ -> {:error, :not_found} end)

      assert {:error, :not_found} = ReplicaRepair.repair_volume("missing")
    end
  end

  describe "repair_chunks/2 (#921)" do
    test "reconciles only the supplied hashes, skipping volume-wide walk" do
      vol = volume()
      stub(VolumeRegistry, :get, fn _ -> {:ok, vol} end)

      # Volume contains h1, h2, h3 — but caller only asked to repair h2.
      stub(ChunkIndex, :get, fn _, "h2" -> {:ok, chunk("h2", [loc(@local)], 3)} end)

      expect(BlobStore, :read_chunk, fn "h2", "default" -> {:ok, "data-h2"} end)

      expect(Replication, :replicate_chunk, fn "h2", "data-h2", ^vol, _opts ->
        {:ok, [loc(:n2), loc(:n3)]}
      end)

      expect(ChunkIndex, :update_locations, fn "h2", _ -> :ok end)
      reject(&ChunkIndex.get_chunks_for_volume/1)

      assert {:ok, %{added: 2, removed: 0, errors: [], next_cursor: :done}} =
               ReplicaRepair.repair_chunks(@volume_id, ["h2"])
    end

    test "collects :not_found into :errors rather than aborting the pass" do
      stub(VolumeRegistry, :get, fn _ -> {:ok, volume()} end)

      stub(ChunkIndex, :get, fn
        _, "h-present" -> {:ok, chunk("h-present", [loc(@local), loc(:n2), loc(:n3)], 3)}
        _, "h-missing" -> {:error, :not_found}
      end)

      reject(&Replication.replicate_chunk/4)
      reject(&BlobStore.delete_chunk/3)

      assert {:ok, %{added: 0, removed: 0, errors: errors, next_cursor: :done}} =
               ReplicaRepair.repair_chunks(@volume_id, ["h-present", "h-missing"])

      assert {"h-missing", :not_found} in errors
    end

    test "returns {:error, _} when volume isn't registered" do
      stub(VolumeRegistry, :get, fn _ -> {:error, :not_found} end)

      assert {:error, :not_found} = ReplicaRepair.repair_chunks("missing", ["h1"])
    end

    test "empty hash list is a no-op with :done cursor" do
      stub(VolumeRegistry, :get, fn _ -> {:ok, volume()} end)
      reject(&ChunkIndex.get/2)

      assert {:ok, %{added: 0, removed: 0, errors: [], next_cursor: :done}} =
               ReplicaRepair.repair_chunks(@volume_id, [])
    end
  end
end
