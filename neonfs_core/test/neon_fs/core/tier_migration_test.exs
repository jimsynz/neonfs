defmodule NeonFS.Core.TierMigrationTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.BlobStore
  alias NeonFS.Core.ChunkIndex
  alias NeonFS.Core.ChunkMeta
  alias NeonFS.Core.TierMigration
  alias NeonFS.Core.TierMigration.LockTable

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_drive_registry()
    start_blob_store()
    start_chunk_index()

    LockTable.init()

    Enum.each(LockTable.list_locks(), fn {hash, _, _} ->
      LockTable.release_lock(hash)
    end)

    on_exit(fn -> cleanup_test_dirs() end)

    :ok
  end

  describe "LockTable" do
    test "acquire_lock/1 returns :ok for new lock" do
      hash = :crypto.strong_rand_bytes(32)
      assert :ok = LockTable.acquire_lock(hash)
      LockTable.release_lock(hash)
    end

    test "acquire_lock/1 returns {:error, :migration_in_progress} for existing lock" do
      hash = :crypto.strong_rand_bytes(32)
      assert :ok = LockTable.acquire_lock(hash)
      assert {:error, :migration_in_progress} = LockTable.acquire_lock(hash)
      LockTable.release_lock(hash)
    end

    test "release_lock/1 allows re-acquisition" do
      hash = :crypto.strong_rand_bytes(32)
      assert :ok = LockTable.acquire_lock(hash)
      LockTable.release_lock(hash)
      assert :ok = LockTable.acquire_lock(hash)
      LockTable.release_lock(hash)
    end

    test "locked?/1 returns true for locked chunk" do
      hash = :crypto.strong_rand_bytes(32)
      LockTable.acquire_lock(hash)
      assert LockTable.locked?(hash)
      LockTable.release_lock(hash)
    end

    test "locked?/1 returns false for unlocked chunk" do
      hash = :crypto.strong_rand_bytes(32)
      refute LockTable.locked?(hash)
    end

    test "list_locks/0 returns all held locks" do
      h1 = :crypto.strong_rand_bytes(32)
      h2 = :crypto.strong_rand_bytes(32)
      LockTable.acquire_lock(h1)
      LockTable.acquire_lock(h2)

      locks = LockTable.list_locks()
      hashes = Enum.map(locks, fn {h, _, _} -> h end)
      assert h1 in hashes
      assert h2 in hashes

      LockTable.release_lock(h1)
      LockTable.release_lock(h2)
    end

    test "release_lock/1 is idempotent" do
      hash = :crypto.strong_rand_bytes(32)
      assert :ok = LockTable.release_lock(hash)
      assert :ok = LockTable.release_lock(hash)
    end
  end

  describe "run_migration/1 lock behavior" do
    test "concurrent migration of same chunk is rejected" do
      hash = :crypto.strong_rand_bytes(32)

      # Manually acquire the lock first
      assert :ok = LockTable.acquire_lock(hash)

      params = %{
        chunk_hash: hash,
        source_drive: "drive1",
        source_node: Node.self(),
        source_tier: :hot,
        target_drive: "drive2",
        target_node: Node.self(),
        target_tier: :warm
      }

      assert {:error, :migration_in_progress} = TierMigration.run_migration(params)

      LockTable.release_lock(hash)
    end
  end

  describe "run_migration/1 with local drives" do
    setup do
      # BlobStore is started by the module-level setup

      # Write a test chunk to the default drive
      data = :crypto.strong_rand_bytes(1024)

      {:ok, hash, _info} =
        BlobStore.write_chunk(data, "default", "hot")

      # Register the chunk in ChunkIndex
      chunk_meta = ChunkMeta.new(hash, 1024, 1024)

      chunk_meta = %{
        chunk_meta
        | locations: [%{node: Node.self(), drive_id: "default", tier: :hot}],
          commit_state: :committed
      }

      :ok = ChunkIndex.put(chunk_meta)

      {:ok, chunk_hash: hash, data: data}
    end

    test "migrates chunk between tiers on same drive", %{chunk_hash: hash} do
      params = %{
        chunk_hash: hash,
        source_drive: "default",
        source_node: Node.self(),
        source_tier: :hot,
        target_drive: "default",
        target_node: Node.self(),
        target_tier: :warm
      }

      result = TierMigration.run_migration(params)
      assert {:ok, :migrated} = result

      # Verify chunk is readable from the target tier
      assert {:ok, _data} =
               BlobStore.read_chunk_with_options(
                 hash,
                 "default",
                 "warm",
                 verify: true
               )

      # Verify metadata was updated
      {:ok, meta} = ChunkIndex.get(hash)

      assert Enum.any?(meta.locations, fn loc ->
               loc.tier == :warm and loc.drive_id == "default"
             end)

      # Lock should be released
      refute LockTable.locked?(hash)
    end
  end

  describe "work_fn/1" do
    test "returns a callable function" do
      params = %{
        chunk_hash: :crypto.strong_rand_bytes(32),
        source_drive: "drive1",
        source_node: Node.self(),
        source_tier: :hot,
        target_drive: "drive2",
        target_node: Node.self(),
        target_tier: :warm
      }

      fun = TierMigration.work_fn(params)
      assert is_function(fun, 0)
    end
  end

  describe "telemetry" do
    setup do
      data = :crypto.strong_rand_bytes(1024)

      {:ok, hash, _info} =
        BlobStore.write_chunk(data, "default", "hot")

      chunk_meta = ChunkMeta.new(hash, 1024, 1024)

      chunk_meta = %{
        chunk_meta
        | locations: [%{node: Node.self(), drive_id: "default", tier: :hot}],
          commit_state: :committed
      }

      :ok = ChunkIndex.put(chunk_meta)

      {:ok, chunk_hash: hash}
    end

    test "emits start and success events on successful migration", %{chunk_hash: hash} do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :tier_migration, :start],
          [:neonfs, :tier_migration, :success]
        ])

      params = %{
        chunk_hash: hash,
        source_drive: "default",
        source_node: Node.self(),
        source_tier: :hot,
        target_drive: "default",
        target_node: Node.self(),
        target_tier: :warm
      }

      assert {:ok, :migrated} = TierMigration.run_migration(params)

      assert_receive {[:neonfs, :tier_migration, :start], ^ref, %{},
                      %{chunk_hash: ^hash, local: true}}

      assert_receive {[:neonfs, :tier_migration, :success], ^ref, %{}, %{chunk_hash: ^hash}}
    end

    test "emits start and failure events on failed migration" do
      # Use a non-existent chunk
      hash = :crypto.strong_rand_bytes(32)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :tier_migration, :start],
          [:neonfs, :tier_migration, :failure]
        ])

      params = %{
        chunk_hash: hash,
        source_drive: "default",
        source_node: Node.self(),
        source_tier: :hot,
        target_drive: "default",
        target_node: Node.self(),
        target_tier: :warm
      }

      assert {:error, _} = TierMigration.run_migration(params)

      assert_receive {[:neonfs, :tier_migration, :start], ^ref, %{}, %{chunk_hash: ^hash}}
      assert_receive {[:neonfs, :tier_migration, :failure], ^ref, %{}, %{chunk_hash: ^hash}}
    end
  end
end
