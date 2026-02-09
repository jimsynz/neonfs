defmodule NeonFS.Core.VolumeRegistryTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.FileIndex
  alias NeonFS.Core.FileMeta
  alias NeonFS.Core.Volume
  alias NeonFS.Core.VolumeRegistry

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_file_index()
    start_volume_registry()
    on_exit(fn -> cleanup_test_dirs() end)
    :ok
  end

  describe "Volume struct" do
    test "new/1 creates volume with defaults" do
      volume = Volume.new("test-volume")

      assert volume.name == "test-volume"
      assert volume.id != nil
      assert volume.owner == nil
      assert volume.durability == %{type: :replicate, factor: 3, min_copies: 2}
      assert volume.write_ack == :local

      assert volume.tiering == %{
               initial_tier: :hot,
               promotion_threshold: 10,
               demotion_delay: 86_400
             }

      assert volume.caching.transformed_chunks == true
      assert volume.caching.max_memory == 268_435_456
      assert volume.io_weight == 100
      assert volume.compression == %{algorithm: :zstd, level: 3, min_size: 4096}
      assert volume.verification == %{on_read: :never, sampling_rate: nil}
      assert volume.logical_size == 0
      assert volume.physical_size == 0
      assert volume.chunk_count == 0
      assert %DateTime{} = volume.created_at
      assert %DateTime{} = volume.updated_at
    end

    test "new/2 creates volume with custom configuration" do
      volume =
        Volume.new("test-volume",
          owner: "alice",
          write_ack: :quorum,
          tiering: %{initial_tier: :warm, promotion_threshold: 5, demotion_delay: 3600},
          compression: %{algorithm: :none}
        )

      assert volume.name == "test-volume"
      assert volume.owner == "alice"
      assert volume.write_ack == :quorum
      assert volume.tiering.initial_tier == :warm
      assert volume.tiering.promotion_threshold == 5
      assert volume.compression == %{algorithm: :none}
    end

    test "update/2 updates allowed fields" do
      volume = Volume.new("test-volume")
      original_updated_at = volume.updated_at

      # Sleep to ensure timestamp changes
      Process.sleep(10)

      updated =
        Volume.update(volume,
          owner: "bob",
          write_ack: :all,
          tiering: %{initial_tier: :cold, promotion_threshold: 20, demotion_delay: 172_800},
          durability: %{type: :replicate, factor: 5, min_copies: 3}
        )

      assert updated.owner == "bob"
      assert updated.write_ack == :all
      assert updated.tiering.initial_tier == :cold
      assert updated.tiering.promotion_threshold == 20
      assert updated.durability == %{type: :replicate, factor: 5, min_copies: 3}
      assert DateTime.compare(updated.updated_at, original_updated_at) == :gt

      # ID, name, and stats should not change
      assert updated.id == volume.id
      assert updated.name == volume.name
      assert updated.logical_size == volume.logical_size
    end

    test "update_stats/2 updates statistics" do
      volume = Volume.new("test-volume")

      updated =
        Volume.update_stats(volume, logical_size: 1024, physical_size: 512, chunk_count: 5)

      assert updated.logical_size == 1024
      assert updated.physical_size == 512
      assert updated.chunk_count == 5
    end

    test "validate/1 accepts valid configuration" do
      volume = Volume.new("test-volume")
      assert Volume.validate(volume) == :ok
    end

    test "validate/1 rejects empty name" do
      volume = %{Volume.new("test-volume") | name: ""}
      assert {:error, msg} = Volume.validate(volume)
      assert msg =~ "name"
    end

    test "validate/1 rejects invalid replication factor" do
      volume = %{
        Volume.new("test-volume")
        | durability: %{type: :replicate, factor: 0, min_copies: 1}
      }

      assert {:error, msg} = Volume.validate(volume)
      assert msg =~ "durability"
    end

    test "validate/1 rejects min_copies > factor" do
      volume = %{
        Volume.new("test-volume")
        | durability: %{type: :replicate, factor: 3, min_copies: 5}
      }

      assert {:error, msg} = Volume.validate(volume)
      assert msg =~ "durability"
    end

    test "validate/1 rejects invalid write_ack" do
      volume = %{Volume.new("test-volume") | write_ack: :invalid}
      assert {:error, msg} = Volume.validate(volume)
      assert msg =~ "write_ack"
    end

    test "validate/1 rejects invalid tiering" do
      volume = %{
        Volume.new("test-volume")
        | tiering: %{initial_tier: :freezing, promotion_threshold: 10, demotion_delay: 86_400}
      }

      assert {:error, msg} = Volume.validate(volume)
      assert msg =~ "tiering"
    end

    test "validate/1 rejects invalid compression algorithm" do
      volume = %{Volume.new("test-volume") | compression: %{algorithm: :gzip}}
      assert {:error, msg} = Volume.validate(volume)
      assert msg =~ "compression"
    end

    test "validate/1 rejects invalid compression level" do
      volume = %{Volume.new("test-volume") | compression: %{algorithm: :zstd, level: 50}}
      assert {:error, msg} = Volume.validate(volume)
      assert msg =~ "compression"
    end

    test "validate/1 accepts :none compression" do
      volume = %{Volume.new("test-volume") | compression: %{algorithm: :none}}
      assert Volume.validate(volume) == :ok
    end

    test "validate/1 rejects invalid verification mode" do
      volume = %{Volume.new("test-volume") | verification: %{on_read: :sometimes}}
      assert {:error, msg} = Volume.validate(volume)
      assert msg =~ "on_read"
    end

    test "validate/1 requires sampling_rate for :sampling mode" do
      volume = %{Volume.new("test-volume") | verification: %{on_read: :sampling}}
      assert {:error, msg} = Volume.validate(volume)
      assert msg =~ "sampling_rate"
    end

    test "validate/1 accepts valid sampling configuration" do
      volume = %{
        Volume.new("test-volume")
        | verification: %{on_read: :sampling, sampling_rate: 0.1}
      }

      assert Volume.validate(volume) == :ok
    end

    test "validate/1 rejects out-of-range sampling_rate" do
      volume = %{
        Volume.new("test-volume")
        | verification: %{on_read: :sampling, sampling_rate: 1.5}
      }

      assert {:error, msg} = Volume.validate(volume)
      assert msg =~ "sampling_rate"
    end
  end

  describe "VolumeRegistry" do
    test "create/1 creates volume with defaults" do
      assert {:ok, volume} = VolumeRegistry.create("test-volume")
      assert volume.name == "test-volume"
      assert volume.write_ack == :local
    end

    test "create/2 creates volume with custom configuration" do
      assert {:ok, volume} =
               VolumeRegistry.create("test-volume",
                 owner: "alice",
                 write_ack: :quorum
               )

      assert volume.name == "test-volume"
      assert volume.owner == "alice"
      assert volume.write_ack == :quorum
    end

    test "create/2 rejects invalid configuration" do
      assert {:error, msg} =
               VolumeRegistry.create("test-volume",
                 durability: %{type: :replicate, factor: 0, min_copies: 1}
               )

      assert msg =~ "durability"
    end

    test "create/1 rejects duplicate names" do
      assert {:ok, _} = VolumeRegistry.create("test-volume")
      assert {:error, msg} = VolumeRegistry.create("test-volume")
      assert msg =~ "already exists"
    end

    test "get/1 retrieves volume by ID" do
      assert {:ok, volume} = VolumeRegistry.create("test-volume")
      assert {:ok, retrieved} = VolumeRegistry.get(volume.id)
      assert retrieved.id == volume.id
      assert retrieved.name == volume.name
    end

    test "get/1 returns error for unknown ID" do
      assert {:error, :not_found} = VolumeRegistry.get("nonexistent-id")
    end

    test "get_by_name/1 retrieves volume by name" do
      assert {:ok, volume} = VolumeRegistry.create("test-volume")
      assert {:ok, retrieved} = VolumeRegistry.get_by_name("test-volume")
      assert retrieved.id == volume.id
      assert retrieved.name == volume.name
    end

    test "get_by_name/1 returns error for unknown name" do
      assert {:error, :not_found} = VolumeRegistry.get_by_name("nonexistent")
    end

    test "list/0 returns all volumes sorted by name" do
      assert {:ok, _} = VolumeRegistry.create("zebra")
      assert {:ok, _} = VolumeRegistry.create("alpha")
      assert {:ok, _} = VolumeRegistry.create("beta")

      volumes = VolumeRegistry.list()
      names = Enum.map(volumes, & &1.name)

      assert names == ["alpha", "beta", "zebra"]
    end

    test "list/0 returns empty list when no volumes" do
      assert VolumeRegistry.list() == []
    end

    test "update/2 updates volume configuration" do
      assert {:ok, volume} = VolumeRegistry.create("test-volume", owner: "alice")
      assert {:ok, updated} = VolumeRegistry.update(volume.id, owner: "bob", write_ack: :quorum)

      assert updated.owner == "bob"
      assert updated.write_ack == :quorum
      assert updated.name == volume.name
    end

    test "update/2 rejects invalid configuration" do
      assert {:ok, volume} = VolumeRegistry.create("test-volume")

      assert {:error, msg} =
               VolumeRegistry.update(volume.id,
                 durability: %{type: :replicate, factor: 0, min_copies: 1}
               )

      assert msg =~ "durability"
    end

    test "update/2 returns error for unknown volume" do
      assert {:error, :not_found} = VolumeRegistry.update("nonexistent", owner: "bob")
    end

    test "update_stats/2 updates volume statistics" do
      assert {:ok, volume} = VolumeRegistry.create("test-volume")

      assert {:ok, updated} =
               VolumeRegistry.update_stats(volume.id,
                 logical_size: 1024,
                 physical_size: 512,
                 chunk_count: 5
               )

      assert updated.logical_size == 1024
      assert updated.physical_size == 512
      assert updated.chunk_count == 5
    end

    test "update_stats/2 returns error for unknown volume" do
      assert {:error, :not_found} =
               VolumeRegistry.update_stats("nonexistent", logical_size: 1024)
    end

    test "delete/1 deletes empty volume" do
      assert {:ok, volume} = VolumeRegistry.create("test-volume")
      assert :ok = VolumeRegistry.delete(volume.id)
      assert {:error, :not_found} = VolumeRegistry.get(volume.id)
      assert {:error, :not_found} = VolumeRegistry.get_by_name("test-volume")
    end

    test "delete/1 prevents deletion of volume with files" do
      # Create volume
      assert {:ok, volume} = VolumeRegistry.create("test-volume")

      # Add a file to the volume
      file = FileMeta.new(volume.id, "/test.txt")
      assert {:ok, _} = FileIndex.create(file)

      # Attempt to delete volume
      assert {:error, msg} = VolumeRegistry.delete(volume.id)
      assert msg =~ "contains"
      assert msg =~ "file"

      # Volume should still exist
      assert {:ok, _} = VolumeRegistry.get(volume.id)
    end

    test "delete/1 returns error for unknown volume" do
      assert {:error, :not_found} = VolumeRegistry.delete("nonexistent")
    end

    test "concurrent reads work correctly" do
      # Create a volume
      assert {:ok, volume} = VolumeRegistry.create("test-volume")

      # Spawn multiple processes to read concurrently
      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            assert {:ok, retrieved} = VolumeRegistry.get(volume.id)
            assert retrieved.name == "test-volume"
          end)
        end

      # Wait for all tasks to complete
      Enum.each(tasks, &Task.await/1)
    end
  end
end
