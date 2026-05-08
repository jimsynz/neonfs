defmodule NeonFS.Core.VolumeRegistryTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.FileIndex
  alias NeonFS.Core.FileMeta
  alias NeonFS.Core.Volume
  alias NeonFS.Core.VolumeEncryption
  alias NeonFS.Core.VolumeRegistry
  alias NeonFS.Error.Invalid, as: InvalidError

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
      # Test env collapses to replicate:1 via `:neonfs_client,
      # :default_durability` — see `config/config.exs`. Production
      # default is `factor=3, min_copies=2`.
      assert volume.durability == %{type: :replicate, factor: 1, min_copies: 1}
      assert volume.write_ack == :local

      assert volume.tiering == %{
               initial_tier: :hot,
               promotion_threshold: 10,
               demotion_delay: 86_400
             }

      assert volume.caching.transformed_chunks == true
      assert volume.io_weight == 100
      assert volume.compression == %{algorithm: :zstd, level: 3, min_size: 4096}

      assert volume.verification == %{
               on_read: :never,
               sampling_rate: nil,
               scrub_interval: 2_592_000
             }

      assert volume.logical_size == 0
      assert volume.physical_size == 0
      assert volume.chunk_count == 0
      assert volume.system == false
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
      assert DateTime.compare(updated.updated_at, original_updated_at) in [:gt, :eq]

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

    test "validate/1 rejects system: true on non-_system volumes" do
      volume = %{Volume.new("my-volume") | system: true}
      assert {:error, msg} = Volume.validate(volume)
      assert msg =~ "system"
    end

    test "validate/1 accepts system: true on _system volume" do
      volume = %{Volume.new("_system") | system: true}
      assert Volume.validate(volume) == :ok
    end

    test "validate/1 accepts system: false on any volume" do
      volume = Volume.new("test-volume")
      assert volume.system == false
      assert Volume.validate(volume) == :ok
    end

    test "system?/1 returns true for system volumes" do
      volume = %{Volume.new("_system") | system: true}
      assert Volume.system?(volume) == true
    end

    test "system?/1 returns false for normal volumes" do
      volume = Volume.new("test-volume")
      assert Volume.system?(volume) == false
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
      assert {:error, %InvalidError{message: msg}} = VolumeRegistry.create("test-volume")
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
      assert {:error, %InvalidError{message: msg}} = VolumeRegistry.delete(volume.id)
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

  describe "reserved names" do
    test "create/1 rejects names starting with underscore" do
      assert {:error, %InvalidError{}} = VolumeRegistry.create("_system")
    end

    test "create/1 rejects any underscore-prefixed name" do
      assert {:error, %InvalidError{}} = VolumeRegistry.create("_reserved")
      assert {:error, %InvalidError{}} = VolumeRegistry.create("_internal")
    end

    test "create/1 allows names with underscores elsewhere" do
      assert {:ok, volume} = VolumeRegistry.create("my_volume")
      assert volume.name == "my_volume"
    end
  end

  describe "system volume" do
    setup %{tmp_dir: tmp_dir} do
      write_cluster_json(tmp_dir, "test-master-key-for-system-volume")
      :ok
    end

    test "create_system_volume/0 creates system volume with correct properties" do
      assert {:ok, volume} = VolumeRegistry.create_system_volume()

      assert volume.name == "_system"
      assert volume.owner == :system
      assert volume.system == true
      assert volume.durability == %{type: :replicate, factor: 1, min_copies: 1}
      assert volume.write_ack == :quorum
      assert volume.tiering.initial_tier == :hot
      assert volume.compression.algorithm == :zstd
      assert volume.compression.level == 3
      assert volume.encryption.mode == :none
      assert volume.verification.on_read == :always
    end

    test "create_system_volume/0 uses deterministic ID" do
      assert {:ok, vol1} = VolumeRegistry.create_system_volume()

      # Compute the expected deterministic ID
      expected_id =
        :crypto.hash(:sha256, "neonfs:system_volume:test-cluster")
        |> Base.encode16(case: :lower)
        |> binary_part(0, 32)

      assert vol1.id == expected_id
    end

    test "create_system_volume/0 rejects duplicate creation" do
      assert {:ok, _} = VolumeRegistry.create_system_volume()
      assert {:error, :already_exists} = VolumeRegistry.create_system_volume()
    end

    test "get_system_volume/0 returns the system volume" do
      assert {:ok, created} = VolumeRegistry.create_system_volume()
      assert {:ok, retrieved} = VolumeRegistry.get_system_volume()
      assert retrieved.id == created.id
      assert retrieved.name == "_system"
    end

    test "get_system_volume/0 returns not_found when no system volume" do
      assert {:error, :not_found} = VolumeRegistry.get_system_volume()
    end

    test "delete/1 rejects deletion of system volume by ID" do
      assert {:ok, volume} = VolumeRegistry.create_system_volume()
      assert {:error, %InvalidError{message: msg}} = VolumeRegistry.delete(volume.id)
      assert msg =~ "system volume"

      # Volume should still exist
      assert {:ok, _} = VolumeRegistry.get_system_volume()
    end

    test "update/2 rejects changes to protected fields on system volume" do
      assert {:ok, volume} = VolumeRegistry.create_system_volume()

      # Owner is protected
      assert {:error, %InvalidError{message: msg}} =
               VolumeRegistry.update(volume.id, owner: "alice")

      assert msg =~ "protected"

      # Encryption is protected
      assert {:error, %InvalidError{}} =
               VolumeRegistry.update(volume.id,
                 encryption: VolumeEncryption.new(mode: :aes_256_gcm)
               )

      # Durability type is protected
      assert {:error, %InvalidError{}} =
               VolumeRegistry.update(volume.id,
                 durability: %{type: :erasure, data_chunks: 4, parity_chunks: 2}
               )
    end

    test "update/2 allows non-protected field changes on system volume" do
      assert {:ok, volume} = VolumeRegistry.create_system_volume()

      # write_ack is not protected
      assert {:ok, updated} = VolumeRegistry.update(volume.id, write_ack: :all)
      assert updated.write_ack == :all

      # compression is not protected
      assert {:ok, updated} =
               VolumeRegistry.update(volume.id,
                 compression: %{algorithm: :zstd, level: 1, min_size: 0}
               )

      assert updated.compression.level == 1
    end

    test "update/2 allows durability factor changes (same type) on system volume" do
      assert {:ok, volume} = VolumeRegistry.create_system_volume()

      # Changing factor (keeping type :replicate) is allowed
      assert {:ok, updated} =
               VolumeRegistry.update(volume.id,
                 durability: %{type: :replicate, factor: 3, min_copies: 1}
               )

      assert updated.durability.factor == 3
    end

    test "adjust_system_volume_replication/1 updates replication factor" do
      assert {:ok, _} = VolumeRegistry.create_system_volume()

      assert {:ok, updated} = VolumeRegistry.adjust_system_volume_replication(3)
      assert updated.durability.factor == 3
      assert updated.durability.min_copies == 1

      assert {:ok, updated} = VolumeRegistry.adjust_system_volume_replication(5)
      assert updated.durability.factor == 5
    end

    test "adjust_system_volume_replication/1 can decrement" do
      assert {:ok, _} = VolumeRegistry.create_system_volume()
      assert {:ok, _} = VolumeRegistry.adjust_system_volume_replication(3)

      assert {:ok, updated} = VolumeRegistry.adjust_system_volume_replication(2)
      assert updated.durability.factor == 2
    end

    test "list/0 excludes system volume by default" do
      assert {:ok, _} = VolumeRegistry.create_system_volume()
      assert {:ok, _} = VolumeRegistry.create("user-volume")

      volumes = VolumeRegistry.list()
      names = Enum.map(volumes, & &1.name)

      assert names == ["user-volume"]
      refute "_system" in names
    end

    test "list/1 includes system volume with include_system: true" do
      assert {:ok, _} = VolumeRegistry.create_system_volume()
      assert {:ok, _} = VolumeRegistry.create("user-volume")

      volumes = VolumeRegistry.list(include_system: true)
      names = Enum.map(volumes, & &1.name)

      assert "_system" in names
      assert "user-volume" in names
    end

    test "list/0 returns empty when only system volume exists" do
      assert {:ok, _} = VolumeRegistry.create_system_volume()
      assert VolumeRegistry.list() == []
    end
  end
end
