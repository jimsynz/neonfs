defmodule NeonFS.Core.MetadataStateMachineTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.MetadataStateMachine

  describe "version/0" do
    test "returns current version" do
      assert MetadataStateMachine.version() == 3
    end
  end

  describe "which_module/1" do
    test "returns the same module for all versions" do
      assert MetadataStateMachine.which_module(1) == MetadataStateMachine
      assert MetadataStateMachine.which_module(2) == MetadataStateMachine
      assert MetadataStateMachine.which_module(3) == MetadataStateMachine
    end
  end

  describe "init/1" do
    test "initializes empty state" do
      state = MetadataStateMachine.init(%{})

      assert state.data == %{}
      assert state.chunks == %{}
      assert state.files == %{}
      assert state.services == %{}
      assert state.volumes == %{}
      assert state.version == 0
    end
  end

  describe "machine version migration 2 -> 3" do
    test "migrates volumes with initial_tier to tiering map" do
      # Simulate a version 2 state with old-style volumes
      state = %{
        data: %{},
        chunks: %{},
        files: %{},
        services: %{},
        volumes: %{
          "vol-1" => %{
            id: "vol-1",
            name: "old-volume",
            initial_tier: :warm,
            durability: %{type: :replicate, factor: 3, min_copies: 2},
            write_ack: :local,
            compression: %{algorithm: :zstd, level: 3, min_size: 4096},
            verification: %{on_read: :never, sampling_rate: nil},
            logical_size: 1024,
            physical_size: 512,
            chunk_count: 5,
            created_at: DateTime.utc_now(),
            updated_at: DateTime.utc_now()
          }
        },
        version: 42
      }

      {new_state, :ok, []} = MetadataStateMachine.apply(%{}, {:machine_version, 2, 3}, state)

      vol = new_state.volumes["vol-1"]

      # Should have tiering map with migrated initial_tier
      assert vol.tiering == %{
               initial_tier: :warm,
               promotion_threshold: 10,
               demotion_delay: 86_400
             }

      # Should no longer have top-level initial_tier
      refute Map.has_key?(vol, :initial_tier)

      # Should have caching defaults
      assert vol.caching == %{
               transformed_chunks: true,
               reconstructed_stripes: true,
               remote_chunks: true,
               max_memory: 268_435_456
             }

      # Should have io_weight default
      assert vol.io_weight == 100
    end

    test "preserves already-migrated volumes" do
      state = %{
        data: %{},
        chunks: %{},
        files: %{},
        services: %{},
        volumes: %{
          "vol-1" => %{
            id: "vol-1",
            name: "new-volume",
            tiering: %{initial_tier: :cold, promotion_threshold: 5, demotion_delay: 3600},
            caching: %{
              transformed_chunks: false,
              reconstructed_stripes: true,
              remote_chunks: true,
              max_memory: 100_000
            },
            io_weight: 200,
            durability: %{type: :replicate, factor: 3, min_copies: 2},
            write_ack: :local,
            compression: %{algorithm: :zstd, level: 3, min_size: 4096},
            verification: %{on_read: :never, sampling_rate: nil},
            logical_size: 0,
            physical_size: 0,
            chunk_count: 0,
            created_at: DateTime.utc_now(),
            updated_at: DateTime.utc_now()
          }
        },
        version: 42
      }

      {new_state, :ok, []} = MetadataStateMachine.apply(%{}, {:machine_version, 2, 3}, state)

      vol = new_state.volumes["vol-1"]

      # Should preserve existing tiering config
      assert vol.tiering.initial_tier == :cold
      assert vol.tiering.promotion_threshold == 5
      assert vol.tiering.demotion_delay == 3600

      # Should preserve existing caching config
      assert vol.caching.transformed_chunks == false
      assert vol.caching.max_memory == 100_000

      # Should preserve existing io_weight
      assert vol.io_weight == 200
    end

    test "handles empty volumes map" do
      state = %{
        data: %{},
        chunks: %{},
        files: %{},
        services: %{},
        volumes: %{},
        version: 10
      }

      {new_state, :ok, []} = MetadataStateMachine.apply(%{}, {:machine_version, 2, 3}, state)

      assert new_state.volumes == %{}
    end

    test "defaults initial_tier to :hot when missing" do
      state = %{
        data: %{},
        chunks: %{},
        files: %{},
        services: %{},
        volumes: %{
          "vol-1" => %{
            id: "vol-1",
            name: "no-tier-volume",
            durability: %{type: :replicate, factor: 3, min_copies: 2},
            write_ack: :local,
            compression: %{algorithm: :zstd, level: 3, min_size: 4096},
            verification: %{on_read: :never, sampling_rate: nil},
            logical_size: 0,
            physical_size: 0,
            chunk_count: 0,
            created_at: DateTime.utc_now(),
            updated_at: DateTime.utc_now()
          }
        },
        version: 5
      }

      {new_state, :ok, []} = MetadataStateMachine.apply(%{}, {:machine_version, 2, 3}, state)

      vol = new_state.volumes["vol-1"]
      assert vol.tiering.initial_tier == :hot
    end
  end
end
