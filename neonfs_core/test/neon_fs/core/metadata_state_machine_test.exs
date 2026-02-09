defmodule NeonFS.Core.MetadataStateMachineTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.MetadataStateMachine

  defp base_state do
    %{
      data: %{},
      chunks: %{},
      files: %{},
      services: %{},
      volumes: %{},
      stripes: %{},
      version: 0
    }
  end

  describe "version/0" do
    test "returns current version" do
      assert MetadataStateMachine.version() == 4
    end
  end

  describe "which_module/1" do
    test "returns the same module for all versions" do
      assert MetadataStateMachine.which_module(1) == MetadataStateMachine
      assert MetadataStateMachine.which_module(2) == MetadataStateMachine
      assert MetadataStateMachine.which_module(3) == MetadataStateMachine
      assert MetadataStateMachine.which_module(4) == MetadataStateMachine
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
      assert state.stripes == %{}
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

  describe "machine version migration 3 -> 4" do
    test "adds empty stripes map to state" do
      state = %{
        data: %{},
        chunks: %{},
        files: %{},
        services: %{},
        volumes: %{},
        version: 50
      }

      {new_state, :ok, []} = MetadataStateMachine.apply(%{}, {:machine_version, 3, 4}, state)

      assert new_state.stripes == %{}
    end

    test "preserves existing stripes map if already present" do
      existing_stripe = %{id: "stripe-1", volume_id: "vol-1", chunks: []}

      state = %{
        data: %{},
        chunks: %{},
        files: %{},
        services: %{},
        volumes: %{},
        stripes: %{"stripe-1" => existing_stripe},
        version: 50
      }

      {new_state, :ok, []} = MetadataStateMachine.apply(%{}, {:machine_version, 3, 4}, state)

      assert new_state.stripes == %{"stripe-1" => existing_stripe}
    end

    test "preserves all other state fields" do
      state = %{
        data: %{key: "value"},
        chunks: %{"hash1" => %{hash: "hash1"}},
        files: %{"file1" => %{id: "file1"}},
        services: %{node1: %{type: :core}},
        volumes: %{"vol1" => %{id: "vol1"}},
        version: 100
      }

      {new_state, :ok, []} = MetadataStateMachine.apply(%{}, {:machine_version, 3, 4}, state)

      assert new_state.data == state.data
      assert new_state.chunks == state.chunks
      assert new_state.files == state.files
      assert new_state.services == state.services
      assert new_state.volumes == state.volumes
      assert new_state.version == state.version
    end
  end

  describe "{:put_stripe, stripe_data}" do
    test "stores stripe in state and returns {:ok, stripe_id}" do
      stripe_data = %{
        id: "stripe-abc",
        volume_id: "vol-1",
        config: %{data_chunks: 10, parity_chunks: 4, chunk_size: 262_144},
        chunks: [],
        partial: false,
        data_bytes: 0,
        padded_bytes: 0
      }

      {new_state, {:ok, "stripe-abc"}, []} =
        MetadataStateMachine.apply(%{}, {:put_stripe, stripe_data}, base_state())

      assert Map.has_key?(new_state.stripes, "stripe-abc")
      assert new_state.stripes["stripe-abc"] == stripe_data
      assert new_state.version == 1
    end

    test "overwrites existing stripe with same id" do
      stripe1 = %{id: "s1", volume_id: "vol-1", chunks: ["a", "b"]}
      stripe2 = %{id: "s1", volume_id: "vol-1", chunks: ["c", "d"]}

      state = %{base_state() | stripes: %{"s1" => stripe1}}

      {new_state, {:ok, "s1"}, []} =
        MetadataStateMachine.apply(%{}, {:put_stripe, stripe2}, state)

      assert new_state.stripes["s1"].chunks == ["c", "d"]
    end
  end

  describe "{:update_stripe, stripe_id, updates}" do
    test "merges updates into existing stripe" do
      stripe = %{
        id: "s1",
        volume_id: "vol-1",
        chunks: [],
        partial: false,
        data_bytes: 0,
        padded_bytes: 0
      }

      state = %{base_state() | stripes: %{"s1" => stripe}}

      {new_state, :ok, []} =
        MetadataStateMachine.apply(
          %{},
          {:update_stripe, "s1", %{chunks: ["h1", "h2"], partial: true}},
          state
        )

      updated = new_state.stripes["s1"]
      assert updated.chunks == ["h1", "h2"]
      assert updated.partial == true
      assert updated.volume_id == "vol-1"
      assert new_state.version == 1
    end

    test "returns error for non-existent stripe" do
      {state, {:error, :not_found}, []} =
        MetadataStateMachine.apply(
          %{},
          {:update_stripe, "nonexistent", %{partial: true}},
          base_state()
        )

      assert state.version == 0
    end
  end

  describe "{:delete_stripe, stripe_id}" do
    test "removes stripe from state" do
      stripe = %{id: "s1", volume_id: "vol-1", chunks: []}
      state = %{base_state() | stripes: %{"s1" => stripe}}

      {new_state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:delete_stripe, "s1"}, state)

      refute Map.has_key?(new_state.stripes, "s1")
      assert new_state.version == 1
    end

    test "is idempotent for non-existent stripe" do
      {new_state, :ok, []} =
        MetadataStateMachine.apply(%{}, {:delete_stripe, "nonexistent"}, base_state())

      assert new_state.stripes == %{}
      assert new_state.version == 1
    end
  end
end
