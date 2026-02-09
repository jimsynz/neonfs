defmodule NeonFS.Core.VolumeTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Volume

  describe "new/2" do
    test "creates volume with name and defaults" do
      vol = Volume.new("test-vol")

      assert vol.name == "test-vol"
      assert is_binary(vol.id)
      assert vol.owner == nil
      assert vol.write_ack == :local
      assert vol.tiering == %{initial_tier: :hot, promotion_threshold: 10, demotion_delay: 86_400}
      assert vol.caching.transformed_chunks == true
      assert vol.caching.reconstructed_stripes == true
      assert vol.caching.remote_chunks == true
      assert vol.caching.max_memory == 268_435_456
      assert vol.io_weight == 100
      assert vol.logical_size == 0
      assert vol.physical_size == 0
      assert vol.chunk_count == 0
      assert %DateTime{} = vol.created_at
      assert %DateTime{} = vol.updated_at
    end

    test "accepts custom options" do
      vol =
        Volume.new("custom",
          owner: "alice",
          write_ack: :quorum,
          tiering: %{initial_tier: :warm, promotion_threshold: 5, demotion_delay: 3600}
        )

      assert vol.name == "custom"
      assert vol.owner == "alice"
      assert vol.write_ack == :quorum
      assert vol.tiering.initial_tier == :warm
      assert vol.tiering.promotion_threshold == 5
      assert vol.tiering.demotion_delay == 3600
    end

    test "accepts custom caching and io_weight" do
      vol =
        Volume.new("custom",
          caching: %{
            transformed_chunks: false,
            reconstructed_stripes: true,
            remote_chunks: false,
            max_memory: 512_000_000
          },
          io_weight: 200
        )

      assert vol.caching.transformed_chunks == false
      assert vol.caching.remote_chunks == false
      assert vol.caching.max_memory == 512_000_000
      assert vol.io_weight == 200
    end

    test "generates unique IDs" do
      vol1 = Volume.new("a")
      vol2 = Volume.new("b")

      refute vol1.id == vol2.id
    end
  end

  describe "default_durability/0" do
    test "returns 3-way replication with min 2 copies" do
      assert Volume.default_durability() == %{type: :replicate, factor: 3, min_copies: 2}
    end
  end

  describe "default_tiering/0" do
    test "returns hot tier with default thresholds" do
      assert Volume.default_tiering() == %{
               initial_tier: :hot,
               promotion_threshold: 10,
               demotion_delay: 86_400
             }
    end
  end

  describe "default_caching/0" do
    test "returns all-enabled with 256MB" do
      assert Volume.default_caching() == %{
               transformed_chunks: true,
               reconstructed_stripes: true,
               remote_chunks: true,
               max_memory: 268_435_456
             }
    end
  end

  describe "default_compression/0" do
    test "returns zstd level 3 with 4KB minimum" do
      assert Volume.default_compression() == %{algorithm: :zstd, level: 3, min_size: 4096}
    end
  end

  describe "default_verification/0" do
    test "returns never-verify config" do
      assert Volume.default_verification() == %{on_read: :never, sampling_rate: nil}
    end
  end

  describe "update/2" do
    test "updates allowed fields" do
      vol = Volume.new("test")
      updated = Volume.update(vol, owner: "bob", write_ack: :all)

      assert updated.owner == "bob"
      assert updated.write_ack == :all
      assert updated.name == "test"
    end

    test "updates tiering, caching, and io_weight" do
      vol = Volume.new("test")

      updated =
        Volume.update(vol,
          tiering: %{initial_tier: :cold, promotion_threshold: 20, demotion_delay: 172_800},
          io_weight: 50
        )

      assert updated.tiering.initial_tier == :cold
      assert updated.tiering.promotion_threshold == 20
      assert updated.io_weight == 50
    end

    test "updates the updated_at timestamp" do
      vol = Volume.new("test")
      Process.sleep(1)
      updated = Volume.update(vol, owner: "bob")

      assert DateTime.compare(updated.updated_at, vol.updated_at) in [:gt, :eq]
    end

    test "ignores non-allowed fields" do
      vol = Volume.new("test")
      updated = Volume.update(vol, name: "hacked", id: "fake")

      assert updated.name == "test"
      assert updated.id == vol.id
    end
  end

  describe "update_stats/2" do
    test "updates size and chunk count" do
      vol = Volume.new("test")
      updated = Volume.update_stats(vol, logical_size: 1024, physical_size: 512, chunk_count: 5)

      assert updated.logical_size == 1024
      assert updated.physical_size == 512
      assert updated.chunk_count == 5
    end

    test "preserves unchanged stats" do
      vol = Volume.new("test") |> Volume.update_stats(logical_size: 100)
      updated = Volume.update_stats(vol, chunk_count: 3)

      assert updated.logical_size == 100
      assert updated.chunk_count == 3
    end
  end

  describe "validate/1" do
    test "accepts a valid volume" do
      vol = Volume.new("test-vol")
      assert :ok = Volume.validate(vol)
    end

    test "rejects empty name" do
      vol = %{Volume.new("x") | name: ""}
      assert {:error, "name must be a non-empty string"} = Volume.validate(vol)
    end

    test "rejects nil name" do
      vol = %{Volume.new("x") | name: nil}
      assert {:error, "name must be a non-empty string"} = Volume.validate(vol)
    end

    test "rejects invalid durability" do
      vol = %{Volume.new("x") | durability: %{type: :replicate, factor: 0, min_copies: 0}}

      assert {:error, "invalid durability" <> _} = Volume.validate(vol)
    end

    test "rejects min_copies > factor" do
      vol = %{Volume.new("x") | durability: %{type: :replicate, factor: 2, min_copies: 3}}

      assert {:error, "invalid durability" <> _} = Volume.validate(vol)
    end

    test "rejects invalid write_ack" do
      vol = %{Volume.new("x") | write_ack: :invalid}
      assert {:error, "write_ack must be" <> _} = Volume.validate(vol)
    end

    test "rejects invalid tiering initial_tier" do
      vol = %{
        Volume.new("x")
        | tiering: %{initial_tier: :invalid, promotion_threshold: 10, demotion_delay: 86_400}
      }

      assert {:error, "invalid tiering" <> _} = Volume.validate(vol)
    end

    test "rejects non-positive promotion_threshold" do
      vol = %{
        Volume.new("x")
        | tiering: %{initial_tier: :hot, promotion_threshold: 0, demotion_delay: 86_400}
      }

      assert {:error, "invalid tiering" <> _} = Volume.validate(vol)
    end

    test "rejects non-positive demotion_delay" do
      vol = %{
        Volume.new("x")
        | tiering: %{initial_tier: :hot, promotion_threshold: 10, demotion_delay: -1}
      }

      assert {:error, "invalid tiering" <> _} = Volume.validate(vol)
    end

    test "accepts valid tiering with all tiers" do
      for tier <- [:hot, :warm, :cold] do
        vol = %{
          Volume.new("x")
          | tiering: %{initial_tier: tier, promotion_threshold: 5, demotion_delay: 3600}
        }

        assert :ok = Volume.validate(vol)
      end
    end

    test "rejects invalid caching" do
      vol = %{
        Volume.new("x")
        | caching: %{
            transformed_chunks: "yes",
            reconstructed_stripes: true,
            remote_chunks: true,
            max_memory: 1000
          }
      }

      assert {:error, "invalid caching" <> _} = Volume.validate(vol)
    end

    test "rejects non-positive caching max_memory" do
      vol = %{
        Volume.new("x")
        | caching: %{
            transformed_chunks: true,
            reconstructed_stripes: true,
            remote_chunks: true,
            max_memory: 0
          }
      }

      assert {:error, "invalid caching" <> _} = Volume.validate(vol)
    end

    test "accepts valid caching config" do
      vol = %{
        Volume.new("x")
        | caching: %{
            transformed_chunks: false,
            reconstructed_stripes: false,
            remote_chunks: false,
            max_memory: 1
          }
      }

      assert :ok = Volume.validate(vol)
    end

    test "rejects non-positive io_weight" do
      vol = %{Volume.new("x") | io_weight: 0}
      assert {:error, "io_weight must be a positive integer"} = Volume.validate(vol)
    end

    test "rejects non-integer io_weight" do
      vol = %{Volume.new("x") | io_weight: 1.5}
      assert {:error, "io_weight must be a positive integer"} = Volume.validate(vol)
    end

    test "accepts valid io_weight" do
      vol = %{Volume.new("x") | io_weight: 250}
      assert :ok = Volume.validate(vol)
    end

    test "rejects invalid compression algorithm" do
      vol = %{Volume.new("x") | compression: %{algorithm: :lz4, level: 3}}
      assert {:error, "compression algorithm" <> _} = Volume.validate(vol)
    end

    test "accepts compression :none" do
      vol = %{Volume.new("x") | compression: %{algorithm: :none}}
      assert :ok = Volume.validate(vol)
    end

    test "rejects zstd with level out of range" do
      vol = %{Volume.new("x") | compression: %{algorithm: :zstd, level: 30}}
      assert {:error, "compression algorithm" <> _} = Volume.validate(vol)
    end

    test "rejects invalid verification" do
      vol = %{Volume.new("x") | verification: %{on_read: :invalid}}
      assert {:error, "on_read must be" <> _} = Volume.validate(vol)
    end

    test "rejects sampling without rate" do
      vol = %{Volume.new("x") | verification: %{on_read: :sampling, sampling_rate: nil}}
      assert {:error, "sampling verification" <> _} = Volume.validate(vol)
    end

    test "accepts sampling with valid rate" do
      vol = %{Volume.new("x") | verification: %{on_read: :sampling, sampling_rate: 0.5}}
      assert :ok = Volume.validate(vol)
    end

    test "accepts erasure durability config" do
      vol = %{Volume.new("x") | durability: %{type: :erasure, data_chunks: 10, parity_chunks: 4}}
      assert :ok = Volume.validate(vol)
    end

    test "rejects erasure with zero data_chunks" do
      vol = %{Volume.new("x") | durability: %{type: :erasure, data_chunks: 0, parity_chunks: 4}}
      assert {:error, "invalid durability" <> _} = Volume.validate(vol)
    end

    test "rejects erasure with zero parity_chunks" do
      vol = %{Volume.new("x") | durability: %{type: :erasure, data_chunks: 10, parity_chunks: 0}}
      assert {:error, "invalid durability" <> _} = Volume.validate(vol)
    end

    test "rejects unknown durability type" do
      vol = %{Volume.new("x") | durability: %{type: :unknown}}
      assert {:error, "invalid durability" <> _} = Volume.validate(vol)
    end
  end

  describe "erasure?/1" do
    test "returns true for erasure durability" do
      vol = %{Volume.new("x") | durability: %{type: :erasure, data_chunks: 10, parity_chunks: 4}}
      assert Volume.erasure?(vol)
    end

    test "returns false for replicate durability" do
      vol = Volume.new("x")
      refute Volume.erasure?(vol)
    end
  end
end
