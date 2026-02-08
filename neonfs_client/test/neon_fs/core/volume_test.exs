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
      assert vol.initial_tier == :hot
      assert vol.logical_size == 0
      assert vol.physical_size == 0
      assert vol.chunk_count == 0
      assert %DateTime{} = vol.created_at
      assert %DateTime{} = vol.updated_at
    end

    test "accepts custom options" do
      vol = Volume.new("custom", owner: "alice", write_ack: :quorum, initial_tier: :warm)

      assert vol.name == "custom"
      assert vol.owner == "alice"
      assert vol.write_ack == :quorum
      assert vol.initial_tier == :warm
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

    test "rejects invalid tier" do
      vol = %{Volume.new("x") | initial_tier: :invalid}
      assert {:error, "initial_tier must be" <> _} = Volume.validate(vol)
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
  end
end
