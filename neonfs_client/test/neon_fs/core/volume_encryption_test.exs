defmodule NeonFS.Core.VolumeEncryptionTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.VolumeEncryption

  describe "new/1" do
    test "defaults to mode :none" do
      enc = VolumeEncryption.new()

      assert enc.mode == :none
      assert enc.current_key_version == 0
      assert enc.keys == %{}
      assert enc.rotation == nil
    end

    test "creates server_side encryption config" do
      now = DateTime.utc_now()

      enc =
        VolumeEncryption.new(
          mode: :server_side,
          current_key_version: 1,
          keys: %{
            1 => %{wrapped_key: <<1, 2, 3>>, created_at: now, deprecated_at: nil}
          }
        )

      assert enc.mode == :server_side
      assert enc.current_key_version == 1
      assert map_size(enc.keys) == 1
      assert enc.keys[1].wrapped_key == <<1, 2, 3>>
      assert enc.keys[1].deprecated_at == nil
    end

    test "creates config with rotation state" do
      now = DateTime.utc_now()

      rotation = %{
        from_version: 1,
        to_version: 2,
        started_at: now,
        progress: %{total_chunks: 1000, migrated: 500}
      }

      enc =
        VolumeEncryption.new(
          mode: :server_side,
          current_key_version: 2,
          keys: %{
            1 => %{wrapped_key: <<1>>, created_at: now, deprecated_at: now},
            2 => %{wrapped_key: <<2>>, created_at: now, deprecated_at: nil}
          },
          rotation: rotation
        )

      assert enc.rotation.from_version == 1
      assert enc.rotation.to_version == 2
      assert enc.rotation.progress.migrated == 500
    end
  end

  describe "active?/1" do
    test "returns false for mode :none" do
      refute VolumeEncryption.active?(VolumeEncryption.new())
    end

    test "returns true for mode :server_side" do
      enc = VolumeEncryption.new(mode: :server_side, current_key_version: 1)
      assert VolumeEncryption.active?(enc)
    end
  end

  describe "rotating?/1" do
    test "returns false when rotation is nil" do
      refute VolumeEncryption.rotating?(VolumeEncryption.new())
    end

    test "returns true when rotation state is present" do
      enc =
        VolumeEncryption.new(
          mode: :server_side,
          current_key_version: 2,
          rotation: %{
            from_version: 1,
            to_version: 2,
            started_at: DateTime.utc_now(),
            progress: %{total_chunks: 100, migrated: 0}
          }
        )

      assert VolumeEncryption.rotating?(enc)
    end
  end

  describe "validate/1" do
    test "accepts mode :none with empty keys" do
      assert :ok = VolumeEncryption.validate(VolumeEncryption.new())
    end

    test "rejects mode :none with keys present" do
      enc =
        VolumeEncryption.new(
          mode: :none,
          keys: %{1 => %{wrapped_key: <<1>>, created_at: DateTime.utc_now(), deprecated_at: nil}}
        )

      assert {:error, "encryption mode :none must not have keys"} =
               VolumeEncryption.validate(enc)
    end

    test "accepts valid server_side config" do
      enc = VolumeEncryption.new(mode: :server_side, current_key_version: 1)
      assert :ok = VolumeEncryption.validate(enc)
    end

    test "rejects server_side with zero key version" do
      enc = VolumeEncryption.new(mode: :server_side, current_key_version: 0)

      assert {:error, "server_side encryption requires" <> _} =
               VolumeEncryption.validate(enc)
    end

    test "rejects server_side with negative key version" do
      enc = VolumeEncryption.new(mode: :server_side, current_key_version: -1)

      assert {:error, "server_side encryption requires" <> _} =
               VolumeEncryption.validate(enc)
    end

    test "rejects unknown mode" do
      enc = %VolumeEncryption{mode: :envelope}

      assert {:error, "encryption mode must be :none or :server_side" <> _} =
               VolumeEncryption.validate(enc)
    end
  end
end
