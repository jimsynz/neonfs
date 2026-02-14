defmodule NeonFS.Core.KeyManagerTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{KeyManager, MetadataStateMachine, RaServer, RaSupervisor, VolumeRegistry}

  @moduletag :tmp_dir

  @test_master_key :crypto.strong_rand_bytes(32) |> Base.encode64()

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    write_cluster_json(tmp_dir, @test_master_key)

    start_ra()
    :ok = RaServer.init_cluster()

    start_volume_registry()

    on_exit(fn -> cleanup_test_dirs() end)

    :ok
  end

  describe "generate_volume_key/1" do
    test "generates a key and stores it in Ra" do
      volume_id = create_test_volume("gen-test")

      assert {:ok, 1} = KeyManager.generate_volume_key(volume_id)

      # Verify key is stored in Ra
      {:ok, keys} = query_encryption_keys(volume_id)
      assert Map.has_key?(keys, 1)
      assert is_binary(keys[1].wrapped_key)
      assert %DateTime{} = keys[1].created_at
      assert keys[1].deprecated_at == nil
    end

    test "generates sequential versions" do
      volume_id = create_test_volume("seq-test")

      assert {:ok, 1} = KeyManager.generate_volume_key(volume_id)
      assert {:ok, 2} = KeyManager.generate_volume_key(volume_id)
      assert {:ok, 3} = KeyManager.generate_volume_key(volume_id)

      {:ok, keys} = query_encryption_keys(volume_id)
      assert map_size(keys) == 3
    end
  end

  describe "get_volume_key/2" do
    test "round-trips a generated key" do
      volume_id = create_test_volume("roundtrip-test")
      {:ok, version} = KeyManager.generate_volume_key(volume_id)

      assert {:ok, key} = KeyManager.get_volume_key(volume_id, version)
      assert byte_size(key) == 32
    end

    test "returns different keys for different volumes" do
      vol1 = create_test_volume("vol1")
      vol2 = create_test_volume("vol2")

      {:ok, v1} = KeyManager.generate_volume_key(vol1)
      {:ok, v2} = KeyManager.generate_volume_key(vol2)

      {:ok, key1} = KeyManager.get_volume_key(vol1, v1)
      {:ok, key2} = KeyManager.get_volume_key(vol2, v2)

      assert key1 != key2
    end

    test "returns different keys for different versions" do
      volume_id = create_test_volume("multi-ver")

      {:ok, v1} = KeyManager.generate_volume_key(volume_id)
      {:ok, v2} = KeyManager.generate_volume_key(volume_id)

      {:ok, key1} = KeyManager.get_volume_key(volume_id, v1)
      {:ok, key2} = KeyManager.get_volume_key(volume_id, v2)

      assert key1 != key2
    end

    test "returns error for unknown key version" do
      volume_id = create_test_volume("unknown-ver")

      assert {:error, :unknown_key_version} = KeyManager.get_volume_key(volume_id, 999)
    end

    test "returns error for unknown volume" do
      assert {:error, :unknown_key_version} =
               KeyManager.get_volume_key("nonexistent-volume", 1)
    end
  end

  describe "get_current_key/1" do
    test "returns the current key and version" do
      volume_id = create_test_volume("current-key")
      {:ok, version} = KeyManager.setup_volume_encryption(volume_id)

      assert {:ok, {key, ^version}} = KeyManager.get_current_key(volume_id)
      assert byte_size(key) == 32
    end

    test "returns error when volume has no encryption config" do
      volume_id = create_test_volume("no-enc")

      assert {:error, :not_encrypted} = KeyManager.get_current_key(volume_id)
    end

    test "returns error for nonexistent volume" do
      assert {:error, :volume_not_found} = KeyManager.get_current_key("nonexistent")
    end
  end

  describe "setup_volume_encryption/1" do
    test "generates first key and sets current version to 1" do
      volume_id = create_test_volume("setup-test")

      assert {:ok, 1} = KeyManager.setup_volume_encryption(volume_id)

      # Verify key version was set on the volume
      {:ok, {key, 1}} = KeyManager.get_current_key(volume_id)
      assert byte_size(key) == 32
    end

    test "stored wrapped key can be unwrapped back" do
      volume_id = create_test_volume("setup-roundtrip")
      {:ok, 1} = KeyManager.setup_volume_encryption(volume_id)

      {:ok, key1} = KeyManager.get_volume_key(volume_id, 1)
      {:ok, {key2, 1}} = KeyManager.get_current_key(volume_id)

      assert key1 == key2
    end
  end

  describe "rotate_volume_key/1" do
    test "generates a new key version and updates current version" do
      volume_id = create_test_volume("rotate-test")
      {:ok, 1} = KeyManager.setup_volume_encryption(volume_id)

      assert {:ok, 2} = KeyManager.rotate_volume_key(volume_id)

      {:ok, {_key, 2}} = KeyManager.get_current_key(volume_id)
    end

    test "previous key version remains accessible" do
      volume_id = create_test_volume("rotate-access")
      {:ok, 1} = KeyManager.setup_volume_encryption(volume_id)

      {:ok, key_v1} = KeyManager.get_volume_key(volume_id, 1)
      {:ok, 2} = KeyManager.rotate_volume_key(volume_id)
      {:ok, key_v1_after} = KeyManager.get_volume_key(volume_id, 1)

      assert key_v1 == key_v1_after
    end

    test "marks the old key as deprecated" do
      volume_id = create_test_volume("rotate-deprecate")
      {:ok, 1} = KeyManager.setup_volume_encryption(volume_id)

      {:ok, 2} = KeyManager.rotate_volume_key(volume_id)

      {:ok, keys} = query_encryption_keys(volume_id)
      assert %DateTime{} = keys[1].deprecated_at
      assert keys[2].deprecated_at == nil
    end

    test "multiple rotations work correctly" do
      volume_id = create_test_volume("rotate-multi")
      {:ok, 1} = KeyManager.setup_volume_encryption(volume_id)

      {:ok, 2} = KeyManager.rotate_volume_key(volume_id)
      {:ok, 3} = KeyManager.rotate_volume_key(volume_id)

      {:ok, {_key, 3}} = KeyManager.get_current_key(volume_id)

      # All keys still accessible
      assert {:ok, _} = KeyManager.get_volume_key(volume_id, 1)
      assert {:ok, _} = KeyManager.get_volume_key(volume_id, 2)
      assert {:ok, _} = KeyManager.get_volume_key(volume_id, 3)
    end

    test "returns error when volume is not encrypted" do
      volume_id = create_test_volume("rotate-no-enc")

      assert {:error, :not_encrypted} = KeyManager.rotate_volume_key(volume_id)
    end
  end

  describe "master key handling" do
    test "returns error when cluster.json is missing", %{tmp_dir: tmp_dir} do
      File.rm!(Path.join(tmp_dir, "cluster.json"))

      volume_id = create_test_volume("no-cluster")

      assert {:error, :master_key_not_found} = KeyManager.generate_volume_key(volume_id)
    end

    test "unwrap fails with wrong master key", %{tmp_dir: tmp_dir} do
      volume_id = create_test_volume("wrong-key")
      {:ok, 1} = KeyManager.setup_volume_encryption(volume_id)

      # Overwrite cluster.json with a different master key
      wrong_key = :crypto.strong_rand_bytes(32) |> Base.encode64()
      write_cluster_json(tmp_dir, wrong_key)

      assert {:error, :unwrap_failed} = KeyManager.get_volume_key(volume_id, 1)
    end
  end

  # Helpers

  defp create_test_volume(name) do
    {:ok, volume} = VolumeRegistry.create(name)
    volume.id
  end

  defp query_encryption_keys(volume_id) do
    RaSupervisor.query(fn state ->
      MetadataStateMachine.get_encryption_keys(state, volume_id)
    end)
  end
end
