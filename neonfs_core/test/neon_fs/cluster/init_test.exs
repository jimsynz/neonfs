defmodule NeonFS.Cluster.InitTest do
  use ExUnit.Case, async: false

  @moduletag :ra

  alias NeonFS.Cluster.{Init, State}

  @tmp_dir "/tmp/neonfs_cluster_init_test"

  setup do
    # Clean up any existing test directory
    File.rm_rf!(@tmp_dir)
    File.mkdir_p!(@tmp_dir)

    # Configure test meta_dir
    Application.put_env(:neonfs_core, :meta_dir, @tmp_dir)

    on_exit(fn ->
      File.rm_rf!(@tmp_dir)
      Application.delete_env(:neonfs_core, :meta_dir)
    end)

    :ok
  end

  describe "init_cluster/1" do
    @tag :ra
    test "successfully initializes cluster on fresh system" do
      assert {:ok, cluster_id} = Init.init_cluster("test-cluster")

      # Should be a valid cluster ID with prefix
      assert String.starts_with?(cluster_id, "clust_")
      assert String.length(cluster_id) == 14
    end

    @tag :ra
    test "creates cluster state file" do
      assert {:ok, _cluster_id} = Init.init_cluster("test-cluster")

      assert State.exists?()
    end

    @tag :ra
    test "saves correct cluster information" do
      assert {:ok, cluster_id} = Init.init_cluster("my-cluster")

      assert {:ok, state} = State.load()
      assert state.cluster_id == cluster_id
      assert state.cluster_name == "my-cluster"
      assert state.this_node.name == Node.self()
      assert String.starts_with?(state.this_node.id, "node_")
      assert state.ra_cluster_members == [Node.self()]
    end

    @tag :ra
    test "generates master key" do
      assert {:ok, _cluster_id} = Init.init_cluster("test-cluster")

      assert {:ok, state} = State.load()
      assert is_binary(state.master_key)
      assert byte_size(Base.decode64!(state.master_key)) == 32
    end

    @tag :ra
    test "returns error if already initialized" do
      # Initialize once
      assert {:ok, _cluster_id} = Init.init_cluster("first-cluster")

      # Try to initialize again
      assert {:error, :already_initialised} = Init.init_cluster("second-cluster")
    end

    test "returns error if node is not named" do
      # This test only works if running without named node
      if Node.self() == :nonode@nohost do
        assert {:error, :node_not_named} = Init.init_cluster("test-cluster")
      else
        # Skip if running with named node
        :ok
      end
    end
  end

  describe "generate_cluster_id/0" do
    test "generates unique IDs" do
      id1 = Init.generate_cluster_id()
      id2 = Init.generate_cluster_id()

      assert id1 != id2
    end

    test "generates IDs with correct format" do
      id = Init.generate_cluster_id()

      assert String.starts_with?(id, "clust_")
      assert String.length(id) == 14
      # Should only contain lowercase letters and numbers
      suffix = String.slice(id, 6..-1//1)
      assert Regex.match?(~r/^[a-z0-9]+$/, suffix)
    end

    test "generates IDs with base32 encoding" do
      # Generate multiple IDs and check they're valid base32
      for _ <- 1..10 do
        id = Init.generate_cluster_id()
        suffix = String.slice(id, 6..-1//1)
        # Base32 uses A-Z and 2-7, but we use lowercase
        assert Regex.match?(~r/^[a-z2-7]+$/, suffix)
      end
    end
  end

  describe "generate_node_id/0" do
    test "generates unique IDs" do
      id1 = Init.generate_node_id()
      id2 = Init.generate_node_id()

      assert id1 != id2
    end

    test "generates IDs with correct format" do
      id = Init.generate_node_id()

      assert String.starts_with?(id, "node_")
      assert String.length(id) == 13
      # Should only contain lowercase letters and numbers
      suffix = String.slice(id, 5..-1//1)
      assert Regex.match?(~r/^[a-z0-9]+$/, suffix)
    end
  end

  describe "generate_master_key/0" do
    test "generates 256-bit key" do
      key = Init.generate_master_key()

      # Should be base64 encoded 32 bytes
      assert is_binary(key)
      decoded = Base.decode64!(key)
      assert byte_size(decoded) == 32
    end

    test "generates unique keys" do
      key1 = Init.generate_master_key()
      key2 = Init.generate_master_key()

      assert key1 != key2
    end

    test "generates cryptographically strong keys" do
      # Generate multiple keys and ensure they have high entropy
      keys = for _ <- 1..10, do: Init.generate_master_key()

      # All should be different
      assert length(Enum.uniq(keys)) == 10

      # Each should be valid base64
      for key <- keys do
        assert {:ok, _decoded} = Base.decode64(key)
      end
    end
  end
end
