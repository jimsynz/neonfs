defmodule NeonFS.Cluster.StateTest do
  use ExUnit.Case, async: false

  alias NeonFS.Cluster.State

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    Application.put_env(:neonfs_core, :meta_dir, tmp_dir)

    on_exit(fn ->
      File.rm_rf!(tmp_dir)
      Application.delete_env(:neonfs_core, :meta_dir)
    end)

    :ok
  end

  describe "new/4" do
    test "creates a new cluster state" do
      node_info = %{
        id: "node_test123",
        name: :neonfs@localhost,
        joined_at: ~U[2024-01-15 10:30:00Z]
      }

      state = State.new("clust_abc123", "test-cluster", "master_key_base64", node_info)

      assert state.cluster_id == "clust_abc123"
      assert state.cluster_name == "test-cluster"
      assert state.master_key == "master_key_base64"
      assert state.this_node.id == "node_test123"
      assert state.this_node.name == :neonfs@localhost
      assert state.known_peers == []
      assert state.ra_cluster_members == [:neonfs@localhost]
    end

    test "sets created_at to current time" do
      node_info = %{
        id: "node_test123",
        name: :neonfs@localhost,
        joined_at: ~U[2024-01-15 10:30:00Z]
      }

      before = DateTime.utc_now()
      state = State.new("clust_abc123", "test-cluster", "master_key_base64", node_info)
      after_time = DateTime.utc_now()

      assert DateTime.compare(state.created_at, before) in [:eq, :gt]
      assert DateTime.compare(state.created_at, after_time) in [:eq, :lt]
    end
  end

  describe "state_file_path/0" do
    test "returns configured path" do
      path = State.state_file_path()
      assert path == Path.join(Application.get_env(:neonfs_core, :meta_dir), "cluster.json")
    end
  end

  describe "exists?/0" do
    test "returns false when file doesn't exist" do
      refute State.exists?()
    end

    test "returns true when file exists" do
      # Create the file
      state = create_test_state()
      :ok = State.save(state)

      assert State.exists?()
    end
  end

  describe "save/1" do
    test "creates cluster.json file" do
      state = create_test_state()

      assert :ok = State.save(state)
      assert File.exists?(State.state_file_path())
    end

    test "writes valid JSON" do
      state = create_test_state()
      :ok = State.save(state)

      {:ok, content} = File.read(State.state_file_path())
      assert {:ok, _json} = Jason.decode(content)
    end

    test "includes all required fields" do
      state = create_test_state()
      :ok = State.save(state)

      {:ok, content} = File.read(State.state_file_path())
      {:ok, json} = Jason.decode(content)

      assert json["cluster_id"] == "clust_abc123"
      assert json["cluster_name"] == "test-cluster"
      assert json["master_key"] == "master_key_base64"
      assert json["this_node"]["id"] == "node_test123"
      assert json["this_node"]["name"] == "neonfs@localhost"
      assert json["known_peers"] == []
      assert json["ra_cluster_members"] == ["neonfs@localhost"]
    end

    test "creates directory if it doesn't exist" do
      # Use a nested path
      nested_dir = Path.join(Application.get_env(:neonfs_core, :meta_dir), "nested/path")
      Application.put_env(:neonfs_core, :meta_dir, nested_dir)

      state = create_test_state()
      assert :ok = State.save(state)
      assert File.exists?(Path.join(nested_dir, "cluster.json"))
    end

    test "uses atomic write (temp file + rename)" do
      state = create_test_state()
      :ok = State.save(state)

      # Temp file should be cleaned up
      temp_path = "#{State.state_file_path()}.tmp"
      refute File.exists?(temp_path)
    end
  end

  describe "load/0" do
    test "returns error when file doesn't exist" do
      assert {:error, :not_found} = State.load()
    end

    test "loads saved state correctly" do
      original = create_test_state()
      :ok = State.save(original)

      assert {:ok, loaded} = State.load()

      assert loaded.cluster_id == original.cluster_id
      assert loaded.cluster_name == original.cluster_name
      assert loaded.master_key == original.master_key
      assert loaded.this_node.id == original.this_node.id
      assert loaded.this_node.name == original.this_node.name
      assert loaded.known_peers == original.known_peers
      assert loaded.ra_cluster_members == original.ra_cluster_members
    end

    test "preserves datetime values" do
      original = create_test_state()
      :ok = State.save(original)

      assert {:ok, loaded} = State.load()

      # Datetimes should be equal (within microsecond precision)
      assert DateTime.diff(loaded.created_at, original.created_at, :microsecond) == 0

      assert DateTime.diff(loaded.this_node.joined_at, original.this_node.joined_at, :microsecond) ==
               0
    end

    test "handles state with peers" do
      node_info = %{
        id: "node_test123",
        name: :neonfs@localhost,
        joined_at: ~U[2024-01-15 10:30:00Z]
      }

      peer1 = %{
        id: "node_peer1",
        name: :"neonfs@node1.example.com",
        last_seen: ~U[2024-01-20 14:22:00Z]
      }

      peer2 = %{
        id: "node_peer2",
        name: :"neonfs@node2.example.com",
        last_seen: ~U[2024-01-20 14:23:00Z]
      }

      state = %State{
        cluster_id: "clust_abc123",
        cluster_name: "test-cluster",
        created_at: ~U[2024-01-15 10:30:00Z],
        master_key: "master_key_base64",
        this_node: node_info,
        known_peers: [peer1, peer2],
        ra_cluster_members: [
          :neonfs@localhost,
          :"neonfs@node1.example.com",
          :"neonfs@node2.example.com"
        ]
      }

      :ok = State.save(state)
      assert {:ok, loaded} = State.load()

      assert length(loaded.known_peers) == 2
      assert Enum.at(loaded.known_peers, 0).id == "node_peer1"
      assert Enum.at(loaded.known_peers, 0).name == :"neonfs@node1.example.com"
      assert length(loaded.ra_cluster_members) == 3
    end

    test "returns error for invalid JSON" do
      # Write invalid JSON
      File.write!(State.state_file_path(), "not valid json")

      assert {:error, :invalid_json} = State.load()
    end

    test "returns error for JSON with missing fields" do
      # Write JSON with missing required fields
      File.write!(State.state_file_path(), ~s({"cluster_id": "test"}))

      assert {:error, :invalid_json} = State.load()
    end
  end

  describe "drives serialisation" do
    test "saves and loads state with drives" do
      state = %State{
        create_test_state()
        | drives: [
            %{"id" => "nvme0", "path" => "/data/nvme0", "tier" => "hot", "capacity" => "1T"},
            %{"id" => "sata0", "path" => "/data/sata0", "tier" => "cold", "capacity" => "4T"}
          ]
      }

      :ok = State.save(state)
      assert {:ok, loaded} = State.load()

      assert length(loaded.drives) == 2
      [nvme, sata] = loaded.drives
      assert nvme["id"] == "nvme0"
      assert nvme["path"] == "/data/nvme0"
      assert nvme["tier"] == "hot"
      assert nvme["capacity"] == "1T"
      assert sata["id"] == "sata0"
      assert sata["tier"] == "cold"
    end

    test "loads state without drives key (backward compat)" do
      # Write JSON manually without a "drives" key
      json =
        :json.format(%{
          "cluster_id" => "clust_abc123",
          "cluster_name" => "test-cluster",
          "created_at" => "2024-01-15T10:30:00Z",
          "master_key" => "master_key_base64",
          "this_node" => %{
            "id" => "node_test123",
            "name" => "neonfs@localhost",
            "joined_at" => "2024-01-15T10:30:00Z"
          },
          "known_peers" => [],
          "ra_cluster_members" => ["neonfs@localhost"],
          "node_type" => "core"
        })

      File.write!(State.state_file_path(), json)
      assert {:ok, loaded} = State.load()
      assert loaded.drives == []
    end

    test "saves drives with atom keys" do
      state = %State{
        create_test_state()
        | drives: [
            %{id: "nvme0", path: "/data/nvme0", tier: :hot, capacity: 1_099_511_627_776}
          ]
      }

      :ok = State.save(state)
      assert {:ok, loaded} = State.load()

      [drive] = loaded.drives
      assert drive["id"] == "nvme0"
      assert drive["tier"] == "hot"
      assert drive["capacity"] == "1099511627776"
    end
  end

  describe "update_drives/1" do
    test "updates drives in persisted state" do
      state = create_test_state()
      :ok = State.save(state)

      drives = [
        %{"id" => "new_drive", "path" => "/data/new", "tier" => "warm", "capacity" => "500G"}
      ]

      assert :ok = State.update_drives(drives)
      assert {:ok, loaded} = State.load()
      assert length(loaded.drives) == 1
      assert hd(loaded.drives)["id"] == "new_drive"
    end

    test "returns error when cluster.json doesn't exist" do
      assert {:error, :not_found} = State.update_drives([])
    end
  end

  describe "peer settings" do
    test "new fields have defaults in struct" do
      state = create_test_state()

      assert state.peer_sync_interval == 30_000
      assert state.peer_connect_timeout == 10_000
      assert state.min_peers_for_operation == 1
      assert state.startup_peer_timeout == 30_000
    end

    test "saves and loads peer settings" do
      state = %State{
        create_test_state()
        | peer_sync_interval: 60_000,
          peer_connect_timeout: 15_000,
          min_peers_for_operation: 2,
          startup_peer_timeout: 45_000
      }

      :ok = State.save(state)
      assert {:ok, loaded} = State.load()

      assert loaded.peer_sync_interval == 60_000
      assert loaded.peer_connect_timeout == 15_000
      assert loaded.min_peers_for_operation == 2
      assert loaded.startup_peer_timeout == 45_000
    end

    test "uses defaults when fields are missing in cluster.json (backward compat)" do
      json =
        :json.format(%{
          "cluster_id" => "clust_abc123",
          "cluster_name" => "test-cluster",
          "created_at" => "2024-01-15T10:30:00Z",
          "master_key" => "master_key_base64",
          "this_node" => %{
            "id" => "node_test123",
            "name" => "neonfs@localhost",
            "joined_at" => "2024-01-15T10:30:00Z"
          },
          "known_peers" => [],
          "ra_cluster_members" => ["neonfs@localhost"],
          "node_type" => "core"
        })

      File.write!(State.state_file_path(), json)
      assert {:ok, loaded} = State.load()

      assert loaded.peer_sync_interval == 30_000
      assert loaded.peer_connect_timeout == 10_000
      assert loaded.min_peers_for_operation == 1
      assert loaded.startup_peer_timeout == 30_000
    end

    test "validates peer settings are positive integers" do
      state = %State{
        create_test_state()
        | peer_sync_interval: -1
      }

      assert {:error, {:validation_failed, errors}} =
               state |> State.save() |> then(fn :ok -> State.load() end)

      assert Enum.any?(errors, &(&1.field == :peer_sync_interval))
    end
  end

  describe "metrics config" do
    test "saves and loads metrics settings" do
      state = %State{
        create_test_state()
        | metrics: %{"enabled" => false, "bind" => "127.0.0.1", "port" => 9768}
      }

      :ok = State.save(state)
      assert {:ok, loaded} = State.load()

      assert loaded.metrics["enabled"] == false
      assert loaded.metrics["bind"] == "127.0.0.1"
      assert loaded.metrics["port"] == 9768
    end

    test "uses default empty metrics map when field is missing" do
      json =
        :json.format(%{
          "cluster_id" => "clust_abc123",
          "cluster_name" => "test-cluster",
          "created_at" => "2024-01-15T10:30:00Z",
          "master_key" => "master_key_base64",
          "this_node" => %{
            "id" => "node_test123",
            "name" => "neonfs@localhost",
            "joined_at" => "2024-01-15T10:30:00Z"
          },
          "known_peers" => [],
          "ra_cluster_members" => ["neonfs@localhost"],
          "node_type" => "core"
        })

      File.write!(State.state_file_path(), json)
      assert {:ok, loaded} = State.load()
      assert loaded.metrics == %{}
    end
  end

  # Helper functions

  defp create_test_state do
    node_info = %{
      id: "node_test123",
      name: :neonfs@localhost,
      joined_at: ~U[2024-01-15 10:30:00Z]
    }

    %State{
      cluster_id: "clust_abc123",
      cluster_name: "test-cluster",
      created_at: ~U[2024-01-15 10:30:00Z],
      master_key: "master_key_base64",
      this_node: node_info,
      known_peers: [],
      ra_cluster_members: [:neonfs@localhost]
    }
  end
end
