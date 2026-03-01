defmodule NeonFS.Cluster.FormationTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Cluster.Formation

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    # Also point :ra data_dir to a clean tmp subdirectory so has_ra_data?
    # doesn't pick up stale data from the default Ra system directory.
    ra_dir = Path.join(tmp_dir, "ra")
    File.mkdir_p!(ra_dir)
    old_ra_dir = Application.get_env(:ra, :data_dir)
    Application.put_env(:ra, :data_dir, to_charlist(ra_dir))

    on_exit(fn ->
      if old_ra_dir do
        Application.put_env(:ra, :data_dir, old_ra_dir)
      else
        Application.delete_env(:ra, :data_dir)
      end

      cleanup_test_dirs()
    end)

    %{tmp_dir: tmp_dir}
  end

  describe "orphaned_data_detected?/0" do
    test "returns false for empty directories", %{tmp_dir: tmp_dir} do
      # Ensure directories exist but are empty (like fresh startup)
      File.mkdir_p!(Path.join(tmp_dir, "blobs"))
      refute Formation.orphaned_data_detected?()
    end

    test "returns false for BlobStore prefix directories without files", %{tmp_dir: tmp_dir} do
      # BlobStore creates prefix directories on startup — these are not orphaned data
      blob_dir = Path.join(tmp_dir, "blobs")

      for prefix <- ["00", "01", "ff"] do
        File.mkdir_p!(Path.join(blob_dir, prefix))
      end

      refute Formation.orphaned_data_detected?()
    end

    test "returns false for fresh DETS files (small/empty)", %{tmp_dir: tmp_dir} do
      # Persistence creates small empty DETS tables on startup
      File.write!(Path.join(tmp_dir, "persistence.dets"), "")
      refute Formation.orphaned_data_detected?()
    end

    test "detects orphaned Ra snapshot data", %{tmp_dir: tmp_dir} do
      # Ra snapshots only exist after consensus operations (init_cluster/join_cluster)
      ra_dir = Path.join(tmp_dir, "ra")
      server_dir = Path.join(ra_dir, "neonfs_meta_node1")
      snapshot_dir = Path.join(server_dir, "snapshots")
      File.mkdir_p!(snapshot_dir)

      assert Formation.orphaned_data_detected?()
    end

    test "detects orphaned DETS files with data", %{tmp_dir: tmp_dir} do
      # DETS files > 1KB indicate prior cluster activity
      large_content = :crypto.strong_rand_bytes(2048)
      File.write!(Path.join(tmp_dir, "persistence.dets"), large_content)
      assert Formation.orphaned_data_detected?()
    end

    test "detects orphaned blob chunk files", %{tmp_dir: tmp_dir} do
      # Actual blob files within prefix directories indicate prior cluster data
      blob_dir = Path.join(tmp_dir, "blobs")
      prefix_dir = Path.join(blob_dir, "ab")
      File.mkdir_p!(prefix_dir)
      File.write!(Path.join(prefix_dir, "abcdef1234567890"), "chunk data")
      assert Formation.orphaned_data_detected?()
    end
  end

  describe "preconditions" do
    test "exits normally when cluster.json already exists", %{tmp_dir: tmp_dir} do
      # Create a valid cluster.json
      write_cluster_json(tmp_dir, Base.encode64(:crypto.strong_rand_bytes(32)))

      {:ok, pid} =
        Formation.start_link(
          cluster_name: "test",
          bootstrap_expect: 1,
          bootstrap_peers: [Node.self()],
          bootstrap_timeout: 5_000
        )

      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000
    end

    test "exits with :orphaned_data_detected when data exists without cluster.json", %{
      tmp_dir: tmp_dir
    } do
      # Create orphaned blob data (actual files, not just directories)
      blob_dir = Path.join(tmp_dir, "blobs")
      prefix_dir = Path.join(blob_dir, "ab")
      File.mkdir_p!(prefix_dir)
      File.write!(Path.join(prefix_dir, "abcdef1234567890"), "chunk data")

      # Trap exits so the linked GenServer's shutdown doesn't crash the test
      Process.flag(:trap_exit, true)

      {:ok, pid} =
        Formation.start_link(
          cluster_name: "test",
          bootstrap_expect: 1,
          bootstrap_peers: [Node.self()],
          bootstrap_timeout: 5_000
        )

      assert_receive {:EXIT, ^pid, {:shutdown, :orphaned_data_detected}}, 5_000
    end
  end

  describe "elect_init_node/1" do
    test "selects the lexicographically lowest node name" do
      state = %Formation{
        cluster_name: "test",
        bootstrap_expect: 3,
        bootstrap_peers: [:charlie@host, :alice@host, :bob@host],
        bootstrap_timeout: 300_000,
        deadline: System.monotonic_time(:millisecond) + 300_000
      }

      # The election includes Node.self() plus bootstrap_peers.
      # Result should be deterministic: the lowest sorted name.
      result = Formation.elect_init_node(state)
      all_nodes = [Node.self() | state.bootstrap_peers] |> Enum.uniq() |> Enum.sort()
      assert result == List.first(all_nodes)
    end

    test "is deterministic regardless of peer list order" do
      peers_a = [:zebra@host, :alpha@host, :middle@host]
      peers_b = [:alpha@host, :middle@host, :zebra@host]

      state_a = %Formation{
        cluster_name: "test",
        bootstrap_expect: 3,
        bootstrap_peers: peers_a,
        bootstrap_timeout: 300_000,
        deadline: System.monotonic_time(:millisecond) + 300_000
      }

      state_b = %Formation{
        cluster_name: "test",
        bootstrap_expect: 3,
        bootstrap_peers: peers_b,
        bootstrap_timeout: 300_000,
        deadline: System.monotonic_time(:millisecond) + 300_000
      }

      assert Formation.elect_init_node(state_a) == Formation.elect_init_node(state_b)
    end

    test "includes self in the candidate list" do
      # Use a peer that sorts after self to verify self is included
      peers = [:zzz_node@localhost]

      state = %Formation{
        cluster_name: "test",
        bootstrap_expect: 2,
        bootstrap_peers: peers,
        bootstrap_timeout: 300_000,
        deadline: System.monotonic_time(:millisecond) + 300_000
      }

      result = Formation.elect_init_node(state)
      all_sorted = [Node.self() | peers] |> Enum.uniq() |> Enum.sort()
      assert result == List.first(all_sorted)
    end
  end
end
