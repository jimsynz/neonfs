defmodule NeonFS.Events.EmissionTest do
  @moduledoc """
  Tests that core metadata modules emit events via Broadcaster after
  successful writes.
  """

  use NeonFS.TestCase
  use ExUnit.Case, async: false

  alias NeonFS.Core.{ACLManager, FileIndex, FileMeta, VolumeACL, VolumeRegistry}
  alias NeonFS.Events.Broadcaster

  alias NeonFS.Events.{
    DirCreated,
    DirRenamed,
    Envelope,
    FileAclChanged,
    FileContentUpdated,
    FileCreated,
    FileDeleted,
    FileRenamed,
    VolumeAclChanged,
    VolumeCreated,
    VolumeDeleted,
    VolumeUpdated
  }

  @pg_scope :neonfs_events
  @volume_id "vol-emission-test"

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    # Start :pg for event broadcasting
    start_supervised!(%{id: :pg_events, start: {:pg, :start_link, [@pg_scope]}})

    # Clean up broadcaster state from previous tests
    cleanup_broadcaster_state()

    # Start required subsystems
    start_file_index()
    start_volume_registry()
    start_audit_log()
    start_acl_manager()

    # Subscribe to volume events
    :pg.join(@pg_scope, {:volume, @volume_id}, self())
    :pg.join(@pg_scope, {:volumes}, self())

    on_exit(fn ->
      cleanup_test_dirs()
      cleanup_broadcaster_state()
    end)

    :ok
  end

  describe "FileIndex event emission" do
    test "create/1 broadcasts FileCreated" do
      file = make_file(@volume_id, "/test.txt")

      assert {:ok, ^file} = FileIndex.create(file)

      assert_receive {:neonfs_event, %Envelope{event: %FileCreated{} = event}}, 1_000
      assert event.volume_id == @volume_id
      assert event.file_id == file.id
      assert event.path == "/test.txt"
    end

    test "update/2 broadcasts FileContentUpdated" do
      file = make_file(@volume_id, "/update-test.txt")
      {:ok, _} = FileIndex.create(file)

      # Drain the FileCreated event
      assert_receive {:neonfs_event, %Envelope{event: %FileCreated{}}}, 1_000

      assert {:ok, _updated} = FileIndex.update(file.id, size: 1024)

      assert_receive {:neonfs_event, %Envelope{event: %FileContentUpdated{} = event}}, 1_000
      assert event.volume_id == @volume_id
      assert event.file_id == file.id
      assert event.path == "/update-test.txt"
    end

    test "delete/1 broadcasts FileDeleted" do
      file = make_file(@volume_id, "/delete-test.txt")
      {:ok, _} = FileIndex.create(file)

      # Drain the FileCreated event
      assert_receive {:neonfs_event, %Envelope{event: %FileCreated{}}}, 1_000

      assert :ok = FileIndex.delete(file.id)

      assert_receive {:neonfs_event, %Envelope{event: %FileDeleted{} = event}}, 1_000
      assert event.volume_id == @volume_id
      assert event.file_id == file.id
      assert event.path == "/delete-test.txt"
    end

    test "mkdir/3 broadcasts DirCreated" do
      assert {:ok, _dir} = FileIndex.mkdir(@volume_id, "/subdir")

      assert_receive {:neonfs_event, %Envelope{event: %DirCreated{} = event}}, 1_000
      assert event.volume_id == @volume_id
      assert event.path == "/subdir"
    end

    test "rename/4 broadcasts FileRenamed for files" do
      file = make_file(@volume_id, "/old-name.txt")
      {:ok, _} = FileIndex.create(file)

      # Drain the FileCreated event
      assert_receive {:neonfs_event, %Envelope{event: %FileCreated{}}}, 1_000

      assert :ok = FileIndex.rename(@volume_id, "/", "old-name.txt", "new-name.txt")

      assert_receive {:neonfs_event, %Envelope{event: %FileRenamed{} = event}}, 1_000
      assert event.volume_id == @volume_id
      assert event.file_id == file.id
      assert event.old_path == "/old-name.txt"
      assert event.new_path == "/new-name.txt"
    end

    test "rename/4 broadcasts DirRenamed for directories" do
      {:ok, _} = FileIndex.mkdir(@volume_id, "/old-dir")

      # Drain the DirCreated event (and any parent dir events)
      drain_events()

      assert :ok = FileIndex.rename(@volume_id, "/", "old-dir", "new-dir")

      assert_receive {:neonfs_event, %Envelope{event: %DirRenamed{} = event}}, 1_000
      assert event.volume_id == @volume_id
      assert event.old_path == "/old-dir"
      assert event.new_path == "/new-dir"
    end

    test "move/4 broadcasts FileRenamed for files" do
      {:ok, _} = FileIndex.mkdir(@volume_id, "/src")
      {:ok, _} = FileIndex.mkdir(@volume_id, "/dest")

      file = make_file(@volume_id, "/src/moveme.txt")
      {:ok, _} = FileIndex.create(file)

      drain_events()

      assert :ok = FileIndex.move(@volume_id, "/src", "/dest", "moveme.txt")

      assert_receive {:neonfs_event, %Envelope{event: %FileRenamed{} = event}}, 1_000
      assert event.volume_id == @volume_id
      assert event.file_id == file.id
      assert event.old_path == "/src/moveme.txt"
      assert event.new_path == "/dest/moveme.txt"
    end

    test "move/4 broadcasts DirRenamed for directories" do
      {:ok, _} = FileIndex.mkdir(@volume_id, "/src")
      {:ok, _} = FileIndex.mkdir(@volume_id, "/dest")
      {:ok, _} = FileIndex.mkdir(@volume_id, "/src/movedir")

      drain_events()

      assert :ok = FileIndex.move(@volume_id, "/src", "/dest", "movedir")

      assert_receive {:neonfs_event, %Envelope{event: %DirRenamed{} = event}}, 1_000
      assert event.volume_id == @volume_id
      assert event.old_path == "/src/movedir"
      assert event.new_path == "/dest/movedir"
    end

    test "failed write does not emit an event" do
      # Try to create a file with an invalid path
      file = %FileMeta{
        id: UUIDv7.generate(),
        volume_id: @volume_id,
        path: "",
        chunks: [],
        size: 0,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        created_at: DateTime.utc_now(),
        modified_at: DateTime.utc_now(),
        version: 1
      }

      assert {:error, _} = FileIndex.create(file)

      refute_receive {:neonfs_event, _}, 200
    end
  end

  describe "VolumeRegistry event emission" do
    test "create/2 broadcasts VolumeCreated" do
      assert {:ok, volume} = VolumeRegistry.create("test-vol")

      # Subscribe to the new volume's group
      :pg.join(@pg_scope, {:volume, volume.id}, self())

      # The VolumeCreated event goes to the {:volumes} group
      assert_receive {:neonfs_event, %Envelope{event: %VolumeCreated{} = event}}, 1_000
      assert event.volume_id == volume.id
    end

    test "delete/1 broadcasts VolumeDeleted" do
      {:ok, volume} = VolumeRegistry.create("delete-vol")

      # Subscribe to the specific volume's group
      :pg.join(@pg_scope, {:volume, volume.id}, self())

      drain_events()

      assert :ok = VolumeRegistry.delete(volume.id)

      assert_receive {:neonfs_event, %Envelope{event: %VolumeDeleted{} = event}}, 1_000
      assert event.volume_id == volume.id
    end

    test "update/2 broadcasts VolumeUpdated" do
      {:ok, volume} = VolumeRegistry.create("update-vol")

      # Subscribe to the specific volume's group
      :pg.join(@pg_scope, {:volume, volume.id}, self())

      drain_events()

      assert {:ok, _updated} = VolumeRegistry.update(volume.id, io_weight: 50)

      assert_receive {:neonfs_event, %Envelope{event: %VolumeUpdated{} = event}}, 1_000
      assert event.volume_id == volume.id
    end

    test "update_stats/2 does NOT emit an event" do
      {:ok, volume} = VolumeRegistry.create("stats-vol")

      # Subscribe to the specific volume's group
      :pg.join(@pg_scope, {:volume, volume.id}, self())

      drain_events()

      assert {:ok, _updated} = VolumeRegistry.update_stats(volume.id, logical_size: 1024)

      refute_receive {:neonfs_event, _}, 200
    end

    test "create_system_volume/0 does NOT emit an event" do
      # Need cluster state for system volume creation
      alias NeonFS.Cluster.State, as: ClusterState

      meta_dir = Application.get_env(:neonfs_core, :meta_dir)
      master_key = :crypto.strong_rand_bytes(32) |> Base.encode16(case: :lower)

      cluster_state =
        ClusterState.new(
          "emission-test-cluster-#{System.unique_integer([:positive])}",
          "emission-test",
          master_key,
          %{id: "node-1", name: node(), joined_at: DateTime.utc_now()}
        )

      Application.put_env(:neonfs_core, :meta_dir, meta_dir)
      :ok = ClusterState.save(cluster_state)

      drain_events()

      case VolumeRegistry.create_system_volume() do
        {:ok, _volume} ->
          refute_receive {:neonfs_event, _}, 200

        {:error, :already_exists} ->
          # System volume already exists, test is still valid
          refute_receive {:neonfs_event, _}, 200
      end
    end
  end

  describe "ACLManager event emission" do
    test "set_volume_acl/2 broadcasts VolumeAclChanged" do
      acl = make_acl(@volume_id)

      assert :ok = ACLManager.set_volume_acl(@volume_id, acl)

      assert_receive {:neonfs_event, %Envelope{event: %VolumeAclChanged{} = event}}, 1_000
      assert event.volume_id == @volume_id
    end

    test "grant/3 broadcasts VolumeAclChanged" do
      acl = make_acl(@volume_id)
      :ok = ACLManager.set_volume_acl(@volume_id, acl)

      drain_events()

      assert :ok = ACLManager.grant(@volume_id, {:uid, 1001}, [:read, :write])

      assert_receive {:neonfs_event, %Envelope{event: %VolumeAclChanged{} = event}}, 1_000
      assert event.volume_id == @volume_id
    end

    test "revoke/2 broadcasts VolumeAclChanged" do
      acl =
        make_acl(@volume_id,
          entries: [%{principal: {:uid, 1001}, permissions: MapSet.new([:read])}]
        )

      :ok = ACLManager.set_volume_acl(@volume_id, acl)

      drain_events()

      assert :ok = ACLManager.revoke(@volume_id, {:uid, 1001})

      assert_receive {:neonfs_event, %Envelope{event: %VolumeAclChanged{} = event}}, 1_000
      assert event.volume_id == @volume_id
    end

    test "delete_volume_acl/1 broadcasts VolumeAclChanged" do
      acl = make_acl(@volume_id)
      :ok = ACLManager.set_volume_acl(@volume_id, acl)

      drain_events()

      assert :ok = ACLManager.delete_volume_acl(@volume_id)

      assert_receive {:neonfs_event, %Envelope{event: %VolumeAclChanged{} = event}}, 1_000
      assert event.volume_id == @volume_id
    end

    test "set_file_acl/3 broadcasts FileAclChanged" do
      file = make_file(@volume_id, "/acl-test.txt")
      {:ok, _} = FileIndex.create(file)

      drain_events()

      acl_entries = [%{principal: {:uid, 1001}, permissions: MapSet.new([:read])}]
      assert :ok = ACLManager.set_file_acl(@volume_id, "/acl-test.txt", acl_entries)

      # Should receive FileAclChanged (from ACLManager) and FileContentUpdated
      # (from FileIndex.update). Collect all events and check for FileAclChanged.
      events = collect_events(1_000)

      assert Enum.any?(events, fn
               %Envelope{event: %FileAclChanged{volume_id: vid, path: path}} ->
                 vid == @volume_id and path == "/acl-test.txt"

               _ ->
                 false
             end)
    end
  end

  describe "event envelope" do
    test "envelopes include correct source_node and monotonic sequences" do
      file1 = make_file(@volume_id, "/seq1.txt")
      file2 = make_file(@volume_id, "/seq2.txt")

      {:ok, _} = FileIndex.create(file1)
      {:ok, _} = FileIndex.create(file2)

      assert_receive {:neonfs_event, %Envelope{source_node: node1, sequence: seq1}}, 1_000
      assert_receive {:neonfs_event, %Envelope{source_node: node2, sequence: seq2}}, 1_000

      assert node1 == node()
      assert node2 == node()
      assert seq2 > seq1
    end

    test "envelopes include valid HLC timestamps" do
      file = make_file(@volume_id, "/hlc-test.txt")
      {:ok, _} = FileIndex.create(file)

      assert_receive {:neonfs_event, %Envelope{hlc_timestamp: {wall_ms, counter, node_id}}},
                     1_000

      assert is_integer(wall_ms) and wall_ms > 0
      assert is_integer(counter) and counter >= 0
      assert node_id == node()
    end
  end

  # Helpers

  defp make_file(volume_id, path) do
    %FileMeta{
      id: UUIDv7.generate(),
      volume_id: volume_id,
      path: path,
      chunks: [],
      size: 0,
      mode: 0o644,
      uid: 1000,
      gid: 1000,
      created_at: DateTime.utc_now(),
      modified_at: DateTime.utc_now(),
      version: 1
    }
  end

  defp make_acl(volume_id, opts \\ []) do
    %VolumeACL{
      volume_id: volume_id,
      owner_uid: Keyword.get(opts, :owner_uid, 1000),
      owner_gid: Keyword.get(opts, :owner_gid, 1000),
      entries: Keyword.get(opts, :entries, [])
    }
  end

  defp drain_events do
    receive do
      {:neonfs_event, _} -> drain_events()
    after
      100 -> :ok
    end
  end

  defp collect_events(timeout) do
    collect_events(timeout, [])
  end

  defp collect_events(timeout, acc) do
    receive do
      {:neonfs_event, envelope} ->
        collect_events(timeout, [envelope | acc])
    after
      timeout -> Enum.reverse(acc)
    end
  end

  defp cleanup_broadcaster_state do
    Process.delete({Broadcaster, :hlc})

    for key <- :persistent_term.get() do
      case key do
        {{NeonFS.Events.Broadcaster, :counter, _}, _} ->
          :persistent_term.erase(elem(key, 0))

        _ ->
          :ok
      end
    end

    :ok
  rescue
    _ -> :ok
  end
end
