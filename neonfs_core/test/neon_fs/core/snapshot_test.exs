defmodule NeonFS.Core.SnapshotTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{MetadataStateMachine, RaServer, RaSupervisor, Snapshot, VolumeRegistry}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    ensure_node_named()
    start_ra()
    :ok = RaServer.init_cluster()
    start_volume_registry()

    on_exit(fn -> cleanup_test_dirs() end)

    :ok
  end

  defp register_volume_root(volume_id, root_chunk_hash) do
    entry = %{
      volume_id: volume_id,
      root_chunk_hash: root_chunk_hash,
      drive_locations: [],
      durability_cache: %{},
      updated_at: DateTime.utc_now()
    }

    {:ok, :ok, _leader} = RaSupervisor.command({:register_volume_root, entry})
    :ok
  end

  describe "create/2" do
    test "snapshots the volume's current root chunk" do
      :ok = register_volume_root("vol-1", <<0xAA, 0xBB>>)

      assert {:ok, %Snapshot{} = snap} = Snapshot.create("vol-1")
      assert snap.volume_id == "vol-1"
      assert snap.root_chunk_hash == <<0xAA, 0xBB>>
      assert is_binary(snap.id)
      assert snap.name == nil
      assert %DateTime{} = snap.created_at
    end

    test "carries the supplied :name through" do
      :ok = register_volume_root("vol-2", <<1>>)

      assert {:ok, %Snapshot{name: "weekly"}} = Snapshot.create("vol-2", name: "weekly")
    end

    test "returns :volume_not_found if the volume isn't registered" do
      assert {:error, :volume_not_found} = Snapshot.create("missing-volume")
    end

    test "two snapshots of the same volume get distinct ids" do
      :ok = register_volume_root("vol-3", <<2>>)

      assert {:ok, snap_a} = Snapshot.create("vol-3")
      assert {:ok, snap_b} = Snapshot.create("vol-3")
      assert snap_a.id != snap_b.id
    end
  end

  describe "get/2" do
    test "returns the snapshot by id" do
      :ok = register_volume_root("vol-1", <<1>>)
      {:ok, snap} = Snapshot.create("vol-1")

      assert {:ok, ^snap} = Snapshot.get("vol-1", snap.id)
    end

    test "returns :not_found for an unknown id" do
      :ok = register_volume_root("vol-1", <<1>>)

      assert {:error, :not_found} = Snapshot.get("vol-1", "no-such-snapshot")
    end

    test "returns :not_found for a snapshot on the wrong volume" do
      :ok = register_volume_root("vol-1", <<1>>)
      :ok = register_volume_root("vol-2", <<2>>)
      {:ok, snap} = Snapshot.create("vol-1")

      assert {:error, :not_found} = Snapshot.get("vol-2", snap.id)
    end
  end

  describe "list/1" do
    test "returns snapshots newest first" do
      :ok = register_volume_root("vol-1", <<1>>)
      {:ok, older} = Snapshot.create("vol-1")
      Process.sleep(2)
      {:ok, newer} = Snapshot.create("vol-1")

      assert {:ok, [first, second]} = Snapshot.list("vol-1")
      assert first.id == newer.id
      assert second.id == older.id
    end

    test "returns an empty list for a volume with no snapshots" do
      assert {:ok, []} = Snapshot.list("never-snapshotted")
    end

    test "scopes the list to the requested volume" do
      :ok = register_volume_root("vol-a", <<1>>)
      :ok = register_volume_root("vol-b", <<2>>)
      {:ok, snap_a} = Snapshot.create("vol-a")
      {:ok, snap_b} = Snapshot.create("vol-b")

      assert {:ok, [a]} = Snapshot.list("vol-a")
      assert a.id == snap_a.id
      assert {:ok, [b]} = Snapshot.list("vol-b")
      assert b.id == snap_b.id
    end
  end

  describe "delete/2" do
    test "removes the pin and leaves list empty" do
      :ok = register_volume_root("vol-1", <<1>>)
      {:ok, snap} = Snapshot.create("vol-1")

      assert :ok = Snapshot.delete("vol-1", snap.id)
      assert {:ok, []} = Snapshot.list("vol-1")
      assert {:error, :not_found} = Snapshot.get("vol-1", snap.id)
    end

    test "is idempotent for missing snapshots" do
      :ok = register_volume_root("vol-1", <<1>>)
      assert :ok = Snapshot.delete("vol-1", "never-existed")
    end

    test "leaves sibling snapshots in place" do
      :ok = register_volume_root("vol-1", <<1>>)
      {:ok, keep} = Snapshot.create("vol-1")
      {:ok, drop} = Snapshot.create("vol-1")

      assert :ok = Snapshot.delete("vol-1", drop.id)
      assert {:ok, [remaining]} = Snapshot.list("vol-1")
      assert remaining.id == keep.id
    end
  end

  describe "promote/4" do
    test "creates a new volume sharing the snapshot's root" do
      {:ok, source} =
        VolumeRegistry.create("source", durability: %{type: :replicate, factor: 1, min_copies: 1})

      :ok = register_volume_root(source.id, <<0xAA, 0xBB>>)
      {:ok, snapshot} = Snapshot.create(source.id, name: "frozen")

      assert {:ok, promoted} = Snapshot.promote(source.id, snapshot.id, "promoted")

      assert promoted.name == "promoted"
      assert promoted.id != source.id

      {:ok, promoted_root} =
        RaSupervisor.local_query(fn state ->
          MetadataStateMachine.get_volume_root(state, promoted.id)
        end)

      assert promoted_root.root_chunk_hash == <<0xAA, 0xBB>>
      assert promoted_root.drive_locations == []

      {:ok, source_root} =
        RaSupervisor.local_query(fn state ->
          MetadataStateMachine.get_volume_root(state, source.id)
        end)

      assert source_root.root_chunk_hash == <<0xAA, 0xBB>>
    end

    test "inherits source storage policy by default" do
      {:ok, source} =
        VolumeRegistry.create("source-inherit",
          durability: %{type: :replicate, factor: 1, min_copies: 1},
          io_weight: 250
        )

      :ok = register_volume_root(source.id, <<1>>)
      {:ok, snapshot} = Snapshot.create(source.id)

      {:ok, promoted} = Snapshot.promote(source.id, snapshot.id, "inherit-target")

      assert promoted.io_weight == 250
      assert promoted.durability == source.durability
    end

    test "applies :volume_opts overrides" do
      {:ok, source} =
        VolumeRegistry.create("source-override",
          durability: %{type: :replicate, factor: 1, min_copies: 1},
          io_weight: 100
        )

      :ok = register_volume_root(source.id, <<1>>)
      {:ok, snapshot} = Snapshot.create(source.id)

      {:ok, promoted} =
        Snapshot.promote(source.id, snapshot.id, "override-target", volume_opts: [io_weight: 999])

      assert promoted.io_weight == 999
    end

    test "refuses to promote a snapshot on a volume that doesn't exist" do
      assert {:error, :volume_not_found} =
               Snapshot.promote("missing-volume", "no-snap", "promoted-missing")
    end

    test "refuses to promote a missing snapshot" do
      {:ok, source} =
        VolumeRegistry.create("source-missing-snap",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      :ok = register_volume_root(source.id, <<1>>)

      assert {:error, :not_found} =
               Snapshot.promote(source.id, "no-such-snapshot", "promoted-missing-snap")
    end

    test "fails fast when the source volume has no bootstrap volume_root entry yet" do
      {:ok, source} =
        VolumeRegistry.create("source-no-root",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      # Manually plant a snapshot to bypass `Snapshot.create/2`'s root
      # lookup — the failure mode under test is "snapshot exists but the
      # source volume_root has been unregistered".
      entry = %{
        id: "manual",
        volume_id: source.id,
        name: nil,
        root_chunk_hash: <<1>>,
        created_at: DateTime.utc_now()
      }

      {:ok, :ok, _} = RaSupervisor.command({:put_snapshot, entry})

      assert {:error, :source_volume_root_unknown} =
               Snapshot.promote(source.id, "manual", "promoted-no-root")
    end
  end

  describe "restore/3" do
    test "swaps the live root to the snapshot's root when the current root is covered" do
      :ok = register_volume_root("vol-r1", <<0xAA>>)
      {:ok, target_snap} = Snapshot.create("vol-r1", name: "target")

      # Advance the live root, then take another snapshot of it so the
      # current root is "covered".
      :ok = register_volume_root("vol-r1", <<0xBB>>)
      {:ok, _cover} = Snapshot.create("vol-r1", name: "cover")

      assert {:ok, result} = Snapshot.restore("vol-r1", target_snap.id)
      assert result.previous_root == <<0xBB>>
      assert result.new_root == <<0xAA>>
      assert result.pre_restore_snapshot == nil

      {:ok, latest_root} =
        RaSupervisor.local_query(fn state ->
          MetadataStateMachine.get_volume_root(state, "vol-r1")
        end)

      assert latest_root.root_chunk_hash == <<0xAA>>
    end

    test "no-op when the current root already matches the snapshot's root" do
      :ok = register_volume_root("vol-noop", <<1>>)
      {:ok, snap} = Snapshot.create("vol-noop")

      assert {:ok, result} = Snapshot.restore("vol-noop", snap.id)
      assert result.previous_root == result.new_root
      assert result.new_root == <<1>>
      assert result.pre_restore_snapshot == nil
    end

    test "refuses to swap when current root is uncovered and neither :safe nor :force set" do
      :ok = register_volume_root("vol-uncov", <<0xAA>>)
      {:ok, target} = Snapshot.create("vol-uncov", name: "target")

      # Advance the live root WITHOUT taking another snapshot — the
      # current root is now unreferenced.
      :ok = register_volume_root("vol-uncov", <<0xBB>>)

      assert {:error, :unreferenced_chunks} =
               Snapshot.restore("vol-uncov", target.id)

      # Bootstrap pointer unchanged.
      {:ok, root} =
        RaSupervisor.local_query(fn state ->
          MetadataStateMachine.get_volume_root(state, "vol-uncov")
        end)

      assert root.root_chunk_hash == <<0xBB>>
    end

    test "`:safe` creates a pre-restore snapshot of the current root before swapping" do
      :ok = register_volume_root("vol-safe", <<0xAA>>)
      {:ok, target} = Snapshot.create("vol-safe", name: "target")

      :ok = register_volume_root("vol-safe", <<0xBB>>)

      assert {:ok, result} = Snapshot.restore("vol-safe", target.id, safe: true)
      assert result.new_root == <<0xAA>>
      assert result.previous_root == <<0xBB>>

      assert %Snapshot{} = pre = result.pre_restore_snapshot
      assert pre.root_chunk_hash == <<0xBB>>
      assert String.starts_with?(pre.name, "pre-restore-")

      # The pre-restore snapshot is now in the list, and the new live
      # root is the target snapshot's root.
      {:ok, snapshots} = Snapshot.list("vol-safe")
      assert pre.id in Enum.map(snapshots, & &1.id)

      {:ok, root} =
        RaSupervisor.local_query(fn state ->
          MetadataStateMachine.get_volume_root(state, "vol-safe")
        end)

      assert root.root_chunk_hash == <<0xAA>>
    end

    test "`:safe` is a no-op for `pre_restore_snapshot` when current root is already covered" do
      :ok = register_volume_root("vol-safe-cov", <<0xAA>>)
      {:ok, target} = Snapshot.create("vol-safe-cov", name: "target")
      :ok = register_volume_root("vol-safe-cov", <<0xBB>>)
      {:ok, _cover} = Snapshot.create("vol-safe-cov", name: "cover")

      assert {:ok, result} =
               Snapshot.restore("vol-safe-cov", target.id, safe: true)

      # Already covered — no pre-restore snapshot is needed.
      assert result.pre_restore_snapshot == nil
    end

    test "`:force` swaps an uncovered live root without creating a safety snapshot" do
      :ok = register_volume_root("vol-force", <<0xAA>>)
      {:ok, target} = Snapshot.create("vol-force", name: "target")
      :ok = register_volume_root("vol-force", <<0xBB>>)

      assert {:ok, result} =
               Snapshot.restore("vol-force", target.id, force: true)

      assert result.previous_root == <<0xBB>>
      assert result.new_root == <<0xAA>>
      assert result.pre_restore_snapshot == nil

      {:ok, root} =
        RaSupervisor.local_query(fn state ->
          MetadataStateMachine.get_volume_root(state, "vol-force")
        end)

      assert root.root_chunk_hash == <<0xAA>>
    end

    test "returns :not_found for a snapshot that doesn't exist" do
      :ok = register_volume_root("vol-missing", <<1>>)

      assert {:error, :not_found} =
               Snapshot.restore("vol-missing", "no-such-snapshot")
    end

    test "returns :volume_not_found when the volume has no bootstrap entry" do
      {:ok, :ok, _} =
        RaSupervisor.command(
          {:put_snapshot,
           %{
             id: "orphan",
             volume_id: "no-volume",
             name: nil,
             root_chunk_hash: <<1>>,
             created_at: DateTime.utc_now()
           }}
        )

      assert {:error, :volume_not_found} =
               Snapshot.restore("no-volume", "orphan")
    end

    test "refuses to swap an uncovered root even when the same snapshot would normally cover it" do
      # The snapshot being restored doesn't count as coverage of the
      # current root — otherwise restoring would always be "safe" the
      # moment a snapshot exists, which defeats the precondition.
      :ok = register_volume_root("vol-self", <<0xAA>>)
      {:ok, target} = Snapshot.create("vol-self", name: "target")
      :ok = register_volume_root("vol-self", <<0xBB>>)

      # The only snapshot has root <<0xAA>>; it doesn't cover <<0xBB>>.
      assert {:error, :unreferenced_chunks} =
               Snapshot.restore("vol-self", target.id)
    end
  end
end
