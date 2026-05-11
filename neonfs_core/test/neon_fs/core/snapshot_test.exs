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
end
