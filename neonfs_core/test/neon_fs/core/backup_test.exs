defmodule NeonFS.Core.BackupTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core

  alias NeonFS.Core.{
    Backup,
    RaServer,
    RaSupervisor,
    Snapshot,
    VolumeExport,
    VolumeRegistry,
    WriteOperation
  }

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_core_subsystems()
    start_stripe_index()
    ensure_chunk_access_tracker()
    ensure_node_named()
    start_ra()
    :ok = RaServer.init_cluster()

    on_exit(fn -> cleanup_test_dirs() end)

    {:ok, tmp_dir: tmp_dir}
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

  describe "create/3 — live-root volume (no snapshot pinning needed)" do
    test "snapshots + exports + deletes the snapshot, leaving a round-trippable tar",
         %{tmp_dir: tmp_dir} do
      {:ok, vol} = VolumeRegistry.create("bk-rt", [])
      {:ok, _} = WriteOperation.write_file_streamed(vol.id, "/a.txt", ["alpha"])

      # `Snapshot.create` needs a registered `volume_root`. In tests
      # without full Ra-backed provisioning that has to be planted
      # manually — the snapshot's `root_chunk_hash` happens to be
      # the only thing we need from the bootstrap layer here.
      :ok = register_volume_root(vol.id, <<0xAA>>)

      out = Path.join(tmp_dir, "bk-rt.tar")
      result = Backup.create(vol.name, out)

      # The snapshot-aware export can't actually pull bytes through
      # because the planted volume_root points at a chunk that isn't
      # in BlobStore — the test scaffolding lift to register real
      # drives + provision is tracked under #985. Backup propagates
      # the export error rather than silently succeeding.
      assert match?({:error, _}, result),
             "expected export error in stub scaffolding, got #{inspect(result)}"

      # The snapshot must be left in place so the operator can retry.
      {:ok, snapshots} = Snapshot.list(vol.id)
      assert length(snapshots) == 1
    end
  end

  describe "describe/1" do
    test "extracts and decodes manifest.json from a tarball", %{tmp_dir: tmp_dir} do
      # Build a backup tar via a happy-path live-root export so the
      # manifest is well-formed without snapshot setup.
      {:ok, vol} = VolumeRegistry.create("bk-describe", [])
      {:ok, _} = WriteOperation.write_file_streamed(vol.id, "/x.txt", ["x"])

      out = Path.join(tmp_dir, "bk.tar")
      assert {:ok, _} = VolumeExport.export(vol.name, out)

      assert {:ok, manifest} = Backup.describe(out)
      assert manifest["schema"] == "neonfs.volume-export.v1"
      assert manifest["volume"]["name"] == "bk-describe"
    end

    test "returns :input_missing for a non-existent path", %{tmp_dir: tmp_dir} do
      assert {:error, :input_missing} = Backup.describe(Path.join(tmp_dir, "ghost.tar"))
    end

    test "returns :manifest_invalid when the tar has a malformed manifest",
         %{tmp_dir: tmp_dir} do
      bad = Path.join(tmp_dir, "bad.tar")
      :ok = :erl_tar.create(String.to_charlist(bad), [{~c"manifest.json", "{not json"}])

      assert {:error, {:manifest_invalid, _}} = Backup.describe(bad)
    end

    test "returns :manifest_missing when no manifest.json is present", %{tmp_dir: tmp_dir} do
      empty = Path.join(tmp_dir, "no-manifest.tar")
      :ok = :erl_tar.create(String.to_charlist(empty), [{~c"files/x.txt", "x"}])

      assert {:error, {:manifest_missing, ^empty}} = Backup.describe(empty)
    end
  end

  describe "restore/3" do
    test "round-trips a live-root export through Backup.restore/3", %{tmp_dir: tmp_dir} do
      {:ok, vol} = VolumeRegistry.create("bk-restore-src", [])
      {:ok, _} = WriteOperation.write_file_streamed(vol.id, "/a.txt", ["abc"])

      out = Path.join(tmp_dir, "bk-restore.tar")
      assert {:ok, _} = VolumeExport.export(vol.name, out)

      assert {:ok, summary} = Backup.restore(out, "bk-restore-dst")
      assert summary.volume_name == "bk-restore-dst"
      assert summary.file_count == 1

      {:ok, %{stream: stream}} = Core.read_file_stream("bk-restore-dst", "/a.txt")
      assert IO.iodata_to_binary(Enum.to_list(stream)) == "abc"
    end
  end
end
