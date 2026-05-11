defmodule NeonFS.Core.VolumeExportTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{VolumeExport, VolumeRegistry, WriteOperation}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_core_subsystems()
    start_stripe_index()
    ensure_chunk_access_tracker()

    # Ra is needed for the snapshot-export tests. Starting it here
    # is harmless for the live-root tests — they just don't touch
    # `Snapshot.create/1` / `MetadataReader.range(.., at_root: ...)`.
    ensure_node_named()
    start_ra()
    :ok = NeonFS.Core.RaServer.init_cluster()

    on_exit(fn -> cleanup_test_dirs() end)

    {:ok, tmp_dir: tmp_dir}
  end

  describe "export/3" do
    test "writes a tarball with a manifest plus one entry per file", %{tmp_dir: tmp_dir} do
      {:ok, vol} = VolumeRegistry.create("export-basic", [])
      {:ok, _} = WriteOperation.write_file_streamed(vol.id, "/alpha.txt", ["alpha\n"])
      {:ok, _} = WriteOperation.write_file_streamed(vol.id, "/dir/beta.txt", ["bbbbbb"])

      out = Path.join(tmp_dir, "export.tar")

      assert {:ok, summary} = VolumeExport.export(vol.name, out)
      assert summary.path == out
      assert summary.file_count == 2
      assert summary.byte_count == byte_size("alpha\n") + byte_size("bbbbbb")
      assert File.exists?(out)

      assert {entries, manifest} = read_tar_entries(out)

      paths = entries |> Enum.map(&elem(&1, 0)) |> Enum.sort()
      assert paths == ["files/alpha.txt", "files/dir/beta.txt"]

      assert Enum.find(entries, fn {n, _} -> n == "files/alpha.txt" end) |> elem(1) == "alpha\n"
      assert Enum.find(entries, fn {n, _} -> n == "files/dir/beta.txt" end) |> elem(1) == "bbbbbb"

      assert manifest["version"] == 1
      assert manifest["schema"] == "neonfs.volume-export.v1"
      assert manifest["volume"]["name"] == "export-basic"
      assert manifest["file_count"] == 2
      assert manifest["total_bytes"] == 12

      manifest_paths = manifest["files"] |> Enum.map(& &1["path"]) |> Enum.sort()
      assert manifest_paths == ["/alpha.txt", "/dir/beta.txt"]
    end

    test "handles an empty volume by emitting a manifest with file_count: 0", %{tmp_dir: tmp_dir} do
      {:ok, vol} = VolumeRegistry.create("export-empty", [])

      out = Path.join(tmp_dir, "empty.tar")
      assert {:ok, summary} = VolumeExport.export(vol.name, out)
      assert summary.file_count == 0
      assert summary.byte_count == 0
      assert File.exists?(out)

      {entries, manifest} = read_tar_entries(out)
      assert entries == []
      assert manifest["file_count"] == 0
      assert manifest["files"] == []
    end

    test "tar is parsable by Erlang's :erl_tar", %{tmp_dir: tmp_dir} do
      {:ok, vol} = VolumeRegistry.create("export-erl-tar", [])
      {:ok, _} = WriteOperation.write_file_streamed(vol.id, "/hello.txt", ["world"])

      out = Path.join(tmp_dir, "valid.tar")
      assert {:ok, _} = VolumeExport.export(vol.name, out)

      assert {:ok, names} = :erl_tar.table(String.to_charlist(out))

      string_names = Enum.map(names, &to_string/1) |> Enum.sort()
      assert "manifest.json" in string_names
      assert "files/hello.txt" in string_names
    end

    test "exports large file via streaming — never buffers the whole file", %{tmp_dir: tmp_dir} do
      {:ok, vol} = VolumeRegistry.create("export-large", [])

      # Two chunks worth of data — verifies that multi-chunk files
      # are streamed through the tar entry correctly.
      chunk_size = 64 * 1024
      payload_chunks = for _ <- 1..3, do: :crypto.strong_rand_bytes(chunk_size)
      total_size = chunk_size * 3
      expected = IO.iodata_to_binary(payload_chunks)

      {:ok, _} = WriteOperation.write_file_streamed(vol.id, "/big.bin", payload_chunks)

      out = Path.join(tmp_dir, "big.tar")
      assert {:ok, summary} = VolumeExport.export(vol.name, out)
      assert summary.byte_count == total_size

      {entries, _manifest} = read_tar_entries(out)
      {_, content} = Enum.find(entries, fn {n, _} -> n == "files/big.bin" end)
      assert content == expected
    end

    test "returns :volume_not_found for an unknown volume", %{tmp_dir: tmp_dir} do
      out = Path.join(tmp_dir, "ghost.tar")
      assert {:error, :volume_not_found} = VolumeExport.export("no-such-volume", out)
      refute File.exists?(out)
    end

    test "creates the output directory if it doesn't exist", %{tmp_dir: tmp_dir} do
      {:ok, vol} = VolumeRegistry.create("export-mkdir", [])
      {:ok, _} = WriteOperation.write_file_streamed(vol.id, "/x.txt", ["x"])

      out = Path.join([tmp_dir, "nested", "subdir", "out.tar"])
      refute File.exists?(Path.dirname(out))

      assert {:ok, _} = VolumeExport.export(vol.name, out)
      assert File.exists?(out)
    end

    test "returns :not_found for an unknown :snapshot_id", %{tmp_dir: tmp_dir} do
      {:ok, vol} = VolumeRegistry.create("export-no-snap", [])

      assert {:error, :not_found} =
               VolumeExport.export(vol.name, Path.join(tmp_dir, "x.tar"), snapshot_id: "ghost")
    end

    test "tags the manifest with :snapshot_id when set", %{tmp_dir: tmp_dir} do
      # Drive a real Snapshot.create via Ra. The volume_root entry
      # has to exist before Snapshot.create can pin it; we plant it
      # by hand because the in-test write path doesn't run the
      # full Ra-backed provisioning that production goes through.
      {:ok, vol} = VolumeRegistry.create("export-snap-tag", [])
      :ok = register_volume_root(vol.id, <<0xAA, 0xBB>>)
      {:ok, snap} = NeonFS.Core.Snapshot.create(vol.id)

      # `MetadataReader.range/5` at the planted root will fail
      # because the chunk isn't in BlobStore — the export aborts
      # before writing anything. That's the right behaviour: the
      # caller sees the error rather than a half-written tar.
      out = Path.join(tmp_dir, "snap-tag.tar")
      result = VolumeExport.export(vol.name, out, snapshot_id: snap.id)

      assert match?({:error, _}, result),
             "expected error from missing snapshot tree, got #{inspect(result)}"
    end

    test "manifest captures POSIX mode bits", %{tmp_dir: tmp_dir} do
      {:ok, vol} = VolumeRegistry.create("export-perm", [])

      {:ok, _} =
        WriteOperation.write_file_streamed(vol.id, "/owned.txt", ["o"], mode: 0o640)

      out = Path.join(tmp_dir, "perm.tar")
      assert {:ok, _} = VolumeExport.export(vol.name, out)

      {_entries, manifest} = read_tar_entries(out)
      [file] = manifest["files"]
      assert file["mode"] == 0o640
    end
  end

  defp register_volume_root(volume_id, root_chunk_hash) do
    entry = %{
      volume_id: volume_id,
      root_chunk_hash: root_chunk_hash,
      drive_locations: [],
      durability_cache: %{},
      updated_at: DateTime.utc_now()
    }

    {:ok, :ok, _leader} = NeonFS.Core.RaSupervisor.command({:register_volume_root, entry})
    :ok
  end

  # Walks a TAR using :erl_tar's extraction so test logic stays
  # independent of the writer-under-test. Returns {entries, manifest_decoded}.
  defp read_tar_entries(path) do
    extract_dir = Path.join(System.tmp_dir!(), "neonfs-export-test-#{:rand.uniform(999_999)}")
    File.mkdir_p!(extract_dir)

    try do
      :ok = :erl_tar.extract(String.to_charlist(path), cwd: String.to_charlist(extract_dir))

      manifest_path = Path.join(extract_dir, "manifest.json")
      manifest = manifest_path |> File.read!() |> Jason.decode!()

      files_root = Path.join(extract_dir, "files")

      entries =
        if File.dir?(files_root) do
          files_root
          |> ls_recursive()
          |> Enum.map(fn full ->
            rel = Path.relative_to(full, extract_dir)
            {rel, File.read!(full)}
          end)
        else
          []
        end

      {entries, manifest}
    after
      File.rm_rf!(extract_dir)
    end
  end

  defp ls_recursive(root) do
    case File.ls(root) do
      {:ok, names} -> Enum.flat_map(names, &expand_entry(root, &1))
      _ -> []
    end
  end

  defp expand_entry(root, name) do
    full = Path.join(root, name)
    if File.dir?(full), do: ls_recursive(full), else: [full]
  end
end
