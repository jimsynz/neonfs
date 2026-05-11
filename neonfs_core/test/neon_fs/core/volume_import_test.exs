defmodule NeonFS.Core.VolumeImportTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core
  alias NeonFS.Core.{FileIndex, VolumeExport, VolumeImport, VolumeRegistry, WriteOperation}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_core_subsystems()
    start_stripe_index()
    ensure_chunk_access_tracker()

    on_exit(fn -> cleanup_test_dirs() end)

    {:ok, tmp_dir: tmp_dir}
  end

  describe "import_archive/3 — round-trip with #965 export" do
    test "imports an exported archive into a new volume with byte-equal content",
         %{tmp_dir: tmp_dir} do
      {:ok, src} = VolumeRegistry.create("rt-src", [])
      {:ok, _} = WriteOperation.write_file_streamed(src.id, "/alpha.txt", ["alpha\n"])
      {:ok, _} = WriteOperation.write_file_streamed(src.id, "/dir/beta.txt", ["beta-content"])

      tar = Path.join(tmp_dir, "rt.tar")
      assert {:ok, _} = VolumeExport.export(src.name, tar)

      assert {:ok, summary} = VolumeImport.import_archive(tar, "rt-dst")
      assert summary.volume_name == "rt-dst"
      assert summary.file_count == 2
      assert summary.byte_count == byte_size("alpha\n") + byte_size("beta-content")
      assert summary.source_volume_name == "rt-src"

      {:ok, "alpha\n"} = read_file("rt-dst", "/alpha.txt")
      {:ok, "beta-content"} = read_file("rt-dst", "/dir/beta.txt")
    end

    test "round-trip preserves a multi-chunk file", %{tmp_dir: tmp_dir} do
      {:ok, src} = VolumeRegistry.create("rt-large-src", [])

      chunk = 64 * 1024
      payload_chunks = for _ <- 1..3, do: :crypto.strong_rand_bytes(chunk)
      expected = IO.iodata_to_binary(payload_chunks)

      {:ok, _} = WriteOperation.write_file_streamed(src.id, "/blob", payload_chunks)

      tar = Path.join(tmp_dir, "rt-large.tar")
      assert {:ok, _} = VolumeExport.export(src.name, tar)

      assert {:ok, summary} = VolumeImport.import_archive(tar, "rt-large-dst")
      assert summary.byte_count == byte_size(expected)

      {:ok, actual} = read_file("rt-large-dst", "/blob")
      assert byte_size(actual) == byte_size(expected)
      assert actual == expected
    end

    test "imports an empty volume", %{tmp_dir: tmp_dir} do
      {:ok, src} = VolumeRegistry.create("rt-empty-src", [])

      tar = Path.join(tmp_dir, "rt-empty.tar")
      assert {:ok, _} = VolumeExport.export(src.name, tar)

      assert {:ok, summary} = VolumeImport.import_archive(tar, "rt-empty-dst")
      assert summary.file_count == 0
      assert summary.byte_count == 0

      {:ok, dst} = VolumeRegistry.get_by_name("rt-empty-dst")
      assert FileIndex.list_volume(dst.id) == []
    end
  end

  describe "import_archive/3 — error cases" do
    test "returns :input_missing for a non-existent path", %{tmp_dir: tmp_dir} do
      assert {:error, :input_missing} =
               VolumeImport.import_archive(Path.join(tmp_dir, "ghost.tar"), "v")
    end

    test "returns :manifest_missing when the first entry isn't the manifest",
         %{tmp_dir: tmp_dir} do
      # Build a tarball whose first entry is a file, not manifest.json.
      tar = Path.join(tmp_dir, "bad.tar")
      :ok = :erl_tar.create(String.to_charlist(tar), [{~c"files/x.txt", "hi"}])

      assert {:error, {:manifest_missing, {:first_entry, "files/x.txt"}}} =
               VolumeImport.import_archive(tar, "import-bad")
    end

    test "returns :manifest_missing for an empty archive", %{tmp_dir: tmp_dir} do
      tar = Path.join(tmp_dir, "empty-archive.tar")
      :ok = :erl_tar.create(String.to_charlist(tar), [])

      assert {:error, {:manifest_missing, :empty_archive}} =
               VolumeImport.import_archive(tar, "import-empty")
    end

    test "rejects an unsupported manifest schema", %{tmp_dir: tmp_dir} do
      tar = Path.join(tmp_dir, "bad-schema.tar")
      manifest = Jason.encode!(%{"schema" => "neonfs.something-else.v9"})
      :ok = :erl_tar.create(String.to_charlist(tar), [{~c"manifest.json", manifest}])

      assert {:error, {:manifest_invalid, {:unsupported_schema, "neonfs.something-else.v9"}}} =
               VolumeImport.import_archive(tar, "import-bad-schema")
    end

    test "rejects a manifest with malformed JSON", %{tmp_dir: tmp_dir} do
      tar = Path.join(tmp_dir, "bad-json.tar")
      :ok = :erl_tar.create(String.to_charlist(tar), [{~c"manifest.json", "{not json"}])

      assert {:error, {:manifest_invalid, _reason}} =
               VolumeImport.import_archive(tar, "import-bad-json")
    end

    test "fails fast on file_count mismatch", %{tmp_dir: tmp_dir} do
      tar = Path.join(tmp_dir, "mismatch.tar")

      manifest =
        Jason.encode!(%{
          "schema" => "neonfs.volume-export.v1",
          "file_count" => 99,
          "total_bytes" => 0
        })

      :ok = :erl_tar.create(String.to_charlist(tar), [{~c"manifest.json", manifest}])

      assert {:error, {:manifest_mismatch, :file_count, 99, 0}} =
               VolumeImport.import_archive(tar, "import-mismatch")
    end
  end

  describe "import_archive/3 — name collision" do
    test "fails when the target volume name already exists", %{tmp_dir: tmp_dir} do
      {:ok, src} = VolumeRegistry.create("collide-src", [])
      tar = Path.join(tmp_dir, "collide.tar")
      assert {:ok, _} = VolumeExport.export(src.name, tar)

      {:ok, _} = VolumeRegistry.create("collide-dst", [])

      # VolumeRegistry.create surfaces a duplicate-name error; the
      # import layer just propagates it.
      assert {:error, _} = VolumeImport.import_archive(tar, "collide-dst")
    end
  end

  defp read_file(volume_name, path) do
    {:ok, %{stream: stream}} = Core.read_file_stream(volume_name, path)
    {:ok, stream |> Enum.to_list() |> IO.iodata_to_binary()}
  end
end
