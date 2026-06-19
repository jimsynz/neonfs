defmodule NeonFS.Core.IncrementalBackupTest do
  @moduledoc """
  File-level incremental backup (#1003): an incremental export carries
  bodies only for new/changed files (plus a `deleted` list); chain
  restore replays the full + incrementals to reproduce the final state
  byte-for-byte.

  Exercised via live-root `VolumeExport.export` (which streams real
  bytes through the in-process BlobStore) rather than the snapshot path,
  so it doesn't need the full Ra provisioning the snapshot tests defer
  to #985. The incremental diff + chain-replay logic is identical either
  way — only the file enumeration differs.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core

  alias NeonFS.Core.{
    Backup,
    FileIndex,
    VolumeExport,
    VolumeImport,
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

    on_exit(fn -> cleanup_test_dirs() end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "incremental carries unchanged files; chain restore reconstructs byte-for-byte",
       %{tmp_dir: tmp_dir} do
    {:ok, src} = VolumeRegistry.create("inc-src", [])
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/keep.txt", ["unchanged-content"])
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/change.txt", ["original"])
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/gone.txt", ["doomed"])

    full = Path.join(tmp_dir, "full.tar")
    assert {:ok, full_summary} = VolumeExport.export(src.name, full)
    assert full_summary.file_count == 3

    baseline = baseline_digests(full)

    # Mutate: change one file, add one, delete one. `keep.txt` untouched.
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/change.txt", ["rewritten-bigger"])
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/new.txt", ["brand-new"])
    {:ok, gone} = FileIndex.get_by_path(src.id, "/gone.txt")
    :ok = FileIndex.delete(gone.id)

    inc = Path.join(tmp_dir, "inc.tar")
    assert {:ok, inc_summary} = VolumeExport.export(src.name, inc, baseline_digests: baseline)

    # Only the changed + new files carry bodies; keep.txt is carried.
    assert inc_summary.file_count == 2
    assert File.stat!(inc).size < File.stat!(full).size

    assert {:ok, dst} = restore_volume([full, inc], "inc-dst")

    assert {:ok, "unchanged-content"} == read_file("inc-dst", "/keep.txt")
    assert {:ok, "rewritten-bigger"} == read_file("inc-dst", "/change.txt")
    assert {:ok, "brand-new"} == read_file("inc-dst", "/new.txt")
    assert {:error, :not_found} == FileIndex.get_by_path(dst.id, "/gone.txt")
  end

  test "an unchanged incremental carries everything (no bodies) and restores identically",
       %{tmp_dir: tmp_dir} do
    {:ok, src} = VolumeRegistry.create("noop-src", [])
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/a.txt", ["aaa"])
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/b.txt", ["bbbb"])

    full = Path.join(tmp_dir, "full.tar")
    assert {:ok, _} = VolumeExport.export(src.name, full)

    inc = Path.join(tmp_dir, "inc.tar")

    assert {:ok, summary} =
             VolumeExport.export(src.name, inc, baseline_digests: baseline_digests(full))

    # Nothing changed → no file bodies in the incremental.
    assert summary.file_count == 0
    assert summary.byte_count == 0

    assert {:ok, _} = restore_volume([full, inc], "noop-dst")
    assert {:ok, "aaa"} == read_file("noop-dst", "/a.txt")
    assert {:ok, "bbbb"} == read_file("noop-dst", "/b.txt")
  end

  defp baseline_digests(archive_path) do
    {:ok, manifest} = Backup.describe(archive_path)
    for %{"path" => p, "content_digest" => d} <- manifest["files"], into: %{}, do: {p, d}
  end

  defp restore_volume(archives, name) do
    with {:ok, summary} <- VolumeImport.restore_chain(archives, name) do
      VolumeRegistry.get(summary.volume_id)
    end
  end

  defp read_file(volume_name, path) do
    {:ok, %{stream: stream}} = Core.read_file_stream(volume_name, path)
    {:ok, stream |> Enum.to_list() |> IO.iodata_to_binary()}
  end
end
