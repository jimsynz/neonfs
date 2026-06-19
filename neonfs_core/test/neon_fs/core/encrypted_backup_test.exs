defmodule NeonFS.Core.EncryptedBackupTest do
  @moduledoc """
  At-rest encrypted backups (#1004): an encrypted export wraps a
  per-archive content key under the passphrase, frames each file body
  with AES-256-GCM, and keeps the manifest plaintext. Import with the
  passphrase reproduces content byte-for-byte; a wrong/missing
  passphrase fails fast with nothing written.

  Exercised via live-root `VolumeExport.export` (real bytes through the
  in-process BlobStore), mirroring the incremental backup test.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core

  alias NeonFS.Core.{
    Backup,
    BackupCrypto,
    VolumeExport,
    VolumeImport,
    VolumeRegistry,
    WriteOperation
  }

  @moduletag :tmp_dir
  @passphrase "correct horse battery staple"

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_core_subsystems()
    start_stripe_index()
    ensure_chunk_access_tracker()

    on_exit(fn -> cleanup_test_dirs() end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "encrypted export round-trips through import with the passphrase", %{tmp_dir: tmp_dir} do
    {:ok, src} = VolumeRegistry.create("enc-src", [])
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/alpha.txt", ["alpha-content"])
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/dir/beta.txt", ["beta-content"])

    archive = Path.join(tmp_dir, "enc.tar")
    assert {:ok, summary} = VolumeExport.export(src.name, archive, passphrase: @passphrase)
    assert summary.file_count == 2

    assert {:ok, _} = VolumeImport.import_archive(archive, "enc-dst", passphrase: @passphrase)
    assert {:ok, "alpha-content"} == read_file("enc-dst", "/alpha.txt")
    assert {:ok, "beta-content"} == read_file("enc-dst", "/dir/beta.txt")
  end

  test "the on-disk body is ciphertext, not the plaintext", %{tmp_dir: tmp_dir} do
    {:ok, src} = VolumeRegistry.create("enc-cipher", [])
    secret = "TOP-SECRET-PLAINTEXT-MARKER"
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/s.txt", [secret])

    archive = Path.join(tmp_dir, "enc.tar")
    assert {:ok, _} = VolumeExport.export(src.name, archive, passphrase: @passphrase)

    raw = File.read!(archive)
    refute String.contains?(raw, secret)
  end

  test "the manifest stays plaintext and carries the encryption envelope", %{tmp_dir: tmp_dir} do
    {:ok, src} = VolumeRegistry.create("enc-manifest", [])
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/x.txt", ["x"])

    archive = Path.join(tmp_dir, "enc.tar")
    assert {:ok, _} = VolumeExport.export(src.name, archive, passphrase: @passphrase)

    assert {:ok, manifest} = Backup.describe(archive)
    assert manifest["encryption"]["cipher"] == "AES-256-GCM"
    # Paths are not confidential in this slice — the manifest lists them.
    assert ["/x.txt"] == Enum.map(manifest["files"], & &1["path"])
  end

  test "a wrong passphrase fails fast and creates no volume", %{tmp_dir: tmp_dir} do
    {:ok, src} = VolumeRegistry.create("enc-wrong", [])
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/x.txt", ["x"])

    archive = Path.join(tmp_dir, "enc.tar")
    assert {:ok, _} = VolumeExport.export(src.name, archive, passphrase: @passphrase)

    assert {:error, :bad_passphrase} =
             VolumeImport.import_archive(archive, "enc-wrong-dst", passphrase: "nope")

    assert {:error, :not_found} = VolumeRegistry.get_by_name("enc-wrong-dst")
  end

  test "a missing passphrase on an encrypted archive errors", %{tmp_dir: tmp_dir} do
    {:ok, src} = VolumeRegistry.create("enc-missing", [])
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/x.txt", ["x"])

    archive = Path.join(tmp_dir, "enc.tar")
    assert {:ok, _} = VolumeExport.export(src.name, archive, passphrase: @passphrase)

    assert {:error, :passphrase_required} =
             VolumeImport.import_archive(archive, "enc-missing-dst")

    assert {:error, :not_found} = VolumeRegistry.get_by_name("enc-missing-dst")
  end

  test "a multi-frame file (larger than one frame) round-trips", %{tmp_dir: tmp_dir} do
    {:ok, src} = VolumeRegistry.create("enc-large", [])

    frame = BackupCrypto.frame_size()
    payload = :crypto.strong_rand_bytes(2 * frame + 12_345)
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/big.bin", [payload])

    archive = Path.join(tmp_dir, "enc.tar")
    assert {:ok, _} = VolumeExport.export(src.name, archive, passphrase: @passphrase)

    assert {:ok, _} =
             VolumeImport.import_archive(archive, "enc-large-dst", passphrase: @passphrase)

    assert {:ok, ^payload} = read_file("enc-large-dst", "/big.bin")
  end

  test "an encrypted incremental chain restores byte-for-byte", %{tmp_dir: tmp_dir} do
    {:ok, src} = VolumeRegistry.create("enc-chain-src", [])
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/keep.txt", ["unchanged"])
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/change.txt", ["original"])

    full = Path.join(tmp_dir, "full.tar")
    assert {:ok, _} = VolumeExport.export(src.name, full, passphrase: @passphrase)
    baseline = baseline_digests(full)

    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/change.txt", ["rewritten"])
    {:ok, _} = WriteOperation.write_file_streamed(src.id, "/new.txt", ["added"])

    inc = Path.join(tmp_dir, "inc.tar")

    assert {:ok, _} =
             VolumeExport.export(src.name, inc,
               passphrase: @passphrase,
               baseline_digests: baseline
             )

    assert {:ok, _} =
             VolumeImport.restore_chain([full, inc], "enc-chain-dst", passphrase: @passphrase)

    assert {:ok, "unchanged"} == read_file("enc-chain-dst", "/keep.txt")
    assert {:ok, "rewritten"} == read_file("enc-chain-dst", "/change.txt")
    assert {:ok, "added"} == read_file("enc-chain-dst", "/new.txt")
  end

  defp baseline_digests(archive_path) do
    {:ok, manifest} = Backup.describe(archive_path)
    for %{"path" => p, "content_digest" => d} <- manifest["files"], into: %{}, do: {p, d}
  end

  defp read_file(volume_name, path) do
    {:ok, %{stream: stream}} = Core.read_file_stream(volume_name, path)
    {:ok, stream |> Enum.to_list() |> IO.iodata_to_binary()}
  end
end
