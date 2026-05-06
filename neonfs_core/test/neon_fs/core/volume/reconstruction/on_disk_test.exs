defmodule NeonFS.Core.Volume.Reconstruction.OnDiskTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Volume.Reconstruction.OnDisk

  @moduletag :tmp_dir

  describe "list_candidate_hashes/1" do
    test "returns empty list when blobs/ is missing", %{tmp_dir: tmp_dir} do
      assert OnDisk.list_candidate_hashes(tmp_dir) == []
    end

    test "extracts the 32-byte hash from each chunk filename across tiers", %{tmp_dir: tmp_dir} do
      hash_a = :crypto.strong_rand_bytes(32)
      hash_b = :crypto.strong_rand_bytes(32)

      write_chunk(tmp_dir, "hot", hash_a, "abcd1234abcd1234")
      write_chunk(tmp_dir, "warm", hash_b, "deadbeefdeadbeef")

      assert hashes = OnDisk.list_candidate_hashes(tmp_dir)
      assert Enum.sort([hash_a, hash_b]) == Enum.sort(hashes)
    end

    test "deduplicates when the same hash appears under multiple codec suffixes",
         %{tmp_dir: tmp_dir} do
      hash = :crypto.strong_rand_bytes(32)

      write_chunk(tmp_dir, "hot", hash, "abcd1234abcd1234")
      write_chunk(tmp_dir, "hot", hash, "11112222aaaabbbb")

      assert [^hash] = OnDisk.list_candidate_hashes(tmp_dir)
    end

    test "skips files that aren't `<64hex>.<suffix>` shaped", %{tmp_dir: tmp_dir} do
      File.mkdir_p!(Path.join([tmp_dir, "blobs", "hot", "ab"]))
      File.write!(Path.join([tmp_dir, "blobs", "hot", "ab", "garbage.txt"]), "")
      File.write!(Path.join([tmp_dir, "blobs", "hot", "ab", "tooshort.codec"]), "")

      assert OnDisk.list_candidate_hashes(tmp_dir) == []
    end

    test "skips files whose hex prefix isn't valid hex", %{tmp_dir: tmp_dir} do
      File.mkdir_p!(Path.join([tmp_dir, "blobs", "hot", "zz"]))

      File.write!(
        Path.join([tmp_dir, "blobs", "hot", "zz", String.duplicate("z", 64) <> ".codec"]),
        ""
      )

      assert OnDisk.list_candidate_hashes(tmp_dir) == []
    end
  end

  describe "read_chunk/2" do
    test "returns {:error, :not_found} when no matching file", %{tmp_dir: tmp_dir} do
      hash = :crypto.strong_rand_bytes(32)
      assert {:error, :not_found} = OnDisk.read_chunk(tmp_dir, hash)
    end

    test "reads the bytes from the first matching file", %{tmp_dir: tmp_dir} do
      hash = :crypto.strong_rand_bytes(32)
      write_chunk(tmp_dir, "hot", hash, "abcd1234abcd1234", "the bytes")

      assert {:ok, "the bytes"} = OnDisk.read_chunk(tmp_dir, hash)
    end

    test "skips .tmp.* files left over from in-progress atomic writes", %{tmp_dir: tmp_dir} do
      hash = :crypto.strong_rand_bytes(32)

      hex = Base.encode16(hash, case: :lower)
      prefix_dir = Path.join([tmp_dir, "blobs", "hot", String.slice(hex, 0..1)])
      File.mkdir_p!(prefix_dir)
      File.write!(Path.join(prefix_dir, "#{hex}.tmp.123"), "garbage")

      assert {:error, :not_found} = OnDisk.read_chunk(tmp_dir, hash)
    end
  end

  ## Helpers

  defp write_chunk(drive_path, tier, hash, codec_suffix, contents \\ "") do
    hex = Base.encode16(hash, case: :lower)
    prefix1 = String.slice(hex, 0..1)
    prefix2 = String.slice(hex, 2..3)

    dir = Path.join([drive_path, "blobs", tier, prefix1, prefix2])
    File.mkdir_p!(dir)

    File.write!(Path.join(dir, "#{hex}.#{codec_suffix}"), contents)
  end
end
