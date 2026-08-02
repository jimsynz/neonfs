defmodule NeonFS.Core.ReplicaCodecTest do
  @moduledoc """
  Every copy of a chunk must be stored under the codec its metadata records.

  Blobs live at `<hash>.<codec-suffix>`, where the suffix is a fingerprint of
  the compression and encryption used. Readers derive that suffix from
  `ChunkMeta`, so a replica written under a different codec is unreadable
  through any metadata-driven path — and, worse, still counts as a copy for
  every check that consults `locations`, which is what makes it a data-loss
  vector rather than a read error.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{ChunkIndex, VolumeRegistry}

  @moduletag :tmp_dir
  @moduletag timeout: 120_000

  setup do
    on_exit(fn ->
      stop_ra()
      cleanup_test_dirs()
    end)

    :ok
  end

  test "every replica of a compressed chunk lands under the recorded codec", %{tmp_dir: tmp_dir} do
    drives =
      for n <- 1..2 do
        path = Path.join(tmp_dir, "drive#{n}")
        File.mkdir_p!(path)
        %{id: "drive#{n}", path: path, tier: :hot, capacity: 0}
      end

    {:ok, _} = start_provisioned_cluster(tmp_dir, drives: drives)

    {:ok, volume} =
      VolumeRegistry.create("compressed",
        durability: %{type: :replicate, factor: 2, min_copies: 1},
        # Synchronous, so the replica is on disk before the assertions run and
        # no background placement supervisor is needed.
        write_ack: :all,
        compression: %{algorithm: :zstd, level: 3, min_size: 0}
      )

    {:ok, _} =
      NeonFS.Core.write_file_at(volume.name, "/f.bin", 0, :crypto.strong_rand_bytes(32 * 1024))

    {:ok, chunks} = ChunkIndex.list_volume_chunks(volume.id)
    refute chunks == [], "expected the write to produce at least one chunk"

    replicated = Enum.filter(chunks, &(length(&1.locations) > 1))

    refute replicated == [],
           "expected at least one chunk with more than one location, otherwise this test " <>
             "asserts nothing about replicas"

    for chunk <- replicated do
      suffixes =
        chunk.locations
        |> Enum.flat_map(fn loc ->
          hex = Base.encode16(chunk.hash, case: :lower)

          Path.wildcard(Path.join([tmp_dir, "**", "#{hex}.*"]))
          |> Enum.filter(&String.contains?(&1, loc.drive_id))
          |> Enum.map(&(&1 |> Path.extname() |> String.trim_leading(".")))
        end)
        |> Enum.uniq()

      assert length(suffixes) <= 1,
             "chunk #{Base.encode16(chunk.hash, case: :lower)} is stored under more than one " <>
               "codec across its replicas (#{inspect(suffixes)}); readers derive the suffix " <>
               "from ChunkMeta, so every copy but one is unreadable"
    end
  end
end
