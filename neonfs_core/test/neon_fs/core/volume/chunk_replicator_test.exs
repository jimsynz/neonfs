defmodule NeonFS.Core.Volume.ChunkReplicatorTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Volume.ChunkReplicator

  describe "write_chunk/3" do
    test "returns {:ok, hash, summary} when all drives succeed" do
      drives = [drive_entry("drv-1"), drive_entry("drv-2"), drive_entry("drv-3")]
      writer = fn _data, _drive_id, _tier, _opts -> {:ok, "hashbytes", %{size: 7}} end

      assert {:ok, "hashbytes", summary} =
               ChunkReplicator.write_chunk("payload", drives,
                 min_copies: 2,
                 writer_fn: writer
               )

      assert Enum.sort(summary.successful) == ["drv-1", "drv-2", "drv-3"]
      assert summary.failed == []
    end

    test "succeeds when min_copies are reached even if some drives fail" do
      drives = [drive_entry("drv-1"), drive_entry("drv-2"), drive_entry("drv-3")]

      writer =
        per_drive(%{
          "drv-1" => {:ok, "h", %{}},
          "drv-2" => {:error, "io error"},
          "drv-3" => {:ok, "h", %{}}
        })

      assert {:ok, "h", summary} =
               ChunkReplicator.write_chunk("payload", drives,
                 min_copies: 2,
                 writer_fn: writer
               )

      assert Enum.sort(summary.successful) == ["drv-1", "drv-3"]
      assert summary.failed == [{"drv-2", "io error"}]
    end

    test "returns insufficient_replicas when below min_copies" do
      drives = [drive_entry("drv-1"), drive_entry("drv-2"), drive_entry("drv-3")]

      writer =
        per_drive(%{
          "drv-1" => {:ok, "h", %{}},
          "drv-2" => {:error, "io error"},
          "drv-3" => {:error, "no space"}
        })

      assert {:error, :insufficient_replicas, info} =
               ChunkReplicator.write_chunk("payload", drives,
                 min_copies: 2,
                 writer_fn: writer
               )

      assert info.needed == 2
      assert info.successful == ["drv-1"]
      assert Enum.sort(info.failed) == [{"drv-2", "io error"}, {"drv-3", "no space"}]
    end

    test "returns insufficient_replicas when all drives fail" do
      drives = [drive_entry("drv-1"), drive_entry("drv-2")]

      writer =
        per_drive(%{
          "drv-1" => {:error, "fail"},
          "drv-2" => {:error, "fail"}
        })

      assert {:error, :insufficient_replicas, info} =
               ChunkReplicator.write_chunk("payload", drives,
                 min_copies: 1,
                 writer_fn: writer
               )

      assert info.successful == []
      assert info.needed == 1
    end

    test "rescues exceptions raised by the writer" do
      drives = [drive_entry("drv-1")]
      writer = fn _, _, _, _ -> raise "boom" end

      assert {:error, :insufficient_replicas, info} =
               ChunkReplicator.write_chunk("payload", drives,
                 min_copies: 1,
                 writer_fn: writer
               )

      assert [{"drv-1", "boom"}] = info.failed
    end

    test "writes happen concurrently" do
      drives = [drive_entry("drv-1"), drive_entry("drv-2"), drive_entry("drv-3")]

      writer = fn _data, _drive_id, _tier, _opts ->
        Process.sleep(100)
        {:ok, "h", %{}}
      end

      start = System.monotonic_time(:millisecond)

      assert {:ok, _, _} =
               ChunkReplicator.write_chunk("payload", drives,
                 min_copies: 2,
                 writer_fn: writer
               )

      elapsed = System.monotonic_time(:millisecond) - start

      assert elapsed < 250, "elapsed=#{elapsed}ms — expected concurrent execution"
    end

    test "emits :write telemetry on success" do
      drives = [drive_entry("drv-1"), drive_entry("drv-2")]
      writer = fn _, _, _, _ -> {:ok, "thehash", %{}} end

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :volume, :chunk_replicator, :write]
        ])

      assert {:ok, _, _} =
               ChunkReplicator.write_chunk("payload", drives,
                 min_copies: 2,
                 writer_fn: writer
               )

      assert_receive {[:neonfs, :volume, :chunk_replicator, :write], ^ref, measurements, metadata}

      assert measurements.successful == 2
      assert measurements.failed == 0
      assert metadata.hash == "thehash"
      assert metadata.total == 2
    end

    test "emits :insufficient_replicas telemetry on quorum failure" do
      drives = [drive_entry("drv-1"), drive_entry("drv-2")]

      writer =
        per_drive(%{
          "drv-1" => {:error, "boom"},
          "drv-2" => {:error, "boom"}
        })

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :volume, :chunk_replicator, :insufficient_replicas]
        ])

      assert {:error, :insufficient_replicas, _} =
               ChunkReplicator.write_chunk("payload", drives,
                 min_copies: 1,
                 writer_fn: writer
               )

      assert_receive {[:neonfs, :volume, :chunk_replicator, :insufficient_replicas], ^ref,
                      measurements, metadata}

      assert measurements.successful == 0
      assert measurements.failed == 2
      assert metadata.needed == 1
      assert metadata.total == 2
    end

    test "raises when :min_copies is missing" do
      assert_raise KeyError, fn ->
        ChunkReplicator.write_chunk("data", [drive_entry("drv-1")],
          writer_fn: fn _, _, _, _ -> {:ok, "h", %{}} end
        )
      end
    end
  end

  ## Helpers

  defp drive_entry(drive_id) do
    %{
      drive_id: drive_id,
      node: node(),
      cluster_id: "clust",
      on_disk_format_version: 1,
      registered_at: DateTime.utc_now()
    }
  end

  defp per_drive(per_drive_results) do
    fn _data, drive_id, _tier, _opts -> Map.fetch!(per_drive_results, drive_id) end
  end
end
