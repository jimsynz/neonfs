defmodule NeonFS.Core.PendingWriteLogTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.PendingWriteLog

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    Application.put_env(:neonfs_core, :meta_dir, tmp_dir)
    :ok = PendingWriteLog.open(meta_dir: tmp_dir)

    on_exit(fn ->
      PendingWriteLog.close()
      Application.delete_env(:neonfs_core, :meta_dir)
    end)

    :ok
  end

  describe "open_write/3 + record_chunk/2 + get/1" do
    test "records start time, volume_id, and appended chunks" do
      :ok = PendingWriteLog.open_write("w-1", "vol-a", "/file.txt")
      :ok = PendingWriteLog.record_chunk("w-1", "hash-1")
      :ok = PendingWriteLog.record_chunk("w-1", "hash-2")

      {:ok, record} = PendingWriteLog.get("w-1")
      assert record.write_id == "w-1"
      assert record.volume_id == "vol-a"
      assert record.path == "/file.txt"
      # New chunks prepended, so the list is in reverse order of writes.
      assert record.chunk_hashes == ["hash-2", "hash-1"]
      assert %DateTime{} = record.started_at
    end

    test "record_chunk/2 on a missing write is a no-op" do
      assert :ok = PendingWriteLog.record_chunk("missing", "hash-x")
      assert {:error, :not_found} = PendingWriteLog.get("missing")
    end
  end

  describe "clear/1" do
    test "removes the record" do
      :ok = PendingWriteLog.open_write("w-2", "vol-b", "/foo")
      :ok = PendingWriteLog.clear("w-2")
      assert {:error, :not_found} = PendingWriteLog.get("w-2")
    end
  end

  describe "list_orphans/1" do
    test "returns only records older than the grace window" do
      :ok = PendingWriteLog.open_write("fresh", "vol-a", "/new")

      old_record = %{
        write_id: "old",
        volume_id: "vol-a",
        path: "/stale",
        chunk_hashes: ["h1", "h2"],
        started_at: DateTime.add(DateTime.utc_now(), -3600, :second)
      }

      :dets.insert(:pending_writes, {"old", old_record})
      :dets.sync(:pending_writes)

      orphans = PendingWriteLog.list_orphans(300)
      orphan_ids = Enum.map(orphans, & &1.write_id)

      assert "old" in orphan_ids
      refute "fresh" in orphan_ids
    end

    test "returns nothing when the window is large" do
      :ok = PendingWriteLog.open_write("w-3", "vol-a", "/foo")

      assert [] = PendingWriteLog.list_orphans(3600)
    end
  end

  describe "persistence across re-open" do
    test "records survive close/open cycles (crash simulation)" do
      :ok = PendingWriteLog.open_write("persist", "vol-a", "/p")
      :ok = PendingWriteLog.record_chunk("persist", "hash-p")

      PendingWriteLog.close()

      assert :ok = PendingWriteLog.open(meta_dir: Application.fetch_env!(:neonfs_core, :meta_dir))
      assert {:ok, record} = PendingWriteLog.get("persist")
      assert record.chunk_hashes == ["hash-p"]
    end
  end
end
