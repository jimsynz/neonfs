defmodule NeonFS.Core.DetachedFileGCTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{DetachedFileGC, FileIndex, FileMeta}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    store = :ets.new(:dgc_test_store, [:set, :public])

    metadata_reader_opts = build_mock_metadata_reader_opts(store)
    metadata_writer_opts = build_mock_metadata_writer_opts(store)

    stop_if_running(NeonFS.Core.FileIndex)
    stop_if_running(NeonFS.Core.DetachedFileGC)
    cleanup_ets_table(:file_index_by_id)

    start_supervised!(
      {NeonFS.Core.FileIndex,
       metadata_reader_opts: metadata_reader_opts,
       metadata_writer_opts: metadata_writer_opts},
      restart: :temporary
    )

    start_supervised!({NeonFS.Core.DetachedFileGC, []}, restart: :temporary)

    on_exit(fn ->
      cleanup_test_dirs()

      try do
        :ets.delete(store)
      rescue
        ArgumentError -> :ok
      end
    end)

    %{store: store}
  end

  describe "single-claim release telemetry" do
    test "decrements the pin and purges when it was the last one" do
      {:ok, created} = FileIndex.create(FileMeta.new("vol1", "/last.txt"))
      {:ok, _} = FileIndex.mark_detached(created.id, ["only-pin"])

      :telemetry.execute(
        [:neonfs, :ra, :command, :release_namespace_claim],
        %{version: 1, released: 1},
        %{
          claim_id: "only-pin",
          claim_path: "/last.txt",
          claim_type: :pinned,
          claim_holder: self()
        }
      )

      assert {:error, :not_found} = FileIndex.get(created.volume_id, created.id)
    end

    test "leaves the tombstone in place when other pins remain" do
      {:ok, created} = FileIndex.create(FileMeta.new("vol1", "/some.txt"))
      {:ok, _} = FileIndex.mark_detached(created.id, ["c1", "c2"])

      :telemetry.execute(
        [:neonfs, :ra, :command, :release_namespace_claim],
        %{version: 1, released: 1},
        %{claim_id: "c1", claim_path: "/some.txt", claim_type: :pinned, claim_holder: self()}
      )

      assert {:ok, %FileMeta{detached: true, pinned_claim_ids: ids}} =
               FileIndex.get(created.volume_id, created.id)

      assert ids == ["c2"]
    end

    test "is a no-op for a release of a claim no detached file references" do
      {:ok, created} = FileIndex.create(FileMeta.new("vol1", "/unrelated.txt"))
      {:ok, _} = FileIndex.mark_detached(created.id, ["mine"])

      :telemetry.execute(
        [:neonfs, :ra, :command, :release_namespace_claim],
        %{version: 1, released: 1},
        %{
          claim_id: "someone-else",
          claim_path: "/elsewhere",
          claim_type: :pinned,
          claim_holder: self()
        }
      )

      assert {:ok, %FileMeta{detached: true, pinned_claim_ids: ["mine"]}} =
               FileIndex.get(created.volume_id, created.id)
    end

    test "tolerates an idempotent re-fire (claim already released)" do
      {:ok, created} = FileIndex.create(FileMeta.new("vol1", "/idem.txt"))
      {:ok, _} = FileIndex.mark_detached(created.id, ["c1"])

      meta = %{claim_id: "c1", claim_path: "/idem.txt", claim_type: :pinned, claim_holder: self()}

      :telemetry.execute(
        [:neonfs, :ra, :command, :release_namespace_claim],
        %{version: 1, released: 1},
        meta
      )

      # File is already gone now. A re-fire of the same telemetry event
      # — Ra command replay or a stale subscriber — must not error.
      assert {:error, :not_found} = FileIndex.get(created.volume_id, created.id)

      :telemetry.execute(
        [:neonfs, :ra, :command, :release_namespace_claim],
        %{version: 1, released: 1},
        meta
      )

      assert {:error, :not_found} = FileIndex.get(created.volume_id, created.id)
    end
  end

  describe "bulk-release telemetry (holder DOWN)" do
    test "decrements every released claim id" do
      {:ok, file_a} = FileIndex.create(FileMeta.new("vol1", "/a.txt"))
      {:ok, file_b} = FileIndex.create(FileMeta.new("vol1", "/b.txt"))
      {:ok, file_c} = FileIndex.create(FileMeta.new("vol1", "/c.txt"))

      {:ok, _} = FileIndex.mark_detached(file_a.id, ["holder-a-1"])
      {:ok, _} = FileIndex.mark_detached(file_b.id, ["holder-a-2", "other-holder"])
      {:ok, _} = FileIndex.mark_detached(file_c.id, ["unrelated"])

      :telemetry.execute(
        [:neonfs, :ra, :command, :release_namespace_claims_for_holder],
        %{version: 1, released: 2},
        %{released_claim_ids: ["holder-a-1", "holder-a-2"], holder: self()}
      )

      # /a's last pin released → purged.
      assert {:error, :not_found} = FileIndex.get(file_a.volume_id, file_a.id)

      # /b had two pins; only one was held by the dead holder → still detached.
      assert {:ok, %FileMeta{detached: true, pinned_claim_ids: ["other-holder"]}} =
               FileIndex.get(file_b.volume_id, file_b.id)

      # /c wasn't pinned by the dead holder → untouched.
      assert {:ok, %FileMeta{detached: true, pinned_claim_ids: ["unrelated"]}} =
               FileIndex.get(file_c.volume_id, file_c.id)
    end

    test "tolerates an empty released_claim_ids list" do
      {:ok, file} = FileIndex.create(FileMeta.new("vol1", "/keep.txt"))
      {:ok, _} = FileIndex.mark_detached(file.id, ["c1"])

      :telemetry.execute(
        [:neonfs, :ra, :command, :release_namespace_claims_for_holder],
        %{version: 1, released: 0},
        %{released_claim_ids: [], holder: self()}
      )

      assert {:ok, %FileMeta{detached: true, pinned_claim_ids: ["c1"]}} =
               FileIndex.get(file.volume_id, file.id)
    end
  end

  describe "telemetry handler lifecycle" do
    test "detaches its handler on shutdown" do
      handler_id = "neonfs-detached-file-gc"

      handlers = :telemetry.list_handlers([:neonfs, :ra, :command, :release_namespace_claim])
      assert Enum.any?(handlers, &(&1.id == handler_id))

      :ok = stop_supervised(DetachedFileGC)

      handlers_after =
        :telemetry.list_handlers([:neonfs, :ra, :command, :release_namespace_claim])

      refute Enum.any?(handlers_after, &(&1.id == handler_id))
    end
  end

  defp stop_if_running(name) do
    case Process.whereis(name) do
      nil ->
        :ok

      pid ->
        ref = Process.monitor(pid)
        GenServer.stop(pid, :normal, 5000)

        receive do
          {:DOWN, ^ref, :process, ^pid, _} -> :ok
        after
          1_000 -> :ok
        end
    end
  end

  defp cleanup_ets_table(table) do
    case :ets.whereis(table) do
      :undefined -> :ok
      ref -> :ets.delete(ref)
    end
  end
end
