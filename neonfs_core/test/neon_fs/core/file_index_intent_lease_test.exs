defmodule NeonFS.Core.FileIndexIntentLeaseTest do
  @moduledoc """
  The conflict lease must fail closed (#1631).

  `FileIndex` used to wrap `IntentLog.try_acquire/1` in a helper that
  returned `{:ok, intent.id}` — a **fabricated** id — whenever Ra was
  unavailable, and from a bare `rescue` and `catch :exit` besides. The
  operation then proceeded with nothing serialising it against a
  concurrent writer on another node: mutual exclusion disappeared exactly
  when contention was most likely.

  These tests drive `FileIndex` with a lease that never grants
  (`RefusingIntentLog`, standing in for an unreachable Ra) and assert that
  each mutation surfaces the failure instead of pretending it holds a
  lease.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{FileIndex, FileMeta}
  alias NeonFS.TestSupport.{RefusingIntentLog, StubIntentLog}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    store = :ets.new(:lease_test_store, [:set, :public])
    on_exit(fn -> cleanup_test_dirs() end)
    %{store: store}
  end

  defp start_index(store, intent_log) do
    start_file_index(
      metadata_reader_opts: build_mock_metadata_reader_opts(store),
      metadata_writer_opts: build_mock_metadata_writer_opts(store),
      intent_log: intent_log
    )
  end

  # Swapping the injected lease after seeding lets a test create its
  # fixture through a granting lease and then exercise the mutation under
  # an unreachable one.
  defp refuse_further_leases do
    :persistent_term.put({FileIndex, :intent_log}, RefusingIntentLog)
    on_exit(fn -> :persistent_term.erase({FileIndex, :intent_log}) end)
  end

  describe "create" do
    test "refuses instead of proceeding unserialised", %{store: store} do
      start_index(store, RefusingIntentLog)

      assert {:error, %{class: :unavailable}} =
               FileIndex.create(FileMeta.new("vol-lease", "/a.txt"))
    end

    test "publishes nothing when the lease is refused", %{store: store} do
      start_index(store, RefusingIntentLog)

      {:error, _} = FileIndex.create(FileMeta.new("vol-lease", "/ghost.txt"))

      assert {:error, :not_found} = FileIndex.get_by_path("vol-lease", "/ghost.txt")
    end

    test "proceeds when the lease is granted", %{store: store} do
      start_index(store, StubIntentLog)

      assert {:ok, %FileMeta{path: "/a.txt"}} =
               FileIndex.create(FileMeta.new("vol-lease", "/a.txt"))
    end
  end

  describe "mutating an existing file" do
    setup %{store: store} do
      start_index(store, StubIntentLog)
      {:ok, seeded} = FileIndex.create(FileMeta.new("vol-lease", "/doomed.txt"))
      %{seeded: seeded}
    end

    test "rename refuses when the lease is unreachable" do
      refuse_further_leases()

      assert {:error, %{class: :unavailable}} =
               FileIndex.rename("vol-lease", "/", "doomed.txt", "renamed.txt")
    end

    test "rename leaves the original name in place when refused" do
      refuse_further_leases()

      {:error, _} = FileIndex.rename("vol-lease", "/", "doomed.txt", "renamed.txt")

      assert {:ok, _} = FileIndex.get_by_path("vol-lease", "/doomed.txt")
      assert {:error, :not_found} = FileIndex.get_by_path("vol-lease", "/renamed.txt")
    end

    # `plan_delete_file/2`'s error arm was already written for "the intent
    # can't be acquired (e.g. quorum down)": it drops the file from the
    # local ETS cache so GC can still identify orphaned chunks, rather than
    # deadlocking a full disk. The degradation meant that arm never ran on
    # the unavailable path; now it does.
    test "delete falls back to a local-only delete rather than a phantom quorum delete", %{
      seeded: seeded
    } do
      refuse_further_leases()

      assert :ok = FileIndex.delete(seeded.id)
      assert [] = :ets.lookup(:file_index_by_id, seeded.id)
    end
  end
end
