defmodule NeonFS.Core.FileIndexLeaseReleaseTest do
  @moduledoc """
  Which side of the commit an operation's conflict lease is released on.

  A lease has to outlive the operation's mutations and no longer. Released
  before the publication, a concurrent writer can interleave. Released
  after it, from `on_commit`, a publisher that does not survive its own
  success strands the lease for the intent's whole TTL — and for rename
  and move the conflict key is the parent directory, so a badly-timed
  crash refuses every rename and move in a directory whose rename already
  committed.

  So the release travels *in* the publishing log entry. These tests pin
  that: a committed batch carries its lease ids on the command, and only a
  batch that never published falls back to failing the intent through
  `IntentLog`.

  `file_index_intent_lease_test.exs` covers the other half of the lease's
  lifecycle — acquisition failing closed.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{FileIndex, FileMeta}

  @moduletag :tmp_dir

  @calls :file_index_lease_release_calls

  defmodule RecordingIntentLog do
    @moduledoc false

    alias NeonFS.Core.Intent

    @calls :file_index_lease_release_calls

    def try_acquire(%Intent{} = intent) do
      record({:acquire, intent.id, intent.conflict_key})
      {:ok, intent.id}
    end

    def complete(intent_id) do
      record({:complete, intent_id})
      :ok
    end

    def fail(intent_id, reason) do
      record({:fail, intent_id, reason})
      :ok
    end

    defp record(call), do: :ets.insert(@calls, {:call, call})
  end

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    store = :ets.new(:lease_release_store, [:set, :public])
    calls = :ets.new(@calls, [:duplicate_bag, :public, :named_table])
    commands = :ets.new(:lease_release_commands, [:duplicate_bag, :public])

    on_exit(fn ->
      cleanup_test_dirs()

      for table <- [store, calls, commands] do
        try do
          :ets.delete(table)
        rescue
          ArgumentError -> :ok
        end
      end
    end)

    %{store: store, commands: commands}
  end

  test "a committed batch releases its leases through the publishing command", ctx do
    start_index(ctx, accepting_registrar(ctx))

    assert {:ok, _} = FileIndex.create(FileMeta.new("vol1", "/leased/before.bin"))
    reset(ctx)

    assert :ok = FileIndex.rename("vol1", "/leased", "before.bin", "after.bin")

    assert [intent_id] = acquired_ids()
    assert [{:cas_update_volume_roots, "vol1", _roots, released}] = commands(ctx)

    assert released == [intent_id],
           "the publication must carry the lease it releases, not release it afterwards"

    refute {:complete, intent_id} in calls(),
           "a separate completion command reopens the window this closes"
  end

  test "every lease in a coalesced batch rides the same command", ctx do
    start_index(ctx, accepting_registrar(ctx))
    reset(ctx)

    # Concurrent callers coalesce into one flush, so the batch is the unit
    # that releases — one command, every participant's lease.
    1..4
    |> Task.async_stream(
      fn n -> FileIndex.create(FileMeta.new("vol1", "/coalesced/#{n}.bin")) end,
      max_concurrency: 4,
      timeout: 30_000
    )
    |> Stream.run()

    released = commands(ctx) |> Enum.flat_map(fn {_cmd, _vol, _roots, ids} -> ids end)

    assert Enum.sort(released) == Enum.sort(acquired_ids()),
           "no lease may be left behind by the batch that published its mutations"
  end

  test "a batch that never published fails its intent instead", ctx do
    start_index(ctx, refusing_registrar(ctx))

    assert {:error, _} = FileIndex.create(FileMeta.new("vol1", "/aborted/file.bin"))

    assert [intent_id] = acquired_ids()

    # No entry carried the release, and waiting out the TTL for a write
    # that never happened would block the caller's own retry on the very
    # key it is retrying.
    assert Enum.any?(calls(), &match?({:fail, ^intent_id, _reason}, &1)),
           "an aborted operation must free its conflict key, got #{inspect(calls())}"
  end

  test "an operation that holds no lease publishes an empty release list", ctx do
    start_index(ctx, accepting_registrar(ctx))

    assert {:ok, file} = FileIndex.create(FileMeta.new("vol1", "/unleased/file.bin"))
    reset(ctx)

    assert {:ok, _} = FileIndex.touch(file.id)

    assert [{:cas_update_volume_roots, "vol1", _roots, []}] = commands(ctx)
    assert acquired_ids() == []
  end

  defp start_index(%{store: store}, registrar) do
    writer_opts =
      store
      |> build_mock_metadata_writer_opts()
      |> Keyword.put(:bootstrap_registrar, registrar)

    start_file_index(
      metadata_reader_opts: build_mock_metadata_reader_opts(store),
      metadata_writer_opts: writer_opts,
      intent_log: RecordingIntentLog
    )
  end

  defp accepting_registrar(%{commands: commands}) do
    fn command ->
      :ets.insert(commands, {:command, command})
      {:ok, :updated}
    end
  end

  # An unambiguous non-commit: the command definitely did not reach
  # consensus, so the writer surfaces the error rather than re-submitting.
  defp refusing_registrar(%{commands: commands}) do
    fn command ->
      :ets.insert(commands, {:command, command})
      {:error, :no_leader}
    end
  end

  defp reset(%{commands: commands}) do
    :ets.delete_all_objects(commands)
    :ets.delete_all_objects(@calls)
  end

  defp commands(%{commands: commands}) do
    commands |> :ets.lookup(:command) |> Enum.map(fn {:command, command} -> command end)
  end

  defp calls do
    @calls |> :ets.lookup(:call) |> Enum.map(fn {:call, call} -> call end)
  end

  defp acquired_ids do
    for {:acquire, id, _conflict_key} <- calls(), do: id
  end
end
