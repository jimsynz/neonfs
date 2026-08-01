defmodule NeonFS.Core.Volume.MetadataWriterRootSetTest do
  @moduledoc """
  A batch spanning several shards must publish them all in one command.

  The rest of the `neonfs_core` unit suite pins `metadata_shard_count` to 1,
  which cannot distinguish "one command per participant" from "one command
  for the set" — the bug this covers only appears above one shard. These
  tests raise the count, so they mutate global application env and run
  synchronously.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.Volume.{MetadataValue, MetadataWriter, Shard}

  @shard_count 64

  setup do
    previous = Application.get_env(:neonfs_core, :metadata_shard_count)
    Application.put_env(:neonfs_core, :metadata_shard_count, @shard_count)
    on_exit(fn -> Application.put_env(:neonfs_core, :metadata_shard_count, previous) end)

    store = :ets.new(:root_set_store, [:set, :public])
    commands = :ets.new(:root_set_commands, [:duplicate_bag, :public])

    on_exit(fn ->
      for table <- [store, commands] do
        try do
          :ets.delete(table)
        rescue
          ArgumentError -> :ok
        end
      end
    end)

    %{store: store, commands: commands}
  end

  test "a cross-shard batch publishes every participant in one command", ctx do
    mutations = mutations_on_distinct_shards(3)
    shards = Enum.map(mutations, &Shard.for_key(elem(&1, 2)))

    assert {:ok, roots} =
             MetadataWriter.apply_batch("vol-1", mutations, capturing_opts(ctx))

    assert Enum.sort(Map.keys(roots)) == Enum.sort(shards)

    assert [{:cas_update_volume_roots, "vol-1", published}] = captured(ctx)
    assert Enum.sort(Map.keys(published)) == Enum.sort(shards)
  end

  test "every published shard carries the root its segment was built against", ctx do
    assert {:ok, _roots} =
             MetadataWriter.apply_batch(
               "vol-1",
               mutations_on_distinct_shards(2),
               capturing_opts(ctx)
             )

    assert [{:cas_update_volume_roots, _volume_id, published}] = captured(ctx)

    Enum.each(published, fn {_shard, {expected, updates}} ->
      # The shared mock resolves every shard to the same bootstrap entry.
      assert expected == <<0::256>>
      assert is_map_key(updates, :root_chunk_hash)
      assert is_map_key(updates, :drive_locations)
    end)
  end

  test "a stale expectation on one shard rebuilds the whole set", ctx do
    mutations = mutations_on_distinct_shards(3)
    shard_count = mutations |> Enum.map(&Shard.for_key(elem(&1, 2))) |> Enum.uniq() |> length()

    assert {:ok, _roots} =
             MetadataWriter.apply_batch(
               "vol-1",
               mutations,
               capturing_opts(ctx, stale_attempts: 1)
             )

    # Two attempts, and the retry re-published every participant rather than
    # only the shard whose expectation went stale.
    assert [first, second] = captured(ctx)
    assert {:cas_update_volume_roots, "vol-1", first_roots} = first
    assert {:cas_update_volume_roots, "vol-1", second_roots} = second
    assert map_size(first_roots) == shard_count
    assert map_size(second_roots) == shard_count
  end

  test "a permanently stale set exhausts the retry budget", ctx do
    opts = capturing_opts(ctx, stale_attempts: :infinity, cas_retries: 2)

    assert {:error, {:cas_retries_exhausted, _}} =
             MetadataWriter.apply_batch("vol-1", mutations_on_distinct_shards(2), opts)
  end

  test "a single-shard batch still publishes through the root-set command", ctx do
    mutations = [put_mutation("only-key")]

    assert {:ok, roots} = MetadataWriter.apply_batch("vol-1", mutations, capturing_opts(ctx))
    assert map_size(roots) == 1

    assert [{:cas_update_volume_roots, "vol-1", published}] = captured(ctx)
    assert map_size(published) == 1
  end

  # ── Fault injection at the commit boundaries (#1633) ────────────────────
  #
  # #1589's acceptance bar. Each of these injects a failure at one boundary
  # and asserts the property the atomic root set is supposed to give: a
  # reader sees all-old or all-new, never a subset.

  test "a failure before publication leaves every participant on its old root", ctx do
    mutations = mutations_on_distinct_shards(3)
    opts = capturing_opts(ctx, registrar_result: {:error, :no_leader})

    assert {:error, {:bootstrap_update_failed, :no_leader}} =
             MetadataWriter.apply_batch("vol-1", mutations, opts)

    # One command was attempted and rejected. Nothing advanced: a later
    # successful batch still builds against the original expectation, which is
    # the observable form of "no participant moved". The
    # replicated-but-unreferenced segment chunks are GC debt, the documented
    # trade — not partial state.
    assert [{:cas_update_volume_roots, "vol-1", _}] = captured(ctx)

    ctx = fresh_tables(ctx)
    assert {:ok, _} = MetadataWriter.apply_batch("vol-1", mutations, capturing_opts(ctx))
    assert [{:cas_update_volume_roots, _, published}] = captured(ctx)

    Enum.each(published, fn {_shard, {expected, _updates}} -> assert expected == <<0::256>> end)
  end

  # The mid-CAS boundary should be unreachable by construction: there is one
  # command, so there is no "after participant 2, before participant 3" for a
  # failure to land in. This asserts that rather than assuming it — a
  # regression to per-shard commands would make the command count grow with
  # the participant count.
  test "there is no partial-publication window to fail inside", ctx do
    for participants <- [2, 5, 9] do
      ctx = fresh_tables(ctx)
      mutations = mutations_on_distinct_shards(participants)

      assert {:ok, _roots} = MetadataWriter.apply_batch("vol-1", mutations, capturing_opts(ctx))

      assert [{:cas_update_volume_roots, "vol-1", published}] = captured(ctx),
             "#{participants} participants must still publish in exactly one command"

      assert map_size(published) == participants
    end
  end

  # The ambiguous case: the command committed, but the caller was told it
  # failed. #1589's bar asks for idempotent recovery here; the writer instead
  # treats a timeout as terminal — only `:stale_pointer` is retried — so the
  # caller is told the operation failed when it durably succeeded.
  #
  # This test documents the current behaviour rather than the desired one.
  # Widening the retry is a mechanism change, not test coverage, and is
  # tracked in #1700; the CAS expectation would make it safe, since a
  # re-submission after a landed write sees `:stale_pointer`, rebuilds and
  # converges. Flip this to `{:ok, _}` when that lands.
  test "an ambiguous publication is reported as failed and not retried", ctx do
    mutations = mutations_on_distinct_shards(3)
    opts = capturing_opts(ctx, lose_first_response: true)

    assert {:error, {:bootstrap_update_failed, :timeout}} =
             MetadataWriter.apply_batch("vol-1", mutations, opts)

    assert [{:cas_update_volume_roots, "vol-1", published}] = captured(ctx),
           "exactly one submission — the timeout is not retried"

    assert map_size(published) == 3
  end

  # The flush window is the atomic unit: callers sharing a batch share fate.
  # Two logical operations in one batch must both land or neither.
  test "two logical operations sharing a batch share its fate", ctx do
    create = put_mutation("file:created")
    rename = put_mutation("dirent:renamed")

    failing = capturing_opts(ctx, registrar_result: {:error, :no_leader})

    assert {:error, {:bootstrap_update_failed, :no_leader}} =
             MetadataWriter.apply_batch("vol-1", [create, rename], failing)

    ctx = fresh_tables(ctx)

    assert {:ok, roots} =
             MetadataWriter.apply_batch("vol-1", [create, rename], capturing_opts(ctx))

    for key <- ["file:created", "dirent:renamed"] do
      assert Map.has_key?(roots, Shard.for_key(key)),
             "#{key}'s shard must be in the published set"
    end
  end

  defp capturing_opts(%{store: store, commands: commands}, extra \\ []) do
    {stale_attempts, extra} = Keyword.pop(extra, :stale_attempts, 0)
    {registrar_result, extra} = Keyword.pop(extra, :registrar_result, nil)
    {lose_first, extra} = Keyword.pop(extra, :lose_first_response, false)

    registrar =
      cond do
        registrar_result -> failing_registrar(commands, registrar_result)
        lose_first -> lossy_registrar(commands)
        true -> capturing_registrar(commands, stale_attempts)
      end

    store
    |> build_mock_metadata_writer_opts()
    |> Keyword.put(:bootstrap_registrar, registrar)
    |> Keyword.merge(extra)
  end

  # Records the attempt, then refuses — nothing is applied.
  defp failing_registrar(commands, result) do
    fn command ->
      :ets.insert(commands, {:command, command})
      result
    end
  end

  # Applies the command and *then* reports failure, which is what a lost
  # reply or a timed-out `:ra.process_command/3` looks like from here: the
  # write landed, the caller cannot know it.
  defp lossy_registrar(commands) do
    fn command ->
      :ets.insert(commands, {:command, command})

      if :ets.info(commands, :size) == 1 do
        {:error, :timeout}
      else
        {:ok, :updated}
      end
    end
  end

  # A clean pair of tables so a second `apply_batch/3` in one test does not
  # see the first one's commands.
  defp fresh_tables(ctx) do
    :ets.delete_all_objects(ctx.commands)
    ctx
  end

  # Rejects the first `stale_attempts` submissions the way the state machine
  # rejects a checked root set whose expectation has moved, then accepts.
  defp capturing_registrar(commands, stale_attempts) do
    fn command ->
      :ets.insert(commands, {:command, command})

      if stale_attempts == :infinity or :ets.info(commands, :size) <= stale_attempts do
        {:error, {:stale_pointer, shard: 0, expected: <<0::256>>, actual: <<1>>}}
      else
        {:ok, :updated}
      end
    end
  end

  defp captured(%{commands: commands}) do
    commands |> :ets.lookup(:command) |> Enum.map(fn {:command, command} -> command end)
  end

  # `Shard.for_key/1` hashes the key, so walk candidates until `count`
  # distinct shards are covered rather than assuming any particular mapping.
  defp mutations_on_distinct_shards(count) do
    Stream.iterate(0, &(&1 + 1))
    |> Stream.map(&"file:key-#{&1}")
    |> Enum.reduce_while({[], MapSet.new()}, fn key, {keys, shards} ->
      shard = Shard.for_key(key)

      cond do
        MapSet.size(shards) == count -> {:halt, {keys, shards}}
        MapSet.member?(shards, shard) -> {:cont, {keys, shards}}
        true -> {:cont, {[key | keys], MapSet.put(shards, shard)}}
      end
    end)
    |> elem(0)
    |> Enum.map(&put_mutation/1)
  end

  defp put_mutation(key), do: {:put, :file_index, key, MetadataValue.encode(%{id: key})}
end
