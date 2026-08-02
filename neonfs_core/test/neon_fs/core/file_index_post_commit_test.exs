defmodule NeonFS.Core.FileIndexPostCommitTest do
  @moduledoc """
  The boundary after a batch commits durably but before its post-commit
  effects finish.

  `FileIndex` coalesces operations into one batch, publishes them, then runs
  each transaction's `on_commit` and replies. Those effects are local
  materialisation — ETS cache writes, event broadcasts — so a failure there
  costs a cache entry, not durability. It must not be reported to the caller
  as a failed write, and it must not stop the batch's *other* transactions
  from being answered.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{FileIndex, FileMeta, VolumeRegistry}

  @moduletag :tmp_dir
  @moduletag timeout: 120_000

  setup %{tmp_dir: tmp_dir} do
    {:ok, _} = start_provisioned_cluster(tmp_dir)
    on_exit(fn -> stop_ra() end)
    :ok
  end

  test "a raising post-commit effect neither fails the write nor strands the batch" do
    {:ok, volume} = VolumeRegistry.get_by_name("_system")

    # `on_commit` for a create writes the file into this cache. Removing the
    # table makes every effect in the batch raise, which is the cheapest way
    # to reach the boundary — the alternative is an event broadcast failing,
    # which is harder to arrange and no more representative.
    :ets.delete(:file_index_by_id)

    results =
      1..4
      |> Task.async_stream(
        fn n ->
          FileIndex.create(%FileMeta{
            id: UUIDv7.generate(),
            volume_id: volume.id,
            path: "/post-commit-#{n}.bin",
            size: 1,
            mode: 0o100644
          })
        end,
        timeout: 30_000,
        max_concurrency: 4
      )
      |> Enum.map(fn {:ok, result} -> result end)

    # Every caller is answered. Before the effects were isolated, the first
    # raise took the GenServer down and the rest waited for a reply that
    # never came.
    assert length(results) == 4

    for result <- results do
      assert match?({:ok, _}, result) or match?({:error, _}, result),
             "expected a reply, got #{inspect(result)}"
    end

    assert Process.alive?(Process.whereis(FileIndex)),
           "FileIndex should survive a post-commit effect raising"
  end
end
