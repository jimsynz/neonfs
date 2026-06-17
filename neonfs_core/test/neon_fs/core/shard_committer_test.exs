defmodule NeonFS.Core.ShardCommitterTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.ShardCommitter
  alias NeonFS.Core.Volume.MetadataValue

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    store = :ets.new(:shard_committer_store, [:set, :public])

    on_exit(fn ->
      cleanup_test_dirs()

      try do
        :ets.delete(store)
      rescue
        ArgumentError -> :ok
      end
    end)

    %{writer_opts: build_mock_metadata_writer_opts(store)}
  end

  test "commit/4 applies a shard's mutations through the writer", %{writer_opts: writer_opts} do
    mutations = [{:put, :file_index, "file:abc", MetadataValue.encode(%{id: "abc"})}]

    assert {:ok, _root} = ShardCommitter.commit("vol-1", 0, mutations, writer_opts)
  end

  test "the same {volume, shard} always routes to one worker; distinct shards may differ",
       %{writer_opts: writer_opts} do
    pid_for = fn shard ->
      ShardCommitter.commit(
        "vol-1",
        shard,
        [{:put, :file_index, "k", MetadataValue.encode(%{id: "k"})}],
        writer_opts
      )

      GenServer.whereis(
        {:via, PartitionSupervisor, {ShardCommitter.Supervisor, {"vol-1", shard}}}
      )
    end

    assert pid_for.(0) == pid_for.(0)
    assert is_pid(pid_for.(0))
  end
end
