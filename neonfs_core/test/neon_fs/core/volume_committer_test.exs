defmodule NeonFS.Core.VolumeCommitterTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.Volume.MetadataValue
  alias NeonFS.Core.VolumeCommitter

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    store = :ets.new(:volume_committer_store, [:set, :public])

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

  test "commit/3 applies a volume's mutations through the writer", %{writer_opts: writer_opts} do
    mutations = [{:put, :file_index, "file:abc", MetadataValue.encode(%{id: "abc"})}]

    assert {:ok, roots} = VolumeCommitter.commit("vol-1", mutations, writer_opts)
    assert map_size(roots) == 1
  end

  test "the same volume always routes to one worker", %{writer_opts: writer_opts} do
    pid_for = fn volume_id ->
      VolumeCommitter.commit(
        volume_id,
        [{:put, :file_index, "k", MetadataValue.encode(%{id: "k"})}],
        writer_opts
      )

      GenServer.whereis({:via, PartitionSupervisor, {VolumeCommitter.Supervisor, volume_id}})
    end

    assert pid_for.("vol-1") == pid_for.("vol-1")
    assert is_pid(pid_for.("vol-1"))
  end
end
