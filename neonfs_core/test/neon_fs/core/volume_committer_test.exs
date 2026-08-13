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

  test "a commit that outruns its deadline is an error, not an exit", %{writer_opts: writer_opts} do
    Application.put_env(:neonfs_core, :volume_commit_timeout_ms, 50)
    on_exit(fn -> Application.delete_env(:neonfs_core, :volume_commit_timeout_ms) end)

    volume = "vol-slow"
    mutations = [{:put, :file_index, "file:slow", MetadataValue.encode(%{id: "slow"})}]

    # Occupy the volume's worker for longer than the deadline, so the commit
    # behind it cannot be answered in time. `FileIndex` calls this holding a
    # whole batch of pending replies, so an exit here would take the index
    # down and strand every one of them.
    worker = GenServer.whereis({:via, PartitionSupervisor, {VolumeCommitter.Supervisor, volume}})
    refute is_nil(worker)
    :sys.suspend(worker)
    on_exit(fn -> :sys.resume(worker) end)

    assert {:error, error} = VolumeCommitter.commit(volume, mutations, writer_opts)
    assert error.class == :unavailable
    assert Exception.message(error) =~ "timed out"
  end
end
