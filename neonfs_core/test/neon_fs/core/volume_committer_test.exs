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

  # The worker is the only place a caller's read-modify-write can be checked
  # against what the volume holds now: checked before the call, a commit that
  # arrived in between would slip through the window the check exists for.
  test "a precondition that holds lets the batch through", %{writer_opts: writer_opts} do
    test_pid = self()
    mutations = [{:put, :file_index, "file:ok", MetadataValue.encode(%{id: "ok"})}]

    opts =
      Keyword.put(writer_opts, :precondition, fn ->
        send(test_pid, {:checked, self()})
        :ok
      end)

    assert {:ok, _roots} = VolumeCommitter.commit("vol-pre", mutations, opts)

    worker =
      GenServer.whereis({:via, PartitionSupervisor, {VolumeCommitter.Supervisor, "vol-pre"}})

    assert_received {:checked, ^worker}
  end

  test "a precondition that fails returns its error and commits nothing", %{
    writer_opts: writer_opts
  } do
    key = "file:refused"
    mutations = [{:put, :file_index, key, MetadataValue.encode(%{id: "refused"})}]
    opts = Keyword.put(writer_opts, :precondition, fn -> {:error, :stale_chunks} end)

    assert {:error, :stale_chunks} = VolumeCommitter.commit("vol-refused", mutations, opts)

    assert {:ok, _roots} = VolumeCommitter.commit("vol-refused", mutations, writer_opts)
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
