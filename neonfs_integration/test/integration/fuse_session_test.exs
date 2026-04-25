defmodule NeonFS.Integration.FuseSessionTest do
  @moduledoc """
  End-to-end integration test for `NeonFS.FUSE.Session` against a real
  FUSE mount served by a remote core node (issue #277).

  Mounts a volume via `fusermount3`, starts a `Session` in the test
  runner BEAM, then drives `ls`, `stat`, `cat`, and `readdir` through
  spawned OS processes to verify the read-path opcodes work
  end-to-end through the new native-BEAM FUSE stack.

  Tagged `@moduletag :fuse` so the test only runs on hosts with
  `/dev/fuse` and `fusermount3` available.
  """
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 120_000
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared
  @moduletag :fuse

  alias FuseServer.Fusermount
  alias NeonFS.Client.{Connection, CostFunction, Discovery}
  alias NeonFS.FUSE.{InodeTable, Session}

  setup_all %{cluster: cluster} do
    {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])
    :ok = wait_for_cluster_stable(cluster)

    {:ok, volume_map} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "session-test",
        %{}
      ])

    volume_id = volume_map[:id]

    # Pre-populate the volume with files we'll later read via the
    # FUSE mount.
    PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file_at, [
      volume_id,
      "/hello.txt",
      0,
      "Hello, FUSE!"
    ])

    PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file_at, [
      volume_id,
      "/world.txt",
      0,
      "world"
    ])

    %{volume_id: volume_id, volume_name: "session-test"}
  end

  setup %{cluster: cluster, volume_id: volume_id, volume_name: volume_name} do
    core_node = PeerCluster.get_node!(cluster, :node1).node

    start_supervised!({Connection, bootstrap_nodes: [core_node]})
    start_supervised!(Discovery)
    start_supervised!(CostFunction)
    start_supervised!(InodeTable)

    :ok =
      wait_until(fn ->
        case Connection.connected_core_node() do
          {:ok, _} -> true
          _ -> false
        end
      end)

    :ok =
      wait_until(
        fn ->
          case Discovery.get_core_nodes() do
            [_ | _] -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )

    mount_point =
      Path.join(System.tmp_dir!(), "neonfs_session_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(mount_point)

    {:ok, fd} =
      Fusermount.mount(mount_point, ["fsname=neonfs_session_test", "subtype=neonfs"])

    {:ok, session} =
      Session.start_link(fd: fd, volume: volume_id, volume_name: volume_name)

    on_exit(fn ->
      if Process.alive?(session) do
        try do
          GenServer.stop(session, :shutdown, 5_000)
        catch
          :exit, _ -> :ok
        end
      end

      _ = Fusermount.unmount(mount_point, lazy: true)
      _ = File.rmdir(mount_point)
    end)

    {:ok, mount_point: mount_point, session: session}
  end

  describe "real-FUSE read path" do
    test "ls lists the pre-populated files", %{mount_point: mp} do
      {output, 0} = System.cmd("ls", ["-1", mp], stderr_to_stdout: true)
      lines = output |> String.split("\n", trim: true) |> Enum.sort()
      assert "hello.txt" in lines
      assert "world.txt" in lines
    end

    test "stat returns the file's size", %{mount_point: mp} do
      {output, 0} =
        System.cmd("stat", ["-c", "%s", Path.join(mp, "hello.txt")], stderr_to_stdout: true)

      assert String.trim(output) == "12"
    end

    test "cat reads the file contents", %{mount_point: mp} do
      {output, 0} =
        System.cmd("cat", [Path.join(mp, "hello.txt")], stderr_to_stdout: true)

      assert output == "Hello, FUSE!"
    end

    test "readdir via ls -la also works (READDIRPLUS path)", %{mount_point: mp} do
      {output, 0} = System.cmd("ls", ["-la", mp], stderr_to_stdout: true)
      assert output =~ "hello.txt"
      assert output =~ "world.txt"
    end
  end
end
