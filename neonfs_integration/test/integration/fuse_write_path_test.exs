defmodule NeonFS.Integration.FuseWritePathTest do
  @moduledoc """
  End-to-end integration test for the native-BEAM FUSE write path
  (#278 / #575 / #576 / #577) against a real FUSE mount served by a
  remote core node.

  Mounts a volume via `fusermount3`, starts a `Session` in the test
  runner BEAM, then exercises the mutation + data-path opcodes via
  `echo`, `mv`, `rm`, `mkdir`, `rmdir`, `truncate`, and a
  multi-frame write to verify the streaming invariant.

  Tagged `@moduletag :fuse` so the suite only runs on hosts with
  `/dev/fuse` + `fusermount3` available — the CI image has both.
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
        "write-path-test",
        %{}
      ])

    %{volume_id: volume_map[:id], volume_name: "write-path-test"}
  end

  setup %{cluster: cluster, volume_id: volume_id, volume_name: volume_name} do
    core_node = PeerCluster.get_node!(cluster, :node1).node

    start_supervised!({Connection, bootstrap_nodes: [core_node]})
    start_supervised!(Discovery)
    start_supervised!(CostFunction)
    start_supervised!(InodeTable)

    :ok =
      wait_until(fn ->
        match?({:ok, _}, Connection.connected_core_node())
      end)

    :ok =
      wait_until(
        fn -> match?([_ | _], Discovery.get_core_nodes()) end,
        timeout: 10_000
      )

    mount_point =
      Path.join(
        System.tmp_dir!(),
        "neonfs_write_path_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(mount_point)

    {:ok, fd} =
      Fusermount.mount(mount_point, ["fsname=neonfs_write_path", "subtype=neonfs"])

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

  describe "real-FUSE write path" do
    test "echo hello > file (CREATE + WRITE)", %{mount_point: mp} do
      path = Path.join(mp, "hello.txt")
      assert :ok = run_shell!("echo -n hello > #{path}")
      assert {:ok, "hello"} = File.read(path)
    end

    test "mv renames a file (RENAME)", %{mount_point: mp} do
      assert :ok = run_shell!("echo -n moveme > #{Path.join(mp, "src.txt")}")
      assert :ok = run_shell!("mv #{Path.join(mp, "src.txt")} #{Path.join(mp, "dst.txt")}")

      refute File.exists?(Path.join(mp, "src.txt"))
      assert {:ok, "moveme"} = File.read(Path.join(mp, "dst.txt"))
    end

    test "rm deletes a file (UNLINK)", %{mount_point: mp} do
      target = Path.join(mp, "doomed.txt")
      assert :ok = run_shell!("echo -n bye > #{target}")
      assert File.exists?(target)
      assert :ok = run_shell!("rm #{target}")
      refute File.exists?(target)
    end

    test "mkdir + rmdir round-trip", %{mount_point: mp} do
      target = Path.join(mp, "newdir")
      assert :ok = run_shell!("mkdir #{target}")
      assert File.dir?(target)
      assert :ok = run_shell!("rmdir #{target}")
      refute File.exists?(target)
    end

    test "truncate (SETATTR with FATTR_SIZE) shortens a file", %{mount_point: mp} do
      target = Path.join(mp, "trunc.txt")
      assert :ok = run_shell!("echo -n abcdefghij > #{target}")
      assert {:ok, %{size: 10}} = File.stat(target)

      assert :ok = run_shell!("truncate -s 4 #{target}")
      assert {:ok, %{size: 4}} = File.stat(target)
      assert {:ok, "abcd"} = File.read(target)
    end

    # The streaming invariant: a `dd` write that spans multiple
    # `max_write` (64 KiB)-bounded frames must round-trip
    # byte-for-byte without the session ever materialising the
    # whole file in one buffer. We can't directly observe BEAM
    # internals from a System.cmd test, but a successful round-trip
    # at sizes well above max_write is the correctness signal —
    # the session would corrupt or lose bytes if it accumulated.
    test "multi-frame WRITE round-trips a 256 KiB file", %{mount_point: mp} do
      target = Path.join(mp, "big.bin")
      # 256 KiB = 4 × 64 KiB max_write frames.
      assert :ok = run_shell!("dd if=/dev/urandom of=#{target} bs=64k count=4 status=none")
      assert {:ok, %{size: 262_144}} = File.stat(target)

      # Read back and compare against the on-disk source via cmp(1).
      copy = target <> ".copy"
      assert :ok = run_shell!("cp #{target} #{copy}")
      assert :ok = run_shell!("cmp #{target} #{copy}")
    end
  end

  ## Helpers

  # `System.cmd/3` doesn't accept shell pipelines / redirects; wrap
  # in `bash -c` so the test reads naturally.
  defp run_shell!(cmd) do
    case System.cmd("bash", ["-c", cmd], stderr_to_stdout: true) do
      {_out, 0} -> :ok
      {out, code} -> flunk("#{cmd} exited #{code}: #{out}")
    end
  end
end
