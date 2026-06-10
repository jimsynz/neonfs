defmodule NeonFS.Docker.RealFuseIntegrationTest do
  @moduledoc """
  Real-FUSE end-to-end test for the VolumeDriver Mount/Unmount plumbing
  (#387).

  Unlike `docker_volume_driver_test.exs` — which stubs the mount with a
  plain directory to test the plugin's HTTP plumbing — this test wires
  the plugin to the real `NeonFS.FUSE.MountManager`, so dockerd's
  `POST /VolumeDriver.Mount` produces a genuine kernel FUSE mount and
  the container's reads/writes round-trip through NeonFS core.

  Requires `/dev/fuse`, `fusermount3`, and a reachable docker daemon —
  the CI `neonfs_docker` job provides all three.
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  @moduletag timeout: 180_000
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared
  @moduletag :fuse

  alias NeonFS.Client.{Connection, CostFunction, Discovery}

  @plugin_spec_dir "/etc/docker/plugins"

  setup_all %{cluster: cluster} do
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["docker-fuse-test"])

    :ok = wait_for_cluster_stable(cluster)
    %{}
  end

  setup %{cluster: cluster} do
    core_node = PeerCluster.get_node!(cluster, :node1).node

    start_supervised!({Connection, bootstrap_nodes: [core_node]})
    start_supervised!(Discovery)
    start_supervised!(CostFunction)

    :ok =
      wait_until(fn ->
        match?({:ok, _}, Connection.connected_core_node())
      end)

    :ok =
      wait_until(
        fn ->
          match?([_ | _], Discovery.get_core_nodes())
        end,
        timeout: 10_000
      )

    # The real FUSE stack the plugin's MountTracker targets on
    # `Node.self()`. No Registrar — service registration isn't under
    # test here.
    start_supervised!(NeonFS.FUSE.InodeTable)
    start_supervised!(NeonFS.FUSE.MountSupervisor)
    start_supervised!(NeonFS.FUSE.MountManager)

    suffix = System.unique_integer([:positive])
    driver_name = "neonfs-fuse-test-#{suffix}"

    socket_path = Path.join(System.tmp_dir!(), "neonfs_docker_fuse_test_#{suffix}.sock")
    mount_root = Path.join(System.tmp_dir!(), "neonfs_docker_fuse_mount_#{suffix}")
    File.mkdir_p!(mount_root)

    Application.put_env(:neonfs_docker, :listener, :socket)
    Application.put_env(:neonfs_docker, :socket_path, socket_path)
    Application.put_env(:neonfs_docker, :register_service, false)
    Application.put_env(:neonfs_docker, :mount_root, mount_root)

    start_supervised!({NeonFS.Docker.Supervisor, []})

    :ok = write_plugin_spec(driver_name, socket_path)

    on_exit(fn ->
      _ = remove_plugin_spec(driver_name)
      _ = File.rm(socket_path)
      _ = File.rm_rf(mount_root)
      Application.delete_env(:neonfs_docker, :listener)
      Application.delete_env(:neonfs_docker, :socket_path)
      Application.delete_env(:neonfs_docker, :register_service)
      Application.delete_env(:neonfs_docker, :mount_root)
    end)

    {:ok, driver_name: driver_name, cluster: cluster}
  end

  describe "real docker daemon + real FUSE mount" do
    test "container writes through the FUSE mount land in NeonFS core; Unmount tears down", %{
      driver_name: driver_name,
      cluster: cluster
    } do
      vol_name = "neonfs-fuse-vol-#{System.unique_integer([:positive])}"
      marker = "hello-through-fuse-#{vol_name}"

      try do
        assert {_, 0} = run_docker(["volume", "create", "-d", driver_name, vol_name])

        # dockerd POSTs /VolumeDriver.Mount before starting the
        # container and /VolumeDriver.Unmount after it exits; the
        # container's write and read both traverse the kernel FUSE
        # mount served by NeonFS.FUSE.Session.
        assert {output, 0} =
                 run_docker([
                   "run",
                   "--rm",
                   "-v",
                   "#{vol_name}:/data",
                   "alpine",
                   "sh",
                   "-c",
                   "echo '#{marker}' > /data/greeting && cat /data/greeting"
                 ])

        assert String.contains?(output, marker)

        # The data must exist in NeonFS itself, not just in the page
        # cache of a stub directory: read it back from the core node.
        {:ok, content} =
          PeerCluster.rpc(cluster, :node1, NeonFS.Core, :read_file, [vol_name, "/greeting"])

        assert String.trim(content) == marker

        # dockerd unmounted after `--rm`; the MountManager should hold
        # no active mounts. Unmount is asynchronous relative to
        # `docker run` returning, so poll.
        :ok =
          wait_until(fn ->
            NeonFS.FUSE.MountManager.list_mounts() == []
          end)
      after
        _ = run_docker(["volume", "rm", "-f", vol_name])
      end
    end
  end

  ## Helpers (mirrors docker_volume_driver_test.exs)

  defp run_docker(args) do
    System.cmd("docker", args, stderr_to_stdout: true)
  end

  defp write_plugin_spec(name, socket_path) do
    File.mkdir_p!(@plugin_spec_dir)

    spec = ~s|{"Name":"#{name}","Addr":"unix://#{socket_path}"}|

    File.write!(Path.join(@plugin_spec_dir, "#{name}.json"), spec)
    :ok
  end

  defp remove_plugin_spec(name) do
    _ = File.rm(Path.join(@plugin_spec_dir, "#{name}.json"))
  end
end
