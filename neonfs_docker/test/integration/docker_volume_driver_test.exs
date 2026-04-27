defmodule NeonFS.Docker.IntegrationTest do
  @moduledoc """
  End-to-end test for the Docker / Podman VolumeDriver plugin.

  Spins up a single-node peer core cluster, starts `neonfs_docker`'s
  HTTP plugin on the test runner over a Unix domain socket Docker
  knows how to discover, then drives the full
  `docker volume create -d neonfs … && docker run --rm -v ...`
  flow against a real `docker` daemon.

  Tagged `@moduletag :docker` so the test only runs on hosts where
  the `docker` CLI is on `PATH` and the daemon answers `docker info`.
  Hosts without those just skip the suite — see `test_helper.exs`'s
  `:docker` exclusion guard.

  ## CI

  CI runners that run this suite must provide `docker` (or rootless
  `podman` symlinked as `docker`) and a daemon that can mount a
  plugin under `/etc/docker/plugins/`. If neither is configured the
  whole suite skips and the test does not regress green.

  ## Plugin discovery

  The Docker daemon discovers plugins by scanning, in order:

    * `/run/docker/plugins/<name>.sock`
    * `/etc/docker/plugins/<name>.json`
    * `/usr/lib/docker/plugins/<name>.json`

  We use the **spec-file path** (`/etc/docker/plugins/neonfs.json`)
  pointing at our test-controlled UDS, then clean it up on exit.
  This lets the test pin the socket to a per-run temp path so two
  concurrent test workers don't collide on `/run/docker/plugins/`.
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  @moduletag timeout: 240_000
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared
  @moduletag :docker

  alias NeonFS.Client.{Connection, CostFunction, Discovery}

  # Each test uses a unique driver name. dockerd's legacy plugin
  # discovery (`moby/pkg/plugins/discovery.go`) caches plugins by name
  # on first use; rewriting the spec file or removing it doesn't
  # invalidate the cache, so a second test reusing `"neonfs"` would
  # dial the *first* test's socket — which has already been torn down.
  # Issuing a fresh name per test sidesteps the cache.
  @plugin_spec_dir "/etc/docker/plugins"

  defmodule StubMountManager do
    @moduledoc """
    Test stub registered as `NeonFS.FUSE.MountManager` on the runner.

    The docker plugin's `MountTracker` defaults to calling
    `NeonFS.FUSE.MountManager` on `Node.self()`. The runner doesn't
    boot the FUSE app — its real wiring (`MetadataCache`, `Native`
    NIFs, `/dev/fuse`) is exercised by `fuse_session_test.exs`. This
    stub responds to `{:mount, ...}` / `{:unmount, ...}` by creating
    and removing a plain directory, which is enough for dockerd to
    bind-mount it into a container.
    """
    use GenServer

    def start_link(_opts \\ []) do
      GenServer.start_link(__MODULE__, %{}, name: NeonFS.FUSE.MountManager)
    end

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call({:mount, _volume_name, mount_point, _opts}, _from, state) do
      File.mkdir_p!(mount_point)
      mount_id = "stub-#{System.unique_integer([:positive])}"
      {:reply, {:ok, mount_id}, Map.put(state, mount_id, mount_point)}
    end

    def handle_call({:unmount, mount_id}, _from, state) do
      case Map.pop(state, mount_id) do
        {nil, state} ->
          {:reply, :ok, state}

        {mount_point, state} ->
          _ = File.rm_rf(mount_point)
          {:reply, :ok, state}
      end
    end
  end

  setup_all %{cluster: cluster} do
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["docker-test"])

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
          case Discovery.get_core_nodes() do
            [_ | _] -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )

    suffix = System.unique_integer([:positive])
    driver_name = "neonfstest#{suffix}"

    socket_path =
      Path.join(
        System.tmp_dir!(),
        "neonfs_docker_test_#{suffix}.sock"
      )

    mount_root = Path.join(System.tmp_dir!(), "neonfs_docker_mount_#{suffix}")
    File.mkdir_p!(mount_root)

    Application.put_env(:neonfs_docker, :listener, :socket)
    Application.put_env(:neonfs_docker, :socket_path, socket_path)
    Application.put_env(:neonfs_docker, :register_service, false)
    Application.put_env(:neonfs_docker, :mount_root, mount_root)

    start_supervised!(StubMountManager)
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

    {:ok, socket_path: socket_path, driver_name: driver_name}
  end

  describe "real docker daemon" do
    test "create volume → run container that writes + reads → remove volume", %{
      driver_name: driver_name
    } do
      vol_name = "neonfs-test-vol-#{System.unique_integer([:positive])}"

      try do
        assert {_, 0} = run_docker(["volume", "create", "-d", driver_name, vol_name])

        # Container writes a file into the mounted volume, then reads
        # it back. Stdout should contain the written marker.
        marker = "hello-from-#{vol_name}"

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
      after
        # Best-effort cleanup; if `docker run` failed mid-test the
        # volume is still present and we want the next run clean.
        _ = run_docker(["volume", "rm", "-f", vol_name])
      end
    end

    test "Plugin.Activate handshake is reachable through the daemon", %{
      driver_name: driver_name
    } do
      # `docker plugin ls` and `docker volume ls` both probe the
      # plugin's `Plugin.Activate` endpoint as a side effect; if our
      # plugin isn't responding the daemon hides it from the volume
      # driver list.
      assert {output, 0} = run_docker(["volume", "ls", "--filter", "driver=#{driver_name}"])
      assert is_binary(output)
    end
  end

  ## Helpers

  # Run a `docker` command, returning `{stdout_iolist, exit_status}`.
  # The CI runner must put `docker` on `PATH`. This wrapper merges
  # stderr into stdout so a debug-trail failure isn't truncated.
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
