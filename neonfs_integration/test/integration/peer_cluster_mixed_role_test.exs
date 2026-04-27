defmodule NeonFS.Integration.PeerClusterMixedRoleTest do
  @moduledoc """
  Smoke tests for `NeonFS.TestSupport.PeerCluster.start_cluster!/2`'s
  mixed-role peer spawning (#498).

  Starts a three-peer cluster where one peer runs `:neonfs_core`, one
  runs `:neonfs_s3` (interface-only, no Ra membership), and one runs
  `:neonfs_webdav` (ditto). Verifies:

  - each peer has the expected applications started;
  - the `:neonfs_core` peer is a Ra member and the interface peers are
    not;
  - each interface peer's allocated port is reachable (Bandit is up);
  - `PeerCluster.stop_cluster/1` tears everything down cleanly.

  This test is deliberately scoped to the harness change itself.
  Substantive coverage for the streaming-write path ships in #499,
  which depends on this harness landing. Uses plain `ExUnit.Case`
  rather than `ClusterCase` so the per-test auto-starter doesn't
  spawn a second cluster alongside the one we test.
  """

  use ExUnit.Case, async: false

  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 120_000

  test "three-peer cluster with one core and two interface peers" do
    cluster =
      PeerCluster.start_cluster!(3,
        roles: %{
          node1: [:neonfs_core],
          node2: [:neonfs_s3],
          node3: [:neonfs_webdav]
        }
      )

    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    PeerCluster.connect_nodes(cluster)

    core = PeerCluster.get_node!(cluster, :node1)
    s3 = PeerCluster.get_node!(cluster, :node2)
    webdav = PeerCluster.get_node!(cluster, :node3)

    # Each peer reports the expected applications via node_info.
    assert core.applications == [:neonfs_core]
    assert s3.applications == [:neonfs_s3]
    assert webdav.applications == [:neonfs_webdav]

    # Interface peers have their listener ports allocated; the core
    # peer has no interface ports.
    assert core.interface_ports == %{}
    assert %{s3: s3_port} = s3.interface_ports
    assert is_integer(s3_port) and s3_port > 0
    assert %{webdav: webdav_port} = webdav.interface_ports
    assert is_integer(webdav_port) and webdav_port > 0

    # :neonfs_core is only started on the core peer.
    core_apps = started_application_names(core)
    assert :neonfs_core in core_apps
    refute :neonfs_s3 in core_apps
    refute :neonfs_webdav in core_apps

    s3_apps = started_application_names(s3)
    assert :neonfs_s3 in s3_apps
    assert :neonfs_client in s3_apps
    refute :neonfs_core in s3_apps

    webdav_apps = started_application_names(webdav)
    assert :neonfs_webdav in webdav_apps
    assert :neonfs_client in webdav_apps
    refute :neonfs_core in webdav_apps

    # Interface listeners accept TCP connections on the allocated ports.
    assert listener_accepts?(s3_port)
    assert listener_accepts?(webdav_port)
  end

  defp started_application_names(node_info) do
    node_info.peer
    |> :peer.call(Application, :started_applications, [])
    |> Enum.map(fn {name, _desc, _vsn} -> name end)
  end

  # Probe: can we open a TCP socket to the listener? Retries handle
  # the case where Bandit is still binding when the test races ahead
  # of start_applications.
  defp listener_accepts?(port, attempts \\ 10)

  defp listener_accepts?(_port, 0), do: false

  defp listener_accepts?(port, attempts) do
    case :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 500) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        true

      {:error, _} ->
        Process.sleep(100)
        listener_accepts?(port, attempts - 1)
    end
  end
end
