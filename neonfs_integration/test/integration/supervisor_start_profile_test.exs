defmodule NeonFS.Integration.SupervisorStartProfileTest do
  @moduledoc """
  Opt-in diagnostic test that attributes `NeonFS.Core.Supervisor`'s
  cold-start cost to individual children (#510).

  Not run by default — tagged `:profile` and excluded by
  `test/test_helper.exs` unless explicitly included. Run with:

      mix test test/integration/supervisor_start_profile_test.exs --include profile

  The test spawns a configured peer with `applications: []` so the
  `:neonfs_core` supervision tree starts cold, attaches
  `NeonFS.TestSupport.SupervisorStartTimer` on the peer before boot,
  then starts `:neonfs_core` and reads the per-child timings back.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Integration.{PeerCluster, SupervisorStartTimer}

  @moduletag timeout: 120_000
  @moduletag nodes: 1
  @moduletag :profile
  # Start the peer with no applications running; we configure :neonfs_core
  # manually below so the cold start is observable.
  @moduletag applications: []

  test "per-child start timings on NeonFS.Core.Supervisor", %{cluster: cluster} do
    node_info = PeerCluster.get_node!(cluster, :node1)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "neonfs_supervisor_profile_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(Path.join(data_dir, "meta"))
    File.mkdir_p!(Path.join(data_dir, "ra"))
    File.mkdir_p!(Path.join(data_dir, "blobs"))

    core_env = [
      data_dir: data_dir,
      meta_dir: Path.join(data_dir, "meta"),
      blob_store_base_dir: Path.join(data_dir, "blobs"),
      ra_data_dir: to_charlist(Path.join(data_dir, "ra")),
      enable_ra: true,
      metrics_enabled: false,
      quorum_timeout_ms: 15_000,
      drives: [
        %{id: "default", path: Path.join(data_dir, "blobs"), tier: :hot, capacity: 0}
      ]
    ]

    ra_env = [data_dir: to_charlist(Path.join(data_dir, "ra"))]

    # Push config onto the peer BEFORE starting :neonfs_core.
    for {k, v} <- core_env do
      :ok = :peer.call(node_info.peer, Application, :put_env, [:neonfs_core, k, v])
    end

    for {k, v} <- ra_env do
      :ok = :peer.call(node_info.peer, Application, :put_env, [:ra, k, v])
    end

    # Install the timer *before* the supervision tree boots.
    :ok = :peer.call(node_info.peer, SupervisorStartTimer, :install, [])

    {:ok, _apps} =
      :peer.call(node_info.peer, :application, :ensure_all_started, [:neonfs_core], 60_000)

    timings = :peer.call(node_info.peer, SupervisorStartTimer, :timings, [])

    # Detach to avoid polluting subsequent invocations; harmless when
    # the table is torn down with the peer.
    :ok = :peer.call(node_info.peer, SupervisorStartTimer, :uninstall, [])

    IO.puts("")
    IO.puts("==== NeonFS.Core.Supervisor per-child start timings ====")
    IO.puts(SupervisorStartTimer.format_summary(timings))
    IO.puts("========================================================")
    IO.puts("")
  end
end
