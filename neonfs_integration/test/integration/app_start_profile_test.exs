defmodule NeonFS.Integration.AppStartProfileTest do
  @moduledoc """
  Opt-in diagnostic test that profiles per-application start costs on
  a peer BEAM for the full `neonfs_core` dependency tree (#507).

  Not run by default — tagged `:profile` and excluded by
  `test/test_helper.exs` unless explicitly included. Run with:

      mix test test/integration/app_start_profile_test.exs --include profile

  The test does not assert anything — it prints a sorted breakdown of
  per-app start durations to stdout so operators can identify the
  biggest contributors to the ~1.4s average `start_applications`
  phase observed in the peer-cluster harness.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Integration.{AppProfiler, PeerCluster}

  @moduletag timeout: 120_000
  @moduletag nodes: 1
  @moduletag :profile
  # Spawn the peer with no applications started — the default is
  # `[:neonfs_core]`, which warms every dependency before the profiler
  # runs and masks true cold-start costs. With `[]`, the peer comes up
  # with only its boot baseline (kernel / stdlib / elixir /
  # application_controller), and AppProfiler starts each dependency
  # individually on a cold VM.
  @moduletag applications: []

  test "per-app start timings on a bare peer node", %{cluster: cluster} do
    node_info = PeerCluster.get_node!(cluster, :node1)

    timings = :peer.call(node_info.peer, AppProfiler, :start_timed, [:neonfs_core], 60_000)

    IO.puts("")
    IO.puts("==== neonfs_core per-app cold start timings ====")
    IO.puts(AppProfiler.format_summary(timings))
    IO.puts("=================================================")
    IO.puts("")
  end
end
