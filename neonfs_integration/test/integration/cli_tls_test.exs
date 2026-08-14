defmodule NeonFS.Integration.CLITLSTest do
  @moduledoc """
  The CLI reaching a cluster whose distribution runs over TLS.

  This is the path an operator always takes — production has no cookie-only
  distribution — and it was the one the CLI tests did not cover. Every other
  CLI test runs against a plain-distribution peer with an empty TLS
  directory, which is a real configuration but not the shipped one.

  Both directions are asserted, because only the pair of them stops the
  coverage lapsing back: that the CLI *reaches* a TLS-only cluster when it
  has a client identity, and that it *cannot* when it has none. Without the
  negative test, an empty TLS directory would silently return this file to
  testing the plain path.
  """

  use ExUnit.Case, async: false

  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 180_000
  @moduletag :integration

  @cli_path Path.expand("../../../neonfs-cli/target/release/neonfs-cli", __DIR__)

  setup do
    unless File.exists?(@cli_path) do
      flunk(
        "CLI binary not found at #{@cli_path}. Build it with: cd neonfs-cli && cargo build --release"
      )
    end

    cluster = PeerCluster.start_cluster!(1, dist: :tls)
    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    # Initialised over the control channel rather than through the CLI, so a
    # failure here reads as "the cluster did not come up" rather than as a
    # TLS failure. What the CLI is asked for below is a cluster that has
    # something to report.
    {:ok, _} =
      PeerCluster.rpc_until_ready(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["cli-tls"])

    # Initialising swaps the certificate the node presents for a
    # cluster-signed one, so the CLI has to be given the cluster CA as well.
    # A real install gets this for free — its CLI and node share one TLS
    # directory.
    :ok = PeerCluster.trust_cluster_ca!(cluster)

    {:ok, cluster: cluster}
  end

  test "the CLI reaches a TLS-only cluster with the identity the harness issued", %{
    cluster: cluster
  } do
    node = PeerCluster.get_node!(cluster, :node1).node

    assert {:ok, output} =
             run_cli(cluster, :node1, ["cluster", "status"], PeerCluster.cli_tls_dir(cluster))

    assert output =~ "running"
    # The peer's own name in the reply is what makes this a completed round
    # trip rather than the CLI reporting on itself.
    assert output =~ Atom.to_string(node)
  end

  # The peer listens on `inet_tls` only, so a plain handshake is refused at
  # the transport. That refusal is what makes the positive test above
  # meaningful: it could not pass by quietly falling back.
  test "the CLI cannot reach it without a client identity", %{cluster: cluster} do
    empty_dir = Path.join(cluster.data_dir, "cli-tls-empty")
    File.mkdir_p!(empty_dir)

    assert {:error, {_code, output}} =
             run_cli(cluster, :node1, ["cluster", "status"], empty_dir)

    refute output =~ "running"
  end

  defp run_cli(cluster, node_name, args, tls_dir) do
    node_info = PeerCluster.get_node!(cluster, node_name)

    env = [
      {"NEONFS_COOKIE", Atom.to_string(cluster.cookie)},
      {"NEONFS_NODE", Atom.to_string(node_info.node)},
      {"NEONFS_DIST_PORT", Integer.to_string(node_info.dist_port)},
      {"NEONFS_TLS_DIR", tls_dir}
    ]

    case System.cmd(@cli_path, args, stderr_to_stdout: true, env: env) do
      {output, 0} -> {:ok, output}
      {output, code} -> {:error, {code, output}}
    end
  end
end
