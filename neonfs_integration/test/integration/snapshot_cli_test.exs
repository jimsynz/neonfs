defmodule NeonFS.Integration.SnapshotCLITest do
  @moduledoc """
  CLI integration test for the snapshot subcommands (#962 / epic #959).
  Exercises `neonfs volume snapshot create / list / show / delete`
  end-to-end against a peer cluster via the real CLI binary.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  @moduletag timeout: 60_000
  @moduletag :integration
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared

  @cli_path Path.expand("../../../neonfs-cli/target/release/neonfs-cli", __DIR__)

  setup_all %{cluster: cluster} do
    {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["snap-cli"])
    :ok = wait_for_cluster_stable(cluster)

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "snap-cli-vol",
        %{"durability" => "replicate:1"}
      ])

    %{volume_name: volume.name}
  end

  describe "neonfs volume snapshot" do
    test "create / list / show / delete round-trip via the CLI binary", %{
      cluster: cluster,
      volume_name: volume_name
    } do
      ensure_cli_binary!()

      assert {:ok, list_empty} =
               run_cli(cluster, :node1, ["volume", "snapshot", "list", volume_name])

      refute list_empty =~ "weekly"

      assert {:ok, create_out} =
               run_cli(cluster, :node1, [
                 "volume",
                 "snapshot",
                 "create",
                 volume_name,
                 "--name",
                 "weekly"
               ])

      assert create_out =~ "weekly"
      assert create_out =~ volume_name

      snapshot_id = extract_field(create_out, "ID:")

      assert {:ok, list_out} =
               run_cli(cluster, :node1, ["volume", "snapshot", "list", volume_name])

      assert list_out =~ snapshot_id
      assert list_out =~ "weekly"

      assert {:ok, show_out} =
               run_cli(cluster, :node1, [
                 "volume",
                 "snapshot",
                 "show",
                 volume_name,
                 snapshot_id
               ])

      assert show_out =~ snapshot_id
      assert show_out =~ "weekly"

      # Address by human-readable name when unique.
      assert {:ok, show_by_name} =
               run_cli(cluster, :node1, [
                 "volume",
                 "snapshot",
                 "show",
                 volume_name,
                 "weekly"
               ])

      assert show_by_name =~ snapshot_id

      assert {:ok, delete_out} =
               run_cli(cluster, :node1, [
                 "volume",
                 "snapshot",
                 "delete",
                 volume_name,
                 snapshot_id,
                 "--yes"
               ])

      assert delete_out =~ "deleted"

      assert {:ok, after_delete} =
               run_cli(cluster, :node1, ["volume", "snapshot", "list", volume_name])

      refute after_delete =~ snapshot_id
    end

    test "delete refuses without --yes (table format)", %{
      cluster: cluster,
      volume_name: volume_name
    } do
      ensure_cli_binary!()

      assert {:error, {_code, output}} =
               run_cli(cluster, :node1, [
                 "volume",
                 "snapshot",
                 "delete",
                 volume_name,
                 "never-existed"
               ])

      assert output =~ "--yes"
    end
  end

  defp ensure_cli_binary! do
    unless File.exists?(@cli_path) do
      flunk(
        "CLI binary not found at #{@cli_path}. Build it with: cd neonfs-cli && cargo build --release"
      )
    end
  end

  defp extract_field(output, label) do
    output
    |> String.split("\n", trim: true)
    |> Enum.find_value(fn line ->
      case String.split(line, label, parts: 2) do
        [_, rest] -> String.trim(rest)
        _ -> nil
      end
    end)
  end

  defp run_cli(cluster, node_name, args) do
    node_info = PeerCluster.get_node!(cluster, node_name)
    cookie_str = Atom.to_string(cluster.cookie)
    node_str = Atom.to_string(node_info.node)

    env = [
      {"NEONFS_COOKIE", cookie_str},
      {"NEONFS_NODE", node_str},
      {"NEONFS_DIST_PORT", Integer.to_string(node_info.dist_port)}
    ]

    case System.cmd(@cli_path, args, stderr_to_stdout: true, env: env) do
      {output, 0} -> {:ok, output}
      {output, code} -> {:error, {code, output}}
    end
  end
end
