defmodule NeonFS.Integration.NFSClusterExportsTest do
  @moduledoc """
  Cluster-state NFS exports (#1175): `nfs export` writes the volume's
  `nfs_export` flag in the core volume registry — no node targeting —
  and every running NFS node mirrors the export set via volume
  lifecycle events (with a periodic resync as a safety net). Any NFS
  node behind a load balancer can therefore serve any exported volume.

  ## Cluster shape

    * `node1`: `:neonfs_core` (Ra member, volume registry).
    * `node2`, `node3`: `:neonfs_nfs` (interface peers, distinct
      listener ports allocated by `PeerCluster`).

  ## Coverage

    * `NeonFS.CLI.Handler.nfs_export/1` on core marks the volume and
      BOTH NFS peers converge on serving it (ExportManager mirror and
      the MOUNT-protocol export list).
    * `nfs_list_exports/0` reports the export against every registered
      NFS node.
    * `nfs_unexport/1` propagates to both peers.
    * An NFS node (re)starting after exports exist serves them from its
      startup resync — the "new node joins" case.
  """

  use ExUnit.Case, async: false

  alias NeonFS.TestSupport.{ClusterCase, PeerCluster}

  @moduletag timeout: 300_000
  @moduletag :integration
  @moduletag :nfs

  # Event delivery is millisecond-scale, but loaded shared runners
  # stall peer scheduling — match the slow-runner conventions.
  @converge_timeout 60_000

  test "exports are cluster state served by every NFS node" do
    cluster =
      PeerCluster.start_cluster!(3,
        roles: %{
          node1: [:neonfs_core],
          node2: [:neonfs_nfs],
          node3: [:neonfs_nfs]
        }
      )

    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    PeerCluster.connect_nodes(cluster)
    :ok = ClusterCase.init_mixed_role_cluster(cluster, name: "nfs-cluster-exports")

    volume = "nfs-exports-vol-#{System.unique_integer([:positive])}"

    volume_opts = %{durability: %{type: :replicate, factor: 1, min_copies: 1}}

    # Volume creation is a Ra-replicated write with a 30 s internal
    # budget (#1167); give the RPC a slow-runner margin beyond that.
    {:ok, _} =
      PeerCluster.rpc(
        cluster,
        :node1,
        NeonFS.CLI.Handler,
        :create_volume,
        [volume, volume_opts],
        60_000
      )

    # Export via the core CLI handler — no NFS node is targeted; the
    # handler only flips cluster state.
    {:ok, info} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :nfs_export, [volume], 60_000)

    assert info.volume_name == volume

    # Both NFS peers converge on serving the export: the ExportManager
    # mirror knows the volume id, and the MOUNT-protocol export list
    # advertises it.
    for peer <- [:node2, :node3] do
      assert_export_served(cluster, peer, volume)
    end

    # The cluster-wide listing reports the export for every registered
    # NFS node.
    assert_eventually(
      fn ->
        case PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :nfs_list_exports, []) do
          {:ok, rows} ->
            nodes_serving =
              rows
              |> Enum.filter(&(&1.volume_name == volume))
              |> MapSet.new(& &1.node)

            node2 = Atom.to_string(PeerCluster.get_node!(cluster, :node2).node)
            node3 = Atom.to_string(PeerCluster.get_node!(cluster, :node3).node)
            MapSet.subset?(MapSet.new([node2, node3]), nodes_serving)

          _ ->
            false
        end
      end,
      timeout: @converge_timeout
    )

    # Unexport propagates to both peers.
    {:ok, %{}} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :nfs_unexport, [volume], 60_000)

    for peer <- [:node2, :node3] do
      assert_eventually(
        fn ->
          PeerCluster.rpc(cluster, peer, NeonFS.NFS.ExportManager, :get_export, [volume]) ==
            {:error, :not_found}
        end,
        timeout: @converge_timeout
      )
    end

    # A node that starts AFTER exports exist must serve them from its
    # startup resync — the "new NFS node joins the cluster" case,
    # exercised by restarting the NFS application on node3.
    {:ok, _info} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :nfs_export, [volume], 60_000)

    :ok = PeerCluster.rpc(cluster, :node3, Application, :stop, [:neonfs_nfs])

    {:ok, _apps} =
      PeerCluster.rpc(cluster, :node3, Application, :ensure_all_started, [:neonfs_nfs], 60_000)

    assert_export_served(cluster, :node3, volume)
  end

  defp assert_export_served(cluster, peer, volume) do
    assert_eventually(
      fn ->
        case PeerCluster.rpc(cluster, peer, NeonFS.NFS.ExportManager, :get_export, [volume]) do
          {:ok, export} -> export.volume_id != nil
          _ -> false
        end
      end,
      timeout: @converge_timeout
    )

    dirs =
      cluster
      |> PeerCluster.rpc(peer, NeonFS.NFS.MountBackend, :list_exports, [%{}])
      |> Enum.map(& &1.dir)

    assert ("/" <> volume) in dirs
  end

  defp assert_eventually(fun, opts) do
    deadline = System.monotonic_time(:millisecond) + Keyword.fetch!(opts, :timeout)
    do_assert_eventually(fun, deadline)
  end

  defp do_assert_eventually(fun, deadline) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) > deadline do
        flunk("assert_eventually timeout")
      else
        Process.sleep(100)
        do_assert_eventually(fun, deadline)
      end
    end
  end
end
