defmodule NeonFS.Integration.ContainerdSpaceAccountingTest do
  @moduledoc """
  Integration test for #727 (third slice of #554) — proves that
  bytes ingested through `neonfs_containerd` show up in
  `NeonFS.Core.StorageMetrics.cluster_capacity/0`'s aggregate
  `used_bytes`.

  ## Architecture

  - 2-peer mixed-role cluster:
      - `node1`: `:neonfs_core`
      - `node2`: `:neonfs_containerd` (interface peer)
  - Snapshot `cluster_capacity/0` via the cluster Router before
    ingest, ingest a 1 MiB blob via `ctr content ingest`, snapshot
    again. The aggregate `used_bytes` delta should be ≥ N (the
    payload size) and ≤ 1.1 × N (allowing for chunk-metadata
    overhead — reasonable for ≥1 MiB blobs per the issue's
    acceptance criteria).
  """

  use ExUnit.Case, async: false

  alias NeonFS.Client.Router
  alias NeonFS.Core.StorageMetrics
  alias NeonFS.Integration.ContainerdDaemon
  alias NeonFS.TestSupport.{ClusterCase, PeerCluster}

  @moduletag :requires_containerd
  @moduletag :requires_root
  @moduletag timeout: 180_000

  @volume_name "containerd-space-accounting"
  @volume_opts %{
    durability: %{type: :replicate, factor: 1, min_copies: 1},
    compression: %{algorithm: :none, level: 0, min_size: 0}
  }
  # 1 MiB — large enough that chunk metadata overhead is a small
  # fraction of payload, per #727's "<10% overhead for blobs ≥ 1 MiB"
  # acceptance bound.
  @payload_bytes 1024 * 1024

  describe "space accounting after containerd ingest" do
    test "cluster_capacity reflects ingested bytes within expected overhead" do
      cluster =
        PeerCluster.start_cluster!(2,
          roles: %{
            node1: [:neonfs_core],
            node2: [:neonfs_containerd]
          }
        )

      on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

      PeerCluster.connect_nodes(cluster)

      :ok =
        ClusterCase.init_mixed_role_cluster(cluster,
          name: "containerd-space-accounting-test",
          volumes: [{@volume_name, @volume_opts}]
        )

      containerd_peer = PeerCluster.get_node!(cluster, :node2)
      socket_path = containerd_peer.interface_ports.containerd

      :ok = wait_for_socket(socket_path, 30_000)

      :ok =
        PeerCluster.rpc(cluster, :node2, Application, :put_env, [
          :neonfs_containerd,
          :volume,
          @volume_name
        ])

      {:ok, daemon} = ContainerdDaemon.start(proxy_socket: socket_path)
      on_exit(fn -> ContainerdDaemon.stop(daemon) end)

      before_used = total_used_bytes(cluster)

      payload = :crypto.strong_rand_bytes(@payload_bytes)
      digest = "sha256:" <> (:crypto.hash(:sha256, payload) |> Base.encode16(case: :lower))

      blob_path = Path.join(daemon.tmp_dir, "blob")
      File.write!(blob_path, payload)

      {ingest_out, ingest_code} =
        System.cmd(
          "sh",
          [
            "-c",
            "ctr --address #{daemon.grpc_address} --namespace test " <>
              "content ingest --expected-digest #{digest} test-ref < #{blob_path}"
          ],
          stderr_to_stdout: true
        )

      assert ingest_code == 0,
             "ctr content ingest failed (#{ingest_code}): #{ingest_out}"

      after_used = total_used_bytes(cluster)

      delta = after_used - before_used

      assert delta >= @payload_bytes,
             "expected used_bytes to grow by at least #{@payload_bytes} (the payload), grew #{delta}"

      # 10% overhead bound from #727's acceptance criteria.
      max_delta = trunc(@payload_bytes * 1.1)

      assert delta <= max_delta,
             "expected used_bytes growth to be within 10% of payload (#{max_delta}), grew #{delta}"
    end
  end

  defp capacity_snapshot(cluster) do
    PeerCluster.rpc(cluster, :node2, Router, :call, [
      StorageMetrics,
      :cluster_capacity,
      []
    ])
  end

  defp total_used_bytes(cluster), do: capacity_snapshot(cluster).total_used

  defp wait_for_socket(path, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_socket(path, deadline)
  end

  defp do_wait_for_socket(path, deadline) do
    cond do
      socket_listening?(path) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, :timeout}

      true ->
        Process.sleep(100)
        do_wait_for_socket(path, deadline)
    end
  end

  defp socket_listening?(path) do
    case File.stat(path) do
      {:ok, %{type: :other}} ->
        case :gen_tcp.connect({:local, path}, 0, [:binary, active: false], 250) do
          {:ok, sock} ->
            :gen_tcp.close(sock)
            true

          {:error, _} ->
            false
        end

      _ ->
        false
    end
  end
end
