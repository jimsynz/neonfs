defmodule NeonFS.Integration.ContainerdContentTest do
  @moduledoc """
  Integration test for #725 (first slice of #554) — proves
  `neonfs_containerd` round-trips bytes through a real `containerd`
  daemon via the `proxy_plugins` mechanism, with the `neonfs_containerd`
  service running on a peer cluster node rather than in the test BEAM.

  ## Architecture

  - 2-peer mixed-role cluster:
      - `node1`: `:neonfs_core`
      - `node2`: `:neonfs_containerd` (interface peer; `:neonfs_client`
        is started transitively via `ensure_all_started/1`).
  - `init_mixed_role_cluster` joins the containerd peer to the cluster,
    establishes the data-plane pool, and waits for `Discovery` to see
    `node1`.
  - `PeerCluster` allocates a per-test UDS socket path under the
    containerd peer's data dir (#733 wired this) and configures
    `:neonfs_containerd` to bind there at startup.
  - The host spawns a `containerd` subprocess with a `[proxy_plugins]`
    config that disables the default content store and routes content
    operations through the peer's UDS.
  - `ctr content ingest` / `ctr content get` against the host
    containerd flow through to `neonfs_containerd` on the peer, which
    in turn pushes bytes through `NeonFS.Client.ChunkWriter` to the
    core peer over the data plane.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Integration.ContainerdDaemon
  alias NeonFS.TestSupport.{ClusterCase, PeerCluster}

  @moduletag :requires_containerd
  @moduletag :requires_root
  @moduletag timeout: 180_000

  @volume_name "containerd-test"
  @volume_opts %{
    durability: %{type: :replicate, factor: 1, min_copies: 1},
    compression: %{algorithm: :none, level: 0, min_size: 0}
  }

  describe "content round-trip through containerd proxy plugin" do
    test "ingest then get returns identical bytes via real ctr" do
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
          name: "containerd-content-test",
          volumes: [{@volume_name, @volume_opts}]
        )

      containerd_peer = PeerCluster.get_node!(cluster, :node2)
      socket_path = containerd_peer.interface_ports.containerd

      :ok = wait_for_socket(socket_path, 30_000)

      # `WriteSession` reads `:neonfs_containerd, :volume` (not
      # `:default_volume`) to pick the target volume — see
      # `default_volume/0` in `write_session.ex`.
      :ok =
        PeerCluster.rpc(cluster, :node2, Application, :put_env, [
          :neonfs_containerd,
          :volume,
          @volume_name
        ])

      {:ok, daemon} = ContainerdDaemon.start(proxy_socket: socket_path)
      on_exit(fn -> ContainerdDaemon.stop(daemon) end)

      payload = :crypto.strong_rand_bytes(64 * 1024)
      digest = "sha256:" <> (:crypto.hash(:sha256, payload) |> Base.encode16(case: :lower))

      blob_path = Path.join(daemon.tmp_dir, "blob")
      File.write!(blob_path, payload)

      # `System.cmd` doesn't redirect stdin from a file, so use
      # `sh -c` to wire `ctr content ingest <ref> < blob`.
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

      {ls_out, ls_code} = ContainerdDaemon.ctr(daemon, "test", ["content", "ls"])

      assert ls_code == 0

      assert ls_out =~ digest,
             "expected digest #{digest} in content ls output, got:\n#{ls_out}"

      {get_out, get_code} = ContainerdDaemon.ctr(daemon, "test", ["content", "get", digest])

      assert get_code == 0,
             "ctr content get failed (#{get_code}): #{get_out}"

      assert get_out == payload,
             "expected get to return the ingested bytes (length #{byte_size(payload)}), " <>
               "got #{byte_size(get_out)} bytes"
    end
  end

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
