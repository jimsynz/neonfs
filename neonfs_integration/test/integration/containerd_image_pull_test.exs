defmodule NeonFS.Integration.ContainerdImagePullTest do
  @moduledoc """
  Integration test for #728 (last slice of #554) — proves a real
  `ctr image pull` against `neonfs_containerd`'s proxy plugin lands
  every layer blob in the dedicated NeonFS volume keyed by digest.

  ## Architecture

  - 2-peer mixed-role cluster:
      - `node1`: `:neonfs_core`
      - `node2`: `:neonfs_containerd`
  - Host `containerd` subprocess pointing at the proxy plugin (same
    pattern as #725 / #726 / #727).
  - The CI runner exposes a `registry:2` sidecar at `registry:5000`
    pre-seeded with a committed 2-layer OCI fixture (alpine base +
    a tiny marker layer). The CI workflow wires this up before this
    test runs; locally, the fixture is at
    `neonfs_integration/test/fixtures/test-image.tar` and `skopeo`
    push runs once at job start.
  - The test pulls the image via `ctr image pull
    registry:5000/neonfs-test-image:v1 --plain-http`. Containerd's
    pull workflow calls `Info` → `Read` per layer through the proxy
    plugin, which writes each blob into the configured NeonFS
    volume.
  - Asserts every layer digest reported in the manifest is
    retrievable via `NeonFS.Containerd.ContentServer.info/2` on
    the containerd peer.

  ## What's deliberately NOT tested here

  `ctr run --rm <image> echo hi` — needs a snapshotter, parked
  under #740. The skipped sub-test below has the assertion frame
  but is `@tag skip:` until that issue lands.
  """

  use ExUnit.Case, async: false

  alias Containerd.Services.Content.V1.{InfoRequest, InfoResponse}
  alias NeonFS.Containerd.ContentServer
  alias NeonFS.Integration.ContainerdDaemon
  alias NeonFS.TestSupport.{ClusterCase, PeerCluster}

  @moduletag :requires_containerd
  @moduletag :requires_root
  @moduletag :requires_test_registry
  @moduletag timeout: 240_000

  @volume_name "containerd-image-pull"
  @volume_opts %{
    durability: %{type: :replicate, factor: 1, min_copies: 1},
    compression: %{algorithm: :none, level: 0, min_size: 0}
  }

  @registry_host "registry:5000"
  @image_ref "#{@registry_host}/neonfs-test-image:v1"

  describe "image pull through containerd proxy plugin" do
    test "every layer blob lands in the dedicated NeonFS volume" do
      cluster = start_cluster()
      daemon = start_daemon(cluster)

      # `ctr content fetch` is the lower-level pull command. It
      # streams every blob (manifest, config, layers) into the
      # content store via `Write` RPCs but does NOT try to unpack
      # the layers via the snapshotter — which is what `ctr image
      # pull` does and what hits #740 right now. Same exercise of
      # neonfs_containerd's proxy plugin minus the post-pull
      # unpack dance.
      {pull_out, pull_code} =
        ContainerdDaemon.ctr(daemon, "test", [
          "content",
          "fetch",
          "--plain-http",
          @image_ref
        ])

      assert pull_code == 0,
             "ctr content fetch failed (#{pull_code}):\n#{pull_out}"

      # Discover layer digests from the host registry's manifest —
      # ground truth for "every layer should land".
      manifest_digests = layer_digests_from_registry()

      assert length(manifest_digests) >= 2,
             "fixture should be multi-layer; got #{length(manifest_digests)} layers"

      for digest <- manifest_digests do
        assert %InfoResponse{info: %{digest: ^digest, size: size}} =
                 PeerCluster.rpc(cluster, :node2, ContentServer, :info, [
                   struct(InfoRequest, digest: digest),
                   nil
                 ])

        assert size > 0, "layer #{digest} reported zero size"
      end
    end

    @tag skip: "snapshotter integration parked under #740"
    test "ctr run starts the container after the pull" do
      cluster = start_cluster()
      daemon = start_daemon(cluster)

      {_, 0} = ContainerdDaemon.ctr(daemon, "test", ["image", "pull", "--plain-http", @image_ref])

      {run_out, run_code} =
        ContainerdDaemon.ctr(daemon, "test", [
          "run",
          "--rm",
          @image_ref,
          "neonfs-test-runner",
          "/bin/sh",
          "-c",
          "echo hi-from-#{:rand.uniform(1_000_000)}"
        ])

      assert run_code == 0,
             "ctr run failed (#{run_code}):\n#{run_out}"

      assert run_out =~ "hi-from-",
             "expected `echo` output in stdout, got:\n#{run_out}"
    end
  end

  ## Helpers

  defp start_cluster do
    cluster =
      PeerCluster.start_cluster!(2,
        roles: %{node1: [:neonfs_core], node2: [:neonfs_containerd]}
      )

    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    PeerCluster.connect_nodes(cluster)

    :ok =
      ClusterCase.init_mixed_role_cluster(cluster,
        name: "containerd-image-pull-test",
        volumes: [{@volume_name, @volume_opts}]
      )

    :ok =
      PeerCluster.rpc(cluster, :node2, Application, :put_env, [
        :neonfs_containerd,
        :volume,
        @volume_name
      ])

    cluster
  end

  defp start_daemon(cluster) do
    socket_path = PeerCluster.get_node!(cluster, :node2).interface_ports.containerd
    :ok = wait_for_socket(socket_path, 30_000)

    {:ok, daemon} = ContainerdDaemon.start(proxy_socket: socket_path)
    on_exit(fn -> ContainerdDaemon.stop(daemon) end)
    daemon
  end

  defp layer_digests_from_registry do
    # Use `skopeo inspect --raw` to fetch the manifest as JSON and
    # extract the layer digests. Plain HTTP because the sidecar
    # doesn't run TLS.
    {raw, 0} =
      System.cmd(
        "skopeo",
        ["inspect", "--tls-verify=false", "--raw", "docker://#{@image_ref}"],
        stderr_to_stdout: true
      )

    raw
    |> Jason.decode!()
    |> Map.fetch!("layers")
    |> Enum.map(& &1["digest"])
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
