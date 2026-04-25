defmodule NeonFS.Integration.ProcessHeapProfileTest do
  @moduledoc """
  Diagnostic profile for the streaming-write process-heap growth
  surfaced in #534.

  Tagged `:profile` so it doesn't run on every check pass — invoke
  with `mix test --include profile test/integration/process_heap_profile_test.exs`
  to dump per-process memory deltas during a real cross-node
  upload, identifying which process owns the per-chunk-size growth.

  The test is the diagnostic; the *fix* (or the documentation that
  the growth is fundamental) lands alongside or after it.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Integration.{ClusterCase, PeerCluster, ProcessMemoryProfile}

  @moduletag timeout: 300_000
  @moduletag :profile

  test "where does the per-chunk process heap go" do
    cluster =
      PeerCluster.start_cluster!(2,
        roles: %{node1: [:neonfs_core], node2: [:neonfs_s3]}
      )

    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    PeerCluster.connect_nodes(cluster)
    :ok = ClusterCase.init_mixed_role_cluster(cluster, name: "profile-534")

    volume_name = "profile-vol-#{System.unique_integer([:positive])}"

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [volume_name, %{}])

    :ok =
      ClusterCase.wait_until(
        fn ->
          match?(
            {:ok, _},
            PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
              volume_name
            ])
          )
        end,
        timeout: 10_000
      )

    seed = :crypto.strong_rand_bytes(1_048_576)
    target_node = PeerCluster.get_node!(cluster, :node1).node

    # Set up an S3 credential so the S3.Backend path is callable.
    {:ok, cred} =
      PeerCluster.rpc(
        cluster,
        :node1,
        NeonFS.Core.S3CredentialManager,
        :create,
        [%{user: "profile"}]
      )

    s3_ctx = %{access_key_id: cred.access_key_id, identity: %{user: "profile"}}

    IO.puts(:stderr, "\n#{header()}\n")

    # Warm-up upload so code-server loading doesn't contaminate the measurement.
    PeerCluster.rpc(cluster, :node2, ProcessMemoryProfile, :run_streaming_profile, [
      volume_name,
      {:chunk_writer, [target_node: target_node, drive_id: "default", strategy: "fastcdc"]},
      seed,
      8 * 1_048_576
    ])

    for path_choice <- [
          {:chunk_writer, [target_node: target_node, drive_id: "default", strategy: "fastcdc"]},
          {:s3, [ctx: s3_ctx]}
        ],
        size_mib <- [16, 64, 256] do
      size = size_mib * 1_048_576
      label = elem(path_choice, 0)

      diagnostics =
        PeerCluster.rpc(cluster, :node2, ProcessMemoryProfile, :run_streaming_profile, [
          volume_name,
          path_choice,
          seed,
          size
        ])

      IO.puts(
        :stderr,
        "\n--- path=#{label}, upload_size=#{size_mib} MiB, refs=#{diagnostics.refs} ---"
      )

      IO.puts(:stderr, "post_upload_total=#{format_bytes(diagnostics.total_delta)}")

      IO.puts(
        :stderr,
        "post_upload_processes_used=#{format_bytes(diagnostics.processes_used_delta)}"
      )

      IO.puts(
        :stderr,
        "peak_processes_used=#{format_bytes(diagnostics.peak_processes_used_delta)}"
      )

      IO.puts(:stderr, "Top process growers (at peak):\n#{diagnostics.top_growers}")
    end
  end

  defp header do
    String.duplicate("=", 60) <>
      "\nProcess heap profile for streaming write (#534)\n" <>
      String.duplicate("=", 60)
  end

  defp format_bytes(b) when b >= 1_048_576, do: "#{Float.round(b / 1_048_576, 2)} MiB"
  defp format_bytes(b) when b >= 1024, do: "#{Float.round(b / 1024, 1)} KiB"
  defp format_bytes(b), do: "#{b} B"
end
