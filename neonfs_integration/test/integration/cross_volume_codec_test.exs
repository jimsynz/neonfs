defmodule NeonFS.Integration.CrossVolumeCodecTest do
  @moduledoc """
  Regression test for issue #270.

  Multiple volumes that share a BlobStore write the same plaintext with
  different codec (compression / encryption) settings. Before the codec
  suffix landed on the on-disk path, the later writes silently overwrote
  the earlier chunks — reads of the earlier volumes then failed or, with
  verification disabled, returned corrupt data.

  With the codec suffix in place the three variants coexist as separate
  files and every volume reads back the original bytes.
  """

  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Core.KeyManager
  alias NeonFS.Core.VolumeEncryption
  alias NeonFS.Core.VolumeRegistry

  @moduletag timeout: 120_000
  @moduletag :integration
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared

  @plaintext "Hello, World!"

  setup_all %{cluster: cluster} do
    :ok = init_single_node_cluster(cluster, name: "codec-xvol")

    # Plain: no compression, no encryption.
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :create, [
        "squeak",
        [compression: %{algorithm: :none}]
      ])

    # Compressed: zstd, no encryption.
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :create, [
        "squashed",
        [compression: %{algorithm: :zstd, level: 3}]
      ])

    # Encrypted + compressed: zstd + AES-256-GCM.
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :create, [
        "secret",
        [
          compression: %{algorithm: :zstd, level: 3},
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1)
        ]
      ])

    {:ok, secret_vol} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :get_by_name, ["secret"])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, KeyManager, :setup_volume_encryption, [secret_vol.id])

    %{}
  end

  test "overlapping-content writes across codec variants round-trip", %{cluster: cluster} do
    path = "/hello.txt"

    for volume <- ["squeak", "squashed", "secret"] do
      {:ok, _file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          volume,
          path,
          @plaintext
        ])
    end

    for volume <- ["squeak", "squashed", "secret"] do
      assert {:ok, @plaintext} =
               PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
                 volume,
                 path
               ]),
             "volume #{volume} failed to read back its own plaintext — cross-volume chunk collision?"
    end
  end
end
