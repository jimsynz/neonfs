defmodule NeonFS.Integration.ContainerdMultiCorePersistenceTest do
  @moduledoc """
  Containerd-layer guard for #1186 / #1190 — proves that a blob ingested
  through `neonfs_containerd` on an interface peer has its per-volume
  metadata (the index-tree node chunks) replicated to the *full* metadata
  drive set, so the blob is resolvable from a core node that did **not**
  handle the commit.

  ## Why this exists separately from `commit_chunks_test.exs`

  The existing #1186 guard (`commit_chunks_test.exs`) exercises the
  cross-node `commit_chunks` round-trip directly against `NeonFS.Core`.
  The containerd suite couldn't express the regression: its tests use a
  single core node with `factor: 1` metadata, so all metadata lives on one
  drive and every read routes there — the multi-core index-tree fan-out is
  never touched. If index-tree node chunks silently stopped replicating to
  the full metadata drive set, those tests would stay green.

  ## What this guards

  - 3-peer cluster: `node1` + `node2` `:neonfs_core`, `node3`
    `:neonfs_containerd`. Volume durability `factor: 2`.
  - The blob is ingested daemon-free through the real containerd write
    state machine (`ContentServer.process_write_stream/2`, the same entry
    point `ContentServerWriteTest` drives) running on the containerd peer,
    so it flows through `ChunkWriter` to the core nodes over the data
    plane — no host `containerd` / `ctr` / root required, so it runs in
    the standard integration suite rather than the `:requires_containerd`
    job.
  - `NeonFS.Core.get_file_meta/2` is then asserted to succeed on **both**
    core nodes (a superset of "the core node that did not handle the
    commit"). If the index-tree chunks stop replicating to the full drive
    set, the read on the non-committing node fails and this test fails.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias Containerd.Services.Content.V1.WriteContentRequest
  alias NeonFS.Containerd.{ContentServer, Digest}
  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 300_000

  @volume_name "containerd-multi-core"
  @volume_opts %{
    durability: %{type: :replicate, factor: 2, min_copies: 2},
    compression: %{algorithm: :none, level: 0, min_size: 0}
  }

  describe "blob metadata replicates to the full drive set across core nodes" do
    test "info-by-digest resolves from a core node that did not commit the blob" do
      cluster =
        PeerCluster.start_cluster!(3,
          roles: %{
            node1: [:neonfs_core],
            node2: [:neonfs_core],
            node3: [:neonfs_containerd]
          }
        )

      on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

      PeerCluster.connect_nodes(cluster)

      :ok =
        init_mixed_role_cluster(cluster,
          name: "containerd-multi-core",
          volumes: [{@volume_name, @volume_opts}]
        )

      :ok =
        PeerCluster.rpc(cluster, :node3, Application, :put_env, [
          :neonfs_containerd,
          :volume,
          @volume_name
        ])

      payload = :binary.copy(<<0x7E>>, 12_000)
      digest = "sha256:" <> (:crypto.hash(:sha256, payload) |> Base.encode16(case: :lower))
      ref = "multi-core-#{System.unique_integer([:positive])}"

      frames = [
        %WriteContentRequest{
          action: :STAT,
          ref: ref,
          total: byte_size(payload),
          expected: digest
        },
        %WriteContentRequest{action: :WRITE, ref: ref, data: payload, offset: 0},
        %WriteContentRequest{
          action: :COMMIT,
          ref: ref,
          offset: byte_size(payload),
          total: byte_size(payload),
          expected: digest
        }
      ]

      # `&Function.identity/1` is a no-op `send_fn` loaded on every node —
      # the streamed replies aren't needed here; the commit's side effect
      # (the blob landing in the volume) is what we assert on below.
      :ok =
        PeerCluster.rpc(cluster, :node3, ContentServer, :process_write_stream, [
          frames,
          &Function.identity/1
        ])

      {:ok, path} = Digest.to_path(digest)

      for core_node <- [:node1, :node2] do
        assert {:ok, meta} =
                 PeerCluster.rpc(cluster, core_node, NeonFS.Core, :get_file_meta, [
                   @volume_name,
                   path
                 ]),
               "blob metadata not resolvable from #{core_node} — index-tree chunks " <>
                 "did not replicate to the full metadata drive set (#1186/#1190)"

        assert meta.size == byte_size(payload)
      end
    end
  end
end
