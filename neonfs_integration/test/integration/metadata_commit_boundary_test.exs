defmodule NeonFS.Integration.MetadataCommitBoundaryTest do
  @moduledoc """
  Fault injection at the one cross-shard commit boundary a writer-level
  harness cannot reach: the gap between a root set reaching consensus and
  the post-commit effects that follow it.

  `metadata_writer_root_set_test.exs` covers the boundaries that live
  inside `MetadataWriter` — before publication, the mid-CAS window that
  the single root-set command removes, the ambiguous reply, and a batch
  carrying several logical operations. Everything after the publication
  belongs to `FileIndex`: materialising the local ETS cache, broadcasting
  the change event, replying to the caller. A crash in that gap is the
  case where the durable outcome and the node's own view of it diverge,
  and it needs a real node to reproduce.

  The operation's conflict lease is deliberately *not* in that list. It is
  released by the publishing log entry itself, so these tests also pin
  that a crashed operation leaves no lease behind — the reason it is not a
  post-commit effect is precisely the crash they inject.

  Each test parks a batch in the volume's suspended commit worker, kills
  the node's `FileIndex`, then lets the publication land — so the root
  set is durable and *none* of its effects ran. The assertions are the
  acceptance bar: authoritative reads from every node see the whole
  operation or none of it, never a dangling dirent, an orphaned
  `FileMeta`, a duplicated rename target, or a `FileMeta.path` that
  disagrees with the surviving dirent.

  Operations are spread across the three nodes rather than all driven
  from `node1`: a core node's supervisor is `one_for_one` with the
  default restart intensity, so repeatedly killing one node's
  `FileIndex` within five seconds would take the whole tree down and
  the failure would look like a product bug rather than a test one.
  Spreading them also means the node that crashed is never the only one
  asked to confirm the outcome.

  Whole-node restart lives in `metadata_commit_boundary_restart_test.exs`,
  which needs a `:per_test` cluster.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.{FileIndex, FileMeta, MetadataStateMachine, RaSupervisor, VolumeRegistry}
  alias NeonFS.Core.Volume.Shard
  alias NeonFS.Integration.CommitBoundary
  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 300_000
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared
  @moduletag :integration

  @volume "commit-boundary"
  @nodes [:node1, :node2, :node3]
  @read_timeout 30_000

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, volumes: [{@volume, %{}}])

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, VolumeRegistry, :get_by_name, [@volume])

    # Every shard calculation below runs in the test process, so its
    # count has to be the cluster's — a mismatch would silently make the
    # "distinct shards" claim false.
    assert PeerCluster.rpc(cluster, :node1, Shard, :count, []) == Shard.count()
    assert Shard.count() > 1, "cross-shard cases prove nothing at a single shard"

    %{volume: volume}
  end

  test "a create that crashes before its effects is complete on every node",
       %{cluster: cluster, volume: volume} do
    dir = "/created"
    name = "file.bin"
    path = Path.join(dir, name)
    file_id = file_id_off_shards([dirent_key(volume.id, dir, name)])
    file = FileMeta.new(volume.id, path, id: file_id)

    roots_before = volume_shards(cluster, volume.id)

    assert {:ok, _roots} =
             crash_before_effects(cluster, :node1, volume.id, [
               FileIndex,
               :create,
               [file]
             ])

    assert_shards_advanced(cluster, volume.id, roots_before, [
      file_key(file_id),
      dirent_key(volume.id, dir, name)
    ])

    assert_file_present(cluster, volume.id, path, file_id)
    assert_dir_contains(cluster, volume.id, dir, [name])

    # Re-issuing the operation is the caller's only recourse — its own
    # reply died with the process. The retry re-publishes byte-identical
    # metadata under the same file id, so it converges on the state that
    # is already there rather than conflicting with itself over a lease
    # its own crash left behind.
    assert {:ok, _} = PeerCluster.rpc(cluster, :node1, FileIndex, :create, [file])
    assert_file_present(cluster, volume.id, path, file_id)

    assert_recovered(cluster, :node1, volume.id)
  end

  test "a delete that crashes before its effects removes both keys on every node",
       %{cluster: cluster, volume: volume} do
    dir = "/deleted"
    name = "file.bin"
    path = Path.join(dir, name)
    file_id = file_id_off_shards([dirent_key(volume.id, dir, name)])

    create_on(cluster, :node2, volume.id, path, file_id)

    roots_before = volume_shards(cluster, volume.id)

    assert {:ok, _roots} =
             crash_before_effects(cluster, :node2, volume.id, [
               FileIndex,
               :delete,
               [file_id]
             ])

    assert_shards_advanced(cluster, volume.id, roots_before, [
      file_key(file_id),
      dirent_key(volume.id, dir, name)
    ])

    assert_file_absent(cluster, volume.id, path, file_id)
    assert_dir_contains(cluster, volume.id, dir, [])

    # The retry resolves the file before it deletes it, so a second
    # attempt finds nothing to delete rather than deleting twice.
    assert {:error, :not_found} =
             PeerCluster.rpc(cluster, :node2, FileIndex, :delete, [file_id])

    assert_recovered(cluster, :node2, volume.id)
  end

  test "a rename that crashes before its effects leaves exactly one target on every node",
       %{cluster: cluster, volume: volume} do
    dir = "/renamed"
    old_name = "before.bin"
    new_name = "after.bin"
    new_path = Path.join(dir, new_name)

    file_id =
      file_id_off_shards([
        dirent_key(volume.id, dir, old_name),
        dirent_key(volume.id, dir, new_name)
      ])

    create_on(cluster, :node3, volume.id, Path.join(dir, old_name), file_id)

    roots_before = volume_shards(cluster, volume.id)

    assert {:ok, _roots} =
             crash_before_effects(cluster, :node3, volume.id, [
               FileIndex,
               :rename,
               [volume.id, dir, old_name, new_name]
             ])

    assert_shards_advanced(cluster, volume.id, roots_before, [
      file_key(file_id),
      dirent_key(volume.id, dir, old_name),
      dirent_key(volume.id, dir, new_name)
    ])

    assert_file_present(cluster, volume.id, new_path, file_id)
    assert_path_absent(cluster, volume.id, Path.join(dir, old_name))
    assert_dir_contains(cluster, volume.id, dir, [new_name])

    # A rename leases the whole parent directory, so a lease the crash
    # left held would refuse every later rename and move here for the
    # intent's TTL — including the retry of the one that already
    # succeeded. It does not, because the entry that published the rename
    # released the lease in the same breath: there was never a window in
    # which the write was durable and its lease was not.
    assert :ok =
             PeerCluster.rpc(cluster, :node3, FileIndex, :rename, [
               volume.id,
               dir,
               new_name,
               "third.bin"
             ])

    assert_file_present(cluster, volume.id, Path.join(dir, "third.bin"), file_id)
    assert_recovered(cluster, :node3, volume.id)
  end

  test "a move-rename that crashes before its effects publishes one coherent path on every node",
       %{cluster: cluster, volume: volume} do
    source_dir = "/move-src"
    dest_dir = "/move-dst"
    name = "original.bin"
    dest_name = "moved.bin"
    dest_path = Path.join(dest_dir, dest_name)

    file_id =
      file_id_off_shards([
        dirent_key(volume.id, source_dir, name),
        dirent_key(volume.id, dest_dir, dest_name)
      ])

    create_on(cluster, :node1, volume.id, Path.join(source_dir, name), file_id)
    assert {:ok, _} = PeerCluster.rpc(cluster, :node1, FileIndex, :mkdir, [volume.id, dest_dir])

    roots_before = volume_shards(cluster, volume.id)

    assert {:ok, _roots} =
             crash_before_effects(cluster, :node1, volume.id, [
               FileIndex,
               :move_rename,
               [volume.id, source_dir, dest_dir, name, dest_name]
             ])

    assert_shards_advanced(cluster, volume.id, roots_before, [
      file_key(file_id),
      dirent_key(volume.id, source_dir, name),
      dirent_key(volume.id, dest_dir, dest_name)
    ])

    assert_file_present(cluster, volume.id, dest_path, file_id)
    assert_path_absent(cluster, volume.id, Path.join(source_dir, name))
    assert_dir_contains(cluster, volume.id, source_dir, [])
    assert_dir_contains(cluster, volume.id, dest_dir, [dest_name])

    assert_recovered(cluster, :node1, volume.id)
  end

  # ─── Assertions ──────────────────────────────────────────────────────

  # Both halves of the operation, from every node. `get_by_path/2`
  # resolves the dirent *and* then the `FileMeta` it points at, so it
  # fails on a dangling dirent; `get/2` reads the `FileMeta` directly, so
  # the pair also pins the `FileMeta.path` against the surviving dirent.
  defp assert_file_present(cluster, volume_id, path, file_id) do
    for node <- @nodes do
      assert_eventually timeout: @read_timeout do
        match?(
          {:ok, %FileMeta{id: ^file_id, path: ^path}},
          PeerCluster.rpc(cluster, node, FileIndex, :get_by_path, [volume_id, path])
        )
      end

      assert {:ok, %FileMeta{id: ^file_id, path: ^path}} =
               PeerCluster.rpc(cluster, node, FileIndex, :get, [volume_id, file_id]),
             "#{node} resolves the dirent for #{path} but not a matching FileMeta"
    end
  end

  defp assert_file_absent(cluster, volume_id, path, file_id) do
    assert_path_absent(cluster, volume_id, path)

    for node <- @nodes do
      assert PeerCluster.rpc(cluster, node, FileIndex, :get, [volume_id, file_id]) ==
               {:error, :not_found},
             "#{node} still resolves an orphaned FileMeta for the deleted #{path}"
    end
  end

  defp assert_path_absent(cluster, volume_id, path) do
    for node <- @nodes do
      assert_eventually timeout: @read_timeout do
        PeerCluster.rpc(cluster, node, FileIndex, :get_by_path, [volume_id, path]) ==
          {:error, :not_found}
      end
    end
  end

  defp assert_dir_contains(cluster, volume_id, dir, names) do
    expected = Enum.sort(names)

    for node <- @nodes do
      assert_eventually timeout: @read_timeout do
        dir_names(cluster, node, volume_id, dir) == expected
      end
    end
  end

  # Every key the operation touched sits on a different root pointer, and
  # every one of those pointers moved. Deriving the shard set from the
  # keys — rather than counting whichever roots happen to differ — is what
  # makes "this case spans shards" a property of the test rather than of
  # the hash landing well on the day.
  defp assert_shards_advanced(cluster, volume_id, roots_before, keys) do
    shards = MapSet.new(keys, &Shard.for_key/1)

    assert MapSet.size(shards) >= 2,
           "#{inspect(keys)} share a shard, so this case cannot cross a boundary"

    assert_eventually timeout: @read_timeout do
      roots_after = volume_shards(cluster, volume_id)
      Enum.all?(shards, &(root_of(roots_after, &1) != root_of(roots_before, &1)))
    end
  end

  # Reads working again only proves the other two nodes are up. This
  # commits fresh metadata through the restarted `FileIndex` itself, on a
  # conflict key the crash never touched, so nothing here can be answered
  # by an abandoned lease.
  defp assert_recovered(cluster, node, volume_id) do
    path = "/recovered/#{node}-#{System.unique_integer([:positive])}.bin"
    file_id = UUIDv7.generate()

    :ok = create_on(cluster, node, volume_id, path, file_id)
    assert_file_present(cluster, volume_id, path, file_id)
  end

  # ─── Helpers ─────────────────────────────────────────────────────────

  # Generously above the helper's own dispatch and publication deadlines,
  # so a stall surfaces as its diagnostic rather than as `:badrpc`.
  defp crash_before_effects(cluster, node, volume_id, [module, function, args]) do
    PeerCluster.rpc(
      cluster,
      node,
      CommitBoundary,
      :crash_before_effects,
      [volume_id, module, function, args],
      120_000
    )
  end

  defp create_on(cluster, node, volume_id, path, file_id) do
    file = FileMeta.new(volume_id, path, id: file_id)
    assert {:ok, _} = PeerCluster.rpc(cluster, node, FileIndex, :create, [file])
    :ok
  end

  defp dir_names(cluster, node, volume_id, dir) do
    case PeerCluster.rpc(cluster, node, FileIndex, :list_dir, [volume_id, dir]) do
      {:ok, children} -> children |> Map.keys() |> Enum.sort()
      other -> other
    end
  end

  defp volume_shards(cluster, volume_id) do
    {:ok, roots} =
      PeerCluster.rpc(cluster, :node1, RaSupervisor, :local_query, [
        &MetadataStateMachine.get_volume_roots/1
      ])

    Map.get(roots, volume_id, %{})
  end

  defp root_of(roots, shard) do
    roots |> Map.get(shard, %{}) |> Map.get(:root_chunk_hash)
  end

  # `Shard.for_key/1` hashes, so a file id can collide with the dirent
  # keys of its own operation. Draw ids until the `file:` key lands
  # somewhere else.
  defp file_id_off_shards(dirent_keys) do
    occupied = MapSet.new(dirent_keys, &Shard.for_key/1)

    Stream.repeatedly(&UUIDv7.generate/0)
    |> Stream.take(1_000)
    |> Enum.find(&(Shard.for_key(file_key(&1)) not in occupied))
    |> case do
      nil -> flunk("no file id landed off #{inspect(MapSet.to_list(occupied))}")
      id -> id
    end
  end

  # `FileIndex` builds these privately; the layout is duplicated here so
  # the test can name the shards an operation touches. Drift shows up as
  # `assert_shards_advanced/4` failing on a root that never moved, not as
  # a silent pass.
  defp file_key(file_id), do: "file:" <> file_id

  defp dirent_key(volume_id, dir_path, name),
    do: "dirent:" <> volume_id <> ":" <> dir_path <> <<0>> <> name
end
