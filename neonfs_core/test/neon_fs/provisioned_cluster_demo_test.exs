defmodule NeonFS.ProvisionedClusterDemoTest do
  @moduledoc """
  Demonstration test for the `#1008` provisioned-cluster helpers in
  `NeonFS.TestCase`. Exercises the chain
  `start_provisioned_cluster/2` → `create_provisioned_volume/2` →
  `WriteOperation.write_file_streamed/3` → `Snapshot.create/2` →
  `MetadataReader.range/5` (at the snapshot root) → `Core.read_file_stream/2`,
  i.e. every step the GC E2E (`#985`), CSI kind-cluster (`#995`), and DR
  restore (`#1005`) integration tests need.

  Stays in `neonfs_core/test/` rather than `neonfs_integration/`
  because no peer cluster is involved — the value is to give future
  contributors a worked example of the helper API.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core
  alias NeonFS.Core.{Snapshot, WriteOperation}
  alias NeonFS.Core.Volume.MetadataReader

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    {:ok, _cluster_id} = start_provisioned_cluster(tmp_dir)

    on_exit(fn ->
      stop_ra()
      cleanup_test_dirs()
    end)

    :ok
  end

  test "write → snapshot → range scan at snapshot root → read back live" do
    {:ok, volume} =
      create_provisioned_volume("demo-vol",
        durability: %{type: :replicate, factor: 1, min_copies: 1}
      )

    payload = "hello from #1008 provisioned helpers"

    {:ok, _meta} =
      WriteOperation.write_file_streamed(volume.id, "/greeting.txt", [payload])

    {:ok, snap} = Snapshot.create(volume.id, name: "initial")
    assert is_binary(snap.root_chunk_hash)
    assert byte_size(snap.root_chunk_hash) == 32

    {:ok, snap_entries} =
      MetadataReader.range(volume.id, :file_index, <<>>, <<>>, at_root: snap.root_chunk_hash)

    assert snap_entries != [],
           "snapshot's file_index tree should contain at least the file + its dir entry"

    {:ok, %{stream: stream}} = Core.read_file_stream(volume.name, "/greeting.txt")
    assert stream |> Enum.to_list() |> IO.iodata_to_binary() == payload
  end
end
