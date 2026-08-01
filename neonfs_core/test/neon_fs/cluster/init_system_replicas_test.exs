defmodule NeonFS.Cluster.InitSystemReplicasTest do
  @moduledoc """
  `cluster init` seeds `_system`'s replication factor from the drives it
  was given.

  Drives are registered before the system volume exists, so the
  drive-registration hook that raises the factor towards the drive count
  finds nothing to raise and does nothing. A cluster initialised with
  several drives in one shot therefore used to start at factor 1 and gain
  redundancy only on the next `drive add` — for a volume holding the CA
  key and cluster identity, whose loss is unrecoverable.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.VolumeRegistry

  @moduletag :tmp_dir

  setup do
    on_exit(fn ->
      stop_ra()
      cleanup_test_dirs()
    end)

    :ok
  end

  describe "the default factor tracks the initial drive count" do
    test "two drives in one shot give factor 2", %{tmp_dir: tmp_dir} do
      assert {:ok, _cluster_id} = start_provisioned_cluster(tmp_dir, drives: drives(tmp_dir, 2))

      assert {:ok, volume} = VolumeRegistry.get_system_volume()
      assert volume.durability.factor == 2
    end

    test "a single drive still gives factor 1", %{tmp_dir: tmp_dir} do
      assert {:ok, _cluster_id} = start_provisioned_cluster(tmp_dir, drives: drives(tmp_dir, 1))

      assert {:ok, volume} = VolumeRegistry.get_system_volume()
      assert volume.durability.factor == 1
    end

    # Past three the extra copies cost more than they buy for a volume this
    # small, which is the same cap `add_drive/1` raises towards.
    test "the factor is capped at three however many drives there are", %{tmp_dir: tmp_dir} do
      assert {:ok, _cluster_id} = start_provisioned_cluster(tmp_dir, drives: drives(tmp_dir, 5))

      assert {:ok, volume} = VolumeRegistry.get_system_volume()
      assert volume.durability.factor == 3
    end
  end

  # The count has to be of drives the volume can actually land on. Counting
  # every drive regardless of tier asks for more copies than there are places
  # to put them, and the cluster-identity write that follows creation fails
  # outright on the missing quorum.
  test "drives in other tiers do not inflate the factor", %{tmp_dir: tmp_dir} do
    hot = Path.join(tmp_dir, "blobs-hot")
    cold = Path.join(tmp_dir, "blobs-cold")
    File.mkdir_p!(hot)
    File.mkdir_p!(cold)

    assert {:ok, _cluster_id} =
             start_provisioned_cluster(tmp_dir,
               drives: [
                 %{id: "hot-1", path: hot, tier: :hot, capacity: 0},
                 %{id: "cold-1", path: cold, tier: :cold, capacity: 0}
               ]
             )

    assert {:ok, volume} = VolumeRegistry.get_system_volume()
    assert volume.tiering.initial_tier == :hot

    assert volume.durability.factor == 1,
           "only one drive is in the volume's tier, so only one copy can be placed"
  end

  describe "an explicit --system-replicas is authoritative" do
    test "a value below the drive count is honoured, not raised", %{tmp_dir: tmp_dir} do
      assert {:ok, _cluster_id} =
               start_provisioned_cluster(tmp_dir,
                 drives: drives(tmp_dir, 3),
                 init_opts: [system_replicas: 1]
               )

      assert {:ok, volume} = VolumeRegistry.get_system_volume()

      assert volume.durability.factor == 1,
             "the operator asked for 1; a drive count is not a reason to override it"
    end
  end

  defp drives(tmp_dir, count) do
    for n <- 1..count do
      path = Path.join(tmp_dir, "blobs-#{n}")
      File.mkdir_p!(path)
      %{id: "drive-#{n}", path: path, tier: :hot, capacity: 0}
    end
  end
end
