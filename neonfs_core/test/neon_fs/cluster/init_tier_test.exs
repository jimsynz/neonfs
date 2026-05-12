defmodule NeonFS.Cluster.InitTierTest do
  @moduledoc """
  Regression coverage for `#1009` — `cluster init` used to hard-code
  the system volume's `initial_tier` to `:hot`, which broke single-disk
  deployments where the operator passed `--tier cold` (or `--tier warm`).

  Sibling to `init_test.exs`: that file exercises the happy hot-only
  path; this file exercises the bootstrap-tier-selection logic.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{SystemVolume, VolumeRegistry}

  @moduletag :tmp_dir

  setup do
    on_exit(fn ->
      stop_ra()
      cleanup_test_dirs()
    end)

    :ok
  end

  describe "init_cluster/2 — bootstrap-tier selection" do
    test "succeeds when the only registered drive is cold", %{tmp_dir: tmp_dir} do
      blob_dir = Path.join(tmp_dir, "blobs")
      File.mkdir_p!(blob_dir)

      assert {:ok, _cluster_id} =
               start_provisioned_cluster(tmp_dir,
                 drives: [%{id: "cold-1", path: blob_dir, tier: :cold, capacity: 0}]
               )

      assert {:ok, volume} = VolumeRegistry.get_system_volume()
      assert volume.tiering.initial_tier == :cold

      # End-to-end: the identity blob must round-trip through the
      # cold-tier write path that previously rejected the volume.
      assert {:ok, _json} = SystemVolume.read("/cluster/identity.json")
    end

    test "succeeds when the only registered drive is warm", %{tmp_dir: tmp_dir} do
      blob_dir = Path.join(tmp_dir, "blobs")
      File.mkdir_p!(blob_dir)

      assert {:ok, _cluster_id} =
               start_provisioned_cluster(tmp_dir,
                 drives: [%{id: "warm-1", path: blob_dir, tier: :warm, capacity: 0}]
               )

      assert {:ok, volume} = VolumeRegistry.get_system_volume()
      assert volume.tiering.initial_tier == :warm
      assert {:ok, _json} = SystemVolume.read("/cluster/identity.json")
    end

    test "still picks hot when a hot drive is present alongside non-hot drives",
         %{tmp_dir: tmp_dir} do
      hot_dir = Path.join(tmp_dir, "blobs-hot")
      cold_dir = Path.join(tmp_dir, "blobs-cold")
      File.mkdir_p!(hot_dir)
      File.mkdir_p!(cold_dir)

      assert {:ok, _cluster_id} =
               start_provisioned_cluster(tmp_dir,
                 drives: [
                   %{id: "hot-1", path: hot_dir, tier: :hot, capacity: 0},
                   %{id: "cold-1", path: cold_dir, tier: :cold, capacity: 0}
                 ]
               )

      assert {:ok, volume} = VolumeRegistry.get_system_volume()
      assert volume.tiering.initial_tier == :hot
    end
  end
end
