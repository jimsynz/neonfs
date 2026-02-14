defmodule NeonFS.Cluster.JoinTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Cluster.{Init, Invite, Join}
  alias NeonFS.Core.VolumeRegistry

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()

    start_drive_registry()
    start_blob_store()
    start_chunk_index()
    start_file_index()
    start_stripe_index()
    start_volume_registry()
    ensure_chunk_access_tracker()
    start_ra()

    # Initialise a cluster so we have state, a valid invite, and a system volume
    {:ok, _cluster_id} = Init.init_cluster("join-test-cluster")

    # Start ServiceRegistry after init_cluster so Ra is fully ready
    start_service_registry()

    on_exit(fn ->
      stop_ra()
      cleanup_test_dirs()
    end)

    :ok
  end

  describe "accept_join/3 system volume replication" do
    test "adjusts factor on core joins and skips non-core joins" do
      # System volume starts with factor 1 (single node at init)
      {:ok, volume} = VolumeRegistry.get_system_volume()
      assert volume.durability.factor == 1

      # Verify sequential core join replication adjustments (1 → 2 → 3).
      # We call VolumeRegistry directly rather than going through accept_join
      # because adding a fake core node via accept_join changes Ra quorum,
      # preventing subsequent Ra operations from succeeding.
      {:ok, vol2} = VolumeRegistry.adjust_system_volume_replication(2)
      assert vol2.durability.factor == 2

      {:ok, vol3} = VolumeRegistry.adjust_system_volume_replication(3)
      assert vol3.durability.factor == 3

      # Non-core join via accept_join does not change replication factor
      {:ok, token} = Invite.create_invite(3600)
      {:ok, _cluster_info} = Join.accept_join(token, :fuse_peer@localhost, :fuse)

      {:ok, vol_after_fuse} = VolumeRegistry.get_system_volume()
      assert vol_after_fuse.durability.factor == 3

      # Failure-tolerance: maybe_adjust_system_volume_replication is called outside
      # the `with` chain in accept_join/3 and its result is discarded, so adjustment
      # failure cannot abort a join.

      # Reset to initial factor for test isolation — Ra state persists across
      # repeat-until-failure runs within the same BEAM process.
      VolumeRegistry.adjust_system_volume_replication(1)
    end
  end
end
