defmodule NeonFS.Cluster.InitSystemFactorTest do
  @moduledoc """
  `cluster init` registers drives in the bootstrap layer before it creates
  the `_system` volume, so the raise that runs on drive registration finds no
  volume to act on. A cluster brought up with several drives in one shot must
  still land on a drive-aware factor rather than waiting for a `drive add`.
  """
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Cluster.Init
  alias NeonFS.Core.{DriveStateRegistry, DriveStateSupervisor, VolumeRegistry}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    drives =
      for id <- ~w[drive-a drive-b] do
        path = Path.join(tmp_dir, id)
        File.mkdir_p!(path)
        %{id: id, path: path, tier: :hot, capacity: 0}
      end

    Application.put_env(:neonfs_core, :drives, drives)

    stop_ra()

    # With real drives configured, `DriveManager` starts a `DriveState` worker
    # per drive — the sibling init tests get away without these because they
    # bootstrap from an empty drive list.
    ExUnit.Callbacks.start_supervised!({Registry, keys: :unique, name: DriveStateRegistry})

    ExUnit.Callbacks.start_supervised!(
      {DynamicSupervisor, name: DriveStateSupervisor, strategy: :one_for_one}
    )

    start_drive_registry()
    start_blob_store()
    start_chunk_index()
    start_file_index()
    start_stripe_index()
    start_volume_registry()
    ensure_chunk_access_tracker()
    ExUnit.Callbacks.start_supervised!(NeonFS.Core.DriveManager, restart: :temporary)
    start_ra()

    on_exit(fn ->
      stop_ra()
      Application.delete_env(:neonfs_core, :drives)
      cleanup_test_dirs()
    end)

    :ok
  end

  test "two drives at init put `_system` at factor 2 without a follow-up drive add" do
    {:ok, _cluster_id} = Init.init_cluster("two-drive-cluster")

    assert {:ok, volume} = VolumeRegistry.get_system_volume()
    assert volume.durability.factor == 2
  end

  # The raise-only path leaves `min_copies` where creation put it, and that is
  # the point: two copies are the target, but a write still commits with one,
  # so losing a drive does not block writes to the volume holding the CA key.
  # Seeding the factor at creation instead would take `min_copies` up with it.
  test "raising the factor does not raise the write requirement with it" do
    {:ok, _cluster_id} = Init.init_cluster("min-copies-cluster")

    assert {:ok, volume} = VolumeRegistry.get_system_volume()
    assert volume.durability.min_copies == 1
  end

  test "an explicit --system-replicas wins over the drive count" do
    {:ok, _cluster_id} = Init.init_cluster("explicit-cluster", nil, system_replicas: 1)

    assert {:ok, volume} = VolumeRegistry.get_system_volume()

    assert volume.durability.factor == 1,
           "a deliberate 1 on a multi-drive cluster is the operator's call to make"
  end
end
