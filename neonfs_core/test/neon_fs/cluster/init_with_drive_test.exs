defmodule NeonFS.Cluster.InitWithDriveTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Cluster.Init

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    # Bootstrap from a truly empty drives config — the daemon doesn't
    # ship with a default drive any more (#975).
    Application.put_env(:neonfs_core, :drives, [])

    stop_ra()
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
      cleanup_test_dirs()
    end)

    :ok
  end

  describe "init_cluster/2" do
    test "refuses to bootstrap without a drive when none are pre-configured" do
      assert {:error, :no_drives_available} =
               Init.init_cluster("empty-cluster", nil)
    end

    test "surfaces an actionable error if the supplied drive path is missing" do
      assert {:error, {:initial_drive_failed, _reason}} =
               Init.init_cluster("bad-drive-cluster", %{
                 "path" => "/var/empty/does-not-exist-#{System.unique_integer([:positive])}",
                 "tier" => "hot"
               })
    end
  end
end
