defmodule NeonFS.Core.BlobStoreResolveDriveIdTest do
  @moduledoc """
  `BlobStore.resolve_drive_id/2` maps the `"default"` sentinel that interface
  writers ship to a real local active drive, so cross-node chunk writes land on
  real storage and record the right drive (#1042).
  """
  use ExUnit.Case, async: false

  alias NeonFS.Core.{BlobStore, Drive, DriveRegistry}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    hot_path = Path.join(tmp_dir, "hot0")
    File.mkdir_p!(hot_path)

    ensure_registry_with_drives([
      %{id: "hot0", path: hot_path, tier: :hot, capacity: 1_000_000_000}
    ])

    :ok
  end

  defp ensure_registry_with_drives(drives_config) do
    case GenServer.whereis(DriveRegistry) do
      nil ->
        {:ok, _pid} = DriveRegistry.start_link(drives: drives_config, sync_interval_ms: 0)

      _pid ->
        :ets.delete_all_objects(:drive_registry)

        drives_config
        |> Enum.map(&Drive.from_config(&1, Node.self()))
        |> Enum.each(&DriveRegistry.register_drive/1)
    end
  end

  describe "resolve_drive_id/2" do
    test "maps the \"default\" sentinel to a local active drive" do
      assert BlobStore.resolve_drive_id("default", "hot") == "hot0"
    end

    test "maps an unregistered drive_id to a local active drive" do
      assert BlobStore.resolve_drive_id("does-not-exist", "hot") == "hot0"
    end

    test "returns a real local active drive_id unchanged" do
      assert BlobStore.resolve_drive_id("hot0", "hot") == "hot0"
    end

    test "accepts a tier atom as well as a string" do
      assert BlobStore.resolve_drive_id("default", :hot) == "hot0"
    end

    test "falls back to any active drive when the tier has none" do
      # No cold drive registered; the sentinel still resolves to the hot drive
      # rather than leaving "default" to fail the write.
      assert BlobStore.resolve_drive_id("default", "cold") == "hot0"
    end
  end
end
