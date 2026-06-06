defmodule NeonFS.Core.VolumeRootNodesTest do
  @moduledoc """
  Covers `NeonFS.Core.volume_root_nodes/1`, the lookup interface nodes use
  (via `NeonFS.Client.RootPlacement`) to route metadata writes to a
  root-holding node (#1046).
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    {:ok, _cluster_id} = start_provisioned_cluster(tmp_dir)

    on_exit(fn ->
      stop_ra()
      cleanup_test_dirs()
    end)

    :ok
  end

  test "returns the nodes holding a provisioned volume's root segment" do
    {:ok, volume} =
      create_provisioned_volume("root-nodes-vol",
        durability: %{type: :replicate, factor: 1, min_copies: 1}
      )

    assert {:ok, nodes} = Core.volume_root_nodes(volume.name)
    assert nodes == [node()]
  end

  test "returns {:error, :not_found} for an unknown volume" do
    assert {:error, :not_found} = Core.volume_root_nodes("no-such-volume")
  end
end
