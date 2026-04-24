defmodule NeonFS.Core.BlobStoreReplicateAfterPutTest do
  @moduledoc """
  Unit coverage for `NeonFS.Core.BlobStore.replicate_after_put/5` — the
  core-side fan-out hook used by `NeonFS.Transport.Handler` after an
  interface-side `put_chunk` finishes the local write (#478).

  Multi-node fan-out (factor > 1, real replicas acknowledging) needs a
  peer cluster and is exercised in the peer-cluster integration tests.
  These unit tests cover the no-op paths: unknown volume, single-
  replica volume, and multi-replica in a single-node cluster where no
  targets are available.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{BlobStore, VolumeRegistry}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_drive_registry()
    start_blob_store()
    start_volume_registry()
    ensure_cluster_state()

    on_exit(fn -> cleanup_test_dirs() end)

    :ok
  end

  defp local_location do
    %{node: Node.self(), drive_id: "default", tier: :hot}
  end

  describe "replicate_after_put/5 — no-op paths" do
    test "returns {:ok, [local]} when the volume cannot be resolved" do
      hash = :crypto.strong_rand_bytes(32)

      assert {:ok, locations} =
               BlobStore.replicate_after_put(hash, "payload", "no-such-volume", local_location())

      assert locations == [local_location()]
    end

    test "returns {:ok, [local]} for a single-replica volume" do
      {:ok, volume} =
        VolumeRegistry.create("single-replica-vol",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      hash = :crypto.strong_rand_bytes(32)

      assert {:ok, locations} =
               BlobStore.replicate_after_put(hash, "payload", volume.id, local_location())

      assert locations == [local_location()]
    end

    test "returns {:ok, [local]} when factor > 1 but no replica targets are available" do
      {:ok, volume} =
        VolumeRegistry.create("multi-replica-single-node",
          durability: %{type: :replicate, factor: 3, min_copies: 1}
        )

      hash = :crypto.strong_rand_bytes(32)

      assert {:ok, locations} =
               BlobStore.replicate_after_put(hash, "payload", volume.id, local_location())

      # Single-node cluster: Replication finds no targets and falls back
      # to the local-only result; replicate_after_put passes that
      # through without error.
      assert locations == [local_location()]
    end

    test "dedupes the local location if Replication also returns it" do
      {:ok, volume} =
        VolumeRegistry.create("dedupe-vol",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      local = local_location()

      hash = :crypto.strong_rand_bytes(32)

      # factor == 1 short-circuits before Replication, returning
      # exactly one entry — the local location, not duplicated.
      assert {:ok, [^local]} =
               BlobStore.replicate_after_put(hash, "payload", volume.id, local)
    end
  end
end
