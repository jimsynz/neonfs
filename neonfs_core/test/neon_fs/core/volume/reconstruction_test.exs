defmodule NeonFS.Core.Volume.ReconstructionTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Drive.Identity
  alias NeonFS.Core.Volume.Reconstruction
  alias NeonFS.Core.Volume.RootSegment

  @cluster_id "clust-test"

  describe "reconstruct/2" do
    test "returns empty result when no drive paths supplied" do
      result =
        Reconstruction.reconstruct([],
          expected_cluster_id: @cluster_id,
          node: node(),
          chunk_lister: fn _ -> [] end,
          chunk_reader: fn _, _ -> {:error, :unreachable} end,
          identity_reader: fn _ -> {:error, :no_drive} end
        )

      assert result.drives == []
      assert result.volumes == %{}
      assert result.commands == []
      assert result.warnings == []
    end

    test "happy path: one drive, one volume → one register_drive + one register_volume_root" do
      identity = sample_identity("drv-1")
      segment = sample_segment("vol-a")
      encoded = RootSegment.encode(segment)
      hash = "the-hash"

      result =
        Reconstruction.reconstruct(["/drv-1"],
          expected_cluster_id: @cluster_id,
          node: node(),
          identity_reader: fn "/drv-1" -> {:ok, identity} end,
          chunk_lister: fn "/drv-1" -> [hash] end,
          chunk_reader: fn "/drv-1", ^hash -> {:ok, encoded} end
        )

      assert [identity] == result.drives
      assert Map.has_key?(result.volumes, "vol-a")
      assert result.volumes["vol-a"].hash == hash

      assert [
               {:register_drive, %{drive_id: "drv-1", cluster_id: @cluster_id, node: this_node}},
               {:register_volume_root,
                %{
                  volume_id: "vol-a",
                  root_chunk_hash: ^hash,
                  drive_locations: [%{node: this_node, drive_id: "drv-1"}]
                }}
             ] = result.commands

      assert this_node == node()
    end

    test "skips drives whose cluster_id doesn't match expected_cluster_id" do
      foreign = %{sample_identity("drv-foreign") | cluster_id: "clust-foreign"}

      result =
        Reconstruction.reconstruct(["/drv-foreign"],
          expected_cluster_id: @cluster_id,
          node: node(),
          identity_reader: fn "/drv-foreign" -> {:ok, foreign} end,
          chunk_lister: fn _ -> [] end,
          chunk_reader: fn _, _ -> {:error, :should_not_be_called} end
        )

      assert result.drives == []
      assert result.commands == []
      assert [{:foreign_cluster, _, %{path: "/drv-foreign"}}] = result.warnings
    end

    test "warns and continues on unreadable drive identity" do
      result =
        Reconstruction.reconstruct(["/dead"],
          expected_cluster_id: @cluster_id,
          node: node(),
          identity_reader: fn "/dead" -> {:error, :enoent} end,
          chunk_lister: fn _ -> [] end,
          chunk_reader: fn _, _ -> {:error, :should_not_be_called} end
        )

      assert result.drives == []
      assert [{:identity_unreadable, _, %{path: "/dead"}}] = result.warnings
    end

    test "skips chunks that don't decode as a root segment (most won't)" do
      identity = sample_identity("drv-1")
      segment = sample_segment("vol-a")
      good_hash = "good"
      garbage_hash = "garbage"

      result =
        Reconstruction.reconstruct(["/drv-1"],
          expected_cluster_id: @cluster_id,
          node: node(),
          identity_reader: fn _ -> {:ok, identity} end,
          chunk_lister: fn _ -> [garbage_hash, good_hash] end,
          chunk_reader: fn
            _, ^garbage_hash -> {:ok, <<0, 1, 2, 3>>}
            _, ^good_hash -> {:ok, RootSegment.encode(segment)}
          end
        )

      assert Map.has_key?(result.volumes, "vol-a")
      # Garbage chunk is silently skipped — most chunks aren't root segments.
      assert result.warnings == []
    end

    test "warns when a chunk decodes as a foreign-cluster root segment" do
      identity = sample_identity("drv-1")

      foreign_segment = %{
        sample_segment("vol-a")
        | cluster_id: "clust-other"
      }

      result =
        Reconstruction.reconstruct(["/drv-1"],
          expected_cluster_id: @cluster_id,
          node: node(),
          identity_reader: fn _ -> {:ok, identity} end,
          chunk_lister: fn _ -> ["h"] end,
          chunk_reader: fn _, "h" -> {:ok, RootSegment.encode(foreign_segment)} end
        )

      assert result.volumes == %{}
      assert [{:foreign_segment, _, %{drive_id: "drv-1", hash: "h"}}] = result.warnings
    end

    test "warns when a chunk read fails" do
      identity = sample_identity("drv-1")

      result =
        Reconstruction.reconstruct(["/drv-1"],
          expected_cluster_id: @cluster_id,
          node: node(),
          identity_reader: fn _ -> {:ok, identity} end,
          chunk_lister: fn _ -> ["h"] end,
          chunk_reader: fn _, "h" -> {:error, :io_error} end
        )

      assert result.volumes == %{}

      assert [{:chunk_unreadable, _, %{drive_id: "drv-1", hash: "h", reason: :io_error}}] =
               result.warnings
    end

    test "newer HLC wins when the same volume_id has multiple root segments" do
      identity = sample_identity("drv-1")

      old_segment = sample_segment("vol-a")

      new_segment = %{
        old_segment
        | hlc: %{old_segment.hlc | last_wall: old_segment.hlc.last_wall + 1000}
      }

      old_hash = "old"
      new_hash = "new"

      result =
        Reconstruction.reconstruct(["/drv-1"],
          expected_cluster_id: @cluster_id,
          node: node(),
          identity_reader: fn _ -> {:ok, identity} end,
          chunk_lister: fn _ -> [old_hash, new_hash] end,
          chunk_reader: fn
            _, ^old_hash -> {:ok, RootSegment.encode(old_segment)}
            _, ^new_hash -> {:ok, RootSegment.encode(new_segment)}
          end
        )

      assert result.volumes["vol-a"].hash == new_hash
    end

    test "same root chunk hash on multiple drives produces a multi-replica drive_locations list" do
      identity_a = sample_identity("drv-a")
      identity_b = sample_identity("drv-b")
      segment = sample_segment("vol-a")
      encoded = RootSegment.encode(segment)
      hash = "shared"

      identity_reader = fn
        "/drv-a" -> {:ok, identity_a}
        "/drv-b" -> {:ok, identity_b}
      end

      result =
        Reconstruction.reconstruct(["/drv-a", "/drv-b"],
          expected_cluster_id: @cluster_id,
          node: node(),
          identity_reader: identity_reader,
          chunk_lister: fn _ -> [hash] end,
          chunk_reader: fn _, ^hash -> {:ok, encoded} end
        )

      [_register_a, _register_b, {:register_volume_root, payload}] = result.commands
      drive_ids = Enum.map(payload.drive_locations, & &1.drive_id) |> Enum.sort()
      assert drive_ids == ["drv-a", "drv-b"]
    end

    test ":dry_run? still populates the result struct (#855)" do
      identity = sample_identity("drv-1")
      segment = sample_segment("vol-a")

      result =
        Reconstruction.reconstruct(["/drv-1"],
          expected_cluster_id: @cluster_id,
          node: node(),
          dry_run?: true,
          identity_reader: fn _ -> {:ok, identity} end,
          chunk_lister: fn _ -> ["h"] end,
          chunk_reader: fn _, "h" -> {:ok, RootSegment.encode(segment)} end
        )

      assert [^identity] = result.drives
      assert Map.has_key?(result.volumes, "vol-a")
      # Commands are built regardless of `:dry_run?` so the CLI's
      # preview output matches the runbook (`commands == drives +
      # volumes`); the handler is what gates actual submission.
      assert length(result.commands) == 2
    end
  end

  ## Helpers

  defp sample_identity(drive_id) do
    %Identity{
      schema_version: 1,
      drive_id: drive_id,
      cluster_id: @cluster_id,
      created_at: DateTime.utc_now(),
      created_by_neonfs_version: "0.0.0",
      on_disk_format_version: 1
    }
  end

  defp sample_segment(volume_id) do
    RootSegment.new(
      volume_id: volume_id,
      volume_name: volume_id,
      cluster_id: @cluster_id,
      cluster_name: "cluster-test",
      durability: %{type: :replicate, factor: 1, min_copies: 1}
    )
  end
end
