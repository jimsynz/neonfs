defmodule NeonFS.Core.ReplicaAuditTest do
  @moduledoc """
  Unit tests for `NeonFS.Core.ReplicaAudit` (#1618) — the pre-flight
  replica guard behind `drive evacuate` / `drive remove`, and the
  under-replication report that shares its traversal.

  `VolumeRegistry` and `ChunkIndex` are stubbed via Mimic so the decisions
  (refuse below `min_copies`, `--force` override, the `_system`-to-zero
  hard refusal, fail-closed on an unreadable tree) are exercised without a
  running cluster. The wiring into `DriveManager.remove_drive/2` and
  `DriveEvacuation.start_evacuation/3` is covered in their own tests.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Core.{ChunkIndex, ChunkMeta, ReplicaAudit, VolumeRegistry}
  alias NeonFS.Error.ReplicaGuard

  setup :verify_on_exit!

  @node_a :neonfs_core@a
  @node_b :neonfs_core@b

  defp volume(id, name, min_copies, opts \\ []) do
    %{
      id: id,
      name: name,
      system: Keyword.get(opts, :system, false),
      durability: %{type: :replicate, factor: 3, min_copies: min_copies}
    }
  end

  defp erasure_volume(id, name) do
    %{
      id: id,
      name: name,
      system: false,
      durability: %{type: :erasure, data_chunks: 4, parity_chunks: 2}
    }
  end

  defp chunk(hash, drives) do
    %ChunkMeta{
      hash: hash,
      original_size: 4,
      stored_size: 4,
      compression: :none,
      crypto: nil,
      locations: Enum.map(drives, fn {node, id} -> %{node: node, drive_id: id, tier: :hot} end),
      target_replicas: 3,
      commit_state: :committed,
      active_write_refs: MapSet.new(),
      volume_ids: MapSet.new([hash]),
      created_at: DateTime.utc_now()
    }
  end

  # `chunks` maps volume id => chunk list, or volume id => {:error, reason}.
  defp stub_cluster(volumes, chunks) do
    stub(VolumeRegistry, :list, fn _opts -> volumes end)

    stub(ChunkIndex, :list_volume_chunks, fn volume_id ->
      case Map.fetch(chunks, volume_id) do
        {:ok, {:error, _} = error} -> error
        {:ok, list} -> {:ok, list}
        :error -> {:ok, []}
      end
    end)
  end

  describe "removal_impact/2" do
    test "counts the copies that would survive the drive disappearing" do
      stub_cluster([volume("v1", "data", 2)], %{
        "v1" => [
          chunk("h1", [{@node_a, "nvme0"}, {@node_b, "nvme1"}]),
          chunk("h2", [{@node_a, "nvme0"}])
        ]
      })

      {:ok, [impact]} = ReplicaAudit.removal_impact(@node_a, "nvme0")

      assert impact.volume_name == "data"
      assert impact.chunk_count == 2
      # h1 keeps one copy (below min_copies 2), h2 keeps none.
      assert impact.below_min_copies == 2
      assert impact.zero_copies == 1
      assert impact.least_copies == 0
    end

    test "an unaffected drive leaves every volume at full copies" do
      stub_cluster([volume("v1", "data", 2)], %{
        "v1" => [chunk("h1", [{@node_a, "nvme0"}, {@node_b, "nvme1"}])]
      })

      {:ok, [impact]} = ReplicaAudit.removal_impact(@node_a, "spare9")

      assert impact.below_min_copies == 0
      assert impact.zero_copies == 0
      assert impact.least_copies == 2
    end

    test "the same drive listed under two tiers counts as one failure domain" do
      chunk = %ChunkMeta{
        chunk("h1", [{@node_a, "nvme0"}])
        | locations: [
            %{node: @node_a, drive_id: "nvme0", tier: :hot},
            %{node: @node_a, drive_id: "nvme0", tier: :warm}
          ]
      }

      stub_cluster([volume("v1", "data", 1)], %{"v1" => [chunk]})

      {:ok, [impact]} = ReplicaAudit.removal_impact(@node_a, "nvme0")

      assert impact.zero_copies == 1
    end

    test "a chunk short of copies elsewhere is repair's backlog, not this drive's fault" do
      stub_cluster([volume("v1", "data", 2)], %{
        "v1" => [
          chunk("elsewhere", [{@node_b, "nvme1"}]),
          chunk("here", [{@node_a, "nvme0"}, {@node_b, "nvme1"}, {@node_b, "nvme2"}])
        ]
      })

      {:ok, [impact]} = ReplicaAudit.removal_impact(@node_a, "nvme0")

      # "elsewhere" is under-replicated but nvme0 does not hold it, and
      # "here" keeps two copies — so this removal changes nothing for the worse.
      assert impact.below_min_copies == 0
      # The worst case the operation leaves behind is still surfaced.
      assert impact.least_copies == 1
      assert :ok = ReplicaAudit.guard_removal(@node_a, "nvme0")
    end

    test "an unprovisioned volume reads back :not_found and carries no risk" do
      stub_cluster([volume("v1", "data", 2)], %{"v1" => {:error, :not_found}})

      {:ok, [impact]} = ReplicaAudit.removal_impact(@node_a, "nvme0")

      assert impact.chunk_count == 0
      assert impact.below_min_copies == 0
      assert impact.least_copies == 2
    end

    test "any other read failure propagates rather than reporting safety" do
      stub_cluster([volume("v1", "data", 2)], %{"v1" => {:error, :quorum_unavailable}})

      assert {:error, {:volume_unreadable, "v1", :quorum_unavailable}} =
               ReplicaAudit.removal_impact(@node_a, "nvme0")
    end

    test "erasure volumes are audited against a one-copy-per-shard floor" do
      stub_cluster([erasure_volume("v1", "archive")], %{
        "v1" => [chunk("shard1", [{@node_a, "nvme0"}])]
      })

      {:ok, [impact]} = ReplicaAudit.removal_impact(@node_a, "nvme0")

      assert impact.min_copies == 1
      assert impact.zero_copies == 1
    end
  end

  describe "guard_removal/3" do
    test "allows a removal that keeps every volume at or above min_copies" do
      stub_cluster([volume("v1", "data", 2)], %{
        "v1" => [chunk("h1", [{@node_a, "nvme0"}, {@node_b, "nvme1"}, {@node_b, "nvme2"}])]
      })

      assert :ok = ReplicaAudit.guard_removal(@node_a, "nvme0")
    end

    test "refuses when a volume would fall below min_copies, and says which" do
      stub_cluster([volume("v1", "data", 2)], %{
        "v1" => [chunk("h1", [{@node_a, "nvme0"}, {@node_b, "nvme1"}])]
      })

      assert {:error, %ReplicaGuard{} = error} = ReplicaAudit.guard_removal(@node_a, "nvme0")
      assert error.reason == :below_min_copies
      assert ReplicaGuard.forceable?(error)
      assert [%{volume_name: "data", min_copies: 2, below_min_copies: 1}] = error.at_risk

      message = Exception.message(error)
      assert message =~ "'nvme0' on #{@node_a}"
      assert message =~ "data (1 chunk(s) below min_copies 2"
      assert message =~ "--force"
    end

    test "force overrides a below-min_copies refusal" do
      stub_cluster([volume("v1", "data", 2)], %{
        "v1" => [chunk("h1", [{@node_a, "nvme0"}, {@node_b, "nvme1"}])]
      })

      assert :ok = ReplicaAudit.guard_removal(@node_a, "nvme0", force: true)
    end

    test "force cannot override _system being left with no surviving copy" do
      stub_cluster([volume("sys", "_system", 1, system: true)], %{
        "sys" => [chunk("ca-key", [{@node_a, "nvme0"}])]
      })

      assert {:error, %ReplicaGuard{reason: :system_zero_copies} = error} =
               ReplicaAudit.guard_removal(@node_a, "nvme0", force: true)

      refute ReplicaGuard.forceable?(error)

      message = Exception.message(error)
      assert message =~ "cluster-critical"
      assert message =~ "no surviving copy of 1 chunk(s)"
      assert message =~ "cannot override"
    end

    test "a replicated _system volume permits removal of one of its drives" do
      stub_cluster([volume("sys", "_system", 1, system: true)], %{
        "sys" => [chunk("ca-key", [{@node_a, "nvme0"}, {@node_b, "nvme1"}])]
      })

      assert :ok = ReplicaAudit.guard_removal(@node_a, "nvme0")
    end

    test "a user volume reaching zero copies is still forceable" do
      stub_cluster([volume("v1", "scratch", 1)], %{
        "v1" => [chunk("h1", [{@node_a, "nvme0"}])]
      })

      assert {:error, %ReplicaGuard{reason: :below_min_copies}} =
               ReplicaAudit.guard_removal(@node_a, "nvme0")

      assert :ok = ReplicaAudit.guard_removal(@node_a, "nvme0", force: true)
    end

    test "fails closed when the replica state cannot be read" do
      stub_cluster([volume("v1", "data", 2)], %{"v1" => {:error, :quorum_unavailable}})

      assert {:error, %ReplicaGuard{reason: :indeterminate} = error} =
               ReplicaAudit.guard_removal(@node_a, "nvme0")

      assert Exception.message(error) =~ "cannot be shown to be safe"
    end

    test "force proceeds through an indeterminate audit so a broken cluster stays repairable" do
      stub_cluster([volume("v1", "data", 2)], %{"v1" => {:error, :quorum_unavailable}})

      assert :ok = ReplicaAudit.guard_removal(@node_a, "nvme0", force: true)
    end

    test "the operation verb reaches the message" do
      stub_cluster([volume("v1", "data", 2)], %{
        "v1" => [chunk("h1", [{@node_a, "nvme0"}, {@node_b, "nvme1"}])]
      })

      {:error, error} = ReplicaAudit.guard_removal(@node_a, "nvme0", operation: "Evacuating")

      assert Exception.message(error) =~ "Evacuating drive 'nvme0'"
    end

    test "emits telemetry on refusal and on a forced override" do
      stub_cluster([volume("v1", "data", 2)], %{
        "v1" => [chunk("h1", [{@node_a, "nvme0"}, {@node_b, "nvme1"}])]
      })

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :replica_audit, :guard_refused],
          [:neonfs, :replica_audit, :guard_forced]
        ])

      {:error, _} = ReplicaAudit.guard_removal(@node_a, "nvme0")

      assert_receive {[:neonfs, :replica_audit, :guard_refused], ^ref, %{at_risk_count: 1},
                      %{drive_id: "nvme0", reason: :below_min_copies}}

      :ok = ReplicaAudit.guard_removal(@node_a, "nvme0", force: true)

      assert_receive {[:neonfs, :replica_audit, :guard_forced], ^ref, %{at_risk_count: 1},
                      %{drive_id: "nvme0"}}
    end
  end

  describe "audit/0" do
    test "reports under-replicated volumes and the drives holding sole copies" do
      stub_cluster(
        [volume("v1", "data", 2), volume("v2", "backups", 1)],
        %{
          "v1" => [
            chunk("h1", [{@node_a, "nvme0"}]),
            chunk("h2", [{@node_a, "nvme0"}, {@node_b, "nvme1"}])
          ],
          "v2" => [chunk("h3", [{@node_a, "nvme0"}]), chunk("h4", [{@node_b, "nvme1"}])]
        }
      )

      {:ok, report} = ReplicaAudit.audit()

      assert Enum.map(report.volumes, & &1.volume_name) == ["data", "backups"]
      assert Enum.map(report.under_replicated, & &1.volume_name) == ["data"]

      # h1 and h3 sit alone on nvme0; h4 alone on nvme1.
      assert report.sole_copy_drives == [
               %{node: @node_a, drive_id: "nvme0", chunk_count: 2},
               %{node: @node_b, drive_id: "nvme1", chunk_count: 1}
             ]
    end

    test "a fully-replicated cluster reports nothing at risk" do
      stub_cluster([volume("v1", "data", 2)], %{
        "v1" => [chunk("h1", [{@node_a, "nvme0"}, {@node_b, "nvme1"}])]
      })

      {:ok, report} = ReplicaAudit.audit()

      assert report.under_replicated == []
      assert report.sole_copy_drives == []
    end

    test "emits a per-volume under-replication event and a summary" do
      stub_cluster([volume("v1", "data", 2)], %{"v1" => [chunk("h1", [{@node_a, "nvme0"}])]})

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :replica_audit, :under_replicated],
          [:neonfs, :replica_audit, :completed]
        ])

      {:ok, _report} = ReplicaAudit.audit()

      assert_receive {[:neonfs, :replica_audit, :under_replicated], ^ref,
                      %{below_min_copies: 1, zero_copies: 0, least_copies: 1},
                      %{volume_name: "data", min_copies: 2, system?: false}}

      assert_receive {[:neonfs, :replica_audit, :completed], ^ref,
                      %{volume_count: 1, under_replicated_count: 1, sole_copy_drive_count: 1},
                      %{}}
    end

    test "propagates an unreadable volume rather than reporting partial health" do
      stub_cluster([volume("v1", "data", 2)], %{"v1" => {:error, :timeout}})

      assert {:error, {:volume_unreadable, "v1", :timeout}} = ReplicaAudit.audit()
    end
  end
end
