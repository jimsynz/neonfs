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

  alias NeonFS.Core.{ChunkIndex, ChunkMeta, ReplicaAudit, Stripe, StripeIndex, VolumeRegistry}
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

  defp stripe(id, volume_id, shard_hashes, opts \\ []) do
    %Stripe{
      id: id,
      volume_id: volume_id,
      config: %{
        data_chunks: Keyword.get(opts, :data_chunks, 4),
        parity_chunks: Keyword.get(opts, :parity_chunks, 2),
        chunk_size: 4
      },
      chunks: shard_hashes
    }
  end

  # `chunks` maps volume id => chunk list, or volume id => {:error, reason}.
  # `stripes` maps volume id => stripe list; a volume absent from it has none.
  defp stub_cluster(volumes, chunks, stripes \\ %{}) do
    stub(VolumeRegistry, :list, fn _opts -> volumes end)

    stub(ChunkIndex, :list_volume_chunks, fn volume_id ->
      case Map.fetch(chunks, volume_id) do
        {:ok, {:error, _} = error} -> error
        {:ok, list} -> {:ok, list}
        :error -> {:ok, []}
      end
    end)

    stub(StripeIndex, :list_by_volume, fn volume_id -> Map.get(stripes, volume_id, []) end)
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

    test "erasure volumes still report shards that would reach zero copies" do
      stub_cluster([erasure_volume("v1", "archive")], %{
        "v1" => [chunk("shard1", [{@node_a, "nvme0"}])]
      })

      {:ok, [impact]} = ReplicaAudit.removal_impact(@node_a, "nvme0")

      assert impact.min_copies == 1
      assert impact.zero_copies == 1
    end

    # The two counts answer different questions and an operator needs both:
    # a shard at zero copies is repair backlog, a stripe below its
    # reconstruction threshold is data already gone.
    test "erasure volumes report shard loss and stripe loss separately" do
      shards = for i <- 1..6, do: "shard#{i}"

      stub_cluster(
        [erasure_volume("v1", "archive")],
        %{"v1" => Enum.map(shards, &chunk(&1, [{@node_a, "nvme0"}]))},
        %{"v1" => [stripe("s1", "v1", shards)]}
      )

      {:ok, [impact]} = ReplicaAudit.removal_impact(@node_a, "nvme0")

      assert impact.erasure?
      assert impact.zero_copies == 6, "every shard loses its only copy"
      assert impact.stripe_count == 1
      assert impact.stripes_at_risk == 1
      assert impact.stripes_lost == 1
    end
  end

  # A 4+2 stripe survives losing up to 2 shards. The old per-shard floor
  # refused any removal that took a shard to zero copies, which is the
  # ordinary case erasure is built for, and stayed quiet about the removal
  # that took a stripe past its parity budget.
  describe "guard_removal/3 on erasure volumes" do
    test "allows a removal that stays within the parity budget" do
      shards = for i <- 1..6, do: "shard#{i}"

      # Two shards on the candidate drive, four elsewhere — 4 survive, which
      # is exactly `data_chunks`, so the stripe still reconstructs.
      chunks =
        Enum.map(Enum.take(shards, 2), &chunk(&1, [{@node_a, "nvme0"}])) ++
          Enum.map(Enum.drop(shards, 2), &chunk(&1, [{@node_b, "nvme1"}]))

      stub_cluster(
        [erasure_volume("v1", "archive")],
        %{"v1" => chunks},
        %{"v1" => [stripe("s1", "v1", shards)]}
      )

      assert :ok = ReplicaAudit.guard_removal(@node_a, "nvme0"),
             "losing 2 of 6 shards in a 4+2 stripe is exactly what parity is for"
    end

    test "refuses a removal that takes a stripe past its parity budget" do
      shards = for i <- 1..6, do: "shard#{i}"

      # Three on the candidate drive — only 3 survive, one short of the 4
      # reconstruction needs.
      chunks =
        Enum.map(Enum.take(shards, 3), &chunk(&1, [{@node_a, "nvme0"}])) ++
          Enum.map(Enum.drop(shards, 3), &chunk(&1, [{@node_b, "nvme1"}]))

      stub_cluster(
        [erasure_volume("v1", "archive")],
        %{"v1" => chunks},
        %{"v1" => [stripe("s1", "v1", shards)]}
      )

      assert {:error, %ReplicaGuard{} = error} = ReplicaAudit.guard_removal(@node_a, "nvme0")
      assert [%{volume_name: "archive", stripes_at_risk: 1}] = error.at_risk
    end

    # Pre-existing damage is repair's backlog, not this operation's doing —
    # the same rule the per-chunk counters follow. A stripe already past its
    # parity budget must not block every future drive operation.
    test "does not refuse for a stripe that was already unreconstructible" do
      shards = for i <- 1..6, do: "shard#{i}"

      # Shards 1-3 are gone from the index entirely, so only 3 remain against
      # the 4 reconstruction needs — already broken before anything is removed.
      # Shard 4 sits on the candidate drive but also on nvme9.
      chunks = [
        chunk("shard4", [{@node_a, "nvme0"}, {@node_a, "nvme9"}]),
        chunk("shard5", [{@node_b, "nvme1"}]),
        chunk("shard6", [{@node_b, "nvme1"}])
      ]

      stub_cluster(
        [erasure_volume("v1", "archive")],
        %{"v1" => chunks},
        %{"v1" => [stripe("s1", "v1", shards)]}
      )

      {:ok, [impact]} = ReplicaAudit.removal_impact(@node_a, "nvme0")

      assert impact.stripes_at_risk == 0,
             "the stripe was below threshold before this drive was considered"

      assert :ok = ReplicaAudit.guard_removal(@node_a, "nvme0")
    end

    # `audit/0` passes no candidate, so the same counters describe the
    # backlog rather than an operation's effect.
    test "audit/0 reports a currently unreconstructible stripe" do
      shards = for i <- 1..6, do: "shard#{i}"

      chunks = [
        chunk("shard5", [{@node_b, "nvme1"}]),
        chunk("shard6", [{@node_b, "nvme1"}])
      ]

      stub_cluster(
        [erasure_volume("v1", "archive")],
        %{"v1" => chunks},
        %{"v1" => [stripe("s1", "v1", shards)]}
      )

      assert {:ok, report} = ReplicaAudit.audit()
      assert [%{volume_name: "archive", stripes_at_risk: 1}] = report.under_replicated
    end

    test "a mixed replicate and erasure cluster decides each volume on its own terms" do
      shards = for i <- 1..6, do: "shard#{i}"

      erasure_chunks =
        Enum.map(Enum.take(shards, 2), &chunk(&1, [{@node_a, "nvme0"}])) ++
          Enum.map(Enum.drop(shards, 2), &chunk(&1, [{@node_b, "nvme1"}]))

      stub_cluster(
        [volume("v1", "data", 2), erasure_volume("v2", "archive")],
        %{
          "v1" => [chunk("h1", [{@node_a, "nvme0"}, {@node_b, "nvme1"}])],
          "v2" => erasure_chunks
        },
        %{"v2" => [stripe("s1", "v2", shards)]}
      )

      assert {:error, %ReplicaGuard{} = error} = ReplicaAudit.guard_removal(@node_a, "nvme0")

      assert [%{volume_name: "data"}] = error.at_risk,
             "the replicated volume is below min_copies; the erasure one is within parity"
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
