defmodule NeonFS.CSI.VolumeHealthTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.Volume
  alias NeonFS.CSI.VolumeHealth

  setup do
    VolumeHealth.reset_table()

    on_exit(fn ->
      Application.delete_env(:neonfs_csi, :core_call_fn)
      VolumeHealth.reset_table()
    end)

    :ok
  end

  defp sample_volume(name, overrides \\ %{}) do
    base = %Volume{
      id: "vid-" <> name,
      name: name,
      durability: %{type: :replicate, factor: 1, min_copies: 1},
      created_at: DateTime.from_unix!(0),
      updated_at: DateTime.from_unix!(0),
      logical_size: 0,
      physical_size: 0,
      chunk_count: 0
    }

    Map.merge(base, overrides)
  end

  defp healthy_capacity, do: %{drives: [%{state: :active}], total_capacity: 100, total_used: 10}

  defp ok_core(volume, opts \\ []) do
    capacity = Keyword.get(opts, :capacity, healthy_capacity())
    cores = Keyword.get(opts, :cores, [%{node: :n1}])
    escalations = Keyword.get(opts, :escalations, [])

    fn
      NeonFS.Core, :get_volume, [_] -> {:ok, volume}
      NeonFS.Core.StorageMetrics, :cluster_capacity, [] -> capacity
      NeonFS.Core.ServiceRegistry, :list_by_type, [:core] -> cores
      NeonFS.Core.Escalation, :list, [[status: :pending]] -> escalations
    end
  end

  describe "controller_condition/2" do
    test "returns abnormal=false for a healthy cluster" do
      assert {:ok, %{abnormal: false, message: ""}} =
               VolumeHealth.controller_condition("v",
                 core_call_fn: ok_core(sample_volume("v"))
               )
    end

    test "returns abnormal=true when replication factor exceeds core nodes" do
      vol = sample_volume("v", %{durability: %{type: :replicate, factor: 3, min_copies: 1}})

      assert {:ok, %{abnormal: true, message: msg}} =
               VolumeHealth.controller_condition("v",
                 core_call_fn: ok_core(vol, cores: [%{node: :n1}])
               )

      assert msg =~ "replication factor 3"
    end

    test "returns abnormal=true when no writable drives in cluster" do
      core =
        ok_core(sample_volume("v"),
          capacity: %{drives: [%{state: :draining}], total_capacity: 100, total_used: 10}
        )

      assert {:ok, %{abnormal: true, message: msg}} =
               VolumeHealth.controller_condition("v", core_call_fn: core)

      assert msg =~ "no writable drives"
    end

    test "returns abnormal=true when capacity utilisation > 95%" do
      core =
        ok_core(sample_volume("v"),
          capacity: %{drives: [%{state: :active}], total_capacity: 100, total_used: 96}
        )

      assert {:ok, %{abnormal: true, message: msg}} =
               VolumeHealth.controller_condition("v", core_call_fn: core)

      assert msg =~ "utilisation"
    end

    test "returns abnormal=true on a pending critical escalation" do
      core =
        ok_core(sample_volume("v"),
          escalations: [%{severity: :critical, category: "drive_failure"}]
        )

      assert {:ok, %{abnormal: true, message: msg}} =
               VolumeHealth.controller_condition("v", core_call_fn: core)

      assert msg =~ "critical escalations"
    end

    test "returns {:error, :not_found} when the volume is gone" do
      core_call_fn = fn NeonFS.Core, :get_volume, _ -> {:error, :not_found} end

      assert {:error, :not_found} =
               VolumeHealth.controller_condition("ghost", core_call_fn: core_call_fn)
    end

    test "tolerates a crashing storage-metrics call" do
      core_call_fn = fn
        NeonFS.Core, :get_volume, _ -> {:ok, sample_volume("v")}
        NeonFS.Core.StorageMetrics, :cluster_capacity, [] -> raise "boom"
        NeonFS.Core.ServiceRegistry, :list_by_type, _ -> [%{node: :n1}]
        NeonFS.Core.Escalation, :list, _ -> []
      end

      assert {:ok, %{abnormal: false}} =
               VolumeHealth.controller_condition("v", core_call_fn: core_call_fn)
    end
  end

  describe "node_condition/3" do
    test "returns abnormal=false when stat succeeds" do
      stat_fn = fn _ -> {:ok, %File.Stat{}} end

      assert %{abnormal: false, message: ""} =
               VolumeHealth.node_condition("v", "/some/path", stat_fn: stat_fn)
    end

    test "returns abnormal=true on stat error" do
      stat_fn = fn _ -> {:error, :enoent} end

      assert %{abnormal: true, message: msg} =
               VolumeHealth.node_condition("v", "/some/path", stat_fn: stat_fn)

      assert msg =~ "enoent"
    end

    test "returns abnormal=true on stat timeout (wedged FUSE mount)" do
      stat_fn = fn _ ->
        Process.sleep(500)
        {:ok, %File.Stat{}}
      end

      assert %{abnormal: true, message: msg} =
               VolumeHealth.node_condition("v", "/some/path",
                 stat_fn: stat_fn,
                 stat_timeout_ms: 50
               )

      assert msg =~ "timed out"
    end
  end

  describe "transition telemetry" do
    setup do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :csi, :volume_condition, :transition]
        ])

      {:ok, ref: ref}
    end

    test "emits exactly one event when condition flips abnormal", %{ref: ref} do
      stat_ok = fn _ -> {:ok, %File.Stat{}} end
      stat_error = fn _ -> {:error, :ehostdown} end

      _ = VolumeHealth.node_condition("v", "/path", stat_fn: stat_ok)

      # First call records the initial state — emits a transition so
      # operators see the baseline.
      assert_received {[:neonfs, :csi, :volume_condition, :transition], ^ref, %{count: 1},
                       %{volume_id: "v", scope: :node, to_abnormal: false}}

      # Same state again — no event.
      _ = VolumeHealth.node_condition("v", "/path", stat_fn: stat_ok)
      refute_received {[:neonfs, :csi, :volume_condition, :transition], ^ref, _, _}

      _ = VolumeHealth.node_condition("v", "/path", stat_fn: stat_error)

      assert_received {[:neonfs, :csi, :volume_condition, :transition], ^ref, %{count: 1},
                       %{volume_id: "v", scope: :node, to_abnormal: true}}
    end

    test "does not re-emit when state is unchanged", %{ref: ref} do
      stat_error = fn _ -> {:error, :ehostdown} end

      _ = VolumeHealth.node_condition("v", "/path", stat_fn: stat_error)
      assert_received {[:neonfs, :csi, :volume_condition, :transition], ^ref, _, _}

      _ = VolumeHealth.node_condition("v", "/path", stat_fn: stat_error)
      refute_received {[:neonfs, :csi, :volume_condition, :transition], ^ref, _, _}
    end

    test "tracks controller and node scopes independently", %{ref: ref} do
      vol = sample_volume("v", %{durability: %{type: :replicate, factor: 5, min_copies: 1}})

      _ =
        VolumeHealth.controller_condition("v",
          core_call_fn: ok_core(vol, cores: [%{node: :n1}])
        )

      assert_received {[:neonfs, :csi, :volume_condition, :transition], ^ref, _,
                       %{scope: :controller}}

      _ =
        VolumeHealth.node_condition("v", "/path", stat_fn: fn _ -> {:error, :enoent} end)

      assert_received {[:neonfs, :csi, :volume_condition, :transition], ^ref, _, %{scope: :node}}
    end
  end
end
