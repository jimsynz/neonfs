defmodule NeonFS.Core.ClusterModeTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core
  alias NeonFS.Core.{ClusterMode, RaServer}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    start_ra()
    :ok = RaServer.init_cluster()
    on_exit(fn -> cleanup_test_dirs() end)
    :ok
  end

  test "an unset cluster defaults to :normal" do
    assert ClusterMode.mode() == :normal
    refute ClusterMode.frozen?()
    refute ClusterMode.recovering?()
    assert ClusterMode.entry() == nil
  end

  test "set_mode round-trips through Ra and records an entry" do
    assert :ok = ClusterMode.set_mode(:frozen, "planned power-down")
    assert ClusterMode.mode() == :frozen
    assert ClusterMode.frozen?()
    refute ClusterMode.recovering?()

    assert %{mode: :frozen, reason: "planned power-down", updated_at: %DateTime{}} =
             ClusterMode.entry()
  end

  test "mode transitions freeze -> recovering -> normal" do
    assert :ok = ClusterMode.set_mode(:frozen)
    assert ClusterMode.frozen?()

    assert :ok = ClusterMode.set_mode(:recovering, "thaw")
    assert ClusterMode.recovering?()
    refute ClusterMode.frozen?()

    assert :ok = ClusterMode.set_mode(:normal)
    assert ClusterMode.mode() == :normal
    refute ClusterMode.recovering?()
  end

  test "reason defaults to nil" do
    assert :ok = ClusterMode.set_mode(:recovering)
    assert %{mode: :recovering, reason: nil} = ClusterMode.entry()
  end

  test "set_cluster_mode emits telemetry with from/to" do
    ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:neonfs, :ra, :command, :set_cluster_mode]
      ])

    assert :ok = ClusterMode.set_mode(:frozen, "freeze")

    assert_receive {[:neonfs, :ra, :command, :set_cluster_mode], ^ref, %{version: _},
                    %{from: :normal, to: :frozen, reason: "freeze"}},
                   1_000

    assert :ok = ClusterMode.set_mode(:recovering)

    assert_receive {[:neonfs, :ra, :command, :set_cluster_mode], ^ref, %{},
                    %{from: :frozen, to: :recovering}},
                   1_000
  end

  describe "frozen write gate (#1438)" do
    test "`NeonFS.Core` reflects the frozen mode via cluster_frozen?/0" do
      refute Core.cluster_frozen?()
      assert :ok = ClusterMode.set_mode(:frozen)
      assert Core.cluster_frozen?()
      assert :ok = ClusterMode.set_mode(:normal)
      refute Core.cluster_frozen?()
    end

    test "write RPCs are rejected with :cluster_frozen while frozen" do
      assert :ok = ClusterMode.set_mode(:frozen)

      # The gate short-circuits before volume resolution, so no volume
      # setup is needed — a frozen cluster refuses the write outright.
      assert {:error, :cluster_frozen} = Core.write_file_at("any-vol", "/f", 0, "data", [])
      assert {:error, :cluster_frozen} = Core.write_file_at_by_id("any-vol", "fid", 0, "d", [])
      assert {:error, :cluster_frozen} = Core.write_file_streamed("any-vol", "/f", [], [])
      assert {:error, :cluster_frozen} = Core.commit_chunks("any-vol", "/f", [], [])
    end
  end
end
