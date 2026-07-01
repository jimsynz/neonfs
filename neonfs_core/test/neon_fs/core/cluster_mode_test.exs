defmodule NeonFS.Core.ClusterModeTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

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
end
