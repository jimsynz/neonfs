defmodule NeonFS.CLI.Handler.JobsTest do
  use ExUnit.Case, async: true

  alias NeonFS.CLI.Handler.Jobs
  alias NeonFS.Core.Job.Runners

  @shipped_runners [
    Runners.ClusterRebalance,
    Runners.DriveEvacuation,
    Runners.GarbageCollection,
    Runners.KeyRotation,
    Runners.ReplicaRepair,
    Runners.Scrub,
    Runners.VolumeAntiEntropy
  ]

  describe "job_runners/0" do
    test "discovers every shipped Job.Runner, including ReplicaRepair (#1580)" do
      runners = Jobs.job_runners()

      for mod <- @shipped_runners do
        assert mod in runners, "#{inspect(mod)} was not discovered as a Job.Runner"
      end
    end

    test "excludes the behaviour module itself" do
      refute NeonFS.Core.Job.Runner in Jobs.job_runners()
    end
  end

  describe "runner_for_label/1" do
    test "resolves replica-repair to ReplicaRepair (#1580)" do
      assert Jobs.runner_for_label("replica-repair") == Runners.ReplicaRepair
    end

    test "round-trips every discovered runner's label back to the runner" do
      for mod <- Jobs.job_runners() do
        assert Jobs.runner_for_label(mod.label()) == mod
      end
    end

    test "returns nil for an unknown label" do
      assert Jobs.runner_for_label("no-such-runner") == nil
    end
  end
end
