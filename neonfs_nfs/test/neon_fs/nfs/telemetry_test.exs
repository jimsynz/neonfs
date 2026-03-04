defmodule NeonFS.NFS.TelemetryTest do
  use ExUnit.Case, async: true

  alias NeonFS.NFS.Telemetry

  test "metrics/0 returns a list of metric definitions" do
    metrics = Telemetry.metrics()
    assert [_ | _] = metrics

    names = Enum.map(metrics, & &1.name)
    assert [:neonfs, :nfs, :operation, :duration] in names
    assert [:neonfs, :nfs, :operation, :count] in names
    assert [:neonfs, :nfs, :metadata_cache, :size] in names
    assert [:neonfs, :nfs, :metadata_cache, :hit, :count] in names
    assert [:neonfs, :nfs, :metadata_cache, :miss, :count] in names
  end

  test "operation metrics use correct event name" do
    metrics = Telemetry.metrics()

    op_duration =
      Enum.find(metrics, &(&1.name == [:neonfs, :nfs, :operation, :duration]))

    assert op_duration.event_name == [:neonfs, :nfs, :request, :stop]
  end

  test "cache metrics use correct event names" do
    metrics = Telemetry.metrics()

    hit =
      Enum.find(metrics, &(&1.name == [:neonfs, :nfs, :metadata_cache, :hit, :count]))

    miss =
      Enum.find(metrics, &(&1.name == [:neonfs, :nfs, :metadata_cache, :miss, :count]))

    assert hit.event_name == [:neonfs, :nfs, :metadata_cache, :hit]
    assert miss.event_name == [:neonfs, :nfs, :metadata_cache, :miss]
  end
end
