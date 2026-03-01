defmodule NeonFS.FUSE.AlertsYmlTest do
  use ExUnit.Case, async: true

  @alerts_path Path.expand("../../../../deploy/prometheus/alerts.yml", __DIR__)

  test "alerts.yml exists and is valid YAML" do
    assert File.exists?(@alerts_path), "deploy/prometheus/alerts.yml not found"
    assert {:ok, content} = YamlElixir.read_from_file(@alerts_path)
    assert is_map(content)
  end

  test "alerts.yml contains required alert groups" do
    {:ok, content} = YamlElixir.read_from_file(@alerts_path)

    groups = content["groups"]
    assert is_list(groups)
    assert groups != []

    all_rules = Enum.flat_map(groups, & &1["rules"])
    alert_names = Enum.map(all_rules, & &1["alert"])

    assert "NeonFSStorageCritical" in alert_names
    assert "NeonFSStorageWarning" in alert_names
    assert "NeonFSClockSkew" in alert_names
    assert "NeonFSNodeDown" in alert_names
    assert "NeonFSReplicationLag" in alert_names
    assert "NeonFSDriveFailed" in alert_names
    assert "NeonFSCacheEvictionHigh" in alert_names
  end

  test "all alert rules have required fields" do
    {:ok, content} = YamlElixir.read_from_file(@alerts_path)

    for group <- content["groups"],
        rule <- group["rules"] do
      assert is_binary(rule["alert"]), "missing alert name in #{inspect(rule)}"
      assert is_binary(rule["expr"]), "missing expr in alert #{rule["alert"]}"
      assert is_binary(rule["for"]), "missing for in alert #{rule["alert"]}"
      assert is_map(rule["labels"]), "missing labels in alert #{rule["alert"]}"
      assert is_binary(rule["labels"]["severity"]), "missing severity in alert #{rule["alert"]}"
      assert is_map(rule["annotations"]), "missing annotations in alert #{rule["alert"]}"
    end
  end
end
