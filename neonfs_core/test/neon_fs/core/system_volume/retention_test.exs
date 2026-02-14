defmodule NeonFS.Core.SystemVolume.RetentionTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Cluster.Init
  alias NeonFS.Core.SystemVolume
  alias NeonFS.Core.SystemVolume.Retention

  @moduletag :tmp_dir

  describe "parse_file_date/1" do
    test "parses YYYY-MM-DD.jsonl filenames" do
      assert Retention.parse_file_date("2026-01-15.jsonl") == {:ok, ~D[2026-01-15]}
    end

    test "parses YYYY-MM-DD filenames without extension" do
      assert Retention.parse_file_date("2025-12-31") == {:ok, ~D[2025-12-31]}
    end

    test "returns :error for non-date filenames" do
      assert Retention.parse_file_date("notes.txt") == :error
      assert Retention.parse_file_date("readme.md") == :error
      assert Retention.parse_file_date("2026-13-01.jsonl") == :error
    end

    test "returns :error for partial dates" do
      assert Retention.parse_file_date("2026-01.jsonl") == :error
      assert Retention.parse_file_date("2026.jsonl") == :error
    end
  end

  describe "prune/0 with system volume" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()

      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_stripe_index()
      start_volume_registry()
      ensure_chunk_access_tracker()
      start_ra()

      {:ok, _cluster_id} = Init.init_cluster("retention-test-cluster")

      on_exit(fn ->
        stop_ra()
        cleanup_test_dirs()
      end)

      :ok
    end

    test "deletes old files and retains recent ones" do
      today = Date.utc_today()
      old_date = today |> Date.add(-100) |> Date.to_iso8601()
      recent_date = today |> Date.add(-10) |> Date.to_iso8601()

      :ok = SystemVolume.write("/audit/intents/#{old_date}.jsonl", "old intent log")
      :ok = SystemVolume.write("/audit/intents/#{recent_date}.jsonl", "recent intent log")

      # With default 90-day retention, old file should be pruned
      :ok = Retention.prune()

      {:ok, remaining} = SystemVolume.list("/audit/intents")
      assert "#{recent_date}.jsonl" in remaining
      refute "#{old_date}.jsonl" in remaining
    end

    test "respects per-directory retention periods" do
      today = Date.utc_today()
      # 200 days ago — past intent retention (90) but within security retention (365)
      mid_date = today |> Date.add(-200) |> Date.to_iso8601()

      :ok = SystemVolume.write("/audit/intents/#{mid_date}.jsonl", "intent log")
      :ok = SystemVolume.write("/audit/security/#{mid_date}.jsonl", "security log")

      :ok = Retention.prune()

      # Intent log (90 day retention) should be pruned
      {:ok, intent_files} = SystemVolume.list("/audit/intents")
      refute "#{mid_date}.jsonl" in intent_files

      # Security log (365 day retention) should be retained
      {:ok, security_files} = SystemVolume.list("/audit/security")
      assert "#{mid_date}.jsonl" in security_files
    end

    test "respects configured retention overrides" do
      today = Date.utc_today()
      date_5d_ago = today |> Date.add(-5) |> Date.to_iso8601()

      :ok = SystemVolume.write("/audit/intents/#{date_5d_ago}.jsonl", "intent log")

      # Override to 3-day retention
      Application.put_env(:neonfs_core, Retention, intent_log_days: 3)

      on_exit(fn ->
        Application.delete_env(:neonfs_core, Retention)
      end)

      :ok = Retention.prune()

      {:ok, files} = SystemVolume.list("/audit/intents")
      refute "#{date_5d_ago}.jsonl" in files
    end

    test "skips files with unparseable dates" do
      today = Date.utc_today()
      old_date = today |> Date.add(-100) |> Date.to_iso8601()

      :ok = SystemVolume.write("/audit/intents/#{old_date}.jsonl", "old")
      :ok = SystemVolume.write("/audit/intents/notes.txt", "not a log")

      :ok = Retention.prune()

      {:ok, files} = SystemVolume.list("/audit/intents")
      assert "notes.txt" in files
      refute "#{old_date}.jsonl" in files
    end

    test "handles empty directories gracefully" do
      assert :ok = Retention.prune()
    end
  end

  describe "prune/0 without system volume" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_stripe_index()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)

      :ok
    end

    test "returns :ok when system volume does not exist" do
      assert :ok = Retention.prune()
    end
  end

  describe "GenServer scheduling" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_stripe_index()
      start_volume_registry()

      on_exit(fn -> cleanup_test_dirs() end)

      :ok
    end

    test "schedules periodic pruning via handle_info" do
      pid = start_supervised!({Retention, prune_interval_ms: 50}, restart: :temporary)

      # The GenServer should be alive and scheduling prune cycles
      assert Process.alive?(pid)

      # Wait long enough for at least one scheduled prune to fire
      Process.sleep(100)

      # Still alive after prune cycle (didn't crash on missing system volume)
      assert Process.alive?(pid)
    end
  end
end
