defmodule NeonFS.Core.SyncOperationTest do
  @moduledoc """
  Unit tests for `NeonFS.Core.SyncOperation` (#1500). `VolumeRegistry`,
  `FileIndex`, and `Replication` are stubbed via Mimic so the barrier's
  control flow — per-chunk `ensure_min_copies`, erasure no-op, telemetry —
  is exercised without a running cluster.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Core.{FileIndex, FileMeta, Replication, SyncOperation, VolumeRegistry}

  setup :verify_on_exit!

  @volume_id "vol-1"
  @file_id "file-1"

  defp replicate_volume(min_copies \\ 2) do
    %{
      id: @volume_id,
      durability: %{type: :replicate, factor: 3, min_copies: min_copies},
      tiering: %{initial_tier: :hot}
    }
  end

  defp erasure_volume do
    %{
      id: @volume_id,
      durability: %{type: :erasure, data_chunks: 4, parity_chunks: 2},
      tiering: %{initial_tier: :hot}
    }
  end

  defp file_meta(chunks) do
    %FileMeta{id: @file_id, volume_id: @volume_id, path: "/f.txt", chunks: chunks, size: 0}
  end

  describe "sync_file_by_id/2" do
    test "drives every chunk to min_copies and returns :ok" do
      stub(VolumeRegistry, :get, fn @volume_id -> {:ok, replicate_volume()} end)
      stub(FileIndex, :get, fn @volume_id, @file_id -> {:ok, file_meta(["h1", "h2"])} end)

      expect(Replication, :ensure_min_copies, fn "h1", _vol -> :ok end)
      expect(Replication, :ensure_min_copies, fn "h2", _vol -> :ok end)

      assert :ok = SyncOperation.sync_file_by_id(@volume_id, @file_id)
    end

    test "halts and returns the error for the first chunk that cannot reach min_copies" do
      stub(VolumeRegistry, :get, fn @volume_id -> {:ok, replicate_volume()} end)
      stub(FileIndex, :get, fn @volume_id, @file_id -> {:ok, file_meta(["h1", "h2"])} end)

      expect(Replication, :ensure_min_copies, fn "h1", _vol ->
        {:error, {:under_replicated, 1, 2}}
      end)

      reject(&Replication.ensure_min_copies/2)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :sync_file, :under_replicated]
        ])

      assert {:error, {:under_replicated, 1, 2}} =
               SyncOperation.sync_file_by_id(@volume_id, @file_id)

      assert_received {[:neonfs, :sync_file, :under_replicated], ^ref, %{},
                       %{chunk_hash: "h1", reason: {:under_replicated, 1, 2}}}
    end

    test "erasure volumes are a no-op (shards written synchronously on the write path)" do
      stub(VolumeRegistry, :get, fn @volume_id -> {:ok, erasure_volume()} end)
      stub(FileIndex, :get, fn @volume_id, @file_id -> {:ok, file_meta([])} end)

      reject(&Replication.ensure_min_copies/2)

      assert :ok = SyncOperation.sync_file_by_id(@volume_id, @file_id)
    end

    test "emits start/stop telemetry around the barrier" do
      stub(VolumeRegistry, :get, fn @volume_id -> {:ok, replicate_volume()} end)
      stub(FileIndex, :get, fn @volume_id, @file_id -> {:ok, file_meta(["h1"])} end)
      stub(Replication, :ensure_min_copies, fn _hash, _vol -> :ok end)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :sync_file, :start],
          [:neonfs, :sync_file, :stop]
        ])

      assert :ok = SyncOperation.sync_file_by_id(@volume_id, @file_id)

      assert_received {[:neonfs, :sync_file, :start], ^ref, %{chunk_count: 1}, _meta}
      assert_received {[:neonfs, :sync_file, :stop], ^ref, %{duration: _}, %{status: :ok}}
    end

    test "returns :file_not_found when the file is missing" do
      stub(VolumeRegistry, :get, fn @volume_id -> {:ok, replicate_volume()} end)
      stub(FileIndex, :get, fn @volume_id, @file_id -> {:error, :not_found} end)

      assert {:error, :file_not_found} = SyncOperation.sync_file_by_id(@volume_id, @file_id)
    end
  end

  describe "sync_file/2" do
    test "resolves by path and drives chunks to min_copies" do
      stub(VolumeRegistry, :get, fn @volume_id -> {:ok, replicate_volume()} end)
      stub(FileIndex, :get_by_path, fn @volume_id, "/f.txt" -> {:ok, file_meta(["h1"])} end)
      expect(Replication, :ensure_min_copies, fn "h1", _vol -> :ok end)

      assert :ok = SyncOperation.sync_file(@volume_id, "/f.txt")
    end
  end
end
