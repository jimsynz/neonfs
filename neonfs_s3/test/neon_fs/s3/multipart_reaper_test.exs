defmodule NeonFS.S3.MultipartReaperTest do
  use ExUnit.Case, async: false

  use Mimic

  alias NeonFS.Client.KV
  alias NeonFS.S3.{MultipartReaper, MultipartStore}
  alias NeonFS.S3.Test.FakeKV

  # Global mode so the reaper's own process sees the FakeKV stub.
  setup :set_mimic_global

  setup do
    FakeKV.stub!()
    :ok
  end

  test "periodically reaps uploads older than the threshold and emits telemetry" do
    {:ok, old} = MultipartStore.create("bucket", "old")
    {:ok, fresh} = MultipartStore.create("bucket", "fresh")

    key = "s3_multipart:" <> old <> ":meta"
    {:ok, meta} = KV.get(key)
    :ok = KV.put(key, %{meta | initiated: DateTime.add(DateTime.utc_now(), -100, :second)})

    ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :s3, :multipart, :reap]])

    start_supervised!({MultipartReaper, max_age_seconds: 10, interval_ms: 20})

    assert_receive {[:neonfs, :s3, :multipart, :reap], ^ref, %{count: count}, _meta}, 1_000
    assert count >= 1

    assert {:error, :not_found} = MultipartStore.get(old)
    assert {:ok, _} = MultipartStore.get(fresh)
  end
end
