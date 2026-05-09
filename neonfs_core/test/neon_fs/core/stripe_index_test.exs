defmodule NeonFS.Core.StripeIndexTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{Stripe, StripeIndex}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    store = :ets.new(:test_stripe_metadata_store, [:set, :public])

    metadata_reader_opts = build_mock_metadata_reader_opts(store)
    metadata_writer_opts = build_mock_metadata_writer_opts(store)

    stop_if_running(NeonFS.Core.StripeIndex)
    cleanup_ets_table(:stripe_index)

    start_supervised!(
      {NeonFS.Core.StripeIndex,
       metadata_reader_opts: metadata_reader_opts, metadata_writer_opts: metadata_writer_opts},
      restart: :temporary
    )

    on_exit(fn ->
      cleanup_test_dirs()

      try do
        :ets.delete(store)
      rescue
        ArgumentError -> :ok
      end
    end)

    # Use unique volume IDs per test to avoid leaking between tests
    vol_suffix = System.unique_integer([:positive])

    %{
      store: store,
      vol1: "vol-1-#{vol_suffix}",
      vol2: "vol-2-#{vol_suffix}"
    }
  end

  defp make_stripe(attrs \\ %{}) do
    Stripe.new(
      Map.merge(
        %{
          volume_id: "vol-1",
          config: %{data_chunks: 10, parity_chunks: 4, chunk_size: 262_144}
        },
        attrs
      )
    )
  end

  describe "put/1" do
    test "stores a stripe and returns its id" do
      stripe = make_stripe()

      assert {:ok, id} = StripeIndex.put(stripe)
      assert id == stripe.id
    end

    test "stripe is retrievable after put" do
      stripe = make_stripe()

      {:ok, _id} = StripeIndex.put(stripe)
      assert {:ok, retrieved} = StripeIndex.get(stripe.volume_id, stripe.id)
      assert retrieved.id == stripe.id
      assert retrieved.volume_id == stripe.volume_id
      assert retrieved.config == stripe.config
    end
  end

  describe "delete/1" do
    test "removes stripe from index" do
      stripe = make_stripe()
      {:ok, _} = StripeIndex.put(stripe)

      assert :ok = StripeIndex.delete(stripe.id)
      assert {:error, :not_found} = StripeIndex.get(stripe.volume_id, stripe.id)
    end

    test "returns :ok for non-existent stripe" do
      assert :ok = StripeIndex.delete("nonexistent-id")
    end

    test "removes from both quorum and ETS", %{store: store} do
      stripe = make_stripe()
      {:ok, _} = StripeIndex.put(stripe)

      assert :ok = StripeIndex.delete(stripe.id)

      # ETS should be empty
      assert [] = :ets.lookup(:stripe_index, stripe.id)

      # Quorum store should be empty
      key = "stripe:" <> stripe.id
      assert [] = :ets.lookup(store, key)
    end

    test "subsequent get returns :not_found via MetadataReader" do
      stripe = make_stripe()
      {:ok, _} = StripeIndex.put(stripe)
      assert :ok = StripeIndex.delete(stripe.id)

      assert {:error, :not_found} = StripeIndex.get(stripe.volume_id, stripe.id)
    end
  end

  describe "get/2 (volume-scoped read via MetadataReader)" do
    test "round-trips through the per-volume metadata read path" do
      stripe = make_stripe()
      {:ok, _} = StripeIndex.put(stripe)

      :ets.delete(:stripe_index, stripe.id)
      assert {:ok, retrieved} = StripeIndex.get(stripe.volume_id, stripe.id)
      assert retrieved.id == stripe.id
    end

    test "returns :not_found for an unknown stripe" do
      assert {:error, :not_found} = StripeIndex.get("vol1", "nonexistent-id")
    end
  end

  describe "exists?/2 (volume-scoped existence check)" do
    test "returns true when the stripe is present" do
      stripe = make_stripe()
      {:ok, _} = StripeIndex.put(stripe)

      assert StripeIndex.exists?(stripe.volume_id, stripe.id)
    end

    test "returns false for an unknown stripe" do
      refute StripeIndex.exists?("vol1", "nonexistent-id")
    end
  end

  describe "list_by_volume/1" do
    test "returns all stripes for a given volume", %{vol1: vol1, vol2: vol2} do
      s1 = make_stripe(%{volume_id: vol1})
      s2 = make_stripe(%{volume_id: vol1})
      s3 = make_stripe(%{volume_id: vol2})

      {:ok, _} = StripeIndex.put(s1)
      {:ok, _} = StripeIndex.put(s2)
      {:ok, _} = StripeIndex.put(s3)

      vol1_stripes = StripeIndex.list_by_volume(vol1)
      assert length(vol1_stripes) == 2

      vol1_ids = Enum.map(vol1_stripes, & &1.id) |> Enum.sort()
      assert vol1_ids == Enum.sort([s1.id, s2.id])
    end

    test "returns empty list for volume with no stripes" do
      assert StripeIndex.list_by_volume("nonexistent-vol") == []
    end

    test "does not include deleted stripes", %{vol1: vol1} do
      stripe = make_stripe(%{volume_id: vol1})
      {:ok, _} = StripeIndex.put(stripe)
      :ok = StripeIndex.delete(stripe.id)

      assert StripeIndex.list_by_volume(vol1) == []
    end
  end

  describe "list_all/0" do
    test "returns all stripes from ETS" do
      stripes =
        for _ <- 1..5 do
          stripe = make_stripe()
          {:ok, _} = StripeIndex.put(stripe)
          stripe
        end

      all = StripeIndex.list_all()
      assert length(all) == 5
      all_ids = Enum.map(all, & &1.id) |> Enum.sort()
      expected_ids = Enum.map(stripes, & &1.id) |> Enum.sort()
      assert all_ids == expected_ids
    end

    test "returns empty list when no stripes exist" do
      assert [] = StripeIndex.list_all()
    end
  end

  describe "multiple operations" do
    test "put then update via re-put" do
      stripe = make_stripe(%{chunks: []})
      {:ok, _} = StripeIndex.put(stripe)

      updated = %{stripe | chunks: ["hash1", "hash2"]}
      {:ok, _} = StripeIndex.put(updated)

      assert {:ok, result} = StripeIndex.get(stripe.volume_id, stripe.id)
      assert result.chunks == ["hash1", "hash2"]
    end

    test "stores stripes with different configs" do
      s1 = make_stripe(%{config: %{data_chunks: 4, parity_chunks: 2, chunk_size: 65_536}})

      s2 =
        make_stripe(%{config: %{data_chunks: 10, parity_chunks: 4, chunk_size: 262_144}})

      {:ok, _} = StripeIndex.put(s1)
      {:ok, _} = StripeIndex.put(s2)

      {:ok, r1} = StripeIndex.get(s1.volume_id, s1.id)
      {:ok, r2} = StripeIndex.get(s2.volume_id, s2.id)

      assert r1.config.data_chunks == 4
      assert r2.config.data_chunks == 10
    end
  end

  describe "ETS cache behaviour" do
    test "writes update both quorum and ETS", %{store: store} do
      stripe = make_stripe()
      {:ok, _} = StripeIndex.put(stripe)

      # Check ETS
      stripe_id = stripe.id
      assert [{^stripe_id, cached}] = :ets.lookup(:stripe_index, stripe.id)
      assert cached.id == stripe.id

      # Check quorum store
      key = "stripe:" <> stripe.id
      assert [{^key, stored}] = :ets.lookup(store, key)
      assert stored[:id] == stripe.id
    end

    test "deletes remove from both quorum and ETS", %{store: store} do
      stripe = make_stripe()
      {:ok, _} = StripeIndex.put(stripe)

      assert :ok = StripeIndex.delete(stripe.id)

      # ETS should be empty
      assert [] = :ets.lookup(:stripe_index, stripe.id)

      # Quorum store should be empty
      key = "stripe:" <> stripe.id
      assert [] = :ets.lookup(store, key)
    end
  end

  describe "key format" do
    test "uses stripe: prefix with stripe id", %{store: store} do
      stripe = make_stripe()
      {:ok, _} = StripeIndex.put(stripe)

      expected_key = "stripe:" <> stripe.id
      assert [{^expected_key, _}] = :ets.lookup(store, expected_key)
    end
  end

  # Private helpers
  defp stop_if_running(name) do
    case Process.whereis(name) do
      nil ->
        :ok

      pid ->
        ref = Process.monitor(pid)
        GenServer.stop(pid, :normal, 5000)

        receive do
          {:DOWN, ^ref, :process, ^pid, _} -> :ok
        after
          1_000 -> :ok
        end
    end
  end

  defp cleanup_ets_table(table) do
    case :ets.whereis(table) do
      :undefined -> :ok
      ref -> :ets.delete(ref)
    end
  end
end
