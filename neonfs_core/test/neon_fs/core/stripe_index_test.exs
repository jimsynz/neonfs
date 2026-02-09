defmodule NeonFS.Core.StripeIndexTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{Stripe, StripeIndex}

  setup do
    start_stripe_index()
    :ok
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
      assert {:ok, retrieved} = StripeIndex.get(stripe.id)
      assert retrieved.id == stripe.id
      assert retrieved.volume_id == stripe.volume_id
      assert retrieved.config == stripe.config
    end
  end

  describe "get/1" do
    test "returns {:ok, stripe} for existing stripe" do
      stripe = make_stripe()
      {:ok, _} = StripeIndex.put(stripe)

      assert {:ok, result} = StripeIndex.get(stripe.id)
      assert result.id == stripe.id
    end

    test "returns {:error, :not_found} for non-existent stripe" do
      assert {:error, :not_found} = StripeIndex.get("nonexistent-id")
    end
  end

  describe "delete/1" do
    test "removes stripe from index" do
      stripe = make_stripe()
      {:ok, _} = StripeIndex.put(stripe)

      assert :ok = StripeIndex.delete(stripe.id)
      assert {:error, :not_found} = StripeIndex.get(stripe.id)
    end

    test "returns :ok for non-existent stripe" do
      assert :ok = StripeIndex.delete("nonexistent-id")
    end
  end

  describe "list_by_volume/1" do
    test "returns all stripes for a given volume" do
      s1 = make_stripe(%{volume_id: "vol-1"})
      s2 = make_stripe(%{volume_id: "vol-1"})
      s3 = make_stripe(%{volume_id: "vol-2"})

      {:ok, _} = StripeIndex.put(s1)
      {:ok, _} = StripeIndex.put(s2)
      {:ok, _} = StripeIndex.put(s3)

      vol1_stripes = StripeIndex.list_by_volume("vol-1")
      assert length(vol1_stripes) == 2

      vol1_ids = Enum.map(vol1_stripes, & &1.id) |> Enum.sort()
      assert vol1_ids == Enum.sort([s1.id, s2.id])
    end

    test "returns empty list for volume with no stripes" do
      assert StripeIndex.list_by_volume("nonexistent-vol") == []
    end

    test "does not include deleted stripes" do
      stripe = make_stripe(%{volume_id: "vol-1"})
      {:ok, _} = StripeIndex.put(stripe)
      :ok = StripeIndex.delete(stripe.id)

      assert StripeIndex.list_by_volume("vol-1") == []
    end
  end

  describe "multiple operations" do
    test "put then update via re-put" do
      stripe = make_stripe(%{chunks: []})
      {:ok, _} = StripeIndex.put(stripe)

      updated = %{stripe | chunks: ["hash1", "hash2"]}
      {:ok, _} = StripeIndex.put(updated)

      assert {:ok, result} = StripeIndex.get(stripe.id)
      assert result.chunks == ["hash1", "hash2"]
    end

    test "stores stripes with different configs" do
      s1 = make_stripe(%{config: %{data_chunks: 4, parity_chunks: 2, chunk_size: 65_536}})

      s2 =
        make_stripe(%{config: %{data_chunks: 10, parity_chunks: 4, chunk_size: 262_144}})

      {:ok, _} = StripeIndex.put(s1)
      {:ok, _} = StripeIndex.put(s2)

      {:ok, r1} = StripeIndex.get(s1.id)
      {:ok, r2} = StripeIndex.get(s2.id)

      assert r1.config.data_chunks == 4
      assert r2.config.data_chunks == 10
    end
  end
end
