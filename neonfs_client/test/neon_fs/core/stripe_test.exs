defmodule NeonFS.Core.StripeTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Stripe

  @valid_config %{data_chunks: 10, parity_chunks: 4, chunk_size: 262_144}

  describe "new/1" do
    test "creates stripe with map attrs" do
      stripe = Stripe.new(%{volume_id: "vol-1", config: @valid_config})

      assert is_binary(stripe.id)
      assert stripe.volume_id == "vol-1"
      assert stripe.config == @valid_config
      assert stripe.chunks == []
      assert stripe.partial == false
      assert stripe.data_bytes == 0
      assert stripe.padded_bytes == 0
    end

    test "creates stripe with keyword attrs" do
      stripe = Stripe.new(volume_id: "vol-2", config: @valid_config, partial: true)

      assert stripe.volume_id == "vol-2"
      assert stripe.partial == true
    end

    test "accepts custom id" do
      stripe = Stripe.new(%{id: "custom-id", config: @valid_config})
      assert stripe.id == "custom-id"
    end

    test "generates unique IDs" do
      s1 = Stripe.new(%{config: @valid_config})
      s2 = Stripe.new(%{config: @valid_config})
      refute s1.id == s2.id
    end

    test "accepts chunks and byte counts" do
      hashes = for _ <- 1..14, do: :crypto.strong_rand_bytes(32)

      stripe =
        Stripe.new(%{
          config: @valid_config,
          chunks: hashes,
          data_bytes: 2_500_000,
          padded_bytes: 121_440
        })

      assert length(stripe.chunks) == 14
      assert stripe.data_bytes == 2_500_000
      assert stripe.padded_bytes == 121_440
    end
  end

  describe "validate/1" do
    test "accepts valid stripe with empty chunks" do
      stripe = Stripe.new(%{config: @valid_config})
      assert :ok = Stripe.validate(stripe)
    end

    test "accepts valid stripe with correct chunk count" do
      hashes = for _ <- 1..14, do: :crypto.strong_rand_bytes(32)
      stripe = Stripe.new(%{config: @valid_config, chunks: hashes})
      assert :ok = Stripe.validate(stripe)
    end

    test "rejects zero data_chunks" do
      stripe =
        Stripe.new(%{config: %{data_chunks: 0, parity_chunks: 4, chunk_size: 262_144}})

      assert {:error, "invalid config" <> _} = Stripe.validate(stripe)
    end

    test "rejects zero parity_chunks" do
      stripe =
        Stripe.new(%{config: %{data_chunks: 10, parity_chunks: 0, chunk_size: 262_144}})

      assert {:error, "invalid config" <> _} = Stripe.validate(stripe)
    end

    test "rejects zero chunk_size" do
      stripe = Stripe.new(%{config: %{data_chunks: 10, parity_chunks: 4, chunk_size: 0}})

      assert {:error, "invalid config" <> _} = Stripe.validate(stripe)
    end

    test "rejects mismatched chunks list length" do
      hashes = for _ <- 1..5, do: :crypto.strong_rand_bytes(32)
      stripe = Stripe.new(%{config: @valid_config, chunks: hashes})

      assert {:error, "chunks list length 5 does not match expected 14"} =
               Stripe.validate(stripe)
    end

    test "rejects negative data_bytes" do
      stripe = Stripe.new(%{config: @valid_config, data_bytes: -1})
      assert {:error, "data_bytes and padded_bytes" <> _} = Stripe.validate(stripe)
    end

    test "rejects negative padded_bytes" do
      stripe = Stripe.new(%{config: @valid_config, padded_bytes: -1})
      assert {:error, "data_bytes and padded_bytes" <> _} = Stripe.validate(stripe)
    end
  end

  describe "total_chunks/1" do
    test "returns sum of data and parity chunks" do
      stripe = Stripe.new(%{config: @valid_config})
      assert Stripe.total_chunks(stripe) == 14
    end

    test "works with different configs" do
      stripe = Stripe.new(%{config: %{data_chunks: 4, parity_chunks: 2, chunk_size: 1024}})
      assert Stripe.total_chunks(stripe) == 6
    end
  end

  describe "data_chunk_hashes/1" do
    test "returns first data_chunks entries" do
      hashes = for i <- 1..14, do: <<i>>
      stripe = Stripe.new(%{config: @valid_config, chunks: hashes})

      data_hashes = Stripe.data_chunk_hashes(stripe)
      assert length(data_hashes) == 10
      assert data_hashes == Enum.take(hashes, 10)
    end

    test "returns empty list when no chunks" do
      stripe = Stripe.new(%{config: @valid_config})
      assert Stripe.data_chunk_hashes(stripe) == []
    end
  end

  describe "parity_chunk_hashes/1" do
    test "returns last parity_chunks entries" do
      hashes = for i <- 1..14, do: <<i>>
      stripe = Stripe.new(%{config: @valid_config, chunks: hashes})

      parity_hashes = Stripe.parity_chunk_hashes(stripe)
      assert length(parity_hashes) == 4
      assert parity_hashes == Enum.drop(hashes, 10)
    end

    test "returns empty list when no chunks" do
      stripe = Stripe.new(%{config: @valid_config})
      assert Stripe.parity_chunk_hashes(stripe) == []
    end
  end
end
