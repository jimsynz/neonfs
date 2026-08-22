defmodule NeonFS.Core.Volume.BlockDeviceTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Volume.BlockDevice
  alias NeonFS.Core.Volume.BlockExtent

  describe "key/0" do
    # The whole point of the key: a *shape* an extent key cannot have, so the
    # header is excluded by a type check rather than by arithmetic that an
    # off-by-one in a range bound can defeat.
    test "cannot be produced by BlockExtent.key/1" do
      refute BlockExtent.extent_key?(BlockDevice.key())
    end

    test "no extent index produces it" do
      for index <- [0, 1, 63, 64, 65_535, 0xFFFFFFFFFFFFFFFF] do
        assert BlockExtent.key(index) != BlockDevice.key()
      end
    end
  end

  describe "shard/0" do
    # Stated rather than derived — the header has no extent index, so there is
    # no `div(extent_index, group_size)` for it to fall out of.
    test "is a group index" do
      assert is_integer(BlockDevice.shard())
      assert BlockDevice.shard() >= 0
    end
  end

  describe "new/1" do
    test "stamps created_at when the caller does not" do
      device = BlockDevice.new(id: "d", path: "/dev.img", size_bytes: 1024, chunk_bytes: 512)

      assert %DateTime{} = device.created_at
    end

    test "keeps a supplied created_at" do
      at = ~U[2026-01-02 03:04:05Z]

      device =
        BlockDevice.new(
          id: "d",
          path: "/dev.img",
          size_bytes: 1024,
          chunk_bytes: 512,
          created_at: at
        )

      assert device.created_at == at
    end

    test "refuses to build a device without geometry, a name or an id" do
      assert_raise KeyError, fn -> BlockDevice.new(id: "d", path: "/d", size_bytes: 1024) end
      assert_raise KeyError, fn -> BlockDevice.new(id: "d", path: "/d", chunk_bytes: 512) end

      assert_raise KeyError, fn ->
        BlockDevice.new(id: "d", size_bytes: 1024, chunk_bytes: 512)
      end

      assert_raise KeyError, fn ->
        BlockDevice.new(path: "/d", size_bytes: 1024, chunk_bytes: 512)
      end
    end
  end

  describe "encode/1 and decode/1" do
    test "round-trips every field" do
      device =
        BlockDevice.new(
          id: "dev-abc",
          path: "/dev.img",
          size_bytes: 64 * 1024 * 1024,
          chunk_bytes: 131_072,
          created_at: ~U[2026-08-20 12:00:00Z]
        )

      assert {:ok, decoded} = BlockDevice.decode(BlockDevice.encode(device))
      assert decoded == device
    end

    # A device whose geometry is guessed addresses the wrong extents, so a
    # value that is not a header has to be refused rather than filled in.
    test "refuses a term that is not a header" do
      assert {:error, {:malformed_device_header, _}} =
               BlockDevice.decode(:erlang.term_to_binary(%{id: "d"}))

      # A header from before the device recorded its own name decodes as a
      # header everywhere the name is not checked, which is how a second
      # device would silently alias onto the volume's only one.
      assert {:error, {:malformed_device_header, _}} =
               BlockDevice.decode(
                 :erlang.term_to_binary(%{
                   id: "d",
                   size_bytes: 1024,
                   chunk_bytes: 512,
                   created_at: ~U[2026-08-20 12:00:00Z]
                 })
               )

      assert {:error, {:malformed_device_header, _}} =
               BlockDevice.decode(:erlang.term_to_binary({:not, :a, :header}))
    end

    test "refuses a binary that is not a term at all" do
      assert {:error, {:malformed_device_header, _}} = BlockDevice.decode(<<0, 1, 2, 3>>)
    end

    # An encoded extent entry is a plausible thing to find at this key if a
    # filter is ever missing, and it must not decode as geometry.
    test "refuses an encoded extent entry" do
      entry = BlockExtent.encode({:chunk, :crypto.hash(:sha256, "x")})

      assert {:error, {:malformed_device_header, _}} = BlockDevice.decode(entry)
    end
  end
end
