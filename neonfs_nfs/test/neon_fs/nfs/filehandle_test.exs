defmodule NeonFS.NFS.FilehandleTest do
  use ExUnit.Case, async: true

  import Bitwise, only: [<<<: 2]

  alias NeonFS.NFS.Filehandle

  @volume_id <<0::96, 0xDEADBEEF::32>>
  @fileid 0x0123_4567_89AB_CDEF
  @generation 7

  describe "encode/decode round-trip" do
    test "encodes a 64-byte filehandle" do
      fh = Filehandle.encode(@volume_id, @fileid, @generation)
      assert byte_size(fh) == 64
      assert byte_size(fh) == Filehandle.size()
    end

    test "decodes back to the original fields" do
      fh = Filehandle.encode(@volume_id, @fileid, @generation)

      assert {:ok, %{volume_id: vol, fileid: id, generation: gen}} = Filehandle.decode(fh)
      assert vol == @volume_id
      assert id == @fileid
      assert gen == @generation
    end

    test "default generation is 0" do
      fh = Filehandle.encode(@volume_id, @fileid)
      assert {:ok, %{generation: 0}} = Filehandle.decode(fh)
    end
  end

  describe "decode/1 rejects malformed handles" do
    test "wrong size" do
      assert {:error, :stale} = Filehandle.decode(<<0::8>>)
      assert {:error, :stale} = Filehandle.decode(<<0::65*8>>)
      assert {:error, :stale} = Filehandle.decode(<<>>)
    end

    test "non-zero reserved bytes" do
      # Pack a valid prefix then dirty the reserved trailer.
      prefix = <<@volume_id::binary, @fileid::64, 0::32>>
      # All zeros in reserved is fine; flip a single bit.
      dirty = prefix <> <<0::35*8, 1>>

      assert byte_size(dirty) == 64
      assert {:error, :stale} = Filehandle.decode(dirty)
    end
  end

  describe "encode/3 guards" do
    test "rejects volume_id with the wrong size" do
      assert_raise FunctionClauseError, fn ->
        Filehandle.encode(<<0::120>>, @fileid)
      end
    end

    test "rejects negative or out-of-range fileid" do
      assert_raise FunctionClauseError, fn ->
        Filehandle.encode(@volume_id, -1)
      end

      assert_raise FunctionClauseError, fn ->
        Filehandle.encode(@volume_id, 1 <<< 64)
      end
    end
  end

  describe "volume_uuid_to_binary / volume_uuid_from_binary" do
    test "round-trips a hyphenated UUID through 16 bytes and back" do
      uuid = "019dc5d8-3fcf-7d13-b4fa-832c4390b0a0"

      assert {:ok, bin} = Filehandle.volume_uuid_to_binary(uuid)
      assert byte_size(bin) == 16

      assert Filehandle.volume_uuid_from_binary(bin) == uuid
    end

    test "rejects malformed UUIDs" do
      assert {:error, :invalid} = Filehandle.volume_uuid_to_binary("not-a-uuid")
      assert {:error, :invalid} = Filehandle.volume_uuid_to_binary("019d-too-short")
      assert {:error, :invalid} = Filehandle.volume_uuid_to_binary(:not_a_string)
    end
  end

  describe "wrong-volume detection" do
    test "decode succeeds even if the volume id doesn't belong to this export" do
      # Filehandle.decode/1 doesn't check the volume id against the
      # active export — that's the backend's job. The codec just
      # validates structure. Two distinct volumes' fhandles both
      # decode cleanly.
      vol_a = <<0::96, 0xAAAAAAAA::32>>
      vol_b = <<0::96, 0xBBBBBBBB::32>>

      fh_a = Filehandle.encode(vol_a, 1)
      fh_b = Filehandle.encode(vol_b, 1)

      assert {:ok, %{volume_id: ^vol_a}} = Filehandle.decode(fh_a)
      assert {:ok, %{volume_id: ^vol_b}} = Filehandle.decode(fh_b)
      refute fh_a == fh_b
    end
  end
end
