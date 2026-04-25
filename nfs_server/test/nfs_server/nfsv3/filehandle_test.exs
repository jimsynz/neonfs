defmodule NFSServer.NFSv3.FilehandleTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NFSServer.NFSv3.Filehandle

  doctest Filehandle

  @vol_id <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>

  describe "encode/3" do
    test "produces a 29-byte binary" do
      fh = Filehandle.encode(@vol_id, 99, 0)
      assert byte_size(fh) == Filehandle.packed_size()
    end

    test "starts with the v1 tag byte" do
      fh = Filehandle.encode(@vol_id, 99, 0)
      assert <<0x01, _rest::binary>> = fh
    end

    test "raises on a non-16-byte vol_id" do
      assert_raise FunctionClauseError, fn ->
        Filehandle.encode(<<1, 2, 3>>, 99, 0)
      end
    end

    test "raises on a negative fileid" do
      assert_raise FunctionClauseError, fn ->
        Filehandle.encode(@vol_id, -1, 0)
      end
    end

    test "raises on an out-of-range generation" do
      assert_raise FunctionClauseError, fn ->
        Filehandle.encode(@vol_id, 99, 0xFFFFFFFF + 1)
      end
    end
  end

  describe "decode/1" do
    test "round-trips a populated handle" do
      fh = Filehandle.encode(@vol_id, 0xDEADBEEFCAFEBABE, 0xC0DE)

      assert {:ok, %{vol_id: vol_id, fileid: fileid, generation: gen}} = Filehandle.decode(fh)
      assert vol_id == @vol_id
      assert fileid == 0xDEADBEEFCAFEBABE
      assert gen == 0xC0DE
    end

    property "round-trips any well-formed handle" do
      check all(
              vol_id <- StreamData.binary(length: 16),
              fileid <- StreamData.integer(0..0xFFFFFFFFFFFFFFFF),
              generation <- StreamData.integer(0..0xFFFFFFFF)
            ) do
        fh = Filehandle.encode(vol_id, fileid, generation)

        assert {:ok, %{vol_id: ^vol_id, fileid: ^fileid, generation: ^generation}} =
                 Filehandle.decode(fh)
      end
    end

    test "rejects an empty binary" do
      assert {:error, :badhandle} = Filehandle.decode(<<>>)
    end

    test "rejects a handle with a different tag byte" do
      bogus = <<0x02, @vol_id::binary, 99::big-unsigned-64, 0::big-unsigned-32>>
      assert {:error, :badhandle} = Filehandle.decode(bogus)
    end

    test "rejects a v1-tagged handle that is too short" do
      bogus = <<0x01, 1, 2, 3, 4>>
      assert {:error, :badhandle} = Filehandle.decode(bogus)
    end

    test "rejects a 29-byte handle with a non-v1 tag" do
      bogus = <<0xFF, @vol_id::binary, 99::big-unsigned-64, 0::big-unsigned-32>>
      assert {:error, :badhandle} = Filehandle.decode(bogus)
    end
  end

  describe "same_volume?/2" do
    test "returns true on a matching vol_id" do
      decoded = %{vol_id: @vol_id, fileid: 1, generation: 0}
      assert Filehandle.same_volume?(decoded, @vol_id)
    end

    test "returns false on a mismatched vol_id" do
      decoded = %{vol_id: @vol_id, fileid: 1, generation: 0}
      other = :crypto.strong_rand_bytes(16)
      refute Filehandle.same_volume?(decoded, other)
    end
  end
end
