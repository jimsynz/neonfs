defmodule NFSServer.NFSv3.TypesTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NFSServer.NFSv3.Types
  alias NFSServer.NFSv3.Types.{Fattr3, Nfstime3, Sattr3, Specdata3, WccAttr, WccData}

  doctest Types

  describe "nfsstat3" do
    test "encodes and decodes :ok" do
      bin = Types.encode_nfsstat3(:ok)
      assert {:ok, :ok, <<>>} = Types.decode_nfsstat3(bin)
    end

    test "encodes and decodes every defined status atom" do
      for status <- [
            :ok,
            :perm,
            :noent,
            :io,
            :nxio,
            :acces,
            :exist,
            :xdev,
            :nodev,
            :notdir,
            :isdir,
            :inval,
            :fbig,
            :nospc,
            :rofs,
            :mlink,
            :nametoolong,
            :notempty,
            :dquot,
            :stale,
            :remote,
            :badhandle,
            :not_sync,
            :bad_cookie,
            :notsupp,
            :too_small,
            :server_fault,
            :bad_type,
            :jukebox
          ] do
        assert {:ok, ^status, <<>>} = Types.decode_nfsstat3(Types.encode_nfsstat3(status))
      end
    end

    test "decodes an unknown code as {:unknown, n}" do
      bin = <<99_999::big-unsigned-32>>
      assert {:ok, {:unknown, 99_999}, <<>>} = Types.decode_nfsstat3(bin)
    end
  end

  describe "ftype3" do
    test "every variant round-trips" do
      for variant <- [:reg, :dir, :blk, :chr, :lnk, :sock, :fifo] do
        assert {:ok, ^variant, <<>>} = Types.decode_ftype3(Types.encode_ftype3(variant))
      end
    end

    test "rejects an unknown variant" do
      bin = <<42::big-signed-32>>
      assert {:error, {:bad_ftype3, 42}} = Types.decode_ftype3(bin)
    end
  end

  describe "fhandle3" do
    test "round-trips a 16-byte handle" do
      fh = :crypto.strong_rand_bytes(16)
      assert {:ok, ^fh, <<>>} = Types.decode_fhandle3(Types.encode_fhandle3(fh))
    end

    property "round-trips any handle ≤ 64 bytes" do
      check all(fh <- StreamData.binary(min_length: 0, max_length: 64)) do
        assert {:ok, ^fh, <<>>} = Types.decode_fhandle3(Types.encode_fhandle3(fh))
      end
    end

    test "rejects a > 64-byte payload" do
      too_big = :crypto.strong_rand_bytes(65)

      assert_raise FunctionClauseError, fn ->
        Types.encode_fhandle3(too_big)
      end
    end
  end

  describe "specdata3" do
    test "round-trips a populated struct" do
      s = %Specdata3{specdata1: 7, specdata2: 9}
      assert {:ok, ^s, <<>>} = Types.decode_specdata3(Types.encode_specdata3(s))
    end
  end

  describe "nfstime3" do
    test "round-trips a populated struct" do
      t = %Nfstime3{seconds: 1_700_000_000, nseconds: 123_456_789}
      assert {:ok, ^t, <<>>} = Types.decode_nfstime3(Types.encode_nfstime3(t))
    end
  end

  describe "fattr3" do
    test "round-trips a populated struct" do
      a = %Fattr3{
        type: :reg,
        mode: 0o644,
        nlink: 1,
        uid: 1000,
        gid: 1000,
        size: 4096,
        used: 8192,
        rdev: %Specdata3{specdata1: 0, specdata2: 0},
        fsid: 1,
        fileid: 999,
        atime: %Nfstime3{seconds: 100, nseconds: 200},
        mtime: %Nfstime3{seconds: 300, nseconds: 400},
        ctime: %Nfstime3{seconds: 500, nseconds: 600}
      }

      assert {:ok, ^a, <<>>} = Types.decode_fattr3(Types.encode_fattr3(a))
    end
  end

  describe "post_op_attr" do
    test "encodes nil as a single FALSE flag" do
      bin = Types.encode_post_op_attr(nil)
      assert byte_size(bin) == 4
      assert {:ok, nil, <<>>} = Types.decode_post_op_attr(bin)
    end

    test "round-trips a populated Fattr3" do
      a = sample_fattr3()
      assert {:ok, ^a, <<>>} = Types.decode_post_op_attr(Types.encode_post_op_attr(a))
    end
  end

  describe "post_op_fh3" do
    test "encodes nil as a single FALSE flag" do
      bin = Types.encode_post_op_fh3(nil)
      assert byte_size(bin) == 4
      assert {:ok, nil, <<>>} = Types.decode_post_op_fh3(bin)
    end

    test "round-trips a populated handle" do
      fh = :crypto.strong_rand_bytes(20)
      assert {:ok, ^fh, <<>>} = Types.decode_post_op_fh3(Types.encode_post_op_fh3(fh))
    end
  end

  describe "wcc_data" do
    test "round-trips both halves" do
      w = %WccData{
        before: %WccAttr{
          size: 100,
          mtime: %Nfstime3{seconds: 1, nseconds: 2},
          ctime: %Nfstime3{seconds: 3, nseconds: 4}
        },
        after: sample_fattr3()
      }

      assert {:ok, ^w, <<>>} = Types.decode_wcc_data(Types.encode_wcc_data(w))
    end

    test "round-trips with both halves nil" do
      w = %WccData{before: nil, after: nil}
      assert {:ok, ^w, <<>>} = Types.decode_wcc_data(Types.encode_wcc_data(w))
    end
  end

  describe "sattr3" do
    test "round-trips an all-nil struct" do
      s = %Sattr3{}
      assert {:ok, ^s, <<>>} = Types.decode_sattr3(Types.encode_sattr3(s))
    end

    test "round-trips a populated struct" do
      s = %Sattr3{
        mode: 0o755,
        uid: 1000,
        gid: 100,
        size: 0,
        atime: :set_to_server_time,
        mtime: {:client, %Nfstime3{seconds: 42, nseconds: 0}}
      }

      assert {:ok, ^s, <<>>} = Types.decode_sattr3(Types.encode_sattr3(s))
    end
  end

  describe "diropargs3" do
    test "round-trips" do
      args = {:crypto.strong_rand_bytes(16), "hello.txt"}
      assert {:ok, ^args, <<>>} = Types.decode_diropargs3(Types.encode_diropargs3(args))
    end
  end

  describe "string-bounded primitives" do
    test "filename3 round-trips" do
      assert {:ok, "name.txt", <<>>} =
               Types.decode_filename3(Types.encode_filename3("name.txt"))
    end

    test "nfspath3 round-trips" do
      assert {:ok, "../etc/passwd", <<>>} =
               Types.decode_nfspath3(Types.encode_nfspath3("../etc/passwd"))
    end
  end

  describe "fixed-size verifiers" do
    test "cookieverf3 round-trips" do
      v = :crypto.strong_rand_bytes(8)
      assert {:ok, ^v, <<>>} = Types.decode_cookieverf3(Types.encode_cookieverf3(v))
    end

    test "createverf3 round-trips" do
      v = :crypto.strong_rand_bytes(8)
      assert {:ok, ^v, <<>>} = Types.decode_createverf3(Types.encode_createverf3(v))
    end

    test "writeverf3 round-trips" do
      v = :crypto.strong_rand_bytes(8)
      assert {:ok, ^v, <<>>} = Types.decode_writeverf3(Types.encode_writeverf3(v))
    end
  end

  defp sample_fattr3 do
    %Fattr3{
      type: :reg,
      mode: 0o644,
      nlink: 1,
      uid: 1000,
      gid: 1000,
      size: 4096,
      used: 8192,
      rdev: %Specdata3{specdata1: 0, specdata2: 0},
      fsid: 1,
      fileid: 999,
      atime: %Nfstime3{seconds: 100, nseconds: 200},
      mtime: %Nfstime3{seconds: 300, nseconds: 400},
      ctime: %Nfstime3{seconds: 500, nseconds: 600}
    }
  end
end
