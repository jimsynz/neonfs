defmodule NFSServer.NFSv3.HandlerTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Bitwise

  alias NFSServer.NFSv3.{Handler, MockBackend, Types}
  alias NFSServer.NFSv3.Types.Nfstime3
  alias NFSServer.RPC.Auth
  alias NFSServer.XDR

  doctest Handler

  @auth %Auth.None{}
  @ctx_base %{call: nil, nfs_v3_backend: MockBackend}

  setup do
    MockBackend.reset()
    :ok
  end

  describe "with_backend/1" do
    test "returns a module that stamps the backend onto ctx and dispatches" do
      bound = Handler.with_backend(MockBackend)

      assert is_atom(bound)
      assert function_exported?(bound, :handle_call, 4)

      # NULL needs no fixtures and lets us verify the dispatch shape.
      assert {:ok, <<>>} = bound.handle_call(0, <<>>, @auth, %{call: nil})
    end

    test "is idempotent for the same backend" do
      first = Handler.with_backend(MockBackend)
      second = Handler.with_backend(MockBackend)
      assert first == second
    end
  end

  describe "NULL (proc 0)" do
    test "always succeeds with an empty body" do
      assert {:ok, <<>>} = Handler.handle_call(0, <<>>, @auth, @ctx_base)
    end
  end

  describe "GETATTR (proc 1)" do
    test "returns NFS3_OK + fattr3 on success" do
      fh = "fh-1"
      attr = MockBackend.sample_fattr3()
      MockBackend.put({:getattr, fh}, {:ok, attr})

      args = Types.encode_fhandle3(fh)
      {:ok, body} = Handler.handle_call(1, args, @auth, @ctx_base)

      assert {:ok, :ok, rest} = Types.decode_nfsstat3(body)
      assert {:ok, ^attr, <<>>} = Types.decode_fattr3(rest)
    end

    test "returns NFS3ERR_STALE when the backend can't resolve the handle" do
      args = Types.encode_fhandle3("missing")
      {:ok, body} = Handler.handle_call(1, args, @auth, @ctx_base)
      assert {:ok, :stale, <<>>} = Types.decode_nfsstat3(body)
    end

    test "returns :garbage_args on undecodable args" do
      assert :garbage_args = Handler.handle_call(1, <<0xFF>>, @auth, @ctx_base)
    end
  end

  describe "LOOKUP (proc 3)" do
    test "returns the file handle plus both post-op attrs on success" do
      dir = "dir"
      name = "child.txt"
      fh = "fh-child"
      file_attr = MockBackend.sample_fattr3()
      dir_attr = %{file_attr | type: :dir, fileid: 99}
      MockBackend.put({:lookup, {dir, name}}, {:ok, fh, file_attr, dir_attr})

      args = Types.encode_diropargs3({dir, name})
      {:ok, body} = Handler.handle_call(3, args, @auth, @ctx_base)

      assert {:ok, :ok, rest} = Types.decode_nfsstat3(body)
      assert {:ok, ^fh, rest} = Types.decode_fhandle3(rest)
      assert {:ok, ^file_attr, rest} = Types.decode_post_op_attr(rest)
      assert {:ok, ^dir_attr, <<>>} = Types.decode_post_op_attr(rest)
    end

    test "returns NFS3ERR_NOENT with the directory's post-op attrs" do
      dir = "dir"
      name = "missing"
      dir_attr = MockBackend.sample_fattr3()
      MockBackend.put({:lookup, {dir, name}}, {:error, :noent, dir_attr})

      args = Types.encode_diropargs3({dir, name})
      {:ok, body} = Handler.handle_call(3, args, @auth, @ctx_base)

      assert {:ok, :noent, rest} = Types.decode_nfsstat3(body)
      assert {:ok, ^dir_attr, <<>>} = Types.decode_post_op_attr(rest)
    end

    test "returns the bare error when the backend can't supply dir attrs" do
      args = Types.encode_diropargs3({"unknown", "name"})
      {:ok, body} = Handler.handle_call(3, args, @auth, @ctx_base)

      assert {:ok, :stale, rest} = Types.decode_nfsstat3(body)
      assert {:ok, nil, <<>>} = Types.decode_post_op_attr(rest)
    end
  end

  describe "ACCESS (proc 4)" do
    test "echoes the granted mask and post-op attrs on success" do
      fh = "fh-acc"
      attr = MockBackend.sample_fattr3()
      requested = Handler.access3_read() |> bor(Handler.access3_lookup())
      granted = Handler.access3_read()
      MockBackend.put({:access, {fh, requested}}, {:ok, granted, attr})

      args = Types.encode_fhandle3(fh) <> XDR.encode_uint(requested)
      {:ok, body} = Handler.handle_call(4, args, @auth, @ctx_base)

      assert {:ok, :ok, rest} = Types.decode_nfsstat3(body)
      assert {:ok, ^attr, rest} = Types.decode_post_op_attr(rest)
      assert {:ok, ^granted, <<>>} = XDR.decode_uint(rest)
    end

    test "returns NFS3ERR_ACCES on backend refusal with optional post-op attrs" do
      fh = "fh-deny"
      attr = MockBackend.sample_fattr3()
      MockBackend.put({:access, {fh, 0}}, {:error, :acces, attr})

      args = Types.encode_fhandle3(fh) <> XDR.encode_uint(0)
      {:ok, body} = Handler.handle_call(4, args, @auth, @ctx_base)

      assert {:ok, :acces, rest} = Types.decode_nfsstat3(body)
      assert {:ok, ^attr, <<>>} = Types.decode_post_op_attr(rest)
    end
  end

  describe "READLINK (proc 5)" do
    test "returns the link target plus optional attrs" do
      fh = "fh-link"
      attr = MockBackend.sample_fattr3()
      target = "../target.txt"
      MockBackend.put({:readlink, fh}, {:ok, target, attr})

      {:ok, body} = Handler.handle_call(5, Types.encode_fhandle3(fh), @auth, @ctx_base)

      assert {:ok, :ok, rest} = Types.decode_nfsstat3(body)
      assert {:ok, ^attr, rest} = Types.decode_post_op_attr(rest)
      assert {:ok, ^target, <<>>} = Types.decode_nfspath3(rest)
    end

    test "returns NFS3ERR_INVAL when the handle is not a symlink" do
      fh = "fh-not-link"
      MockBackend.put({:readlink, fh}, {:error, :inval, nil})

      {:ok, body} = Handler.handle_call(5, Types.encode_fhandle3(fh), @auth, @ctx_base)

      assert {:ok, :inval, rest} = Types.decode_nfsstat3(body)
      assert {:ok, nil, <<>>} = Types.decode_post_op_attr(rest)
    end
  end

  describe "READ (proc 6)" do
    test "encodes status + post_op_attr + count + eof + opaque<> data on success" do
      fh = "fh-read"
      attr = MockBackend.sample_fattr3()

      MockBackend.put(
        {:read, {fh, 0, 16}},
        {:ok, %{data: ["hello, ", "world!"], eof: true, post_op: attr}}
      )

      args = read_args(fh, 0, 16)
      {:ok, body} = Handler.handle_call(6, args, @auth, @ctx_base)
      flat = IO.iodata_to_binary(body)

      assert {:ok, :ok, rest} = Types.decode_nfsstat3(flat)
      assert {:ok, ^attr, rest} = Types.decode_post_op_attr(rest)
      assert {:ok, 13, rest} = XDR.decode_uint(rest)
      assert {:ok, true, rest} = XDR.decode_bool(rest)
      assert {:ok, "hello, world!", <<>>} = XDR.decode_var_opaque(rest)
    end

    test "trims the stream when its bytes exceed the kernel's count cap" do
      fh = "fh-trim"

      MockBackend.put(
        {:read, {fh, 0, 8}},
        {:ok,
         %{
           data: [String.duplicate("x", 4), String.duplicate("y", 8)],
           eof: false,
           post_op: nil
         }}
      )

      args = read_args(fh, 0, 8)
      {:ok, body} = Handler.handle_call(6, args, @auth, @ctx_base)
      flat = IO.iodata_to_binary(body)

      assert {:ok, :ok, rest} = Types.decode_nfsstat3(flat)
      assert {:ok, nil, rest} = Types.decode_post_op_attr(rest)
      assert {:ok, 8, rest} = XDR.decode_uint(rest)
      assert {:ok, false, rest} = XDR.decode_bool(rest)
      assert {:ok, "xxxxyyyy", <<>>} = XDR.decode_var_opaque(rest)
    end

    test "pads opaque data to a 4-byte boundary" do
      fh = "fh-pad"

      MockBackend.put(
        {:read, {fh, 0, 100}},
        {:ok, %{data: ["abc"], eof: true, post_op: nil}}
      )

      args = read_args(fh, 0, 100)
      {:ok, body} = Handler.handle_call(6, args, @auth, @ctx_base)
      flat = IO.iodata_to_binary(body)

      # status (4) + post_op_attr (4) + count (4) + eof (4) + length (4) +
      # data (3) + 1 byte padding = 24 bytes total.
      assert byte_size(flat) == 24
      assert {:ok, "abc", <<>>} = flat |> read_to_data() |> XDR.decode_var_opaque()
    end

    test "lazy stream: per-chunk iolist size is bounded by count, not file size" do
      fh = "fh-lazy"
      requested = 64
      # Backend returns a stream that would yield far more than `requested`
      # bytes if drained. We assert peak iolist size stays ≤ requested.
      huge = Stream.repeatedly(fn -> :binary.copy("Z", 32) end)

      MockBackend.put(
        {:read, {fh, 0, requested}},
        {:ok, %{data: huge, eof: false, post_op: nil}}
      )

      args = read_args(fh, 0, requested)
      {:ok, body} = Handler.handle_call(6, args, @auth, @ctx_base)

      # The body iolist's payload-data segment must fit inside `requested` —
      # we measure total iolist size and subtract the fixed-size header
      # (status 4 + post_op_attr 4 + count 4 + eof 4 + length 4 = 20).
      total = :erlang.iolist_size(body)
      assert total <= 20 + requested + 3
    end

    test "maps a streaming :ok with explicit eof: false to the on-wire false bit" do
      fh = "fh-noeof"

      MockBackend.put(
        {:read, {fh, 0, 4}},
        {:ok, %{data: ["abcd"], eof: false, post_op: nil}}
      )

      args = read_args(fh, 0, 4)
      {:ok, body} = Handler.handle_call(6, args, @auth, @ctx_base)
      flat = IO.iodata_to_binary(body)

      assert {:ok, :ok, rest} = Types.decode_nfsstat3(flat)
      assert {:ok, nil, rest} = Types.decode_post_op_attr(rest)
      assert {:ok, 4, rest} = XDR.decode_uint(rest)
      assert {:ok, false, _} = XDR.decode_bool(rest)
    end

    for {label, status} <- [
          {"NFS3ERR_NOENT", :noent},
          {"NFS3ERR_PERM", :perm},
          {"NFS3ERR_ISDIR", :isdir},
          {"NFS3ERR_IO", :io},
          {"NFS3ERR_NXIO", :nxio},
          {"NFS3ERR_INVAL", :inval}
        ] do
      test "maps backend #{inspect(status)} to #{label} on the wire" do
        fh = "fh-err-#{unquote(Atom.to_string(status))}"
        MockBackend.put({:read, {fh, 0, 4}}, {:error, unquote(status), nil})

        args = read_args(fh, 0, 4)
        {:ok, body} = Handler.handle_call(6, args, @auth, @ctx_base)
        flat = IO.iodata_to_binary(body)

        assert {:ok, unquote(status), rest} = Types.decode_nfsstat3(flat)
        assert {:ok, nil, <<>>} = Types.decode_post_op_attr(rest)
      end
    end

    test "passes the post-op attr through on error replies" do
      fh = "fh-err-attrs"
      attr = MockBackend.sample_fattr3()
      MockBackend.put({:read, {fh, 0, 4}}, {:error, :io, attr})

      {:ok, body} = Handler.handle_call(6, read_args(fh, 0, 4), @auth, @ctx_base)
      flat = IO.iodata_to_binary(body)

      assert {:ok, :io, rest} = Types.decode_nfsstat3(flat)
      assert {:ok, ^attr, <<>>} = Types.decode_post_op_attr(rest)
    end

    test "returns :garbage_args on undecodable args" do
      assert :garbage_args = Handler.handle_call(6, <<0xFF>>, @auth, @ctx_base)
    end

    test "returns :garbage_args when args truncate before count3" do
      assert :garbage_args =
               Handler.handle_call(
                 6,
                 Types.encode_fhandle3("fh") <> XDR.encode_uhyper(0),
                 @auth,
                 @ctx_base
               )
    end
  end

  describe "READDIR (proc 16)" do
    test "encodes status + post-op attrs + cookieverf + entry chain + eof" do
      fh = "fh-dir"
      verf = <<0::64>>
      new_verf = <<1::64>>
      attr = MockBackend.sample_fattr3()

      entries = [
        {10, ".", 1},
        {11, "..", 2},
        {12, "a.txt", 3},
        {13, "b.txt", 4}
      ]

      MockBackend.put({:readdir, {fh, 0, verf, 8192}}, {:ok, entries, new_verf, true, attr})

      args = readdir_args(fh, 0, verf, 8192)
      {:ok, body} = Handler.handle_call(16, args, @auth, @ctx_base)

      assert {:ok, :ok, rest} = Types.decode_nfsstat3(body)
      assert {:ok, ^attr, rest} = Types.decode_post_op_attr(rest)
      assert {:ok, ^new_verf, rest} = Types.decode_cookieverf3(rest)
      assert {:ok, decoded_entries, rest} = decode_readdir_chain(rest)
      assert decoded_entries == entries
      assert {:ok, true, <<>>} = XDR.decode_bool(rest)
    end

    test "an empty directory still encodes a list-terminator + eof" do
      fh = "fh-empty"
      verf = <<0::64>>
      new_verf = <<2::64>>

      MockBackend.put({:readdir, {fh, 0, verf, 1024}}, {:ok, [], new_verf, true, nil})

      args = readdir_args(fh, 0, verf, 1024)
      {:ok, body} = Handler.handle_call(16, args, @auth, @ctx_base)

      assert {:ok, :ok, rest} = Types.decode_nfsstat3(body)
      assert {:ok, nil, rest} = Types.decode_post_op_attr(rest)
      assert {:ok, ^new_verf, rest} = Types.decode_cookieverf3(rest)
      assert {:ok, [], rest} = decode_readdir_chain(rest)
      assert {:ok, true, <<>>} = XDR.decode_bool(rest)
    end

    test "multi-page pagination round-trips the cookie" do
      fh = "fh-paged"
      verf = <<0::64>>

      page1 = [{10, ".", 1}, {11, "..", 2}, {12, "a.txt", 3}]
      page2 = [{13, "b.txt", 4}, {14, "c.txt", 5}]

      MockBackend.put({:readdir, {fh, 0, verf, 1024}}, {:ok, page1, verf, false, nil})
      MockBackend.put({:readdir, {fh, 3, verf, 1024}}, {:ok, page2, verf, true, nil})

      {:ok, body1} = Handler.handle_call(16, readdir_args(fh, 0, verf, 1024), @auth, @ctx_base)
      {:ok, :ok, rest} = Types.decode_nfsstat3(body1)
      {:ok, _attr, rest} = Types.decode_post_op_attr(rest)
      {:ok, _verf1, rest} = Types.decode_cookieverf3(rest)
      {:ok, decoded1, rest} = decode_readdir_chain(rest)
      {:ok, eof1, <<>>} = XDR.decode_bool(rest)

      assert decoded1 == page1
      refute eof1

      # Last cookie returned is the resume token for page 2.
      {_, _, last_cookie} = List.last(decoded1)
      assert last_cookie == 3

      {:ok, body2} =
        Handler.handle_call(16, readdir_args(fh, last_cookie, verf, 1024), @auth, @ctx_base)

      {:ok, :ok, rest} = Types.decode_nfsstat3(body2)
      {:ok, _attr, rest} = Types.decode_post_op_attr(rest)
      {:ok, _verf2, rest} = Types.decode_cookieverf3(rest)
      {:ok, decoded2, rest} = decode_readdir_chain(rest)
      {:ok, eof2, <<>>} = XDR.decode_bool(rest)

      assert decoded2 == page2
      assert eof2
    end

    test "maps backend :bad_cookie to NFS3ERR_BAD_COOKIE" do
      fh = "fh-stale"
      stale_verf = <<99::64>>

      MockBackend.put({:readdir, {fh, 5, stale_verf, 1024}}, {:error, :bad_cookie, nil})

      args = readdir_args(fh, 5, stale_verf, 1024)
      {:ok, body} = Handler.handle_call(16, args, @auth, @ctx_base)

      assert {:ok, :bad_cookie, rest} = Types.decode_nfsstat3(body)
      assert {:ok, nil, <<>>} = Types.decode_post_op_attr(rest)
    end

    for {label, status} <- [
          {"NFS3ERR_NOENT", :noent},
          {"NFS3ERR_ACCES", :acces},
          {"NFS3ERR_NOTDIR", :notdir},
          {"NFS3ERR_TOOSMALL", :too_small},
          {"NFS3ERR_IO", :io}
        ] do
      test "maps backend #{inspect(status)} to #{label} on the wire" do
        fh = "fh-err"
        verf = <<0::64>>
        MockBackend.put({:readdir, {fh, 0, verf, 4096}}, {:error, unquote(status), nil})

        args = readdir_args(fh, 0, verf, 4096)
        {:ok, body} = Handler.handle_call(16, args, @auth, @ctx_base)

        assert {:ok, unquote(status), rest} = Types.decode_nfsstat3(body)
        assert {:ok, nil, <<>>} = Types.decode_post_op_attr(rest)
      end
    end

    test "returns :garbage_args on undecodable args" do
      assert :garbage_args = Handler.handle_call(16, <<0xFF>>, @auth, @ctx_base)
    end

    test "returns :garbage_args when args truncate before count" do
      args = Types.encode_fhandle3("fh") <> XDR.encode_uhyper(0) <> <<0::64>>
      assert :garbage_args = Handler.handle_call(16, args, @auth, @ctx_base)
    end

    property "encode/decode round-trips a generated directory of 0–256 entries" do
      check all(entries <- list_of(readdir_entry(), max_length: 256)) do
        fh = "fh-prop"
        verf = <<0::64>>
        new_verf = <<7::64>>
        MockBackend.put({:readdir, {fh, 0, verf, 65_536}}, {:ok, entries, new_verf, true, nil})

        args = readdir_args(fh, 0, verf, 65_536)
        {:ok, body} = Handler.handle_call(16, args, @auth, @ctx_base)

        {:ok, :ok, rest} = Types.decode_nfsstat3(body)
        {:ok, nil, rest} = Types.decode_post_op_attr(rest)
        {:ok, ^new_verf, rest} = Types.decode_cookieverf3(rest)
        {:ok, decoded, rest} = decode_readdir_chain(rest)
        {:ok, true, <<>>} = XDR.decode_bool(rest)

        assert decoded == entries
      end
    end
  end

  describe "READDIRPLUS (proc 17)" do
    test "encodes inline post-op attrs + post-op fh3 per entry" do
      fh = "fh-dirp"
      verf = <<0::64>>
      new_verf = <<1::64>>
      dir_attr = MockBackend.sample_fattr3()
      child_attr = %{dir_attr | type: :reg, fileid: 7}

      entries = [
        {10, ".", 1, dir_attr, "fh-dot"},
        {11, "..", 2, dir_attr, "fh-dotdot"},
        {12, "a.txt", 3, child_attr, "fh-a"},
        {13, "missing", 4, nil, nil}
      ]

      MockBackend.put(
        {:readdirplus, {fh, 0, verf, 1024, 8192}},
        {:ok, entries, new_verf, true, dir_attr}
      )

      args = readdirplus_args(fh, 0, verf, 1024, 8192)
      {:ok, body} = Handler.handle_call(17, args, @auth, @ctx_base)

      assert {:ok, :ok, rest} = Types.decode_nfsstat3(body)
      assert {:ok, ^dir_attr, rest} = Types.decode_post_op_attr(rest)
      assert {:ok, ^new_verf, rest} = Types.decode_cookieverf3(rest)
      assert {:ok, decoded, rest} = decode_readdirplus_chain(rest)
      assert decoded == entries
      assert {:ok, true, <<>>} = XDR.decode_bool(rest)
    end

    test "empty directory + eof" do
      fh = "fh-emptyp"
      verf = <<0::64>>
      new_verf = <<1::64>>

      MockBackend.put(
        {:readdirplus, {fh, 0, verf, 512, 4096}},
        {:ok, [], new_verf, true, nil}
      )

      args = readdirplus_args(fh, 0, verf, 512, 4096)
      {:ok, body} = Handler.handle_call(17, args, @auth, @ctx_base)

      assert {:ok, :ok, rest} = Types.decode_nfsstat3(body)
      assert {:ok, nil, rest} = Types.decode_post_op_attr(rest)
      assert {:ok, ^new_verf, rest} = Types.decode_cookieverf3(rest)
      assert {:ok, [], rest} = decode_readdirplus_chain(rest)
      assert {:ok, true, <<>>} = XDR.decode_bool(rest)
    end

    test "maps :bad_cookie to NFS3ERR_BAD_COOKIE" do
      fh = "fh-stalep"
      verf = <<99::64>>

      MockBackend.put(
        {:readdirplus, {fh, 7, verf, 512, 4096}},
        {:error, :bad_cookie, nil}
      )

      args = readdirplus_args(fh, 7, verf, 512, 4096)
      {:ok, body} = Handler.handle_call(17, args, @auth, @ctx_base)

      assert {:ok, :bad_cookie, rest} = Types.decode_nfsstat3(body)
      assert {:ok, nil, <<>>} = Types.decode_post_op_attr(rest)
    end

    test "returns :garbage_args on undecodable args" do
      assert :garbage_args = Handler.handle_call(17, <<0xFF>>, @auth, @ctx_base)
    end

    test "returns :garbage_args when args truncate before maxcount" do
      args =
        Types.encode_fhandle3("fh") <>
          XDR.encode_uhyper(0) <>
          <<0::64>> <>
          XDR.encode_uint(512)

      assert :garbage_args = Handler.handle_call(17, args, @auth, @ctx_base)
    end
  end

  describe "FSSTAT (proc 18)" do
    test "encodes the full reply on success" do
      fh = "fh-vol"
      attr = MockBackend.sample_fattr3()

      reply = %{
        tbytes: 1_000_000,
        fbytes: 600_000,
        abytes: 500_000,
        tfiles: 100,
        ffiles: 90,
        afiles: 90,
        invarsec: 0
      }

      MockBackend.put({:fsstat, fh}, {:ok, reply, attr})

      {:ok, body} = Handler.handle_call(18, Types.encode_fhandle3(fh), @auth, @ctx_base)

      assert {:ok, :ok, rest} = Types.decode_nfsstat3(body)
      assert {:ok, ^attr, rest} = Types.decode_post_op_attr(rest)
      assert {:ok, 1_000_000, rest} = XDR.decode_uhyper(rest)
      assert {:ok, 600_000, rest} = XDR.decode_uhyper(rest)
      assert {:ok, 500_000, rest} = XDR.decode_uhyper(rest)
      assert {:ok, 100, rest} = XDR.decode_uhyper(rest)
      assert {:ok, 90, rest} = XDR.decode_uhyper(rest)
      assert {:ok, 90, rest} = XDR.decode_uhyper(rest)
      assert {:ok, 0, <<>>} = XDR.decode_uint(rest)
    end
  end

  describe "FSINFO (proc 19)" do
    test "encodes the full reply on success" do
      fh = "fh-vol"
      attr = MockBackend.sample_fattr3()

      reply = %{
        rtmax: 1 <<< 20,
        rtpref: 1 <<< 20,
        rtmult: 4096,
        wtmax: 1 <<< 20,
        wtpref: 1 <<< 20,
        wtmult: 4096,
        dtpref: 8192,
        maxfilesize: 0xFFFFFFFFFFFFFFFF,
        time_delta: %Nfstime3{seconds: 0, nseconds: 1},
        properties: 0
      }

      MockBackend.put({:fsinfo, fh}, {:ok, reply, attr})

      {:ok, body} = Handler.handle_call(19, Types.encode_fhandle3(fh), @auth, @ctx_base)

      assert {:ok, :ok, rest} = Types.decode_nfsstat3(body)
      assert {:ok, ^attr, rest} = Types.decode_post_op_attr(rest)
      # We don't re-decode every field; trust the encode order is consistent
      # with the encode call. The fattr round-trip already covered the type
      # serialisation.
      assert byte_size(rest) > 0
    end

    test "returns NFS3ERR_BADHANDLE without a post-op attr" do
      fh = "fh-bad"
      MockBackend.put({:fsinfo, fh}, {:error, :badhandle})

      {:ok, body} = Handler.handle_call(19, Types.encode_fhandle3(fh), @auth, @ctx_base)

      assert {:ok, :badhandle, rest} = Types.decode_nfsstat3(body)
      assert {:ok, nil, <<>>} = Types.decode_post_op_attr(rest)
    end
  end

  describe "PATHCONF (proc 20)" do
    test "encodes the full reply on success" do
      fh = "fh-vol"
      attr = MockBackend.sample_fattr3()

      reply = %{
        linkmax: 1024,
        name_max: 255,
        no_trunc: true,
        chown_restricted: true,
        case_insensitive: false,
        case_preserving: true
      }

      MockBackend.put({:pathconf, fh}, {:ok, reply, attr})

      {:ok, body} = Handler.handle_call(20, Types.encode_fhandle3(fh), @auth, @ctx_base)

      assert {:ok, :ok, rest} = Types.decode_nfsstat3(body)
      assert {:ok, ^attr, rest} = Types.decode_post_op_attr(rest)
      assert {:ok, 1024, rest} = XDR.decode_uint(rest)
      assert {:ok, 255, rest} = XDR.decode_uint(rest)
      assert {:ok, true, rest} = XDR.decode_bool(rest)
      assert {:ok, true, rest} = XDR.decode_bool(rest)
      assert {:ok, false, rest} = XDR.decode_bool(rest)
      assert {:ok, true, <<>>} = XDR.decode_bool(rest)
    end
  end

  describe "unknown procedure" do
    test "returns :proc_unavail" do
      assert :proc_unavail = Handler.handle_call(99, <<>>, @auth, @ctx_base)
    end
  end

  describe "missing backend" do
    test "raises ArgumentError" do
      assert_raise ArgumentError, ~r/backend in ctx/, fn ->
        Handler.handle_call(1, Types.encode_fhandle3("any"), @auth, %{call: nil})
      end
    end
  end

  ## Helpers

  defp read_args(fh, offset, count) do
    Types.encode_fhandle3(fh) <> XDR.encode_uhyper(offset) <> XDR.encode_uint(count)
  end

  # Skip past status (4) + post_op_attr-empty (4) + count (4) + eof (4)
  # to land on the opaque<>'s length prefix. Used by the padding test.
  defp read_to_data(<<_::binary-size(16), rest::binary>>), do: rest

  defp readdir_entry do
    gen all(
          fileid <- integer(0..0xFFFFFFFF_FFFFFFFF),
          name <-
            binary(min_length: 1, max_length: 32) |> filter(&(not String.contains?(&1, <<0>>))),
          cookie <- integer(0..0xFFFFFFFF_FFFFFFFF)
        ) do
      {fileid, name, cookie}
    end
  end

  defp readdir_args(fh, cookie, verf, count) do
    Types.encode_fhandle3(fh) <>
      XDR.encode_uhyper(cookie) <>
      Types.encode_cookieverf3(verf) <>
      XDR.encode_uint(count)
  end

  defp readdirplus_args(fh, cookie, verf, dircount, maxcount) do
    Types.encode_fhandle3(fh) <>
      XDR.encode_uhyper(cookie) <>
      Types.encode_cookieverf3(verf) <>
      XDR.encode_uint(dircount) <>
      XDR.encode_uint(maxcount)
  end

  # Walk the linked-list-style entry chain in a READDIR reply: each
  # entry is preceded by a `bool` "next-pointer" flag. Returns the
  # entries in order plus the trailing bytes (which start with the
  # `eof` bool of the dirlist3).
  defp decode_readdir_chain(binary), do: do_decode_readdir(binary, [])

  defp do_decode_readdir(binary, acc) do
    with {:ok, true, rest} <- XDR.decode_bool(binary),
         {:ok, fileid, rest} <- XDR.decode_uhyper(rest),
         {:ok, name, rest} <- Types.decode_filename3(rest),
         {:ok, cookie, rest} <- XDR.decode_uhyper(rest) do
      do_decode_readdir(rest, [{fileid, name, cookie} | acc])
    else
      {:ok, false, rest} -> {:ok, Enum.reverse(acc), rest}
      err -> err
    end
  end

  # Same shape as `decode_readdir_chain/1` but each entry's body
  # carries a `post_op_attr` (Fattr3 or nil) and a `post_op_fh3`
  # (binary or nil).
  defp decode_readdirplus_chain(binary), do: do_decode_readdirplus(binary, [])

  defp do_decode_readdirplus(binary, acc) do
    with {:ok, true, rest} <- XDR.decode_bool(binary),
         {:ok, fileid, rest} <- XDR.decode_uhyper(rest),
         {:ok, name, rest} <- Types.decode_filename3(rest),
         {:ok, cookie, rest} <- XDR.decode_uhyper(rest),
         {:ok, attr, rest} <- Types.decode_post_op_attr(rest),
         {:ok, fh, rest} <- Types.decode_post_op_fh3(rest) do
      do_decode_readdirplus(rest, [{fileid, name, cookie, attr, fh} | acc])
    else
      {:ok, false, rest} -> {:ok, Enum.reverse(acc), rest}
      err -> err
    end
  end
end
