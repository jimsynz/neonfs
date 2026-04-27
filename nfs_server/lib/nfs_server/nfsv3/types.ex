defmodule NFSServer.NFSv3.Types do
  @moduledoc """
  XDR types for NFS v3 — [RFC 1813 §2.5](https://www.rfc-editor.org/rfc/rfc1813#section-2.5).

  Shape constants:

      NFS3_FHSIZE       = 64    # max v3 file-handle bytes
      NFS3_COOKIEVERFSIZE = 8   # cookie verifier
      NFS3_CREATEVERFSIZE = 8   # create verifier
      NFS3_WRITEVERFSIZE  = 8   # write verifier

  Each XDR primitive in §2.5 has a sibling `encode_*/1` /
  `decode_*/1` pair. Composite types follow the same shape and
  destructure into the structs declared in this module
  (`Fattr3`, `WccAttr`, `WccData`, `Specdata3`, `Nfstime3`,
  `Sattr3`).

  Procedure-specific request / reply types
  (e.g. `LOOKUP3args` / `LOOKUP3res`) are intentionally *not* defined
  here; each handler procedure builds its own out of these primitives.
  Keeping the type module focused on §2.5 keeps it reviewable
  independently of the procedure handlers.
  """

  alias NFSServer.XDR

  @nfs3_fhsize 64
  @nfs3_cookieverfsize 8
  @nfs3_createverfsize 8
  @nfs3_writeverfsize 8

  # ——— Primitive aliases ————————————————————————————————————————

  @typedoc "uint32; mode bits."
  @type mode3 :: non_neg_integer()

  @typedoc "uint32; user id."
  @type uid3 :: non_neg_integer()

  @typedoc "uint32; group id."
  @type gid3 :: non_neg_integer()

  @typedoc "uint64; file size in bytes."
  @type size3 :: non_neg_integer()

  @typedoc "uint64; byte offset within a file."
  @type offset3 :: non_neg_integer()

  @typedoc "uint32; byte count for a request/response."
  @type count3 :: non_neg_integer()

  @typedoc "uint64; persistent inode-style id."
  @type fileid3 :: non_neg_integer()

  @typedoc "uint64; opaque READDIR cursor."
  @type nfs_cookie3 :: non_neg_integer()

  @typedoc "Fixed 8-byte READDIR / READDIRPLUS cookie verifier."
  @type cookieverf3 :: <<_::64>>

  @typedoc "Fixed 8-byte CREATE verifier (used by EXCLUSIVE creates)."
  @type createverf3 :: <<_::64>>

  @typedoc "Fixed 8-byte WRITE verifier (used by COMMIT)."
  @type writeverf3 :: <<_::64>>

  @typedoc "Variable-length file handle, ≤ 64 bytes."
  @type fhandle3 :: binary()

  @typedoc "UTF-8-ish file name (NFSv3 doesn't constrain encoding)."
  @type filename3 :: binary()

  @typedoc "UTF-8-ish symlink target."
  @type nfspath3 :: binary()

  # ——— Constants ————————————————————————————————————————————————

  @doc "Max v3 file-handle byte size."
  @spec nfs3_fhsize() :: 64
  def nfs3_fhsize, do: @nfs3_fhsize

  @doc "READDIR / READDIRPLUS cookie verifier size."
  @spec nfs3_cookieverfsize() :: 8
  def nfs3_cookieverfsize, do: @nfs3_cookieverfsize

  @doc "CREATE verifier size."
  @spec nfs3_createverfsize() :: 8
  def nfs3_createverfsize, do: @nfs3_createverfsize

  @doc "WRITE verifier size."
  @spec nfs3_writeverfsize() :: 8
  def nfs3_writeverfsize, do: @nfs3_writeverfsize

  # ——— nfsstat3 ————————————————————————————————————————————————

  @typedoc "RFC 1813 §2.6 status codes. `:ok` is success, the rest are POSIX-flavoured errnos."
  @type nfsstat3 ::
          :ok
          | :perm
          | :noent
          | :io
          | :nxio
          | :acces
          | :exist
          | :xdev
          | :nodev
          | :notdir
          | :isdir
          | :inval
          | :fbig
          | :nospc
          | :rofs
          | :mlink
          | :nametoolong
          | :notempty
          | :dquot
          | :stale
          | :remote
          | :badhandle
          | :not_sync
          | :bad_cookie
          | :notsupp
          | :too_small
          | :server_fault
          | :bad_type
          | :jukebox

  @stat_codes %{
    ok: 0,
    perm: 1,
    noent: 2,
    io: 5,
    nxio: 6,
    acces: 13,
    exist: 17,
    xdev: 18,
    nodev: 19,
    notdir: 20,
    isdir: 21,
    inval: 22,
    fbig: 27,
    nospc: 28,
    rofs: 30,
    mlink: 31,
    nametoolong: 63,
    notempty: 66,
    dquot: 69,
    stale: 70,
    remote: 71,
    badhandle: 10_001,
    not_sync: 10_002,
    bad_cookie: 10_003,
    notsupp: 10_004,
    too_small: 10_005,
    server_fault: 10_006,
    bad_type: 10_007,
    jukebox: 10_008
  }

  @code_to_stat Map.new(@stat_codes, fn {atom, code} -> {code, atom} end)

  @doc "Encode an `nfsstat3` atom as its 32-bit wire integer."
  @spec encode_nfsstat3(nfsstat3()) :: binary()
  def encode_nfsstat3(stat) when is_atom(stat) do
    code = Map.fetch!(@stat_codes, stat)
    XDR.encode_uint(code)
  end

  @doc """
  Decode a 32-bit `nfsstat3` code back to its atom. Unknown codes
  surface as `{:unknown, n}` so handlers can report it without
  crashing.
  """
  @spec decode_nfsstat3(binary()) ::
          {:ok, nfsstat3() | {:unknown, non_neg_integer()}, binary()} | {:error, term()}
  def decode_nfsstat3(binary) do
    with {:ok, code, rest} <- XDR.decode_uint(binary) do
      atom = Map.get(@code_to_stat, code, {:unknown, code})
      {:ok, atom, rest}
    end
  end

  # ——— ftype3 ——————————————————————————————————————————————————

  @typedoc "RFC 1813 §2.5 file types."
  @type ftype3 :: :reg | :dir | :blk | :chr | :lnk | :sock | :fifo

  @ftype_codes %{
    reg: 1,
    dir: 2,
    blk: 3,
    chr: 4,
    lnk: 5,
    sock: 6,
    fifo: 7
  }

  @code_to_ftype Map.new(@ftype_codes, fn {atom, code} -> {code, atom} end)

  @doc "Encode an `ftype3` atom as its 32-bit wire enum."
  @spec encode_ftype3(ftype3()) :: binary()
  def encode_ftype3(t) when is_atom(t) do
    XDR.encode_enum(Map.fetch!(@ftype_codes, t))
  end

  @doc "Decode a 32-bit `ftype3` enum to its atom."
  @spec decode_ftype3(binary()) :: {:ok, ftype3(), binary()} | {:error, term()}
  def decode_ftype3(binary) do
    with {:ok, code, rest} <- XDR.decode_enum(binary) do
      case Map.fetch(@code_to_ftype, code) do
        {:ok, atom} -> {:ok, atom, rest}
        :error -> {:error, {:bad_ftype3, code}}
      end
    end
  end

  # ——— Bounded primitives ——————————————————————————————————————

  @doc "Encode `fhandle3` (variable opaque ≤ 64 bytes)."
  @spec encode_fhandle3(fhandle3()) :: binary()
  def encode_fhandle3(fh) when is_binary(fh) and byte_size(fh) <= @nfs3_fhsize,
    do: XDR.encode_var_opaque(fh)

  @doc "Decode `fhandle3`."
  @spec decode_fhandle3(binary()) :: {:ok, fhandle3(), binary()} | {:error, term()}
  def decode_fhandle3(binary) do
    with {:ok, fh, rest} <- XDR.decode_var_opaque(binary),
         :ok <- check_max_size(fh, @nfs3_fhsize, :fhandle_too_long) do
      {:ok, fh, rest}
    end
  end

  @doc "Encode `filename3` (a non-empty XDR string)."
  @spec encode_filename3(filename3()) :: binary()
  def encode_filename3(name) when is_binary(name), do: XDR.encode_string(name)

  @doc "Decode `filename3`."
  @spec decode_filename3(binary()) :: {:ok, filename3(), binary()} | {:error, term()}
  def decode_filename3(binary), do: XDR.decode_string(binary)

  @doc "Encode `nfspath3` (an XDR string)."
  @spec encode_nfspath3(nfspath3()) :: binary()
  def encode_nfspath3(path) when is_binary(path), do: XDR.encode_string(path)

  @doc "Decode `nfspath3`."
  @spec decode_nfspath3(binary()) :: {:ok, nfspath3(), binary()} | {:error, term()}
  def decode_nfspath3(binary), do: XDR.decode_string(binary)

  @doc "Encode an 8-byte cookie verifier."
  @spec encode_cookieverf3(cookieverf3()) :: binary()
  def encode_cookieverf3(verf) when is_binary(verf) and byte_size(verf) == 8,
    do: XDR.encode_fixed_opaque(verf, 8)

  @doc "Decode an 8-byte cookie verifier."
  @spec decode_cookieverf3(binary()) :: {:ok, cookieverf3(), binary()} | {:error, term()}
  def decode_cookieverf3(binary), do: XDR.decode_fixed_opaque(binary, 8)

  @doc "Encode an 8-byte CREATE verifier."
  @spec encode_createverf3(createverf3()) :: binary()
  def encode_createverf3(verf) when is_binary(verf) and byte_size(verf) == 8,
    do: XDR.encode_fixed_opaque(verf, 8)

  @doc "Decode an 8-byte CREATE verifier."
  @spec decode_createverf3(binary()) :: {:ok, createverf3(), binary()} | {:error, term()}
  def decode_createverf3(binary), do: XDR.decode_fixed_opaque(binary, 8)

  @doc "Encode an 8-byte WRITE verifier."
  @spec encode_writeverf3(writeverf3()) :: binary()
  def encode_writeverf3(verf) when is_binary(verf) and byte_size(verf) == 8,
    do: XDR.encode_fixed_opaque(verf, 8)

  @doc "Decode an 8-byte WRITE verifier."
  @spec decode_writeverf3(binary()) :: {:ok, writeverf3(), binary()} | {:error, term()}
  def decode_writeverf3(binary), do: XDR.decode_fixed_opaque(binary, 8)

  # ——— specdata3 ————————————————————————————————————————————————

  defmodule Specdata3 do
    @moduledoc "Major / minor device numbers. RFC 1813 §2.5."
    defstruct specdata1: 0, specdata2: 0

    @type t :: %__MODULE__{
            specdata1: non_neg_integer(),
            specdata2: non_neg_integer()
          }
  end

  @doc "Encode `specdata3`."
  @spec encode_specdata3(Specdata3.t()) :: binary()
  def encode_specdata3(%Specdata3{specdata1: s1, specdata2: s2}) do
    XDR.encode_uint(s1) <> XDR.encode_uint(s2)
  end

  @doc "Decode `specdata3`."
  @spec decode_specdata3(binary()) :: {:ok, Specdata3.t(), binary()} | {:error, term()}
  def decode_specdata3(binary) do
    with {:ok, s1, rest} <- XDR.decode_uint(binary),
         {:ok, s2, rest2} <- XDR.decode_uint(rest) do
      {:ok, %Specdata3{specdata1: s1, specdata2: s2}, rest2}
    end
  end

  # ——— nfstime3 ————————————————————————————————————————————————

  defmodule Nfstime3 do
    @moduledoc """
    Wall-clock time (seconds + nanoseconds). RFC 1813 §2.5.

    NFS expects POSIX `ctime` semantics for `fattr3.ctime` — that
    maps to NeonFS's `FileMeta.changed_at`, **not** `created_at`.
    """
    defstruct seconds: 0, nseconds: 0

    @type t :: %__MODULE__{
            seconds: non_neg_integer(),
            nseconds: non_neg_integer()
          }
  end

  @doc "Encode `nfstime3`."
  @spec encode_nfstime3(Nfstime3.t()) :: binary()
  def encode_nfstime3(%Nfstime3{seconds: s, nseconds: n}) do
    XDR.encode_uint(s) <> XDR.encode_uint(n)
  end

  @doc "Decode `nfstime3`."
  @spec decode_nfstime3(binary()) :: {:ok, Nfstime3.t(), binary()} | {:error, term()}
  def decode_nfstime3(binary) do
    with {:ok, s, rest} <- XDR.decode_uint(binary),
         {:ok, n, rest2} <- XDR.decode_uint(rest) do
      {:ok, %Nfstime3{seconds: s, nseconds: n}, rest2}
    end
  end

  # ——— fattr3 ————————————————————————————————————————————————

  defmodule Fattr3 do
    @moduledoc "Full file attributes. RFC 1813 §2.5."
    defstruct [
      :type,
      mode: 0,
      nlink: 1,
      uid: 0,
      gid: 0,
      size: 0,
      used: 0,
      rdev: %NFSServer.NFSv3.Types.Specdata3{},
      fsid: 0,
      fileid: 0,
      atime: %NFSServer.NFSv3.Types.Nfstime3{},
      mtime: %NFSServer.NFSv3.Types.Nfstime3{},
      ctime: %NFSServer.NFSv3.Types.Nfstime3{}
    ]

    @type t :: %__MODULE__{
            type: NFSServer.NFSv3.Types.ftype3() | nil,
            mode: NFSServer.NFSv3.Types.mode3(),
            nlink: non_neg_integer(),
            uid: NFSServer.NFSv3.Types.uid3(),
            gid: NFSServer.NFSv3.Types.gid3(),
            size: NFSServer.NFSv3.Types.size3(),
            used: non_neg_integer(),
            rdev: NFSServer.NFSv3.Types.Specdata3.t(),
            fsid: non_neg_integer(),
            fileid: NFSServer.NFSv3.Types.fileid3(),
            atime: NFSServer.NFSv3.Types.Nfstime3.t(),
            mtime: NFSServer.NFSv3.Types.Nfstime3.t(),
            ctime: NFSServer.NFSv3.Types.Nfstime3.t()
          }
  end

  @doc "Encode `fattr3`."
  @spec encode_fattr3(Fattr3.t()) :: binary()
  def encode_fattr3(%Fattr3{} = a) do
    encode_ftype3(a.type) <>
      XDR.encode_uint(a.mode) <>
      XDR.encode_uint(a.nlink) <>
      XDR.encode_uint(a.uid) <>
      XDR.encode_uint(a.gid) <>
      XDR.encode_uhyper(a.size) <>
      XDR.encode_uhyper(a.used) <>
      encode_specdata3(a.rdev) <>
      XDR.encode_uhyper(a.fsid) <>
      XDR.encode_uhyper(a.fileid) <>
      encode_nfstime3(a.atime) <>
      encode_nfstime3(a.mtime) <>
      encode_nfstime3(a.ctime)
  end

  @doc "Decode `fattr3`."
  @spec decode_fattr3(binary()) :: {:ok, Fattr3.t(), binary()} | {:error, term()}
  def decode_fattr3(binary) do
    with {:ok, type, rest} <- decode_ftype3(binary),
         {:ok, mode, rest} <- XDR.decode_uint(rest),
         {:ok, nlink, rest} <- XDR.decode_uint(rest),
         {:ok, uid, rest} <- XDR.decode_uint(rest),
         {:ok, gid, rest} <- XDR.decode_uint(rest),
         {:ok, size, rest} <- XDR.decode_uhyper(rest),
         {:ok, used, rest} <- XDR.decode_uhyper(rest),
         {:ok, rdev, rest} <- decode_specdata3(rest),
         {:ok, fsid, rest} <- XDR.decode_uhyper(rest),
         {:ok, fileid, rest} <- XDR.decode_uhyper(rest),
         {:ok, atime, rest} <- decode_nfstime3(rest),
         {:ok, mtime, rest} <- decode_nfstime3(rest),
         {:ok, ctime, rest} <- decode_nfstime3(rest) do
      {:ok,
       %Fattr3{
         type: type,
         mode: mode,
         nlink: nlink,
         uid: uid,
         gid: gid,
         size: size,
         used: used,
         rdev: rdev,
         fsid: fsid,
         fileid: fileid,
         atime: atime,
         mtime: mtime,
         ctime: ctime
       }, rest}
    end
  end

  # ——— post_op_attr / post_op_fh3 ——————————————————————————————

  @doc """
  Encode `post_op_attr` — `nil` means "no attrs available", otherwise
  the `Fattr3` is encoded prefixed with a TRUE flag.
  """
  @spec encode_post_op_attr(Fattr3.t() | nil) :: binary()
  def encode_post_op_attr(nil), do: XDR.encode_bool(false)
  def encode_post_op_attr(%Fattr3{} = a), do: XDR.encode_bool(true) <> encode_fattr3(a)

  @doc "Decode `post_op_attr`."
  @spec decode_post_op_attr(binary()) :: {:ok, Fattr3.t() | nil, binary()} | {:error, term()}
  def decode_post_op_attr(binary) do
    XDR.decode_optional(binary, &decode_fattr3/1)
  end

  @doc """
  Encode `post_op_fh3` — `nil` means "no handle available", otherwise
  the fhandle3 is encoded prefixed with a TRUE flag.
  """
  @spec encode_post_op_fh3(fhandle3() | nil) :: binary()
  def encode_post_op_fh3(nil), do: XDR.encode_bool(false)

  def encode_post_op_fh3(fh) when is_binary(fh),
    do: XDR.encode_bool(true) <> encode_fhandle3(fh)

  @doc "Decode `post_op_fh3`."
  @spec decode_post_op_fh3(binary()) :: {:ok, fhandle3() | nil, binary()} | {:error, term()}
  def decode_post_op_fh3(binary) do
    XDR.decode_optional(binary, &decode_fhandle3/1)
  end

  # ——— wcc_attr / wcc_data ————————————————————————————————————

  defmodule WccAttr do
    @moduledoc "Subset of attributes used by the weak cache consistency check. RFC 1813 §2.6."
    defstruct size: 0,
              mtime: %NFSServer.NFSv3.Types.Nfstime3{},
              ctime: %NFSServer.NFSv3.Types.Nfstime3{}

    @type t :: %__MODULE__{
            size: NFSServer.NFSv3.Types.size3(),
            mtime: NFSServer.NFSv3.Types.Nfstime3.t(),
            ctime: NFSServer.NFSv3.Types.Nfstime3.t()
          }
  end

  @doc "Encode a `wcc_attr` (size + mtime + ctime)."
  @spec encode_wcc_attr(WccAttr.t()) :: binary()
  def encode_wcc_attr(%WccAttr{} = w) do
    XDR.encode_uhyper(w.size) <>
      encode_nfstime3(w.mtime) <>
      encode_nfstime3(w.ctime)
  end

  @doc "Decode a `wcc_attr`."
  @spec decode_wcc_attr(binary()) :: {:ok, WccAttr.t(), binary()} | {:error, term()}
  def decode_wcc_attr(binary) do
    with {:ok, size, rest} <- XDR.decode_uhyper(binary),
         {:ok, mtime, rest} <- decode_nfstime3(rest),
         {:ok, ctime, rest} <- decode_nfstime3(rest) do
      {:ok, %WccAttr{size: size, mtime: mtime, ctime: ctime}, rest}
    end
  end

  @doc """
  Encode `pre_op_attr` (the optional `wcc_attr` half of a `wcc_data`).
  `nil` means no pre-op attrs were captured.
  """
  @spec encode_pre_op_attr(WccAttr.t() | nil) :: binary()
  def encode_pre_op_attr(nil), do: XDR.encode_bool(false)
  def encode_pre_op_attr(%WccAttr{} = w), do: XDR.encode_bool(true) <> encode_wcc_attr(w)

  @doc "Decode `pre_op_attr`."
  @spec decode_pre_op_attr(binary()) :: {:ok, WccAttr.t() | nil, binary()} | {:error, term()}
  def decode_pre_op_attr(binary) do
    XDR.decode_optional(binary, &decode_wcc_attr/1)
  end

  defmodule WccData do
    @moduledoc "Pre+post op attributes for a directory/file modified by the operation. RFC 1813 §2.6."
    defstruct before: nil, after: nil

    @type t :: %__MODULE__{
            before: NFSServer.NFSv3.Types.WccAttr.t() | nil,
            after: NFSServer.NFSv3.Types.Fattr3.t() | nil
          }
  end

  @doc "Encode `wcc_data` (`pre_op_attr` + `post_op_attr`)."
  @spec encode_wcc_data(WccData.t()) :: binary()
  def encode_wcc_data(%WccData{before: b, after: a}) do
    encode_pre_op_attr(b) <> encode_post_op_attr(a)
  end

  @doc "Decode `wcc_data`."
  @spec decode_wcc_data(binary()) :: {:ok, WccData.t(), binary()} | {:error, term()}
  def decode_wcc_data(binary) do
    with {:ok, before, rest} <- decode_pre_op_attr(binary),
         {:ok, after_, rest} <- decode_post_op_attr(rest) do
      {:ok, %WccData{before: before, after: after_}, rest}
    end
  end

  # ——— sattr3 ————————————————————————————————————————————————

  defmodule Sattr3 do
    @moduledoc """
    Settable attributes for SETATTR / CREATE / MKDIR. RFC 1813 §2.5.

    Each field is optional: `nil` means "leave unchanged". `:set_to_server_time`
    on `atime` / `mtime` requests the server's current time. `:set_to_client_time`
    pairs with the explicit `Nfstime3` on the wire — if you need it, pass a
    `{:client, %Nfstime3{...}}` tuple.
    """
    defstruct mode: nil, uid: nil, gid: nil, size: nil, atime: nil, mtime: nil

    @type time_set ::
            nil
            | :set_to_server_time
            | {:client, NFSServer.NFSv3.Types.Nfstime3.t()}

    @type t :: %__MODULE__{
            mode: NFSServer.NFSv3.Types.mode3() | nil,
            uid: NFSServer.NFSv3.Types.uid3() | nil,
            gid: NFSServer.NFSv3.Types.gid3() | nil,
            size: NFSServer.NFSv3.Types.size3() | nil,
            atime: time_set(),
            mtime: time_set()
          }
  end

  @doc "Encode a `sattr3` struct."
  @spec encode_sattr3(Sattr3.t()) :: binary()
  def encode_sattr3(%Sattr3{} = s) do
    XDR.encode_optional(s.mode, &XDR.encode_uint/1) <>
      XDR.encode_optional(s.uid, &XDR.encode_uint/1) <>
      XDR.encode_optional(s.gid, &XDR.encode_uint/1) <>
      XDR.encode_optional(s.size, &XDR.encode_uhyper/1) <>
      encode_time_set(s.atime) <>
      encode_time_set(s.mtime)
  end

  @doc "Decode a `sattr3` struct."
  @spec decode_sattr3(binary()) :: {:ok, Sattr3.t(), binary()} | {:error, term()}
  def decode_sattr3(binary) do
    with {:ok, mode, rest} <- XDR.decode_optional(binary, &XDR.decode_uint/1),
         {:ok, uid, rest} <- XDR.decode_optional(rest, &XDR.decode_uint/1),
         {:ok, gid, rest} <- XDR.decode_optional(rest, &XDR.decode_uint/1),
         {:ok, size, rest} <- XDR.decode_optional(rest, &XDR.decode_uhyper/1),
         {:ok, atime, rest} <- decode_time_set(rest),
         {:ok, mtime, rest} <- decode_time_set(rest) do
      {:ok, %Sattr3{mode: mode, uid: uid, gid: gid, size: size, atime: atime, mtime: mtime}, rest}
    end
  end

  # ——— stable_how ————————————————————————————————————————————

  @typedoc """
  WRITE / COMMIT stability hints from RFC 1813 §3.3.7. Determines
  whether the server may lose the write before COMMIT.

    * `:unstable` — volatile cache, client must COMMIT.
    * `:data_sync` — data persistent, metadata may not be.
    * `:file_sync` — data + metadata persistent.
  """
  @type stable_how :: :unstable | :data_sync | :file_sync

  @doc "Encode a `stable_how` atom to its wire integer."
  @spec encode_stable_how(stable_how()) :: binary()
  def encode_stable_how(:unstable), do: XDR.encode_uint(0)
  def encode_stable_how(:data_sync), do: XDR.encode_uint(1)
  def encode_stable_how(:file_sync), do: XDR.encode_uint(2)

  @doc "Decode a `stable_how` from the wire integer."
  @spec decode_stable_how(binary()) :: {:ok, stable_how(), binary()} | {:error, term()}
  def decode_stable_how(binary) do
    with {:ok, n, rest} <- XDR.decode_uint(binary) do
      case n do
        0 -> {:ok, :unstable, rest}
        1 -> {:ok, :data_sync, rest}
        2 -> {:ok, :file_sync, rest}
        other -> {:error, {:bad_stable_how, other}}
      end
    end
  end

  # ——— createhow3 ————————————————————————————————————————————

  @typedoc """
  Discriminated union from RFC 1813 §3.3.8 CREATE arguments.

    * `{:unchecked, %Sattr3{}}` — overwrite if exists.
    * `{:guarded, %Sattr3{}}` — fail with `NFS3ERR_EXIST` if exists.
    * `{:exclusive, createverf3}` — atomic create-if-not-exists keyed
      by the 8-byte verifier.
  """
  @type createhow3 ::
          {:unchecked, Sattr3.t()}
          | {:guarded, Sattr3.t()}
          | {:exclusive, createverf3()}

  @doc "Encode a `createhow3` discriminated union."
  @spec encode_createhow3(createhow3()) :: binary()
  def encode_createhow3({:unchecked, %Sattr3{} = s}),
    do: XDR.encode_uint(0) <> encode_sattr3(s)

  def encode_createhow3({:guarded, %Sattr3{} = s}),
    do: XDR.encode_uint(1) <> encode_sattr3(s)

  def encode_createhow3({:exclusive, verf}) when is_binary(verf) and byte_size(verf) == 8,
    do: XDR.encode_uint(2) <> encode_createverf3(verf)

  @doc "Decode a `createhow3` discriminated union."
  @spec decode_createhow3(binary()) :: {:ok, createhow3(), binary()} | {:error, term()}
  def decode_createhow3(binary) do
    with {:ok, mode, rest} <- XDR.decode_uint(binary) do
      case mode do
        0 -> wrap_create_attrs(rest, :unchecked)
        1 -> wrap_create_attrs(rest, :guarded)
        2 -> wrap_create_verf(rest)
        n -> {:error, {:bad_createmode, n}}
      end
    end
  end

  defp wrap_create_attrs(binary, tag) do
    with {:ok, sattr, rest} <- decode_sattr3(binary) do
      {:ok, {tag, sattr}, rest}
    end
  end

  defp wrap_create_verf(binary) do
    with {:ok, verf, rest} <- decode_createverf3(binary) do
      {:ok, {:exclusive, verf}, rest}
    end
  end

  # ——— diropargs3 ————————————————————————————————————————————

  @typedoc "Directory + file-name pair for LOOKUP / CREATE / etc."
  @type diropargs3 :: {fhandle3(), filename3()}

  @doc "Encode a `diropargs3` (dir fhandle + name)."
  @spec encode_diropargs3(diropargs3()) :: binary()
  def encode_diropargs3({fh, name}) do
    encode_fhandle3(fh) <> encode_filename3(name)
  end

  @doc "Decode a `diropargs3`."
  @spec decode_diropargs3(binary()) :: {:ok, diropargs3(), binary()} | {:error, term()}
  def decode_diropargs3(binary) do
    with {:ok, fh, rest} <- decode_fhandle3(binary),
         {:ok, name, rest} <- decode_filename3(rest) do
      {:ok, {fh, name}, rest}
    end
  end

  # ——— Internal ————————————————————————————————————————————————

  # `time_set` is a discriminated union (RFC 1813 §3.3.2):
  #   DONT_CHANGE       (0) → no body
  #   SET_TO_SERVER_TIME(1) → no body
  #   SET_TO_CLIENT_TIME(2) → nfstime3
  defp encode_time_set(nil), do: XDR.encode_uint(0)
  defp encode_time_set(:set_to_server_time), do: XDR.encode_uint(1)

  defp encode_time_set({:client, %Nfstime3{} = t}),
    do: XDR.encode_uint(2) <> encode_nfstime3(t)

  defp decode_time_set(binary) do
    with {:ok, disc, rest} <- XDR.decode_uint(binary) do
      case disc do
        0 -> {:ok, nil, rest}
        1 -> {:ok, :set_to_server_time, rest}
        2 -> wrap_client_time(rest)
        n -> {:error, {:bad_time_set, n}}
      end
    end
  end

  defp wrap_client_time(binary) do
    with {:ok, t, rest} <- decode_nfstime3(binary) do
      {:ok, {:client, t}, rest}
    end
  end

  defp check_max_size(value, max, _err) when byte_size(value) <= max, do: :ok
  defp check_max_size(_value, _max, err), do: {:error, err}
end
