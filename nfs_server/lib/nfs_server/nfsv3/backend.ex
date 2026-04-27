defmodule NFSServer.NFSv3.Backend do
  @moduledoc """
  Behaviour for the filesystem layer the NFSv3 handler delegates to.

  Keeps `nfs_server` decoupled from any particular backing store —
  the same handler can be wired to a NeonFS volume, an in-memory test
  fixture, a posix filesystem mount, etc. The procedure handler
  itself is purely XDR-decode → callback → XDR-encode; every
  semantic decision lives in the backend.

  ## Callback shape

  Every callback returns `{:ok, ...} | {:error, nfsstat3()}`. The
  handler maps the status atom to the wire integer. Where the RFC
  expects a `post_op_attr` even on failure (most procedures), the
  handler attaches `nil` if the backend doesn't supply one, or the
  backend can return `{:error, nfsstat3, post_op_attr}` for a richer
  error reply (see individual callbacks).

  ## Mutation callbacks land incrementally

  Read-path callbacks landed in #529. Mutation procedures (#285)
  ship as separate slices and extend this behaviour one at a time —
  starting with `c:setattr/5` (#621). Subsequent slices add CREATE,
  MKDIR, REMOVE, RENAME, WRITE, COMMIT, etc.

  ## ctx

  The `ctx` map is the RPC context the dispatcher hands to the
  handler — `%{call: NFSServer.RPC.Message.Call.t()}` plus any
  keys downstream code attaches (auth credential, peer address,
  etc.). Treat it as opaque; pull what you need by key.
  """

  alias NFSServer.NFSv3.Types
  alias NFSServer.RPC.Auth

  @typedoc "Opaque context — the handler passes the RPC ctx through."
  @type ctx :: map()

  @typedoc """
  An entry returned by `c:readdir/6` — `{fileid, name, cookie}`.
  Cookies are opaque to the handler; the backend chooses how they
  encode position.
  """
  @type readdir_entry :: {Types.fileid3(), Types.filename3(), Types.nfs_cookie3()}

  @typedoc """
  An entry returned by `c:readdirplus/7` — `readdir_entry` plus a
  `post_op_attr` and `post_op_fh3` per RFC 1813 §3.3.17. Both
  optional fields may be `nil`.
  """
  @type readdirplus_entry ::
          {Types.fileid3(), Types.filename3(), Types.nfs_cookie3(), Types.Fattr3.t() | nil,
           Types.fhandle3() | nil}

  @typedoc """
  Read-back from `c:read/5` — a chunk-iterator (any `Enumerable.t()`
  whose elements are binaries), the EOF flag, and a post-op attr
  for the file. The iterator must not materialise the whole file
  in memory; see `CLAUDE.md` and Codebase-Patterns wiki for the
  no-whole-file-buffering rule.
  """
  @type read_reply :: %{
          required(:data) => Enumerable.t(),
          required(:eof) => boolean(),
          optional(:post_op) => Types.Fattr3.t() | nil
        }

  @typedoc """
  RFC 1813 §3.3.18 FSSTAT reply body (everything except the leading
  `post_op_attr`).
  """
  @type fsstat_reply :: %{
          required(:tbytes) => non_neg_integer(),
          required(:fbytes) => non_neg_integer(),
          required(:abytes) => non_neg_integer(),
          required(:tfiles) => non_neg_integer(),
          required(:ffiles) => non_neg_integer(),
          required(:afiles) => non_neg_integer(),
          required(:invarsec) => non_neg_integer()
        }

  @typedoc """
  RFC 1813 §3.3.19 FSINFO reply body. `properties` is an OR of the
  FSF3 flag constants (see `NFSServer.NFSv3.Types`-adjacent
  documentation in the handler).
  """
  @type fsinfo_reply :: %{
          required(:rtmax) => non_neg_integer(),
          required(:rtpref) => non_neg_integer(),
          required(:rtmult) => non_neg_integer(),
          required(:wtmax) => non_neg_integer(),
          required(:wtpref) => non_neg_integer(),
          required(:wtmult) => non_neg_integer(),
          required(:dtpref) => non_neg_integer(),
          required(:maxfilesize) => non_neg_integer(),
          required(:time_delta) => Types.Nfstime3.t(),
          required(:properties) => non_neg_integer()
        }

  @typedoc """
  RFC 1813 §3.3.20 PATHCONF reply body.
  """
  @type pathconf_reply :: %{
          required(:linkmax) => non_neg_integer(),
          required(:name_max) => non_neg_integer(),
          required(:no_trunc) => boolean(),
          required(:chown_restricted) => boolean(),
          required(:case_insensitive) => boolean(),
          required(:case_preserving) => boolean()
        }

  # ——— Metadata callbacks ————————————————————————————————————

  @doc "GETATTR — RFC 1813 §3.3.1."
  @callback getattr(Types.fhandle3(), Auth.credential(), ctx()) ::
              {:ok, Types.Fattr3.t()} | {:error, Types.nfsstat3()}

  @doc """
  ACCESS — RFC 1813 §3.3.4. `granted_mask` is the subset of
  `requested_mask` (the same ACCESS3_* bit flags) the server is
  willing to permit. Even on success the server includes a
  `post_op_attr` so the client can refresh its cache.
  """
  @callback access(
              Types.fhandle3(),
              requested_mask :: non_neg_integer(),
              Auth.credential(),
              ctx()
            ) ::
              {:ok, granted_mask :: non_neg_integer(), Types.Fattr3.t() | nil}
              | {:error, Types.nfsstat3(), Types.Fattr3.t() | nil}

  @doc """
  LOOKUP — RFC 1813 §3.3.3. Returns the looked-up file's handle, its
  optional `post_op_attr`, and the directory's `post_op_attr`. On
  failure, the directory `post_op_attr` is still returned where
  available.
  """
  @callback lookup(
              dir :: Types.fhandle3(),
              name :: Types.filename3(),
              Auth.credential(),
              ctx()
            ) ::
              {:ok, Types.fhandle3(), Types.Fattr3.t() | nil, Types.Fattr3.t() | nil}
              | {:error, Types.nfsstat3(), Types.Fattr3.t() | nil}

  @doc "READLINK — RFC 1813 §3.3.5."
  @callback readlink(Types.fhandle3(), Auth.credential(), ctx()) ::
              {:ok, Types.nfspath3(), Types.Fattr3.t() | nil}
              | {:error, Types.nfsstat3(), Types.Fattr3.t() | nil}

  @doc """
  READ — RFC 1813 §3.3.6. The `data` field of the success reply must
  be a chunk-iterator (`Enumerable.t()` of binary segments) whose
  total bytes equal at most `count`. Returning a flat binary is
  equivalent to a one-element iterator but defeats the streaming
  invariant the handler relies on.
  """
  @callback read(
              Types.fhandle3(),
              Types.offset3(),
              Types.count3(),
              Auth.credential(),
              ctx()
            ) ::
              {:ok, read_reply()}
              | {:error, Types.nfsstat3(), Types.Fattr3.t() | nil}

  @doc """
  READDIR — RFC 1813 §3.3.16. The handler decides where to splice in
  `.` / `..`; the backend just returns its own entries. Cookies
  are opaque — the backend may encode position in any way it
  prefers.
  """
  @callback readdir(
              Types.fhandle3(),
              cookie :: Types.nfs_cookie3(),
              cookieverf :: Types.cookieverf3(),
              count :: Types.count3(),
              Auth.credential(),
              ctx()
            ) ::
              {:ok, [readdir_entry()], Types.cookieverf3(), eof :: boolean(),
               Types.Fattr3.t() | nil}
              | {:error, Types.nfsstat3(), Types.Fattr3.t() | nil}

  @doc """
  READDIRPLUS — RFC 1813 §3.3.17. `dircount` is the byte budget for
  entry names; `maxcount` is the byte budget for the whole reply
  (including post-op attrs and fhandles). The backend should honour
  `maxcount` and stop early if the next entry would push the
  budget over.
  """
  @callback readdirplus(
              Types.fhandle3(),
              cookie :: Types.nfs_cookie3(),
              cookieverf :: Types.cookieverf3(),
              dircount :: Types.count3(),
              maxcount :: Types.count3(),
              Auth.credential(),
              ctx()
            ) ::
              {:ok, [readdirplus_entry()], Types.cookieverf3(), eof :: boolean(),
               Types.Fattr3.t() | nil}
              | {:error, Types.nfsstat3(), Types.Fattr3.t() | nil}

  @doc "FSSTAT — RFC 1813 §3.3.18."
  @callback fsstat(Types.fhandle3(), Auth.credential(), ctx()) ::
              {:ok, fsstat_reply(), Types.Fattr3.t() | nil}
              | {:error, Types.nfsstat3(), Types.Fattr3.t() | nil}

  @doc "FSINFO — RFC 1813 §3.3.19."
  @callback fsinfo(Types.fhandle3(), Auth.credential(), ctx()) ::
              {:ok, fsinfo_reply(), Types.Fattr3.t() | nil}
              | {:error, Types.nfsstat3(), Types.Fattr3.t() | nil}

  @doc "PATHCONF — RFC 1813 §3.3.20."
  @callback pathconf(Types.fhandle3(), Auth.credential(), ctx()) ::
              {:ok, pathconf_reply(), Types.Fattr3.t() | nil}
              | {:error, Types.nfsstat3(), Types.Fattr3.t() | nil}

  # ——— Mutation callbacks ————————————————————————————————————

  @doc """
  SETATTR — RFC 1813 §3.3.2. Apply the requested `sattr3` fields
  (any field set to `nil` means "leave unchanged"; `:atime` /
  `:mtime` carry RFC 1813's `time_set` semantics —
  `:set_to_server_time` or `{:client, %Nfstime3{}}`).

  When `guard_ctime` is non-nil, the backend MUST compare it to the
  file's current `ctime` and return `{:error, :not_sync, wcc}` if
  they differ — RFC 1813 §3.3.2's "guarded SETATTR" precondition.

  Reply carries `wcc_data` (`pre_op` `wcc_attr` + `post_op` `fattr3`)
  on both success and failure so the client can refresh its
  close-to-open cache. Backends that cannot supply pre- or post-op
  attrs may pass `nil` for either side.
  """
  @callback setattr(
              Types.fhandle3(),
              Types.Sattr3.t(),
              guard_ctime :: Types.Nfstime3.t() | nil,
              Auth.credential(),
              ctx()
            ) ::
              {:ok, Types.WccData.t()}
              | {:error, Types.nfsstat3(), Types.WccData.t()}
              | {:error, Types.nfsstat3()}

  @doc """
  CREATE — RFC 1813 §3.3.8. Create a regular file under `dir_fh /
  name`. The `mode` discriminated union (`createhow3`) selects:

    * `{:unchecked, %Sattr3{}}` — overwrite if exists.
    * `{:guarded, %Sattr3{}}` — fail with `NFS3ERR_EXIST` if exists.
    * `{:exclusive, createverf3}` — atomic create-if-not-exists keyed
      by the 8-byte verifier. Retried CREATE with the same verf must
      be idempotent: the second call observes the same file rather
      than `NFS3ERR_EXIST`.

  On success returns the new file's optional `post_op_fh3` plus its
  `post_op_attr`, plus the parent directory's `wcc_data`. On failure
  returns the parent's `wcc_data` so the client can refresh its
  cache.
  """
  @callback create(
              dir_fh :: Types.fhandle3(),
              name :: Types.filename3(),
              mode :: Types.createhow3(),
              Auth.credential(),
              ctx()
            ) ::
              {:ok, Types.fhandle3() | nil, Types.Fattr3.t() | nil, Types.WccData.t()}
              | {:error, Types.nfsstat3(), Types.WccData.t()}
              | {:error, Types.nfsstat3()}

  @doc """
  MKDIR — RFC 1813 §3.3.9. Create a directory at `dir_fh / name`.
  Reply carries the new dir's optional `post_op_fh3`, optional
  `post_op_attr`, and the parent's `wcc_data`.
  """
  @callback mkdir(
              dir_fh :: Types.fhandle3(),
              name :: Types.filename3(),
              attrs :: Types.Sattr3.t(),
              Auth.credential(),
              ctx()
            ) ::
              {:ok, Types.fhandle3() | nil, Types.Fattr3.t() | nil, Types.WccData.t()}
              | {:error, Types.nfsstat3(), Types.WccData.t()}
              | {:error, Types.nfsstat3()}

  @doc """
  REMOVE — RFC 1813 §3.3.12. Unlink a file by `(dir_fh, name)`.
  Reply carries the parent's `wcc_data`.
  """
  @callback remove(
              dir_fh :: Types.fhandle3(),
              name :: Types.filename3(),
              Auth.credential(),
              ctx()
            ) ::
              {:ok, Types.WccData.t()}
              | {:error, Types.nfsstat3(), Types.WccData.t()}
              | {:error, Types.nfsstat3()}

  @doc """
  RMDIR — RFC 1813 §3.3.13. Remove an empty directory by
  `(dir_fh, name)`. Returns `NFS3ERR_NOTEMPTY` if the directory is
  non-empty and `NFS3ERR_NOTDIR` if the target is not a directory.
  Reply carries the parent's `wcc_data`.
  """
  @callback rmdir(
              dir_fh :: Types.fhandle3(),
              name :: Types.filename3(),
              Auth.credential(),
              ctx()
            ) ::
              {:ok, Types.WccData.t()}
              | {:error, Types.nfsstat3(), Types.WccData.t()}
              | {:error, Types.nfsstat3()}

  @doc """
  WRITE — RFC 1813 §3.3.7. Write `data` at `offset` with the
  requested `stable` hint. The server returns the actual count
  written (may be less than the request), the stability level it
  achieved (must be `>= stable` requested), and the per-instance
  `writeverf3`. Backends that always commit synchronously may
  return `:file_sync` regardless of the requested stability — the
  client gets stronger guarantees than asked for, which RFC 1813
  permits.

  The handler trims `data` against the per-write `count` cap before
  this callback fires, so backends can rely on `byte_size(data) ==
  count`.
  """
  @callback write(
              fh :: Types.fhandle3(),
              offset :: Types.offset3(),
              data :: binary(),
              stable :: Types.stable_how(),
              Auth.credential(),
              ctx()
            ) ::
              {:ok,
               %{
                 required(:wcc) => Types.WccData.t(),
                 required(:count) => non_neg_integer(),
                 required(:committed) => Types.stable_how(),
                 required(:verf) => Types.writeverf3()
               }}
              | {:error, Types.nfsstat3(), Types.WccData.t()}
              | {:error, Types.nfsstat3()}

  @doc """
  RENAME — RFC 1813 §3.3.14. Cross-directory rename. The reply
  carries `wcc_data` for **both** parent directories (`fromdir_wcc`
  and `todir_wcc`) so the client can refresh the parent caches on
  either side.

  Backends that fence cross-node renames atomically (NeonFS does so
  via `NeonFS.Core.NamespaceCoordinator.claim_rename/3`, #304)
  return `NFS3ERR_INVAL` for cycles like `rename(/a, /a/b)` and
  `NFS3ERR_NOTEMPTY` if the destination is a non-empty directory.
  """
  @callback rename(
              from_dir :: Types.fhandle3(),
              from_name :: Types.filename3(),
              to_dir :: Types.fhandle3(),
              to_name :: Types.filename3(),
              Auth.credential(),
              ctx()
            ) ::
              {:ok, Types.WccData.t(), Types.WccData.t()}
              | {:error, Types.nfsstat3(), Types.WccData.t(), Types.WccData.t()}
              | {:error, Types.nfsstat3()}

  @doc """
  COMMIT — RFC 1813 §3.3.21. Flush any unstable writes covering
  `[offset, offset+count)` to stable storage. Returns the
  parent file's `wcc_data` plus the per-instance `writeverf3` —
  clients use the verf to detect a server restart and resend lost
  unstable writes.

  `count == 0` means "commit through end-of-file", per RFC 1813.
  """
  @callback commit(
              fh :: Types.fhandle3(),
              offset :: Types.offset3(),
              count :: Types.count3(),
              Auth.credential(),
              ctx()
            ) ::
              {:ok, %{required(:wcc) => Types.WccData.t(), required(:verf) => Types.writeverf3()}}
              | {:error, Types.nfsstat3(), Types.WccData.t()}
              | {:error, Types.nfsstat3()}
end
