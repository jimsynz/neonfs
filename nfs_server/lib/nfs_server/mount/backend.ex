defmodule NFSServer.Mount.Backend do
  @moduledoc """
  Behaviour for the export resolution layer the MOUNT v3 handler
  delegates to. Keeps `nfs_server` decoupled from any particular
  backing store (NeonFS volumes, a static config file, an in-memory
  test fixture, …).

  The `MountList` and `ExportNode` types live in
  `NFSServer.Mount.Types`.

  ## Callbacks

    * `c:resolve/2` — given the path a client passed to `MNT`,
      return the file-handle the server will hand back, or an error
      atom mapped to a `mountstat3` (`:perm`, `:noent`, `:notdir`,
      `:acces`, …).
    * `c:list_exports/1` — return the list of currently-published
      exports (used by `MOUNTPROC3_EXPORT`).
    * `c:list_mounts/1` — optional bookkeeping: list the
      currently-active mounts (used by `MOUNTPROC3_DUMP`).
      Implementations that don't track mount state can return `[]`.
    * `c:record_mount/3` / `c:forget_mount/3` /
      `c:forget_all_mounts/2` — bookkeeping hooks invoked by the
      handler on `MNT` / `UMNT` / `UMNTALL`. Default-implementable as
      no-ops; the handler will still send the protocol-correct
      reply to the client either way.

  Every callback receives an `auth` term (`NFSServer.RPC.Auth.credential()`)
  and an opaque `ctx` map drawn from the original RPC envelope, so
  backend implementations can do AUTH_SYS-aware lookup decisions
  (uid / gid / hostname-from-AUTH_SYS) without the handler having
  to flatten them out.
  """

  alias NFSServer.Mount.Types
  alias NFSServer.RPC.Auth

  @typedoc "Opaque context — the handler passes the RPC ctx through."
  @type ctx :: map()

  @typedoc "Reasons resolve/2 may refuse with."
  @type resolve_error ::
          :perm | :noent | :io | :acces | :notdir | :inval | :nametoolong

  @doc """
  Resolve a `MNT` request to a file-handle.

  Return `{:ok, fhandle, auth_flavors}` on success — `auth_flavors`
  is the list of RPC auth flavour numbers the export accepts (e.g.
  `[1]` for AUTH_SYS only).

  Return `{:error, reason}` to refuse the mount; the handler maps
  `reason` to the RFC 1813 `mountstat3` code.
  """
  @callback resolve(path :: Types.dirpath(), ctx()) ::
              {:ok, Types.fhandle3(), [non_neg_integer()]}
              | {:error, resolve_error()}

  @doc """
  Return all exports currently published by the server. The handler
  encodes the list as the `MOUNTPROC3_EXPORT` reply.
  """
  @callback list_exports(ctx()) :: [Types.ExportNode.t()]

  @doc """
  Return the list of mounts the backend wants to advertise via
  `MOUNTPROC3_DUMP`. Backends that don't track mount state should
  return an empty list — the protocol allows that.
  """
  @callback list_mounts(ctx()) :: [Types.MountList.t()]

  @doc """
  Record a successful mount. Called from the handler after
  `resolve/2` returns `:ok`. Default no-op.
  """
  @callback record_mount(client :: String.t(), path :: Types.dirpath(), Auth.credential()) ::
              :ok

  @doc """
  Forget one client's mount entry for `path`. Called on
  `MOUNTPROC3_UMNT`. Default no-op.
  """
  @callback forget_mount(client :: String.t(), path :: Types.dirpath(), Auth.credential()) ::
              :ok

  @doc """
  Forget all of a client's mount entries. Called on
  `MOUNTPROC3_UMNTALL`. Default no-op.
  """
  @callback forget_all_mounts(client :: String.t(), Auth.credential()) :: :ok

  @optional_callbacks [
    list_mounts: 1,
    record_mount: 3,
    forget_mount: 3,
    forget_all_mounts: 2
  ]
end
