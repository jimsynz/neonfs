defmodule NFSServer.Mount.Handler do
  @moduledoc """
  ONC RPC handler for MOUNT v3 (program 100005, version 3) — see
  [RFC 1813 appendix I](https://www.rfc-editor.org/rfc/rfc1813#appendix-I).

  Implements the six MOUNT v3 procedures:

  | Proc | Name      |
  |------|-----------|
  | 0    | NULL      |
  | 1    | MNT       |
  | 2    | DUMP      |
  | 3    | UMNT      |
  | 4    | UMNTALL   |
  | 5    | EXPORT    |

  Export resolution is delegated to a `NFSServer.Mount.Backend`
  module; the handler stays NeonFS-agnostic.

  ## Wiring

  Pass the backend module via the dispatcher's program registry
  using `with_backend/1`:

      programs = %{100_005 => %{3 => NFSServer.Mount.Handler.with_backend(MyBackend)}}

  `with_backend/1` returns a tiny shim module that conforms to the
  `NFSServer.RPC.Handler` callback and stamps `MyBackend` into the
  call's `ctx` so the dispatcher's stateless invocation can still
  reach the backend.

  Alternatively, set `:nfs_mount_backend` on the dispatcher ctx and
  invoke this module directly — used by the test suite.
  """

  @behaviour NFSServer.RPC.Handler

  alias NFSServer.Mount.Types
  alias NFSServer.RPC.Auth

  @program 100_005
  @version 3

  @proc_null 0
  @proc_mnt 1
  @proc_dump 2
  @proc_umnt 3
  @proc_umntall 4
  @proc_export 5

  @doc "MOUNT program number (always 100005)."
  @spec program() :: 100_005
  def program, do: @program

  @doc "MOUNT version this handler implements (always 3)."
  @spec version() :: 3
  def version, do: @version

  @doc """
  Build a thin handler module that dispatches to `backend`. Since
  `NFSServer.RPC.Dispatcher` only carries module references, this
  generates a tiny anonymous-named module per backend that delegates
  back into `NFSServer.Mount.Handler.handle_call/4` with the
  backend stashed in `ctx`.
  """
  @spec with_backend(module()) :: module()
  def with_backend(backend) when is_atom(backend) do
    suffix = backend |> Module.split() |> Enum.join("_")
    name = Module.concat([__MODULE__, "Bound", suffix])

    case Code.ensure_loaded(name) do
      {:module, _} ->
        name

      _ ->
        contents =
          quote do
            @behaviour NFSServer.RPC.Handler
            @backend unquote(backend)

            @impl true
            def handle_call(proc, args, auth, ctx) do
              ctx = Map.put(ctx, :nfs_mount_backend, @backend)
              NFSServer.Mount.Handler.handle_call(proc, args, auth, ctx)
            end
          end

        {:module, ^name, _bin, _exports} =
          Module.create(name, contents, Macro.Env.location(__ENV__))

        name
    end
  end

  @impl true
  def handle_call(@proc_null, _args, _auth, _ctx), do: {:ok, <<>>}

  def handle_call(@proc_mnt, args, auth, ctx) do
    case Types.decode_dirpath(args) do
      {:ok, path, _} -> do_mnt(path, auth, ctx)
      {:error, _} -> :garbage_args
    end
  end

  def handle_call(@proc_dump, _args, _auth, ctx) do
    backend = fetch_backend!(ctx)

    mounts =
      if function_exported?(backend, :list_mounts, 1), do: backend.list_mounts(ctx), else: []

    {:ok, Types.encode_chain(mounts, &Types.encode_mountlist_entry/1)}
  end

  def handle_call(@proc_umnt, args, auth, ctx) do
    case Types.decode_dirpath(args) do
      {:ok, path, _} -> do_umnt(path, auth, ctx)
      {:error, _} -> :garbage_args
    end
  end

  def handle_call(@proc_umntall, _args, auth, ctx) do
    backend = fetch_backend!(ctx)

    if function_exported?(backend, :forget_all_mounts, 2) do
      backend.forget_all_mounts(client_name(auth), auth)
    end

    {:ok, <<>>}
  end

  def handle_call(@proc_export, _args, _auth, ctx) do
    backend = fetch_backend!(ctx)
    exports = backend.list_exports(ctx)
    {:ok, Types.encode_chain(exports, &Types.encode_exportnode/1)}
  end

  def handle_call(_proc, _args, _auth, _ctx), do: :proc_unavail

  # ——— Internal ————————————————————————————————————————————————

  defp do_mnt(path, auth, ctx) do
    backend = fetch_backend!(ctx)

    case backend.resolve(path, ctx) do
      {:ok, fhandle, auth_flavors} ->
        if function_exported?(backend, :record_mount, 3) do
          backend.record_mount(client_name(auth), path, auth)
        end

        {:ok, Types.encode_mountres3_ok(fhandle, auth_flavors)}

      {:error, reason} ->
        {:ok, Types.encode_mountres3_err(reason)}
    end
  end

  defp do_umnt(path, auth, ctx) do
    backend = fetch_backend!(ctx)

    if function_exported?(backend, :forget_mount, 3) do
      backend.forget_mount(client_name(auth), path, auth)
    end

    # MOUNTPROC3_UMNT has a void reply.
    {:ok, <<>>}
  end

  # AUTH_SYS surfaces the client's hostname; AUTH_NONE doesn't, so
  # fall back to a generic anonymous tag. Backends that need the IP
  # can pull it from the future `ctx.peer` slot.
  defp client_name(%Auth.Sys{machinename: name}) when is_binary(name) and name != "", do: name
  defp client_name(_), do: "anonymous"

  defp fetch_backend!(ctx) do
    case Map.fetch(ctx, :nfs_mount_backend) do
      {:ok, backend} when is_atom(backend) ->
        backend

      _ ->
        raise ArgumentError,
              "NFSServer.Mount.Handler invoked without a backend in ctx; use `with_backend/1` to register"
    end
  end
end
