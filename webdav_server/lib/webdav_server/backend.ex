defmodule WebdavServer.Backend do
  @moduledoc """
  Behaviour for WebDAV storage backends.

  Implement this behaviour to connect WebdavServer to any storage system.
  The backend handles resource resolution, content operations, and collection
  management. All path arguments are lists of URI-decoded path segments.

  ## Example

      defmodule MyApp.DavBackend do
        @behaviour WebdavServer.Backend

        @impl true
        def authenticate(_conn), do: {:ok, %{user: "anonymous"}}

        @impl true
        def resolve(_auth, path) do
          # Look up the resource at path
        end

        # ... implement remaining callbacks
      end
  """

  @type auth_context :: term()
  @type path :: [String.t()]
  @type property :: {namespace :: String.t(), name :: String.t()}
  @type resource :: WebdavServer.Resource.t()
  @type error :: WebdavServer.Error.t()

  # --- Authentication ---

  @doc """
  Authenticate the request. Called before any operation.

  Return `{:ok, auth_context}` where `auth_context` is passed to all
  subsequent callbacks, or `{:error, :unauthorized}`.
  """
  @callback authenticate(conn :: Plug.Conn.t()) ::
              {:ok, auth_context()} | {:error, :unauthorized}

  # --- Resource resolution ---

  @doc """
  Resolve a path to a resource. Return `{:error, not_found_error}` if
  the resource does not exist.
  """
  @callback resolve(auth_context(), path()) ::
              {:ok, resource()} | {:error, error()}

  # --- Properties ---

  @doc """
  Return values for the requested properties on a resource.

  Each property is a `{namespace, name}` tuple. Return a list of
  `{property, {:ok, value}}` or `{property, {:error, :not_found}}` pairs.
  The value should be a string or a Saxy XML element.
  """
  @callback get_properties(auth_context(), resource(), [property()]) ::
              [{property(), {:ok, term()} | {:error, :not_found}}]

  @doc """
  Set or remove properties on a resource.

  Operations are `{:set, property, value}` or `{:remove, property}` tuples.
  Must be atomic — either all succeed or none are applied.
  """
  @callback set_properties(
              auth_context(),
              resource(),
              [{:set, property(), term()} | {:remove, property()}]
            ) ::
              :ok | {:error, error()}

  # --- File operations ---

  @doc """
  Read file content. `opts` may include `:range` as `{start, end}`.

  Return `{:ok, iodata}` or `{:ok, enumerable}` for streaming.
  """
  @callback get_content(auth_context(), resource(), opts :: map()) ::
              {:ok, iodata() | Enumerable.t()} | {:error, error()}

  @doc """
  Create or replace a file at the given path.

  Return `{:ok, resource}` with the created/updated resource.
  """
  @callback put_content(auth_context(), path(), body :: iodata(), opts :: map()) ::
              {:ok, resource()} | {:error, error()}

  @doc """
  Delete a resource. For collections, the backend should handle recursive
  deletion. Return `:ok` on full success, or `{:error, error}` with a list
  of partial failures via `{:partial, [{path, error}]}` for 207 responses.
  """
  @callback delete(auth_context(), resource()) ::
              :ok | {:error, error()} | {:partial, [{path(), error()}]}

  @doc """
  Copy a resource to the destination path.

  `overwrite?` indicates whether existing resources at the destination
  should be replaced. Return `{:ok, :created}` or `{:ok, :no_content}`
  to distinguish new vs overwritten destinations.
  """
  @callback copy(auth_context(), resource(), dest_path :: path(), overwrite? :: boolean()) ::
              {:ok, :created | :no_content} | {:error, error()}

  @doc """
  Move a resource to the destination path.

  `overwrite?` indicates whether existing resources at the destination
  should be replaced. Should be atomic when possible.
  """
  @callback move(auth_context(), resource(), dest_path :: path(), overwrite? :: boolean()) ::
              {:ok, :created | :no_content} | {:error, error()}

  # --- Collection operations ---

  @doc """
  Create a collection (directory) at the given path.
  """
  @callback create_collection(auth_context(), path()) ::
              :ok | {:error, error()}

  @doc """
  List the direct members of a collection.
  """
  @callback get_members(auth_context(), resource()) ::
              {:ok, [resource()]} | {:error, error()}
end
