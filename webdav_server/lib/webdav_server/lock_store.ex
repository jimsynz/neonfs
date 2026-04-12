defmodule WebdavServer.LockStore do
  @moduledoc """
  Behaviour for WebDAV lock storage.

  The default implementation `WebdavServer.LockStore.ETS` provides in-memory
  locking suitable for single-node deployments. Implement this behaviour for
  distributed or persistent locking.
  """

  @type path :: [String.t()]
  @type lock_token :: String.t()
  @type lock_scope :: :exclusive | :shared
  @type lock_type :: :write

  @type lock_info :: %{
          token: lock_token(),
          path: path(),
          scope: lock_scope(),
          type: lock_type(),
          owner: String.t() | nil,
          timeout: pos_integer(),
          expires_at: integer()
        }

  @doc """
  Acquire a lock on the given path. Returns the lock token on success.
  """
  @callback lock(path(), lock_scope(), lock_type(), String.t() | nil, pos_integer()) ::
              {:ok, lock_token()} | {:error, :conflict}

  @doc """
  Release a lock by token.
  """
  @callback unlock(lock_token()) :: :ok | {:error, :not_found}

  @doc """
  Refresh an existing lock's timeout. Returns updated lock info.
  """
  @callback refresh(lock_token(), pos_integer()) :: {:ok, lock_info()} | {:error, :not_found}

  @doc """
  Return all active locks on the given path.
  """
  @callback get_locks(path()) :: [lock_info()]

  @doc """
  Check whether a lock token is valid for the given path.
  """
  @callback check_token(path(), lock_token()) :: :ok | {:error, :invalid_token}
end
