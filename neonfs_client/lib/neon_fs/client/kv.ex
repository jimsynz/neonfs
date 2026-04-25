defmodule NeonFS.Client.KV do
  @moduledoc """
  Client-side access to the cluster-wide Ra-backed key/value store
  (`NeonFS.Core.KVStore`).

  Intended for orchestration-layer packages that need to read and
  write durable cluster state from an interface node without taking a
  direct dependency on `neonfs_core`. Calls are routed through
  `NeonFS.Client.Router` to a core node that owns the Ra replica.

  Reads honour the `:consistency` option (`:weak` → local replica,
  `:strong` → leader-mediated consistent read). Writes are serialised
  through Ra and prefer the leader to avoid the follower-forward hop.

  Keys are arbitrary Erlang terms. See `NeonFS.Core.KVStore` for the
  full option reference.
  """

  alias NeonFS.Client.Router
  alias NeonFS.Core.KVStore
  alias NeonFS.Error.Unavailable

  @type key :: term()
  @type value :: term()
  @type opts :: keyword()

  @doc """
  Insert or replace the record for `key`. See `NeonFS.Core.KVStore.put/3`.
  """
  @spec put(key(), value(), opts()) :: :ok | {:error, NeonFS.Error.t()}
  def put(key, value, opts \\ []) do
    metadata_call(:put, [key, value, opts], opts)
  end

  @doc """
  Delete the record for `key`. See `NeonFS.Core.KVStore.delete/2`.
  """
  @spec delete(key(), opts()) :: :ok | {:error, NeonFS.Error.t()}
  def delete(key, opts \\ []) do
    metadata_call(:delete, [key, opts], opts)
  end

  @doc """
  Atomically read, transform, and write back the record for `key`.
  See `NeonFS.Core.KVStore.update/3`.
  """
  @spec update(key(), (value() -> value()), opts()) ::
          {:ok, value()} | {:error, NeonFS.Error.t()}
  def update(key, fun, opts \\ []) when is_function(fun, 1) do
    metadata_call(:update, [key, fun, opts], opts)
  end

  @doc """
  Fetch a record. See `NeonFS.Core.KVStore.get/2`.
  """
  @spec get(key(), opts()) :: {:ok, value()} | {:error, NeonFS.Error.t()}
  def get(key, opts \\ []) do
    call(:get, [key, opts], opts)
  end

  @doc """
  List every record. See `NeonFS.Core.KVStore.list/1`.
  """
  @spec list(opts()) :: {:ok, Enumerable.t()} | {:error, NeonFS.Error.t()}
  def list(opts \\ []) do
    call(:list, [opts], opts)
  end

  @doc """
  Filter records by a predicate that runs on the core node. See
  `NeonFS.Core.KVStore.query/2`.
  """
  @spec query(({key(), value()} -> as_boolean(term())), opts()) ::
          {:ok, Enumerable.t()} | {:error, NeonFS.Error.t()}
  def query(filter, opts \\ []) when is_function(filter, 1) do
    call(:query, [filter, opts], opts)
  end

  ## Private

  # The user's `:timeout` is enforced by the inner Ra call inside
  # KVStore. Passing `:infinity` to the Router's RPC layer prevents it
  # from short-circuiting that with its own timeout (which would
  # otherwise trigger Router retries instead of surfacing a clean
  # structured error).
  defp metadata_call(function, args, _opts) do
    KVStore
    |> Router.metadata_call(function, args, timeout: :infinity)
    |> normalise()
  end

  defp call(function, args, _opts) do
    KVStore
    |> Router.call(function, args, timeout: :infinity)
    |> normalise()
  end

  defp normalise({:error, %_{__exception__: true}} = err), do: err
  defp normalise({:error, %{__exception__: true} = exc}), do: {:error, exc}

  defp normalise({:error, reason}) do
    {:error, Unavailable.exception(message: inspect(reason))}
  end

  defp normalise(other), do: other
end
