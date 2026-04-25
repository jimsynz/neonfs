defmodule NeonFS.Client.KV do
  @moduledoc """
  Client-side access to the cluster-wide Ra-backed key/value store
  (`NeonFS.Core.KVStore`).

  Intended for orchestration-layer packages that need to read and
  write durable cluster state from an interface node without taking a
  direct dependency on `neonfs_core`. Calls are routed through
  `NeonFS.Client.Router` to a core node that owns the Ra replica.

  Reads are served from the core node's ETS cache; writes are
  serialised through Ra consensus. Writes prefer the Ra leader to
  avoid the extra hop from follower forwarding.

  Keys are arbitrary binaries. Consumers that want to partition the
  namespace pick their own prefix (e.g. `"iam_user:<uuid>"`) and scan
  within it using `list_prefix/1`.
  """

  alias NeonFS.Client.Router

  @type key :: binary()
  @type value :: term()

  @doc """
  Insert or replace the record for `key`. Goes through Ra — the reply
  lands once the write has been committed on a quorum of core nodes.
  """
  @spec put(key(), value()) :: :ok | {:error, term()}
  def put(key, value) when is_binary(key) do
    Router.metadata_call(NeonFS.Core.KVStore, :put, [key, value])
  end

  @doc """
  Delete the record for `key`. Goes through Ra.
  """
  @spec delete(key()) :: :ok | {:error, term()}
  def delete(key) when is_binary(key) do
    Router.metadata_call(NeonFS.Core.KVStore, :delete, [key])
  end

  @doc """
  Fetch the record for `key` from a core node's ETS cache.
  """
  @spec get(key()) :: {:ok, value()} | {:error, :not_found | term()}
  def get(key) when is_binary(key) do
    Router.call(NeonFS.Core.KVStore, :get, [key])
  end

  @doc """
  List every record as `[{key, value}]` from a core node's ETS cache.
  """
  @spec list() :: [{key(), value()}]
  def list do
    Router.call(NeonFS.Core.KVStore, :list, [])
  end

  @doc """
  List every record whose key starts with `prefix` from a core node's
  ETS cache.
  """
  @spec list_prefix(binary()) :: [{key(), value()}]
  def list_prefix(prefix) when is_binary(prefix) do
    Router.call(NeonFS.Core.KVStore, :list_prefix, [prefix])
  end
end
