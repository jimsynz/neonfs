defmodule NeonFS.Core.KVStore do
  @moduledoc """
  Cluster-wide key/value store backed by Ra consensus.

  Stateless module — every public function is a direct call into Ra.
  Writes go through `:ra.process_command` which replicates via Raft
  and applies synchronously on every cluster member; reads go through
  `:ra.local_query` on the node the caller happens to land on, which
  is equivalent to every other replica's view since the state machine
  apply is synchronous on commit.

  Keys are arbitrary binaries. Consumers that need namespacing pick a
  prefix convention (e.g. `"iam_user:<uuid>"`) and use `list_prefix/1`.
  """

  alias NeonFS.Core.{MetadataStateMachine, RaSupervisor}

  @type key :: binary()
  @type value :: term()

  @doc """
  Insert or replace the record for `key`. Persists via Ra.
  """
  @spec put(key(), value()) :: :ok | {:error, term()}
  def put(key, value) when is_binary(key) do
    ra_command({:kv_put, key, value})
  end

  @doc """
  Delete the record for `key`. Persists via Ra.
  """
  @spec delete(key()) :: :ok | {:error, term()}
  def delete(key) when is_binary(key) do
    ra_command({:kv_delete, key})
  end

  @doc """
  Fetch a record directly from the local Ra replica. Returns
  `{:ok, value}` or `{:error, :not_found}`. Safe to call from any
  process.
  """
  @spec get(key()) :: {:ok, value()} | {:error, :not_found}
  def get(key) when is_binary(key) do
    case read_kv() do
      {:ok, table} ->
        case Map.fetch(table, key) do
          {:ok, value} -> {:ok, value}
          :error -> {:error, :not_found}
        end

      {:error, _} ->
        {:error, :not_found}
    end
  end

  @doc """
  List every record as `[{key, value}]`.
  """
  @spec list() :: [{key(), value()}]
  def list do
    case read_kv() do
      {:ok, table} -> Enum.to_list(table)
      {:error, _} -> []
    end
  end

  @doc """
  List every record whose key starts with `prefix`.

  Consumers that namespace their keys (e.g. `"iam_user:<uuid>"`) use
  this to scan within their own namespace without having to filter the
  whole table.
  """
  @spec list_prefix(binary()) :: [{key(), value()}]
  def list_prefix(prefix) when is_binary(prefix) do
    case read_kv() do
      {:ok, table} ->
        Enum.filter(table, fn {key, _value} -> String.starts_with?(key, prefix) end)

      {:error, _} ->
        []
    end
  end

  # ——— Private ———————————————————————————————————————————————————

  defp read_kv do
    RaSupervisor.local_query(&MetadataStateMachine.get_kv/1)
  catch
    :exit, _ -> {:error, :ra_not_available}
  end

  defp ra_command(cmd) do
    case RaSupervisor.command(cmd) do
      {:ok, :ok, _leader} -> :ok
      {:ok, other, _leader} -> {:error, {:unexpected_reply, other}}
      {:error, :noproc} -> {:error, :ra_not_available}
      {:error, reason} -> {:error, reason}
      {:timeout, _node} -> {:error, :timeout}
    end
  catch
    :exit, _ -> {:error, :ra_not_available}
  end
end
