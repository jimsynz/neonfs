defmodule NeonFS.Core.KVStore do
  @moduledoc """
  Cluster-wide key/value store backed by Ra consensus.

  Stateless module — every public function is a direct call into Ra.
  Writes (`put/3`, `delete/2`, `update/3`) go through
  `:ra.process_command` which replicates via Raft and applies
  synchronously on every cluster member; reads (`get/2`, `list/1`,
  `query/2`) go through either `:ra.local_query` (when
  `consistency: :weak`, the default) or `:ra.consistent_query` (when
  `consistency: :strong`).

  Keys are arbitrary Erlang terms.

  ## Common options

    * `:timeout` — `:infinity` or a non-negative integer of
      milliseconds. Defaults to `5_000`.
    * `:consistency` — `:weak` (default, local replica read) or
      `:strong` (leader-mediated consistent read). Only meaningful
      for read operations.

  Errors are returned as Splode structs under `NeonFS.Error.*`.
  """

  alias NeonFS.Core.{MetadataStateMachine, RaSupervisor}

  alias NeonFS.Error.{Internal, KeyExists, KeyNotFound, Unavailable}

  @type key :: term()
  @type value :: term()
  @type opts :: keyword()

  @default_timeout 5_000

  @doc """
  Insert or replace the record for `key`. Persists via Ra.

  ## Options

    * `:overwrite?` — when `false`, the write fails with
      `NeonFS.Error.KeyExists` if the key already has a record.
      Defaults to `true`.
    * `:timeout` — see module docs.
  """
  @spec put(key(), value(), opts()) :: :ok | {:error, NeonFS.Error.t()}
  def put(key, value, opts \\ []) do
    overwrite? = Keyword.get(opts, :overwrite?, true)
    ra_command({:kv_put, key, value, overwrite?}, opts, key)
  end

  @doc """
  Delete the record for `key`. Idempotent — succeeds whether or not
  the key was present.
  """
  @spec delete(key(), opts()) :: :ok | {:error, NeonFS.Error.t()}
  def delete(key, opts \\ []) do
    ra_command({:kv_delete, key}, opts, key)
  end

  @doc """
  Atomically read, transform, and write back the record for `key`.

  The transformation function runs inside the Ra state machine and is
  replicated as part of the command, so it executes on every cluster
  member. **It must be deterministic and side-effect free** —
  non-determinism diverges replica state.

  ## Options

    * `:default` — when supplied, applied to `fun` if `key` is
      absent. When omitted and the key is absent, returns
      `NeonFS.Error.KeyNotFound`.
    * `:timeout` — see module docs.
  """
  @spec update(key(), (value() -> value()), opts()) ::
          {:ok, value()} | {:error, NeonFS.Error.t()}
  def update(key, fun, opts \\ []) when is_function(fun, 1) do
    default_marker =
      case Keyword.fetch(opts, :default) do
        {:ok, default} -> {:some, default}
        :error -> :none
      end

    case ra_command_raw({:kv_update, key, fun, default_marker}, opts) do
      {:ok, new_value} -> {:ok, new_value}
      {:error, :not_found} -> {:error, KeyNotFound.exception(key: key)}
      {:error, %_{} = err} -> {:error, err}
      {:error, other} -> {:error, Internal.exception(message: inspect(other))}
    end
  end

  @doc """
  Fetch a record from a Ra replica.
  """
  @spec get(key(), opts()) :: {:ok, value()} | {:error, NeonFS.Error.t()}
  def get(key, opts \\ []) do
    case read_kv(opts) do
      {:ok, table} ->
        case Map.fetch(table, key) do
          {:ok, value} -> {:ok, value}
          :error -> {:error, KeyNotFound.exception(key: key)}
        end

      {:error, _} = err ->
        err
    end
  end

  @doc """
  List every record as `[{key, value}]` from a Ra replica.

  The result satisfies `Enumerable.t/0`. Pattern-matching on `[...]`
  will work today but is discouraged — a future implementation may
  return a lazy stream.
  """
  @spec list(opts()) :: {:ok, Enumerable.t()} | {:error, NeonFS.Error.t()}
  def list(opts \\ []) do
    case read_kv(opts) do
      {:ok, table} -> {:ok, Enum.to_list(table)}
      {:error, _} = err -> err
    end
  end

  @doc """
  Return every entry whose `{key, value}` pair satisfies `filter`.

  The filter runs inside the Ra read on the core node, so the entire
  table is never shipped across the wire.

  The result satisfies `Enumerable.t/0`. See `list/1` for the same
  forward-compatibility caveat.
  """
  @spec query(({key(), value()} -> as_boolean(term())), opts()) ::
          {:ok, Enumerable.t()} | {:error, NeonFS.Error.t()}
  def query(filter, opts \\ []) when is_function(filter, 1) do
    case ra_query(&MetadataStateMachine.kv_query(&1, filter), opts) do
      {:ok, entries} -> {:ok, entries}
      {:error, _} = err -> err
    end
  end

  # ——— Private ———————————————————————————————————————————————————

  defp read_kv(opts) do
    ra_query(&MetadataStateMachine.get_kv/1, opts)
  end

  defp ra_query(fun, opts) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    consistency = Keyword.get(opts, :consistency, :weak)

    do_ra_query(fun, timeout, consistency)
  catch
    :exit, _ -> {:error, Unavailable.exception(message: "Ra not available")}
  end

  defp do_ra_query(fun, timeout, :weak) do
    case RaSupervisor.local_query(fun, timeout) do
      {:ok, result} -> {:ok, result}
      {:error, :timeout} -> {:error, Unavailable.exception(message: "Ra read timed out")}
      {:error, reason} -> {:error, ra_error(reason)}
    end
  end

  defp do_ra_query(fun, timeout, :strong) do
    case RaSupervisor.query(fun, timeout) do
      {:ok, result} -> {:ok, result}
      {:error, :timeout} -> {:error, Unavailable.exception(message: "Ra read timed out")}
      {:error, reason} -> {:error, ra_error(reason)}
    end
  end

  defp ra_command(cmd, opts, key) do
    case ra_command_raw(cmd, opts) do
      :ok -> :ok
      {:error, :key_exists} -> {:error, KeyExists.exception(key: key)}
      {:error, %_{} = err} -> {:error, err}
      {:error, other} -> {:error, Internal.exception(message: inspect(other))}
    end
  end

  defp ra_command_raw(cmd, opts) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    case RaSupervisor.command(cmd, timeout) do
      {:ok, :ok, _leader} ->
        :ok

      {:ok, {:error, reason}, _leader} ->
        {:error, reason}

      {:ok, {:ok, _value} = ok, _leader} ->
        ok

      {:ok, other, _leader} ->
        {:error, Internal.exception(message: "unexpected reply: #{inspect(other)}")}

      {:error, :noproc} ->
        {:error, Unavailable.exception(message: "Ra not available")}

      {:error, reason} ->
        {:error, ra_error(reason)}

      {:timeout, _node} ->
        {:error, Unavailable.exception(message: "Ra command timed out")}
    end
  catch
    :exit, _ -> {:error, Unavailable.exception(message: "Ra not available")}
  end

  defp ra_error(:noproc), do: Unavailable.exception(message: "Ra not available")
  defp ra_error(:nodedown), do: Unavailable.exception(message: "Ra leader down")
  defp ra_error(reason), do: Internal.exception(message: inspect(reason))
end
