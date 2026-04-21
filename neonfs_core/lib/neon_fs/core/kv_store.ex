defmodule NeonFS.Core.KVStore do
  @moduledoc """
  Cluster-wide key/value store backed by Ra consensus and cached in
  public ETS.

  Writes go through `GenServer.call/3` which serialises them into a
  single `{:kv_put, key, value}` / `{:kv_delete, key}` Ra command and
  updates the local ETS cache on commit. Reads go direct to ETS from
  any caller — no GenServer call on the hot path.

  Keys are arbitrary binaries. Consumers that need namespacing should
  adopt a prefix convention (e.g. `"iam_user:<uuid>"`) and use
  `list_prefix/1` to scan within their namespace.

  On startup, `handle_continue(:load_from_ra, ...)` hydrates ETS from
  the Ra state machine — Ra's own log + snapshot provide durability,
  so no local DETS snapshot is written.

  ## Known limitation

  The local ETS cache is only updated on the node that received the
  `put/2` / `delete/1` call. Writes committed on a different node
  update every node's Ra state machine but do NOT propagate into
  followers' ETS caches until the process restarts and re-hydrates.
  This makes cross-node reads eventually-consistent-at-best. See the
  tracking issue for the broader cleanup of Ra-backed caches.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{MetadataStateMachine, RaSupervisor}

  @table :neonfs_kv

  @type key :: binary()
  @type value :: term()

  # ——— Client API ———————————————————————————————————————————————

  @doc "Start the store under the given supervision tree."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Insert or replace the record for `key`. Persists via Ra and caches
  in ETS.
  """
  @spec put(key(), value()) :: :ok | {:error, term()}
  def put(key, value) when is_binary(key) do
    GenServer.call(__MODULE__, {:put, key, value})
  end

  @doc """
  Delete the record for `key`. Persists via Ra and removes the ETS
  cache entry.
  """
  @spec delete(key()) :: :ok | {:error, term()}
  def delete(key) when is_binary(key) do
    GenServer.call(__MODULE__, {:delete, key})
  end

  @doc """
  Fetch a record directly from ETS. Returns `{:ok, value}` or
  `{:error, :not_found}`. Safe to call from any process.
  """
  @spec get(key()) :: {:ok, value()} | {:error, :not_found}
  def get(key) when is_binary(key) do
    case :ets.whereis(@table) do
      :undefined ->
        {:error, :not_found}

      _ ->
        case :ets.lookup(@table, key) do
          [{^key, value}] -> {:ok, value}
          [] -> {:error, :not_found}
        end
    end
  end

  @doc """
  List every record as `[{key, value}]`. Direct ETS read.
  """
  @spec list() :: [{key(), value()}]
  def list do
    case :ets.whereis(@table) do
      :undefined -> []
      _ -> :ets.tab2list(@table)
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
    case :ets.whereis(@table) do
      :undefined ->
        []

      _ ->
        :ets.tab2list(@table)
        |> Enum.filter(fn {key, _value} -> String.starts_with?(key, prefix) end)
    end
  end

  # ——— Server callbacks ——————————————————————————————————————————

  @impl true
  def init(_opts) do
    ensure_table()
    {:ok, %{}, {:continue, :load_from_ra}}
  end

  @impl true
  def handle_continue(:load_from_ra, state) do
    load_from_ra()
    {:noreply, state}
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    reply =
      case maybe_ra_command({:kv_put, key, value}) do
        {:ok, :ok} ->
          :ets.insert(@table, {key, value})
          :ok

        {:error, :ra_not_available} ->
          :ets.insert(@table, {key, value})
          :ok

        {:error, reason} ->
          {:error, reason}
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, key}, _from, state) do
    reply =
      case maybe_ra_command({:kv_delete, key}) do
        {:ok, :ok} ->
          :ets.delete(@table, key)
          :ok

        {:error, :ra_not_available} ->
          :ets.delete(@table, key)
          :ok

        {:error, reason} ->
          {:error, reason}
      end

    {:reply, reply, state}
  end

  # ——— Private ———————————————————————————————————————————————————

  defp ensure_table do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [:set, :named_table, :public, read_concurrency: true])

      _ref ->
        :ok
    end
  end

  defp load_from_ra do
    query_fn = &MetadataStateMachine.get_kv/1

    case RaSupervisor.query(query_fn) do
      {:ok, table} when is_map(table) ->
        Enum.each(table, fn {key, value} -> :ets.insert(@table, {key, value}) end)
        count = map_size(table)
        if count > 0, do: Logger.info("Loaded KVStore table from Ra", count: count)

      {:error, reason} ->
        Logger.debug("Could not load KVStore table from Ra", reason: inspect(reason))
    end
  rescue
    e -> Logger.debug("Could not load KVStore table from Ra", reason: inspect(e))
  catch
    :exit, reason ->
      Logger.debug("Could not load KVStore table from Ra (exit)", reason: inspect(reason))
  end

  defp maybe_ra_command(cmd) do
    case RaSupervisor.command(cmd) do
      {:ok, result, _leader} -> {:ok, result}
      {:error, :noproc} -> {:error, :ra_not_available}
      {:error, reason} -> {:error, reason}
      {:timeout, _node} -> {:error, :timeout}
    end
  catch
    :exit, _ -> {:error, :ra_not_available}
  end
end
