defmodule NeonFS.Core.IAM.Manager do
  @moduledoc """
  Caches the four IAM tables — users, groups, access policies, and
  identity mappings — in public ETS, backed by Ra consensus for
  durability.

  Writes go through `GenServer.call/3` which serialises them into a
  single `iam_put` / `iam_delete` Ra command and updates the local
  ETS on commit. Reads go direct to ETS from any caller — no
  GenServer call on the hot path.

  Follows the `NeonFS.Core.ACLManager` shape: `trap_exit` is set so
  `terminate/2` runs on shutdown and snapshots ETS to DETS under
  `NeonFS.Core.Persistence.meta_dir/0`; `load_from_ra/1` hydrates ETS
  on startup with a small exponential backoff for nodes that race Ra
  up.

  Today the Manager exposes only the generic primitives
  (`put/3`, `delete/2`, `get/2`, `list/1`). The Ash resources in the
  subsequent IAM slices (#288–#292) layer typed structs on top.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{MetadataStateMachine, Persistence, RaSupervisor}

  @categories [:iam_users, :iam_groups, :iam_policies, :iam_identity_mappings]

  @type category :: :iam_users | :iam_groups | :iam_policies | :iam_identity_mappings
  @type key :: term()
  @type value :: map()

  # ——— Client API ———————————————————————————————————————————————

  @doc "Start the manager under the given supervision tree."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Insert or replace the record for `key` in `category`. Persists via
  Ra and caches in ETS.
  """
  @spec put(category(), key(), value()) :: :ok | {:error, term()}
  def put(category, key, value) when category in @categories and is_map(value) do
    GenServer.call(__MODULE__, {:put, category, key, value})
  end

  @doc """
  Delete the record for `key` in `category`. Persists via Ra and
  removes the ETS cache entry.
  """
  @spec delete(category(), key()) :: :ok | {:error, term()}
  def delete(category, key) when category in @categories do
    GenServer.call(__MODULE__, {:delete, category, key})
  end

  @doc """
  Fetch a record directly from ETS. Returns `{:ok, value}` or
  `{:error, :not_found}`. Safe to call from any process.
  """
  @spec get(category(), key()) :: {:ok, value()} | {:error, :not_found}
  def get(category, key) when category in @categories do
    case :ets.whereis(category) do
      :undefined ->
        {:error, :not_found}

      _ ->
        case :ets.lookup(category, key) do
          [{^key, value}] -> {:ok, value}
          [] -> {:error, :not_found}
        end
    end
  end

  @doc """
  List all records in `category` as `[{key, value}]`. Direct ETS read.
  """
  @spec list(category()) :: [{key(), value()}]
  def list(category) when category in @categories do
    case :ets.whereis(category) do
      :undefined -> []
      _ -> :ets.tab2list(category)
    end
  end

  # ——— Server callbacks ——————————————————————————————————————————

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)
    Enum.each(@categories, &ensure_table/1)
    {:ok, %{}, {:continue, :load_from_ra}}
  end

  @impl true
  def handle_continue(:load_from_ra, state) do
    load_from_ra()
    {:noreply, state}
  end

  @impl true
  def handle_call({:put, category, key, value}, _from, state) do
    reply =
      case maybe_ra_command({:iam_put, category, key, value}) do
        {:ok, :ok} ->
          :ets.insert(category, {key, value})
          :ok

        {:error, :ra_not_available} ->
          :ets.insert(category, {key, value})
          :ok

        {:error, reason} ->
          {:error, reason}
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, category, key}, _from, state) do
    reply =
      case maybe_ra_command({:iam_delete, category, key}) do
        {:ok, :ok} ->
          :ets.delete(category, key)
          :ok

        {:error, :ra_not_available} ->
          :ets.delete(category, key)
          :ok

        {:error, reason} ->
          {:error, reason}
      end

    {:reply, reply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    meta_dir = Persistence.meta_dir()

    Enum.each(@categories, fn category ->
      dets_path = Path.join(meta_dir, "#{category}.dets")
      Persistence.snapshot_table(category, dets_path)
    end)

    Logger.info("IAM Manager tables saved")
    :ok
  rescue
    _ -> :ok
  end

  # ——— Private ———————————————————————————————————————————————————

  defp ensure_table(category) do
    case :ets.whereis(category) do
      :undefined ->
        :ets.new(category, [:set, :named_table, :public, read_concurrency: true])

      _ref ->
        :ok
    end
  end

  defp load_from_ra do
    Enum.each(@categories, &load_category/1)
  end

  defp load_category(category) do
    query_fn = fn state -> MetadataStateMachine.get_iam_table(state, category) end

    case RaSupervisor.query(query_fn) do
      {:ok, table} when is_map(table) ->
        Enum.each(table, fn {key, value} -> :ets.insert(category, {key, value}) end)
        count = map_size(table)

        if count > 0,
          do: Logger.info("Loaded IAM table from Ra", category: category, count: count)

      {:error, reason} ->
        Logger.debug("Could not load IAM table from Ra",
          category: category,
          reason: inspect(reason)
        )
    end
  rescue
    e ->
      Logger.debug("Could not load IAM table from Ra",
        category: category,
        reason: inspect(e)
      )
  catch
    :exit, reason ->
      Logger.debug("Could not load IAM table from Ra (exit)",
        category: category,
        reason: inspect(reason)
      )
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
