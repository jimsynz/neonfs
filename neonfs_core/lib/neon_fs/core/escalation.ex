defmodule NeonFS.Core.Escalation do
  @moduledoc """
  Decision escalation system for surfacing ambiguous situations to operators.

  When the cluster encounters a decision where auto-resolution could be silently
  wrong (quorum loss mid-write, drive reporting intermittent I/O errors, cert
  expiry on a stale node, etc.), the caller raises an escalation via `create/1`
  instead of guessing. The escalation sits in a queue until an operator resolves
  it via the CLI or a webhook handler.

  Uses ETS for concurrent read access with serialised writes through GenServer.
  Backed by Ra for cluster-wide persistence and replication — every core node
  sees the same queue.

  Escalations expire if unresolved past their expiry, at which point they are
  reaped by the periodic `:expire` tick.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.Persistence
  alias NeonFS.Core.RaServer
  alias NeonFS.Core.RaSupervisor

  @type id :: String.t()
  @type severity :: :info | :warning | :critical
  @type status :: :pending | :resolved | :expired

  @type option :: %{
          value: String.t(),
          label: String.t()
        }

  @type t :: %{
          id: id(),
          category: String.t(),
          severity: severity(),
          description: String.t(),
          options: [option()],
          status: status(),
          choice: String.t() | nil,
          created_at: DateTime.t(),
          expires_at: DateTime.t() | nil,
          resolved_at: DateTime.t() | nil
        }

  @ets_table :escalations
  @expire_interval_ms 60_000

  # Client API

  @doc """
  Raise a new escalation. `attrs` requires `:category`, `:severity`,
  `:description`, and `:options` (list of `%{value:, label:}`). Optional keys
  are `:expires_at` (DateTime) and `:id` (defaults to a UUID).
  """
  @spec create(map()) :: {:ok, t()} | {:error, term()}
  def create(attrs) when is_map(attrs) do
    GenServer.call(__MODULE__, {:create, attrs}, 10_000)
  end

  @doc """
  Resolve a pending escalation by choosing one of its options. Returns the
  updated record, or `{:error, :not_found}` / `{:error, :already_resolved}` /
  `{:error, {:invalid_choice, choice}}`.
  """
  @spec resolve(id(), String.t()) :: {:ok, t()} | {:error, term()}
  def resolve(id, choice) when is_binary(id) and is_binary(choice) do
    GenServer.call(__MODULE__, {:resolve, id, choice}, 10_000)
  end

  @doc """
  Delete an escalation by ID. Primarily for administrative cleanup.
  """
  @spec delete(id()) :: :ok | {:error, :not_found}
  def delete(id) when is_binary(id) do
    GenServer.call(__MODULE__, {:delete, id}, 10_000)
  end

  @doc """
  List escalations, optionally filtered by `:status` (default: all) and
  `:category`. Results are sorted by `:created_at` ascending.
  """
  @spec list(keyword()) :: [t()]
  def list(opts \\ []) do
    status = Keyword.get(opts, :status)
    category = Keyword.get(opts, :category)

    @ets_table
    |> :ets.tab2list()
    |> Enum.map(fn {_key, escalation} -> escalation end)
    |> filter_by_status(status)
    |> filter_by_category(category)
    |> Enum.sort_by(& &1.created_at, DateTime)
  end

  @doc """
  Fetch a single escalation by ID.
  """
  @spec get(id()) :: {:ok, t()} | {:error, :not_found}
  def get(id) when is_binary(id) do
    case :ets.lookup(@ets_table, id) do
      [{^id, escalation}] -> {:ok, escalation}
      [] -> lookup_from_ra(id)
    end
  end

  @doc """
  Starts the escalation manager.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    :ets.new(@ets_table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true
    ])

    restored =
      case restore_from_ra() do
        {:ok, count} ->
          Logger.info("Escalation manager started, restored escalations from Ra", count: count)
          true

        {:error, reason} ->
          Logger.debug("Escalation manager started but Ra not ready yet, will retry",
            reason: reason
          )

          schedule_restore_retry(1_000)
          false
      end

    schedule_expire_tick()
    emit_pending_metrics()

    {:ok, %{restored: restored, restore_backoff: 1_000}}
  end

  @impl true
  def terminate(_reason, _state) do
    Logger.info("Escalation manager shutting down, saving table...")
    meta_dir = Persistence.meta_dir()

    Persistence.snapshot_table(
      @ets_table,
      Path.join(meta_dir, "escalations.dets")
    )

    Logger.info("Escalation manager table saved")
    :ok
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    reply = do_create(attrs)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:resolve, id, choice}, _from, state) do
    reply = do_resolve(id, choice)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    reply = do_delete(id)
    {:reply, reply, state}
  end

  @impl true
  def handle_info(:retry_restore_from_ra, %{restored: true} = state) do
    {:noreply, state}
  end

  def handle_info(:retry_restore_from_ra, state) do
    case restore_from_ra() do
      {:ok, count} ->
        Logger.info("Escalation manager restored from Ra on retry", count: count)
        emit_pending_metrics()
        {:noreply, %{state | restored: true}}

      {:error, _reason} ->
        next_backoff = min(state.restore_backoff * 2, 30_000)
        schedule_restore_retry(next_backoff)
        {:noreply, %{state | restore_backoff: next_backoff}}
    end
  end

  def handle_info(:expire_tick, state) do
    expire_overdue()
    emit_pending_metrics()
    schedule_expire_tick()
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Private helpers

  defp do_create(attrs) do
    with {:ok, escalation} <- build_escalation(attrs),
         :ok <- persist(escalation) do
      :telemetry.execute(
        [:neonfs, :escalation, :raised],
        %{count: 1},
        %{id: escalation.id, category: escalation.category, severity: escalation.severity}
      )

      emit_pending_metrics()
      {:ok, escalation}
    end
  end

  defp do_resolve(id, choice) do
    with {:ok, existing} <- get(id),
         :ok <- ensure_pending(existing),
         :ok <- ensure_valid_choice(existing, choice) do
      resolved = %{
        existing
        | status: :resolved,
          choice: choice,
          resolved_at: DateTime.utc_now()
      }

      with :ok <- persist(resolved) do
        :telemetry.execute(
          [:neonfs, :escalation, :resolved],
          %{count: 1},
          %{id: id, category: resolved.category, choice: choice}
        )

        emit_pending_metrics()
        {:ok, resolved}
      end
    end
  end

  defp do_delete(id) do
    case :ets.lookup(@ets_table, id) do
      [{^id, _}] ->
        case delete_persisted(id) do
          :ok ->
            emit_pending_metrics()
            :ok

          {:error, reason} ->
            {:error, reason}
        end

      [] ->
        {:error, :not_found}
    end
  end

  defp build_escalation(attrs) do
    required = [:category, :severity, :description, :options]

    case Enum.find(required, fn key -> not Map.has_key?(attrs, key) end) do
      nil ->
        escalation = %{
          id: Map.get(attrs, :id, generate_id()),
          category: attrs.category,
          severity: attrs.severity,
          description: attrs.description,
          options: Enum.map(attrs.options, &normalise_option/1),
          status: :pending,
          choice: nil,
          created_at: DateTime.utc_now(),
          expires_at: Map.get(attrs, :expires_at),
          resolved_at: nil
        }

        {:ok, escalation}

      missing ->
        {:error, {:missing_field, missing}}
    end
  end

  defp normalise_option(%{value: value, label: label}),
    do: %{value: to_string(value), label: to_string(label)}

  defp normalise_option(value) when is_binary(value) or is_atom(value),
    do: %{value: to_string(value), label: to_string(value)}

  defp ensure_pending(%{status: :pending}), do: :ok
  defp ensure_pending(_), do: {:error, :already_resolved}

  defp ensure_valid_choice(%{options: options}, choice) do
    if Enum.any?(options, &(&1.value == choice)) do
      :ok
    else
      {:error, {:invalid_choice, choice}}
    end
  end

  defp persist(escalation) do
    case maybe_ra_command({:put_escalation, escalation_to_map(escalation)}) do
      {:ok, :ok} ->
        :ets.insert(@ets_table, {escalation.id, escalation})
        :ok

      {:error, :ra_not_available} ->
        :ets.insert(@ets_table, {escalation.id, escalation})
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp delete_persisted(id) do
    case maybe_ra_command({:delete_escalation, id}) do
      {:ok, :ok} ->
        :ets.delete(@ets_table, id)
        :ok

      {:error, :ra_not_available} ->
        :ets.delete(@ets_table, id)
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp lookup_from_ra(id) do
    query_fn = fn state ->
      state
      |> Map.get(:escalations, %{})
      |> Map.get(id)
    end

    case RaSupervisor.query(query_fn) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, map} -> cache_and_return(map)
      {:error, _} -> {:error, :not_found}
    end
  catch
    :exit, _ -> {:error, :not_found}
  end

  defp cache_and_return(map) do
    escalation = map_to_escalation(map)
    :ets.insert(@ets_table, {escalation.id, escalation})
    {:ok, escalation}
  end

  defp restore_from_ra do
    case RaSupervisor.query(fn state -> Map.get(state, :escalations, %{}) end) do
      {:ok, escalations} when is_map(escalations) ->
        count =
          Enum.reduce(escalations, 0, fn {_id, map}, acc ->
            escalation = map_to_escalation(map)
            :ets.insert(@ets_table, {escalation.id, escalation})
            acc + 1
          end)

        {:ok, count}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp expire_overdue do
    now = DateTime.utc_now()

    for escalation <- list(status: :pending),
        not is_nil(escalation.expires_at),
        DateTime.compare(escalation.expires_at, now) == :lt do
      expired = %{escalation | status: :expired, resolved_at: now}

      case persist(expired) do
        :ok ->
          :telemetry.execute(
            [:neonfs, :escalation, :expired],
            %{count: 1},
            %{id: escalation.id, category: escalation.category}
          )

        {:error, reason} ->
          Logger.warning("Failed to expire escalation #{escalation.id}",
            reason: inspect(reason)
          )
      end
    end
  end

  defp emit_pending_metrics do
    pending = list(status: :pending)
    pending_count = length(pending)

    by_category =
      pending
      |> Enum.group_by(& &1.category)
      |> Map.new(fn {cat, list} -> {cat, length(list)} end)

    :telemetry.execute(
      [:neonfs, :escalation, :state],
      %{pending_count: pending_count},
      %{by_category: by_category}
    )

    for {category, count} <- by_category do
      :telemetry.execute(
        [:neonfs, :escalation, :pending_by_category],
        %{count: count},
        %{category: category}
      )
    end
  end

  defp maybe_ra_command(cmd) do
    case RaSupervisor.command(cmd) do
      {:ok, result, _leader} ->
        {:ok, result}

      {:error, :noproc} ->
        ra_unavailable_reason()

      {:error, reason} ->
        {:error, reason}

      {:timeout, _node} ->
        {:error, :timeout}
    end
  catch
    :exit, {:noproc, _} ->
      ra_unavailable_reason()

    kind, reason ->
      Logger.debug("Ra command error", kind: kind, reason: reason)

      if RaServer.initialized?() do
        {:error, {:ra_error, {kind, reason}}}
      else
        {:error, :ra_not_available}
      end
  end

  defp ra_unavailable_reason do
    if RaServer.initialized?() do
      {:error, :ra_unavailable}
    else
      {:error, :ra_not_available}
    end
  end

  defp schedule_restore_retry(delay_ms),
    do: Process.send_after(self(), :retry_restore_from_ra, delay_ms)

  defp schedule_expire_tick,
    do: Process.send_after(self(), :expire_tick, @expire_interval_ms)

  defp escalation_to_map(escalation) do
    %{
      id: escalation.id,
      category: escalation.category,
      severity: escalation.severity,
      description: escalation.description,
      options: escalation.options,
      status: escalation.status,
      choice: escalation.choice,
      created_at: escalation.created_at,
      expires_at: escalation.expires_at,
      resolved_at: escalation.resolved_at
    }
  end

  defp map_to_escalation(map) do
    %{
      id: get_field(map, :id),
      category: get_field(map, :category),
      severity: severity_from(get_field(map, :severity)),
      description: get_field(map, :description),
      options: Enum.map(get_field(map, :options) || [], &map_to_option/1),
      status: status_from(get_field(map, :status)),
      choice: get_field(map, :choice),
      created_at: get_field(map, :created_at) || DateTime.utc_now(),
      expires_at: get_field(map, :expires_at),
      resolved_at: get_field(map, :resolved_at)
    }
  end

  defp map_to_option(%{value: v, label: l}), do: %{value: to_string(v), label: to_string(l)}

  defp map_to_option(map) when is_map(map) do
    %{
      value: to_string(get_field(map, :value)),
      label: to_string(get_field(map, :label) || get_field(map, :value))
    }
  end

  defp severity_from(value) when value in [:info, :warning, :critical], do: value
  defp severity_from("info"), do: :info
  defp severity_from("warning"), do: :warning
  defp severity_from("critical"), do: :critical
  defp severity_from(_), do: :info

  defp status_from(value) when value in [:pending, :resolved, :expired], do: value
  defp status_from("pending"), do: :pending
  defp status_from("resolved"), do: :resolved
  defp status_from("expired"), do: :expired
  defp status_from(_), do: :pending

  defp get_field(map, key) when is_atom(key) do
    cond do
      Map.has_key?(map, key) -> Map.get(map, key)
      Map.has_key?(map, Atom.to_string(key)) -> Map.get(map, Atom.to_string(key))
      true -> nil
    end
  end

  defp filter_by_status(list, nil), do: list
  defp filter_by_status(list, status), do: Enum.filter(list, &(&1.status == status))

  defp filter_by_category(list, nil), do: list
  defp filter_by_category(list, category), do: Enum.filter(list, &(&1.category == category))

  defp generate_id do
    "esc-" <> (:crypto.strong_rand_bytes(12) |> Base.url_encode64(padding: false))
  end
end
