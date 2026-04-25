defmodule NeonFS.Core.Escalation do
  @moduledoc """
  Decision escalation system for surfacing ambiguous situations to operators.

  When the cluster encounters a decision where auto-resolution could be silently
  wrong (quorum loss mid-write, drive reporting intermittent I/O errors, cert
  expiry on a stale node, etc.), the caller raises an escalation via `create/1`
  instead of guessing. The escalation sits in a queue until an operator resolves
  it via the CLI or a webhook handler.

  Stateless facade over Ra: reads go through `RaSupervisor.local_query/2` against
  the `MetadataStateMachine`; writes go through Raft consensus via
  `RaSupervisor.command/2`. There is no ETS cache and no DETS snapshot. The
  periodic expiry sweep lives in `NeonFS.Core.Escalation.Ticker`.

  Escalations expire if unresolved past their expiry, at which point they are
  reaped by the periodic tick.
  """

  require Logger

  alias NeonFS.Core.{MetadataStateMachine, RaSupervisor}

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

  @doc """
  Raise a new escalation. `attrs` requires `:category`, `:severity`,
  `:description`, and `:options` (list of `%{value:, label:}`). Optional keys
  are `:expires_at` (DateTime) and `:id` (defaults to a UUID).
  """
  @spec create(map()) :: {:ok, t()} | {:error, term()}
  def create(attrs) when is_map(attrs) do
    with {:ok, escalation} <- build_escalation(attrs),
         :ok <- ra_command({:put_escalation, escalation_to_map(escalation)}) do
      :telemetry.execute(
        [:neonfs, :escalation, :raised],
        %{count: 1},
        %{id: escalation.id, category: escalation.category, severity: escalation.severity}
      )

      emit_pending_metrics()
      {:ok, escalation}
    end
  end

  @doc """
  Resolve a pending escalation by choosing one of its options. Returns the
  updated record, or `{:error, :not_found}` / `{:error, :already_resolved}` /
  `{:error, {:invalid_choice, choice}}`.
  """
  @spec resolve(id(), String.t()) :: {:ok, t()} | {:error, term()}
  def resolve(id, choice) when is_binary(id) and is_binary(choice) do
    with {:ok, existing} <- get(id),
         :ok <- ensure_pending(existing),
         :ok <- ensure_valid_choice(existing, choice) do
      resolved = %{
        existing
        | status: :resolved,
          choice: choice,
          resolved_at: DateTime.utc_now()
      }

      with :ok <- ra_command({:put_escalation, escalation_to_map(resolved)}) do
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

  @doc """
  Delete an escalation by ID. Primarily for administrative cleanup.
  """
  @spec delete(id()) :: :ok | {:error, :not_found}
  def delete(id) when is_binary(id) do
    case get(id) do
      {:ok, _} ->
        case ra_command({:delete_escalation, id}) do
          :ok ->
            emit_pending_metrics()
            :ok

          {:error, _} = error ->
            error
        end

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @doc """
  List escalations, optionally filtered by `:status` (default: all) and
  `:category`. Results are sorted by `:created_at` ascending.
  """
  @spec list(keyword()) :: [t()]
  def list(opts \\ []) do
    status = Keyword.get(opts, :status)
    category = Keyword.get(opts, :category)

    case read_escalations() do
      {:ok, escalations_map} ->
        escalations_map
        |> Map.values()
        |> Enum.map(&map_to_escalation/1)
        |> filter_by_status(status)
        |> filter_by_category(category)
        |> Enum.sort_by(& &1.created_at, DateTime)

      {:error, _} ->
        []
    end
  end

  @doc """
  Fetch a single escalation by ID.
  """
  @spec get(id()) :: {:ok, t()} | {:error, :not_found}
  def get(id) when is_binary(id) do
    case read_escalation(id) do
      {:ok, map} when is_map(map) -> {:ok, map_to_escalation(map)}
      {:ok, nil} -> {:error, :not_found}
      {:error, _} -> {:error, :not_found}
    end
  end

  @doc false
  # Invoked from `Escalation.Ticker` — walks pending escalations and moves
  # overdue ones to `:expired`. Exposed as a module function so the ticker
  # stays a dumb timer and so tests can drive expiry synchronously.
  @spec expire_overdue() :: :ok
  def expire_overdue do
    now = DateTime.utc_now()

    for escalation <- list(status: :pending),
        not is_nil(escalation.expires_at),
        DateTime.compare(escalation.expires_at, now) == :lt do
      expired = %{escalation | status: :expired, resolved_at: now}

      case ra_command({:put_escalation, escalation_to_map(expired)}) do
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

    :ok
  end

  @doc false
  @spec emit_pending_metrics() :: :ok
  def emit_pending_metrics do
    # Use a consistent read so the emitted count reflects the latest
    # committed Ra state, not whatever this node's local apply loop
    # has caught up to. Without it, an emit fired immediately after a
    # `resolve` / `create` / `delete` ra_command can race the local
    # apply and report stale numbers — which broke `escalation_test`
    # under contended CI (#565). The cost is one extra leader
    # round-trip per emit; acceptable for an operator-facing metric.
    pending = list_pending_consistent()
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

    :ok
  end

  # Private

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

  defp read_escalation(id) do
    RaSupervisor.local_query(&MetadataStateMachine.get_escalation(&1, id))
  catch
    :exit, _ -> {:error, :ra_not_available}
  end

  defp read_escalations do
    RaSupervisor.local_query(&MetadataStateMachine.get_escalations/1)
  catch
    :exit, _ -> {:error, :ra_not_available}
  end

  # Same as `read_escalations/0` but uses `RaSupervisor.query/1`
  # (Ra `consistent_query`) — leader heartbeats majority then returns
  # the leader's fully-applied state, so the read reflects every
  # committed entry up to the call. Used by `emit_pending_metrics/0`
  # so post-resolve / post-create metric emits don't race the local
  # apply loop. See #565.
  defp list_pending_consistent do
    case RaSupervisor.query(&MetadataStateMachine.get_escalations/1) do
      {:ok, escalations_map} when is_map(escalations_map) ->
        escalations_map
        |> Map.values()
        |> Enum.map(&map_to_escalation/1)
        |> Enum.filter(&(&1.status == :pending))

      _ ->
        []
    end
  catch
    :exit, _ -> []
  end

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

  defp ra_command(cmd) do
    case RaSupervisor.command(cmd) do
      {:ok, :ok, _leader} -> :ok
      {:ok, {:error, reason}, _leader} -> {:error, reason}
      {:ok, other, _leader} -> {:error, {:unexpected_reply, other}}
      {:error, :noproc} -> {:error, :ra_not_available}
      {:error, reason} -> {:error, reason}
      {:timeout, _node} -> {:error, :timeout}
    end
  catch
    :exit, _ -> {:error, :ra_not_available}
  end
end
