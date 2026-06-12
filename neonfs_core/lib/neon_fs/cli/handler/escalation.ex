defmodule NeonFS.CLI.Handler.Escalation do
  @moduledoc """
  CLI command handlers for operator escalations: listing pending
  escalations, resolving one by choosing an option, and fetching a
  single record.

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates its `handle_escalation_*` RPC entry points here, so the CLI
  wire contract is unchanged.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.{AuditLog, Escalation}
  alias NeonFS.Error.{Invalid, NotFound}

  @doc """
  Lists escalations, optionally filtered by `:status` or `:category`.
  """
  @spec handle_escalation_list(map()) :: {:ok, [map()]} | {:error, term()}
  def handle_escalation_list(filters \\ %{}) when is_map(filters) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      opts = escalation_filter_opts(filters)
      escalations = Escalation.list(opts) |> Enum.map(&escalation_to_serialisable/1)
      {:ok, escalations}
    end
  end

  @doc """
  Resolves a pending escalation by choosing one of its options.
  """
  @spec handle_escalation_resolve(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def handle_escalation_resolve(id, choice)
      when is_binary(id) and is_binary(choice) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, escalation} <- Escalation.resolve(id, choice) do
      AuditLog.log_event(
        event_type: :escalation_resolved,
        actor_uid: 0,
        resource: id,
        details: %{choice: choice, category: escalation.category}
      )

      {:ok, escalation_to_serialisable(escalation)}
    else
      {:error, :not_found} ->
        {:error, NotFound.exception(message: "Escalation '#{id}' not found")}

      {:error, :already_resolved} ->
        {:error, Invalid.exception(message: "Escalation '#{id}' is not pending")}

      {:error, {:invalid_choice, bad}} ->
        {:error, Invalid.exception(message: "Invalid choice '#{bad}' for escalation '#{id}'")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Fetches a single escalation by ID.
  """
  @spec handle_escalation_show(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_escalation_show(id) when is_binary(id) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, escalation} <- Escalation.get(id) do
      {:ok, escalation_to_serialisable(escalation)}
    else
      {:error, :not_found} ->
        {:error, NotFound.exception(message: "Escalation '#{id}' not found")}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  # Private

  defp escalation_filter_opts(filters) do
    []
    |> put_filter_opt(filters, "status", :status, &parse_escalation_status/1)
    |> put_filter_opt(filters, "category", :category, & &1)
  end

  defp put_filter_opt(opts, filters, string_key, atom_key, transform) do
    case Map.get(filters, string_key) || Map.get(filters, atom_key) do
      nil -> opts
      value -> Keyword.put(opts, atom_key, transform.(value))
    end
  end

  defp parse_escalation_status(value) when is_atom(value), do: value
  defp parse_escalation_status("pending"), do: :pending
  defp parse_escalation_status("resolved"), do: :resolved
  defp parse_escalation_status("expired"), do: :expired
  defp parse_escalation_status(other), do: other

  defp escalation_to_serialisable(escalation) do
    %{
      id: escalation.id,
      category: escalation.category,
      severity: Atom.to_string(escalation.severity),
      description: escalation.description,
      options: escalation.options,
      status: Atom.to_string(escalation.status),
      choice: escalation.choice,
      created_at: DateTime.to_iso8601(escalation.created_at),
      expires_at: serialise_datetime(escalation.expires_at),
      resolved_at: serialise_datetime(escalation.resolved_at)
    }
  end

  defp serialise_datetime(nil), do: nil
  defp serialise_datetime(%DateTime{} = dt), do: DateTime.to_iso8601(dt)
end
