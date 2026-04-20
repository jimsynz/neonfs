defmodule NeonFS.Core.EscalationWebhook do
  @moduledoc """
  Outbound webhook dispatcher for escalation events.

  Subscribes to `[:neonfs, :escalation, :raised]` telemetry events and POSTs
  the escalation record as JSON to the configured URL. Retries with
  exponential backoff on non-2xx responses or transport errors.

  Configuration (application env `:neonfs_core`):

      config :neonfs_core,
        escalation_webhook_url: "https://operator.example/hooks/escalation",
        escalation_webhook_max_attempts: 4,
        escalation_webhook_base_backoff_ms: 2_000,
        escalation_webhook_timeout_ms: 10_000

  If `:escalation_webhook_url` is not set the dispatcher is inert — it still
  starts and attaches a handler, but all events are dropped. The HTTP client
  (`:httpc`) is started lazily on first dispatch.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.Escalation

  @telemetry_handler_id "neonfs-escalation-webhook"

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  @spec dispatch(map()) :: :ok
  def dispatch(escalation) when is_map(escalation) do
    GenServer.cast(__MODULE__, {:dispatch, escalation, 1})
  end

  @doc false
  @spec handle_event([atom()], map(), map(), term()) :: :ok
  def handle_event(_event, _measurements, %{id: id}, _config) when is_binary(id) do
    case Escalation.get(id) do
      {:ok, escalation} -> dispatch(escalation)
      _ -> :ok
    end
  end

  def handle_event(_event, _measurements, _metadata, _config), do: :ok

  # Server callbacks

  @impl true
  def init(_opts) do
    attach_telemetry()
    {:ok, %{}}
  end

  @impl true
  def terminate(_reason, _state) do
    :telemetry.detach(@telemetry_handler_id)
    :ok
  end

  @impl true
  def handle_cast({:dispatch, escalation, attempt}, state) do
    maybe_dispatch(escalation, attempt)
    {:noreply, state}
  end

  @impl true
  def handle_info({:retry, escalation, attempt}, state) do
    maybe_dispatch(escalation, attempt)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Private helpers

  defp attach_telemetry do
    :telemetry.attach(
      @telemetry_handler_id,
      [:neonfs, :escalation, :raised],
      &__MODULE__.handle_event/4,
      nil
    )
  end

  defp maybe_dispatch(escalation, attempt) do
    case webhook_url() do
      nil ->
        :ok

      url ->
        do_dispatch(url, escalation, attempt)
    end
  end

  defp do_dispatch(url, escalation, attempt) do
    max = max_attempts()

    if attempt > max do
      Logger.error("Escalation webhook gave up on #{escalation.id}", max_attempts: max)

      :telemetry.execute(
        [:neonfs, :escalation, :webhook, :failed],
        %{attempts: max},
        %{id: escalation.id, category: escalation.category}
      )
    else
      post_webhook(url, escalation, attempt)
    end
  end

  defp post_webhook(url, escalation, attempt) do
    body = encode(escalation)
    request = {String.to_charlist(url), [], ~c"application/json", body}
    ensure_inets_started()

    case :httpc.request(:post, request, [{:timeout, request_timeout_ms()}], []) do
      {:ok, {{_, status, _}, _headers, _resp_body}} when status >= 200 and status < 300 ->
        :telemetry.execute(
          [:neonfs, :escalation, :webhook, :ok],
          %{attempt: attempt, status: status},
          %{id: escalation.id, category: escalation.category}
        )

        :ok

      {:ok, {{_, status, _}, _headers, resp_body}} ->
        Logger.warning(
          "Escalation webhook non-2xx (#{status}) for #{escalation.id}: #{truncate(resp_body)}",
          attempt: attempt
        )

        schedule_retry(escalation, attempt + 1)

      {:error, reason} ->
        Logger.warning("Escalation webhook transport error for #{escalation.id}",
          attempt: attempt,
          reason: inspect(reason)
        )

        schedule_retry(escalation, attempt + 1)
    end
  end

  defp schedule_retry(escalation, attempt) do
    if attempt > max_attempts() do
      do_dispatch(webhook_url(), escalation, attempt)
    else
      base = base_backoff_ms()
      delay = base * trunc(:math.pow(2, attempt - 1))
      Process.send_after(self(), {:retry, escalation, attempt}, delay)
    end
  end

  defp encode(escalation) do
    escalation
    |> serialise()
    |> :json.encode()
    |> IO.iodata_to_binary()
  end

  defp serialise(e) do
    %{
      id: e.id,
      category: e.category,
      severity: Atom.to_string(e.severity),
      description: e.description,
      options: e.options,
      status: Atom.to_string(e.status),
      created_at: DateTime.to_iso8601(e.created_at),
      expires_at: datetime_or_nil(e.expires_at),
      resolved_at: datetime_or_nil(e.resolved_at),
      choice: e.choice
    }
  end

  defp datetime_or_nil(nil), do: nil
  defp datetime_or_nil(%DateTime{} = dt), do: DateTime.to_iso8601(dt)

  defp truncate(body) do
    body
    |> IO.iodata_to_binary()
    |> String.slice(0, 200)
  end

  defp ensure_inets_started do
    case :application.ensure_all_started(:inets) do
      {:ok, _} -> :ok
      {:error, _} = err -> err
    end
  end

  defp webhook_url do
    Application.get_env(:neonfs_core, :escalation_webhook_url)
  end

  defp max_attempts,
    do: Application.get_env(:neonfs_core, :escalation_webhook_max_attempts, 4)

  defp base_backoff_ms,
    do: Application.get_env(:neonfs_core, :escalation_webhook_base_backoff_ms, 2_000)

  defp request_timeout_ms,
    do: Application.get_env(:neonfs_core, :escalation_webhook_timeout_ms, 10_000)
end
