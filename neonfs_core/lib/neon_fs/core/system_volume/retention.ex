defmodule NeonFS.Core.SystemVolume.Retention do
  @moduledoc """
  Periodically prunes old files from the system volume's audit directories.

  Files are date-partitioned using ISO 8601 filenames (`YYYY-MM-DD.jsonl`),
  so retention is based on parsing the date from each filename and comparing
  against configurable age thresholds.

  ## Configuration

      config :neonfs_core, NeonFS.Core.SystemVolume.Retention,
        intent_log_days: 90,
        security_audit_days: 365,
        prune_interval_ms: 86_400_000

  ## Manual use

      NeonFS.Core.SystemVolume.Retention.prune()
  """

  use GenServer

  require Logger

  alias NeonFS.Core.SystemVolume

  @default_intent_log_days 90
  @default_security_audit_days 365
  @default_prune_interval_ms 86_400_000

  @audit_directories [
    {"/audit/intents", :intent_log_days, @default_intent_log_days},
    {"/audit/security", :security_audit_days, @default_security_audit_days}
  ]

  # Public API

  @doc """
  Starts the retention GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Manually triggers a prune cycle.

  Lists files in `/audit/intents/` and `/audit/security/`, parses dates from
  filenames, and deletes files older than their respective retention period.

  Returns `:ok` regardless of individual file failures. Files with unparseable
  dates are skipped. If the system volume does not exist, returns `:ok`.
  """
  @spec prune() :: :ok
  def prune do
    config = retention_config()

    for {path, config_key, default_days} <- @audit_directories do
      max_age_days = Keyword.get(config, config_key, default_days)
      prune_directory(path, max_age_days)
    end

    :ok
  end

  # GenServer callbacks

  @impl true
  def init(opts) do
    config = retention_config()

    state = %{
      prune_interval_ms:
        Keyword.get(
          opts,
          :prune_interval_ms,
          Keyword.get(config, :prune_interval_ms, @default_prune_interval_ms)
        )
    }

    schedule_prune(state)
    {:ok, state}
  end

  @impl true
  def handle_info(:prune, state) do
    prune()
    :telemetry.execute([:neonfs, :system_volume, :retention, :pruned], %{}, %{})
    schedule_prune(state)
    {:noreply, state}
  end

  # Private

  defp schedule_prune(%{prune_interval_ms: ms}) do
    Process.send_after(self(), :prune, ms)
  end

  defp retention_config do
    Application.get_env(:neonfs_core, __MODULE__, [])
  end

  @doc false
  @spec prune_directory(String.t(), non_neg_integer()) :: :ok
  def prune_directory(path, max_age_days) do
    cutoff = Date.utc_today() |> Date.add(-max_age_days)

    case SystemVolume.list(path) do
      {:ok, files} ->
        Enum.each(files, fn filename ->
          prune_file(path, filename, cutoff)
        end)

      {:error, :system_volume_not_found} ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to list directory for retention pruning",
          path: path,
          reason: inspect(reason)
        )

        :ok
    end
  end

  defp prune_file(dir, filename, cutoff) do
    with {:ok, date} <- parse_file_date(filename),
         true <- Date.compare(date, cutoff) == :lt do
      delete_expired_file(Path.join(dir, filename))
    else
      :error ->
        Logger.warning("Retention skipping file with unparseable date",
          path: Path.join(dir, filename)
        )

      false ->
        :ok
    end
  end

  defp delete_expired_file(path) do
    case SystemVolume.delete(path) do
      :ok ->
        Logger.info("Retention deleted file", path: path)

      {:error, reason} ->
        Logger.warning("Retention failed to delete file",
          path: path,
          reason: inspect(reason)
        )
    end
  end

  @doc """
  Parses a date from a filename in the format `YYYY-MM-DD.jsonl` or `YYYY-MM-DD`.

  Returns `{:ok, Date.t()}` on success or `:error` if the filename cannot be parsed.
  """
  @spec parse_file_date(String.t()) :: {:ok, Date.t()} | :error
  def parse_file_date(filename) do
    basename = Path.rootname(filename)

    case Date.from_iso8601(basename) do
      {:ok, date} -> {:ok, date}
      {:error, _} -> :error
    end
  end
end
