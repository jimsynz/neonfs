defmodule NeonFS.WebDAV.LockStore.Cleaner do
  @moduledoc """
  Periodic sweep of expired entries from the WebDAV lock store ETS table.

  Lock-null entries (and regular lock entries) are cleaned up lazily on access,
  but abandoned entries — where a client disconnects without unlocking — remain
  in ETS until something queries that path. This GenServer periodically removes
  expired entries so the table stays bounded.

  ## Telemetry

    * `[:neonfs, :webdav, :lock_store, :cleanup]` — emitted after each sweep
      * Measurements: `%{expired_count: non_neg_integer()}`
      * Metadata: `%{}`
  """

  use GenServer

  @default_interval_ms 180_000

  @doc """
  Starts the lock store cleaner.

  ## Options

    * `:interval_ms` — sweep interval in milliseconds (default: #{@default_interval_ms})
    * `:table` — ETS table name to sweep (default: `NeonFS.WebDAV.LockStore`)
    * `:name` — GenServer name (default: `__MODULE__`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    interval_ms = Keyword.get(opts, :interval_ms, @default_interval_ms)
    table = Keyword.get(opts, :table, NeonFS.WebDAV.LockStore)

    schedule_sweep(interval_ms)

    {:ok, %{interval_ms: interval_ms, table: table}}
  end

  @impl true
  def handle_info(:sweep, state) do
    expired_count = sweep_expired(state.table)

    :telemetry.execute(
      [:neonfs, :webdav, :lock_store, :cleanup],
      %{expired_count: expired_count},
      %{}
    )

    schedule_sweep(state.interval_ms)

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp sweep_expired(table) do
    now = System.system_time(:second)

    expired =
      :ets.select(table, [
        {{:"$1", %{expires_at: :"$2"}}, [{:<, :"$2", now}], [:"$1"]}
      ])

    Enum.each(expired, &:ets.delete(table, &1))

    length(expired)
  end

  defp schedule_sweep(interval_ms) do
    Process.send_after(self(), :sweep, interval_ms)
  end
end
