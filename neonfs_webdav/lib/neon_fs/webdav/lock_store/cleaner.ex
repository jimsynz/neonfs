defmodule NeonFS.WebDAV.LockStore.Cleaner do
  @moduledoc """
  Periodic sweep of expired entries from the WebDAV lock store ETS table.

  Lock-null entries (and regular lock entries) are cleaned up lazily on access,
  but abandoned entries — where a client disconnects without unlocking — remain
  in ETS until something queries that path. This GenServer periodically removes
  expired entries so the table stays bounded.

  Entries that hold a `NeonFS.Core.NamespaceCoordinator` claim
  (`Depth: infinity` collection locks and lock-null reservations per
  #302) have the claim released on the coordinator before the ETS row
  is deleted, so the claim doesn't outlive the WebDAV-side TTL. The
  coordinator's holder-pid `:DOWN` cleanup is the durable backstop;
  this sweep is the cooperative path.

  ## Telemetry

    * `[:neonfs, :webdav, :lock_store, :cleanup]` — emitted after each sweep
      * Measurements: `%{expired_count: non_neg_integer()}`
      * Metadata: `%{}`
  """

  use GenServer

  alias NeonFS.WebDAV.LockStore

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
        {{:"$1", :"$2"}, [{:<, {:map_get, :expires_at, :"$2"}, now}], [{{:"$1", :"$2"}}]}
      ])

    Enum.each(expired, fn {token, info} ->
      release_namespace_claim_for(info)
      :ets.delete(table, token)
    end)

    length(expired)
  end

  defp release_namespace_claim_for(%{namespace_claim_id: claim_id}) when is_binary(claim_id) do
    LockStore.release_namespace_claim(claim_id)
  end

  defp release_namespace_claim_for(_info), do: :ok

  defp schedule_sweep(interval_ms) do
    Process.send_after(self(), :sweep, interval_ms)
  end
end
