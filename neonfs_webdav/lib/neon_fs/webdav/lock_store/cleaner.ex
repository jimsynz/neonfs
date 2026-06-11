defmodule NeonFS.WebDAV.LockStore.Cleaner do
  @moduledoc """
  Periodic sweep of expired entries from the cluster KV lock index.

  Lock entries are cleaned up lazily on access, but abandoned entries —
  where a client disconnects without unlocking — remain in the KV store
  until something queries that token. This GenServer periodically
  removes expired entries so the index stays bounded.

  Entries that hold a `NeonFS.Core.NamespaceCoordinator` claim
  (`Depth: infinity` collection locks and lock-null reservations per
  #302) have the claim released on the coordinator before the KV entry
  is deleted, so the claim doesn't outlive the WebDAV-side TTL. The
  coordinator's holder-pid `:DOWN` cleanup is the durable backstop;
  this sweep is the cooperative path.

  Every WebDAV node runs a cleaner over the shared index; concurrent
  sweeps are safe — deletes are idempotent and claim release is
  best-effort.

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

    schedule_sweep(interval_ms)

    {:ok, %{interval_ms: interval_ms}}
  end

  @impl true
  def handle_info(:sweep, state) do
    expired_count = sweep_expired()

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

  defp sweep_expired do
    now = System.system_time(:second)

    expired =
      [include_expired: true]
      |> LockStore.active_locks()
      |> Enum.filter(&(&1.expires_at <= now))

    Enum.each(expired, fn info ->
      release_namespace_claim_for(info)
      LockStore.delete_entry(info.token)
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
