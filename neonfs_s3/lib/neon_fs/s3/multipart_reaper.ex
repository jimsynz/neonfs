defmodule NeonFS.S3.MultipartReaper do
  @moduledoc """
  Periodically sweeps abandoned multipart uploads from the cluster KV
  store (#1181).

  Multipart bookkeeping lives in the Ra-backed KV store, so an upload a
  client never completes or aborts would otherwise accumulate in cluster
  state forever. This is the baseline equivalent of S3's
  `AbortIncompleteMultipartUpload` lifecycle rule: drop an upload's
  `:meta` and `:part:` keys once `initiated` is older than a threshold
  (default 7 days).

  The sweep (`MultipartStore.expire_abandoned/1`) is idempotent — a
  `KV.delete` of an already-removed key is a no-op — so it's harmless for
  every S3 node to run its own reaper; whichever fires first wins and the
  rest find nothing to do.

  Configurable via application env:

    * `:multipart_max_age_seconds` — abandonment threshold (default 7 days)
    * `:multipart_reaper_interval_ms` — sweep cadence (default 1 hour)
  """

  use GenServer

  require Logger

  alias NeonFS.S3.MultipartStore

  @default_max_age_seconds 7 * 24 * 60 * 60
  @default_interval_ms 60 * 60 * 1000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    state = %{
      max_age_seconds:
        Keyword.get(
          opts,
          :max_age_seconds,
          config(:multipart_max_age_seconds, @default_max_age_seconds)
        ),
      interval_ms:
        Keyword.get(
          opts,
          :interval_ms,
          config(:multipart_reaper_interval_ms, @default_interval_ms)
        )
    }

    schedule(state.interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_info(:sweep, state) do
    swept = MultipartStore.expire_abandoned(state.max_age_seconds)

    if swept != [] do
      Logger.info("Reaped #{length(swept)} abandoned multipart upload(s)")
    end

    :telemetry.execute([:neonfs, :s3, :multipart, :reap], %{count: length(swept)}, %{})

    schedule(state.interval_ms)
    {:noreply, state}
  end

  defp schedule(interval_ms), do: Process.send_after(self(), :sweep, interval_ms)

  defp config(key, default), do: Application.get_env(:neonfs_s3, key, default)
end
