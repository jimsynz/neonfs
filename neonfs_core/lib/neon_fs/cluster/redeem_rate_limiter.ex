defmodule NeonFS.Cluster.RedeemRateLimiter do
  @moduledoc """
  Per-source-IP fixed-window rate limiter for the invite-redemption HTTP
  endpoint (#1198).

  Redemption is a low-volume operation — a node redeems an invite once
  when it joins — so requests are serialised through this GenServer
  rather than a lock-free ETS counter. The throughput ceiling is
  irrelevant for the join path, and serialising keeps the window
  bookkeeping race-free.

  This is defence-in-depth, not the security boundary: single-use
  enforcement (the Ra `:redeem_invite` command) is what prevents token
  replay. `allow?/2` therefore fails *open* if the limiter process is not
  running, so a limiter crash can never wedge the join flow.

  The limit is configurable via `config :neonfs_core, :redeem_rate_limit,
  {max_requests, window_seconds}`; it defaults to 10 requests per 60 s
  per IP.
  """

  use GenServer

  @default_max 10
  @default_window_s 60
  @sweep_interval_ms 60_000

  @type bucket :: {window_start :: integer(), count :: pos_integer()}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  Records a request from `ip` and returns whether it is within the limit.

  Fails open (returns `true`) if the limiter process is unavailable —
  rate limiting is defence-in-depth, not the replay-prevention boundary.
  """
  @spec allow?(term(), GenServer.server()) :: boolean()
  def allow?(ip, server \\ __MODULE__) do
    GenServer.call(server, {:allow, ip})
  catch
    :exit, _ -> true
  end

  @impl true
  def init(opts) do
    {max, window_s} = config(opts)
    schedule_sweep()
    {:ok, %{buckets: %{}, max: max, window_s: window_s}}
  end

  @impl true
  def handle_call({:allow, ip}, _from, state) do
    now = System.os_time(:second)
    {allowed, buckets} = check_and_record(state.buckets, ip, now, state.max, state.window_s)
    {:reply, allowed, %{state | buckets: buckets}}
  end

  @impl true
  def handle_info(:sweep, state) do
    now = System.os_time(:second)
    live = :maps.filter(fn _ip, {start, _} -> now - start < state.window_s end, state.buckets)
    schedule_sweep()
    {:noreply, %{state | buckets: live}}
  end

  defp check_and_record(buckets, ip, now, max, window_s) do
    case Map.get(buckets, ip) do
      {start, count} when now - start < window_s and count >= max ->
        {false, buckets}

      {start, count} when now - start < window_s ->
        {true, Map.put(buckets, ip, {start, count + 1})}

      _expired_or_absent ->
        {true, Map.put(buckets, ip, {now, 1})}
    end
  end

  defp config(opts) do
    case Keyword.get(opts, :rate_limit) ||
           Application.get_env(:neonfs_core, :redeem_rate_limit) do
      {max, window_s}
      when is_integer(max) and max > 0 and is_integer(window_s) and window_s > 0 ->
        {max, window_s}

      _ ->
        {@default_max, @default_window_s}
    end
  end

  defp schedule_sweep, do: Process.send_after(self(), :sweep, @sweep_interval_ms)
end
