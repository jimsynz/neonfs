defmodule NeonFS.Core.BanditLauncher do
  @moduledoc """
  Wraps `Bandit.start_link/1` with retry on transient `{:inet_async, :timeout}`
  failures.

  Under heavy concurrent load the ThousandIsland listener that Bandit sits on
  can time out during `gen_tcp.listen`, fail asynchronously with
  `{:inet_async, :timeout}`, and bring the Bandit supervisor down with it.
  The underlying condition is transient — no socket is actually bound — and
  retrying succeeds. This module encapsulates the retry so a flake on
  `MetricsSupervisor` startup doesn't cascade into the peer node's whole
  supervision tree failing (observed in #433, hits `:shared`-mode
  integration tests).

  Drop-in child-spec replacement: change

      %{id: Bandit, start: {Bandit, :start_link, [opts]}}

  to

      %{id: Bandit, start: {NeonFS.Core.BanditLauncher, :start_link, [opts]}}

  and the retry is applied transparently. Options, return shape, and
  supervisor semantics are identical to `Bandit.start_link/1`.

  Models the pattern established in
  `NeonFS.Integration.ClusterCase.start_bandit_with_retry/2`, which covers
  the same flake at the test-process level. This module covers the
  peer-node / production process level.
  """

  require Logger

  @default_retries 5
  @default_backoff_ms 200

  @doc """
  Start a Bandit HTTP server, retrying on transient
  `{:inet_async, :timeout}` failures.

  Supervisor-compatible MFA target — use in a child spec in place of
  `{Bandit, :start_link, [opts]}`. Accepts the same keyword options as
  `Bandit.start_link/1` and returns `{:ok, pid} | {:error, term()}` with
  identical semantics on success; transient listener flakes are absorbed
  via up to #{@default_retries} retries with a #{@default_backoff_ms}ms
  backoff between each.
  """
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(bandit_opts) when is_list(bandit_opts) do
    previous_trap = Process.flag(:trap_exit, true)

    try do
      do_start(bandit_opts, @default_retries)
    after
      Process.flag(:trap_exit, previous_trap)
    end
  end

  defp do_start(_bandit_opts, 0) do
    {:error, :bandit_inet_async_timeout}
  end

  defp do_start(bandit_opts, attempts_left) do
    case Bandit.start_link(bandit_opts) do
      {:ok, server} ->
        case confirm_listener(server) do
          :ok ->
            {:ok, server}

          :transient ->
            Logger.debug(
              "Bandit listener flaked during startup, retrying (#{attempts_left - 1} left)"
            )

            kill_failed(server)
            Process.sleep(@default_backoff_ms)
            do_start(bandit_opts, attempts_left - 1)
        end

      {:error, reason} ->
        if transient_listener_error?(reason) do
          Logger.debug(
            "Bandit.start_link returned transient error (#{inspect(reason)}), " <>
              "retrying (#{attempts_left - 1} left)"
          )

          Process.sleep(@default_backoff_ms)
          do_start(bandit_opts, attempts_left - 1)
        else
          {:error, reason}
        end
    end
  end

  # After `Bandit.start_link` returns `{:ok, pid}` the listener can still
  # crash asynchronously with `{:inet_async, :timeout}` — the EXIT arrives
  # via the link we just acquired. Settle briefly so any delayed EXIT
  # lands in our mailbox (`trap_exit` ensures it does), then confirm the
  # listener is reachable via its live GenServer API.
  defp confirm_listener(server) do
    receive do
      {:EXIT, ^server, reason} ->
        if transient_listener_error?(reason), do: :transient, else: exit(reason)
    after
      150 ->
        try do
          case ThousandIsland.listener_info(server) do
            {:ok, {_ip, _port}} -> :ok
            :error -> :transient
          end
        catch
          :exit, _ -> :transient
        end
    end
  end

  defp kill_failed(server) do
    Process.unlink(server)
    Process.exit(server, :kill)

    # Drain any EXIT message that arrives after the unlink races the kill.
    receive do
      {:EXIT, ^server, _} -> :ok
    after
      0 -> :ok
    end
  end

  defp transient_listener_error?({:inet_async, :timeout}), do: true

  defp transient_listener_error?({:shutdown, {:failed_to_start_child, _, inner}}),
    do: transient_listener_error?(inner)

  defp transient_listener_error?(_), do: false
end
