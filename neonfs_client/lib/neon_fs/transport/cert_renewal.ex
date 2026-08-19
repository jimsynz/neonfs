defmodule NeonFS.Transport.CertRenewal do
  @moduledoc """
  Periodically checks the local node certificate's expiry and renews it
  before it expires.

  When within `renewal_threshold_days` of expiry (default 30 days), generates
  a new ECDSA P-256 keypair and CSR, sends it to a core node for signing via
  `NeonFS.Client.Router.call/4`, and writes the new credentials locally.

  On failure, retries with exponential backoff (1h, 2h, 4h, max 24h).

  Started by `NeonFS.Client.Application`, so every node type that depends on
  `neonfs_client` gets it exactly once — including omnibus, where they share a
  BEAM node. A node that holds no certificate is a daily no-op.

  ## When renewal cannot happen

  An expired certificate cannot be renewed. Distribution verifies peers at the
  TLS handshake, and `Router.call/4` needs the very connection the expired
  certificate is what establishes. Recovery is a documented delete-and-redeem
  procedure — the invite redemption runs over plain HTTP and needs no node
  certificate — described under "Recovering a node whose certificate expired"
  on the wiki's Cluster CA page.

  `health_check/0` is what warns before that point is reached; it is registered
  as the `client_cert_expiry` subsystem.

  ## Telemetry Events

    * `[:neonfs, :cert_renewal, :check]` — expiry check performed
      - Measurements: `%{days_remaining: integer()}`
      - Metadata: `%{action: :not_due | :renewal_triggered | :no_cert}`

    * `[:neonfs, :cert_renewal, :success]` — certificate renewed and written
      - Measurements: `%{}`
      - Metadata: `%{old_serial: integer(), new_serial: integer()}`

    * `[:neonfs, :cert_renewal, :failure]` — renewal attempt failed
      - Measurements: `%{}`
      - Metadata: `%{reason: term(), attempt: integer()}`
  """

  use GenServer
  require Logger

  alias NeonFS.Client.Router
  alias NeonFS.Transport.TLS

  @default_check_interval_ms 86_400_000
  @initial_backoff_ms 3_600_000
  @max_backoff_ms 86_400_000

  # Checks run daily, so this still leaves roughly seven attempts between the
  # alarm being raised and the certificate actually expiring.
  @unhealthy_threshold_days 7

  ## Client API

  @doc """
  Starts the CertRenewal GenServer.

  ## Options
  - `:check_interval_ms` — interval between expiry checks (default from app env
    `:cert_check_interval_ms`, fallback #{@default_check_interval_ms} ms / 24h)
  - `:renew_fun` — `(TLS.csr(), String.t() -> {:ok, TLS.cert(), TLS.cert()} | {:error, term()})`,
    override for testing; defaults to `Router.call(CertificateAuthority, :sign_node_csr, ...)`
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Reports the local certificate's remaining life as a
  `NeonFS.Client.HealthCheck` subsystem report.

    * `:unhealthy` — under #{@unhealthy_threshold_days} days to expiry, or the
      renewal process is not running.
    * `:degraded` — inside the renewal threshold *and* renewal has already
      failed at least once. Being inside the window is not itself a fault: every
      node passes through it annually while renewing normally.
    * `:healthy` — otherwise, including a node that holds no certificate at all.

  A node under #{@unhealthy_threshold_days} days reports `:unhealthy` while it
  is still serving correctly, so a readiness probe or load balancer may pull it.
  That is deliberate: raising the alarm only once the certificate has expired
  reports the outage instead of warning of it.
  """
  @spec health_check() :: NeonFS.Client.HealthCheck.subsystem_report()
  def health_check(name \\ __MODULE__) do
    case Process.whereis(name) do
      nil -> %{status: :unhealthy, reason: :not_running}
      pid -> expiry_report(GenServer.call(pid, :consecutive_failures))
    end
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    check_interval =
      Keyword.get_lazy(opts, :check_interval_ms, fn ->
        Application.get_env(:neonfs_client, :cert_check_interval_ms, @default_check_interval_ms)
      end)

    state = %{
      check_interval_ms: check_interval,
      consecutive_failures: 0,
      renew_fun: Keyword.get(opts, :renew_fun)
    }

    schedule_check(check_interval)
    {:ok, state}
  end

  @impl true
  def handle_call(:consecutive_failures, _from, state) do
    {:reply, state.consecutive_failures, state}
  end

  @impl true
  def handle_info(:check_renewal, state) do
    case check_and_maybe_renew(state) do
      :no_cert ->
        schedule_check(state.check_interval_ms)
        {:noreply, state}

      :not_due ->
        schedule_check(state.check_interval_ms)
        {:noreply, %{state | consecutive_failures: 0}}

      :ok ->
        schedule_check(state.check_interval_ms)
        {:noreply, %{state | consecutive_failures: 0}}

      {:error, reason} ->
        failures = state.consecutive_failures + 1
        backoff = calculate_backoff(failures)

        Logger.warning("Certificate renewal failed",
          attempt: failures,
          reason: inspect(reason),
          retry_minutes: div(backoff, 60_000)
        )

        schedule_check(backoff)
        {:noreply, %{state | consecutive_failures: failures}}
    end
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  ## Internal — exposed for testing via @doc false

  @doc false
  @spec calculate_backoff(pos_integer()) :: non_neg_integer()
  def calculate_backoff(consecutive_failures) do
    backoff = trunc(@initial_backoff_ms * :math.pow(2, consecutive_failures - 1))
    min(backoff, @max_backoff_ms)
  end

  ## Private

  defp expiry_report(consecutive_failures) do
    case TLS.read_local_cert() do
      {:error, :not_found} ->
        %{status: :healthy, reason: :no_cert}

      {:ok, cert} ->
        days_remaining = TLS.days_until_expiry(cert)

        %{
          status: expiry_status(days_remaining, consecutive_failures),
          days_remaining: days_remaining,
          consecutive_failures: consecutive_failures
        }
    end
  end

  defp expiry_status(days_remaining, _consecutive_failures)
       when days_remaining <= @unhealthy_threshold_days,
       do: :unhealthy

  defp expiry_status(days_remaining, consecutive_failures) when consecutive_failures > 0 do
    if days_remaining <= TLS.renewal_threshold_days(), do: :degraded, else: :healthy
  end

  defp expiry_status(_days_remaining, _consecutive_failures), do: :healthy

  defp check_and_maybe_renew(state) do
    case TLS.read_local_cert() do
      {:error, :not_found} ->
        Logger.debug("No local certificate found, skipping renewal check")

        :telemetry.execute(
          [:neonfs, :cert_renewal, :check],
          %{days_remaining: -1},
          %{action: :no_cert}
        )

        :no_cert

      {:ok, cert} ->
        days_remaining = TLS.days_until_expiry(cert)
        threshold = TLS.renewal_threshold_days()

        if days_remaining <= threshold do
          Logger.info("Certificate expiring, initiating renewal",
            days_remaining: days_remaining,
            threshold: threshold
          )

          :telemetry.execute(
            [:neonfs, :cert_renewal, :check],
            %{days_remaining: days_remaining},
            %{action: :renewal_triggered}
          )

          do_renew(cert, state)
        else
          Logger.debug("Certificate not due for renewal", days_remaining: days_remaining)

          :telemetry.execute(
            [:neonfs, :cert_renewal, :check],
            %{days_remaining: days_remaining},
            %{action: :not_due}
          )

          :not_due
        end
    end
  end

  defp do_renew(old_cert, state) do
    node_name = Atom.to_string(Node.self())
    hostname = node_name |> String.split("@") |> List.last()
    node_key = TLS.generate_node_key()
    csr = TLS.create_csr(node_key, node_name)

    case sign_csr(csr, hostname, state) do
      {:ok, node_cert, ca_cert} ->
        TLS.write_local_tls(ca_cert, node_cert, node_key)

        old_info = TLS.certificate_info(old_cert)
        new_info = TLS.certificate_info(node_cert)

        Logger.info("Certificate renewed successfully",
          old_expiry: old_info.not_after,
          new_expiry: new_info.not_after
        )

        :telemetry.execute(
          [:neonfs, :cert_renewal, :success],
          %{},
          %{old_serial: old_info.serial, new_serial: new_info.serial}
        )

        :ok

      {:error, reason} ->
        :telemetry.execute(
          [:neonfs, :cert_renewal, :failure],
          %{},
          %{reason: reason, attempt: state.consecutive_failures + 1}
        )

        {:error, reason}
    end
  end

  defp sign_csr(csr, hostname, %{renew_fun: fun}) when is_function(fun, 2) do
    fun.(csr, hostname)
  end

  defp sign_csr(csr, hostname, _state) do
    Router.call(NeonFS.Core.CertificateAuthority, :sign_node_csr, [csr, hostname])
  end

  defp schedule_check(interval_ms) do
    Process.send_after(self(), :check_renewal, interval_ms)
  end
end
