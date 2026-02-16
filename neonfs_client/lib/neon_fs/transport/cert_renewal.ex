defmodule NeonFS.Transport.CertRenewal do
  @moduledoc """
  Periodically checks the local node certificate's expiry and renews it
  before it expires.

  When within `renewal_threshold_days` of expiry (default 30 days), generates
  a new ECDSA P-256 keypair and CSR, sends it to a core node for signing via
  `NeonFS.Client.Router.call/4`, and writes the new credentials locally.

  On failure, retries with exponential backoff (1h, 2h, 4h, max 24h).
  """

  use GenServer
  require Logger

  alias NeonFS.Client.Router
  alias NeonFS.Transport.TLS

  @default_check_interval_ms 86_400_000
  @initial_backoff_ms 3_600_000
  @max_backoff_ms 86_400_000

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

        Logger.warning(
          "Certificate renewal failed (attempt #{failures}): #{inspect(reason)}. " <>
            "Retrying in #{div(backoff, 60_000)} minutes"
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

  defp check_and_maybe_renew(state) do
    case TLS.read_local_cert() do
      {:error, :not_found} ->
        Logger.debug("No local certificate found, skipping renewal check")
        :no_cert

      {:ok, cert} ->
        days_remaining = TLS.days_until_expiry(cert)
        threshold = TLS.renewal_threshold_days()

        if days_remaining <= threshold do
          Logger.info(
            "Certificate expires in #{days_remaining} days " <>
              "(threshold: #{threshold}), initiating renewal"
          )

          do_renew(cert, state)
        else
          Logger.debug("Certificate expires in #{days_remaining} days, no renewal needed")

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

        Logger.info(
          "Certificate renewed successfully. " <>
            "Old expiry: #{old_info.not_after}, new expiry: #{new_info.not_after}"
        )

        :ok

      {:error, reason} ->
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
