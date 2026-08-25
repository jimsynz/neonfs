defmodule NeonFS.Transport.CAExpiry do
  @moduledoc """
  Reports how much life the cluster CA certificate has left.

  The cluster CA is issued for `:ca_validity_days` — 3650 by default — and
  **nothing renews it**. Node certificates renew unattended via
  `NeonFS.Transport.CertRenewal`; their trust anchor does not, so on day 3651
  of a cluster's life every distribution handshake fails path validation and
  the cluster stops being a cluster.

  This check is what makes that visible far enough ahead to schedule the
  rotation an operator already has: `cluster ca rotate`. Rotation stays
  operator-triggered. It generates a fresh CA keypair and reissues every
  certificate in the fleet, which is not something to fire on a calendar date
  with nobody watching.

  Registered as the `client_ca_expiry` subsystem by
  `NeonFS.Client.Application`, separately from `client_cert_expiry`. Folding
  the two together would let the worse status win, so a CA at 179 days would
  mark the node certificate degraded — masking the one certificate that does
  have an automated remedy.

  ## Thresholds

  `:degraded` at 180 days and `:unhealthy` at 30, against the node
  certificate's 30 and 7. The node's numbers leave room for roughly seven
  daily renewal attempts. There is no automated retry here, so these are
  instead how long an operator needs to notice, schedule and run a disruptive
  cluster-wide operation. Both are configurable —
  `:ca_renewal_threshold_days` and `:ca_unhealthy_threshold_days` — because
  they will not be revisited until they matter, which is years out.

  ## This is a cluster-level signal wearing a node-level shape

  Every node reports the same CA, so a 20-node cluster emits 20 copies of one
  problem and whatever scrapes them has to deduplicate. Worse, when the CA
  does reach 30 days every node goes `:unhealthy` at once and there is no
  healthy node to fail over to — so anything routing on `/health` should
  treat this subsystem as cluster-wide rather than as a reason to pull a
  node.

  A node whose report disagrees with its peers is itself the signal: it means
  that node's `ca.crt` is stale relative to the rest of the cluster.
  """

  alias NeonFS.Client.HealthCheck
  alias NeonFS.Transport.TLS

  @default_renewal_threshold_days 180
  @default_unhealthy_threshold_days 30

  @doc """
  Reports the cluster CA's remaining life as a
  `NeonFS.Client.HealthCheck` subsystem report.

    * `:unhealthy` — at or under `ca_unhealthy_threshold_days/0`.
    * `:degraded` — at or under `ca_renewal_threshold_days/0`.
    * `:healthy` — otherwise, including a node that holds no cluster CA at
      all. A node that has not joined yet has nothing to warn about.
  """
  @spec health_check() :: HealthCheck.subsystem_report()
  def health_check do
    case TLS.read_local_ca_cert() do
      {:error, :not_found} ->
        %{status: :healthy, reason: :no_ca}

      {:ok, ca_cert} ->
        days_remaining = TLS.days_until_expiry(ca_cert)

        %{status: expiry_status(days_remaining), days_remaining: days_remaining}
    end
  end

  @doc """
  Days of CA life below which the check reports `:degraded`.
  """
  @spec ca_renewal_threshold_days() :: pos_integer()
  def ca_renewal_threshold_days do
    Application.get_env(
      :neonfs_client,
      :ca_renewal_threshold_days,
      @default_renewal_threshold_days
    )
  end

  @doc """
  Days of CA life below which the check reports `:unhealthy`.
  """
  @spec ca_unhealthy_threshold_days() :: pos_integer()
  def ca_unhealthy_threshold_days do
    Application.get_env(
      :neonfs_client,
      :ca_unhealthy_threshold_days,
      @default_unhealthy_threshold_days
    )
  end

  defp expiry_status(days_remaining) do
    cond do
      days_remaining <= ca_unhealthy_threshold_days() -> :unhealthy
      days_remaining <= ca_renewal_threshold_days() -> :degraded
      true -> :healthy
    end
  end
end
