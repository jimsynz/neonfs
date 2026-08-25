defmodule NeonFS.Transport.CAExpiryTest do
  use ExUnit.Case

  alias NeonFS.Transport.{CAExpiry, TLS}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    File.mkdir_p!(tmp_dir)
    Application.put_env(:neonfs_client, :tls_dir, tmp_dir)

    on_exit(fn ->
      for key <- [
            :tls_dir,
            :ca_validity_days,
            :ca_renewal_threshold_days,
            :ca_unhealthy_threshold_days
          ] do
        Application.delete_env(:neonfs_client, key)
      end

      File.rm_rf!(tmp_dir)
    end)

    :ok
  end

  describe "health_check/0" do
    # A node that has not joined yet has no cluster CA and nothing to warn
    # about — the same shape `CertRenewal` uses for a node with no cert.
    test "is healthy on a node that holds no cluster CA" do
      assert %{status: :healthy, reason: :no_ca} = CAExpiry.health_check()
    end

    test "is healthy on a freshly issued CA", %{tmp_dir: tmp_dir} do
      write_ca(tmp_dir, 3650)

      assert %{status: :healthy, days_remaining: days} = CAExpiry.health_check()
      assert days > CAExpiry.ca_renewal_threshold_days()
    end

    # The point of the whole check: enough notice to schedule a rolling
    # rotation, not a report that one is overdue.
    test "is degraded inside the scheduling window", %{tmp_dir: tmp_dir} do
      write_ca(tmp_dir, 100)

      assert %{status: :degraded, days_remaining: days} = CAExpiry.health_check()
      assert days <= CAExpiry.ca_renewal_threshold_days()
      assert days > CAExpiry.ca_unhealthy_threshold_days()
    end

    test "is unhealthy once the rotation can no longer comfortably be scheduled", %{
      tmp_dir: tmp_dir
    } do
      write_ca(tmp_dir, 10)

      assert %{status: :unhealthy, days_remaining: days} = CAExpiry.health_check()
      assert days <= CAExpiry.ca_unhealthy_threshold_days()
    end

    # 180 and 30 are a guess at how long an operator needs, and will not be
    # revisited until they matter — years out. Being able to move them
    # without a release is the whole reason they are config.
    test "honours configured thresholds", %{tmp_dir: tmp_dir} do
      write_ca(tmp_dir, 100)
      assert %{status: :degraded} = CAExpiry.health_check()

      Application.put_env(:neonfs_client, :ca_renewal_threshold_days, 50)
      assert %{status: :healthy} = CAExpiry.health_check()

      Application.put_env(:neonfs_client, :ca_unhealthy_threshold_days, 120)
      assert %{status: :unhealthy} = CAExpiry.health_check()
    end

    # `cert_expiry` reads `node.crt` and this reads `ca.crt`. Reading the
    # wrong one would look healthy for 3650 days on a node whose *node*
    # certificate is a year from expiry, or vice versa.
    test "reports the CA's life, not the node certificate's" do
      Application.put_env(:neonfs_client, :ca_validity_days, 3650)
      {ca_cert, ca_key} = TLS.generate_ca("ca-expiry-test")

      node_key = TLS.generate_node_key()

      node_cert =
        node_key
        |> TLS.create_csr(Atom.to_string(Node.self()))
        |> TLS.sign_csr("localhost", ca_cert, ca_key, 1)

      :ok = TLS.write_local_tls(ca_cert, node_cert, node_key)

      assert %{days_remaining: ca_days} = CAExpiry.health_check()
      assert ca_days > TLS.days_until_expiry(node_cert)
    end
  end

  # Registration only runs when `:start_children?` is true, which it is not
  # under test — so without this the one line that actually exposes the
  # subsystem over `/health` would ship unexercised.
  describe "registration" do
    test "is wired into the shared client health checks" do
      checks = NeonFS.Client.Application.health_checks()

      assert %{status: :healthy, reason: :no_ca} = Keyword.fetch!(checks, :ca_expiry).()

      # Separate from `cert_expiry` on purpose: folded together the worse
      # status wins, and a CA at 179 days would mark the node certificate
      # degraded.
      assert Keyword.has_key?(checks, :cert_expiry)
    end
  end

  defp write_ca(tmp_dir, validity_days) do
    Application.put_env(:neonfs_client, :ca_validity_days, validity_days)
    {ca_cert, _ca_key} = TLS.generate_ca("ca-expiry-test")
    File.write!(Path.join(tmp_dir, "ca.crt"), TLS.encode_cert(ca_cert))
  end
end
