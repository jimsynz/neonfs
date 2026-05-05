defmodule NeonFS.Transport.CertRenewalTest do
  use ExUnit.Case

  alias NeonFS.Transport.{CertRenewal, TLS}

  @moduletag :tmp_dir

  # Short interval for tests (50ms)
  @test_check_interval 50

  # Budget for `assert_receive` on telemetry events. 5s is comfortable
  # locally (renewal completes in <100ms there) and tolerates the
  # scheduling pauses we see on busy CI runners between
  # "initiating renewal" and the success event reaching the test
  # mailbox. The previous 2s budget flaked on slow runners (#763).
  @receive_timeout_ms 5_000

  setup %{tmp_dir: tmp_dir} do
    File.mkdir_p!(tmp_dir)
    Application.put_env(:neonfs_client, :tls_dir, tmp_dir)

    on_exit(fn ->
      Application.delete_env(:neonfs_client, :tls_dir)
      Application.delete_env(:neonfs_client, :renewal_threshold_days)
      File.rm_rf!(tmp_dir)
    end)

    %{tmp_dir: tmp_dir}
  end

  defp generate_ca do
    TLS.generate_ca("cert-renewal-test")
  end

  defp write_cert_with_validity(_tmp_dir, ca_cert, ca_key, validity_days) do
    Application.put_env(:neonfs_client, :node_validity_days, validity_days)
    node_key = TLS.generate_node_key()
    node_name = Atom.to_string(Node.self())
    csr = TLS.create_csr(node_key, node_name)
    node_cert = TLS.sign_csr(csr, "localhost", ca_cert, ca_key, 1)
    TLS.write_local_tls(ca_cert, node_cert, node_key)
    Application.delete_env(:neonfs_client, :node_validity_days)
    {node_cert, node_key}
  end

  defp make_renew_fun(ca_cert, ca_key, serial) do
    fn csr, hostname ->
      node_cert = TLS.sign_csr(csr, hostname, ca_cert, ca_key, serial)
      {:ok, node_cert, ca_cert}
    end
  end

  describe "starts without crashing" do
    test "when no local certificate exists" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :cert_renewal, :check]
        ])

      pid =
        start_supervised!(
          {CertRenewal,
           name: :"renewal_no_cert_#{:rand.uniform(100_000)}",
           check_interval_ms: @test_check_interval}
        )

      assert Process.alive?(pid)

      # Wait for at least one check cycle to complete without crash
      assert_receive {[:neonfs, :cert_renewal, :check], ^ref, _, _}, @receive_timeout_ms
      assert Process.alive?(pid)
    end
  end

  describe "renewal triggered when cert is near expiry" do
    test "renews certificate within threshold", %{tmp_dir: tmp_dir} do
      {ca_cert, ca_key} = generate_ca()
      # 5-day cert, default threshold is 30 — so renewal should trigger
      write_cert_with_validity(tmp_dir, ca_cert, ca_key, 5)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :cert_renewal, :check]
        ])

      start_supervised!(
        {CertRenewal,
         name: :"renewal_near_#{:rand.uniform(100_000)}",
         check_interval_ms: @test_check_interval,
         renew_fun: make_renew_fun(ca_cert, ca_key, 200)}
      )

      assert_receive {[:neonfs, :cert_renewal, :check], ^ref, %{days_remaining: _},
                      %{action: :renewal_triggered}},
                     @receive_timeout_ms
    end

    test "writes new cert to local filesystem after renewal", %{tmp_dir: tmp_dir} do
      {ca_cert, ca_key} = generate_ca()
      {old_cert, _old_key} = write_cert_with_validity(tmp_dir, ca_cert, ca_key, 5)
      old_info = TLS.certificate_info(old_cert)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :cert_renewal, :success]
        ])

      start_supervised!(
        {CertRenewal,
         name: :"renewal_write_#{:rand.uniform(100_000)}",
         check_interval_ms: @test_check_interval,
         renew_fun: make_renew_fun(ca_cert, ca_key, 999)}
      )

      # Wait for the success telemetry event (emitted after write_local_tls)
      assert_receive {[:neonfs, :cert_renewal, :success], ^ref, %{},
                      %{old_serial: _, new_serial: 999}},
                     @receive_timeout_ms

      # Verify the cert on disk matches
      {:ok, new_cert} = TLS.read_local_cert()
      new_info = TLS.certificate_info(new_cert)
      assert new_info.serial == 999
      assert new_info.serial != old_info.serial
    end
  end

  describe "renewal not triggered when cert has plenty of time" do
    test "does not renew when expiry is beyond threshold", %{tmp_dir: tmp_dir} do
      {ca_cert, ca_key} = generate_ca()
      # 365-day cert, threshold default 30 — no renewal needed
      write_cert_with_validity(tmp_dir, ca_cert, ca_key, 365)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :cert_renewal, :check],
          [:neonfs, :cert_renewal, :success]
        ])

      start_supervised!(
        {CertRenewal,
         name: :"renewal_far_#{:rand.uniform(100_000)}",
         check_interval_ms: @test_check_interval,
         renew_fun: make_renew_fun(ca_cert, ca_key, 200)}
      )

      # Should get a :not_due check event, never a :success
      assert_receive {[:neonfs, :cert_renewal, :check], ^ref, _, %{action: :not_due}},
                     @receive_timeout_ms

      refute_received {[:neonfs, :cert_renewal, :success], ^ref, _, _}
    end
  end

  describe "failure handling and backoff" do
    test "retries on failure without crashing", %{tmp_dir: tmp_dir} do
      {ca_cert, ca_key} = generate_ca()
      write_cert_with_validity(tmp_dir, ca_cert, ca_key, 5)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :cert_renewal, :failure]
        ])

      renew_fun = fn _csr, _hostname ->
        {:error, :test_failure}
      end

      pid =
        start_supervised!(
          {CertRenewal,
           name: :"renewal_fail_#{:rand.uniform(100_000)}",
           check_interval_ms: @test_check_interval,
           renew_fun: renew_fun}
        )

      # First attempt should emit a failure event
      assert_receive {[:neonfs, :cert_renewal, :failure], ^ref, %{},
                      %{reason: :test_failure, attempt: 1}},
                     @receive_timeout_ms

      assert Process.alive?(pid)

      # Since backoff is 1h in real config, we won't see a second attempt
      # in a short test. The key assertion is that it didn't crash.
      assert Process.alive?(pid)
    end
  end

  describe "calculate_backoff/1" do
    test "first failure backs off to 1 hour" do
      assert CertRenewal.calculate_backoff(1) == 3_600_000
    end

    test "second failure backs off to 2 hours" do
      assert CertRenewal.calculate_backoff(2) == 7_200_000
    end

    test "third failure backs off to 4 hours" do
      assert CertRenewal.calculate_backoff(3) == 14_400_000
    end

    test "backs off caps at 24 hours" do
      # 2^4 * 1h = 16h, 2^5 * 1h = 32h -> capped to 24h
      assert CertRenewal.calculate_backoff(5) == 57_600_000
      assert CertRenewal.calculate_backoff(6) == 86_400_000
      assert CertRenewal.calculate_backoff(7) == 86_400_000
      assert CertRenewal.calculate_backoff(100) == 86_400_000
    end
  end

  describe "configurable interval and threshold" do
    test "respects custom check_interval_ms", %{tmp_dir: tmp_dir} do
      {ca_cert, ca_key} = generate_ca()
      write_cert_with_validity(tmp_dir, ca_cert, ca_key, 5)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :cert_renewal, :success]
        ])

      custom_interval = 100

      start_supervised!(
        {CertRenewal,
         name: :"renewal_interval_#{:rand.uniform(100_000)}",
         check_interval_ms: custom_interval,
         renew_fun: make_renew_fun(ca_cert, ca_key, 300)}
      )

      # Should receive the success event within a reasonable time
      assert_receive {[:neonfs, :cert_renewal, :success], ^ref, %{}, _}, @receive_timeout_ms
    end

    test "respects renewal_threshold_days from application env", %{tmp_dir: tmp_dir} do
      {ca_cert, ca_key} = generate_ca()
      # 60-day cert
      write_cert_with_validity(tmp_dir, ca_cert, ca_key, 60)

      # Set threshold to 90 days — so 60-day cert should trigger renewal
      Application.put_env(:neonfs_client, :renewal_threshold_days, 90)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :cert_renewal, :check]
        ])

      start_supervised!(
        {CertRenewal,
         name: :"renewal_thresh_#{:rand.uniform(100_000)}",
         check_interval_ms: @test_check_interval,
         renew_fun: make_renew_fun(ca_cert, ca_key, 400)}
      )

      assert_receive {[:neonfs, :cert_renewal, :check], ^ref, _, %{action: :renewal_triggered}},
                     @receive_timeout_ms
    end

    test "does not renew 60-day cert with default 30-day threshold", %{tmp_dir: tmp_dir} do
      {ca_cert, ca_key} = generate_ca()
      write_cert_with_validity(tmp_dir, ca_cert, ca_key, 60)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :cert_renewal, :check],
          [:neonfs, :cert_renewal, :success]
        ])

      start_supervised!(
        {CertRenewal,
         name: :"renewal_no_thresh_#{:rand.uniform(100_000)}",
         check_interval_ms: @test_check_interval,
         renew_fun: make_renew_fun(ca_cert, ca_key, 500)}
      )

      assert_receive {[:neonfs, :cert_renewal, :check], ^ref, _, %{action: :not_due}},
                     @receive_timeout_ms

      refute_received {[:neonfs, :cert_renewal, :success], ^ref, _, _}
    end
  end
end
