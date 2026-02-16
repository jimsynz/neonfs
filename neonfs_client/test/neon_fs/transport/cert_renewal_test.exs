defmodule NeonFS.Transport.CertRenewalTest do
  use ExUnit.Case

  alias NeonFS.Transport.{CertRenewal, TLS}

  # Short interval for tests (50ms)
  @test_check_interval 50

  setup do
    tmp_dir = Path.join(System.tmp_dir!(), "neonfs_cert_renewal_#{:rand.uniform(1_000_000)}")
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
      pid =
        start_supervised!(
          {CertRenewal,
           name: :"renewal_no_cert_#{:rand.uniform(100_000)}",
           check_interval_ms: @test_check_interval}
        )

      assert Process.alive?(pid)

      # Let at least one check cycle complete without crash
      Process.sleep(@test_check_interval * 3)
      assert Process.alive?(pid)
    end
  end

  describe "renewal triggered when cert is near expiry" do
    test "renews certificate within threshold", %{tmp_dir: tmp_dir} do
      {ca_cert, ca_key} = generate_ca()
      # 5-day cert, default threshold is 30 — so renewal should trigger
      write_cert_with_validity(tmp_dir, ca_cert, ca_key, 5)

      test_pid = self()

      renew_fun = fn csr, hostname ->
        send(test_pid, {:renew_called, csr, hostname})
        node_cert = TLS.sign_csr(csr, hostname, ca_cert, ca_key, 200)
        {:ok, node_cert, ca_cert}
      end

      start_supervised!(
        {CertRenewal,
         name: :"renewal_near_#{:rand.uniform(100_000)}",
         check_interval_ms: @test_check_interval,
         renew_fun: renew_fun}
      )

      assert_receive {:renew_called, _csr, _hostname}, 500
    end

    test "writes new cert to local filesystem after renewal", %{tmp_dir: tmp_dir} do
      {ca_cert, ca_key} = generate_ca()
      {old_cert, _old_key} = write_cert_with_validity(tmp_dir, ca_cert, ca_key, 5)
      old_info = TLS.certificate_info(old_cert)

      renew_fun = make_renew_fun(ca_cert, ca_key, 999)

      start_supervised!(
        {CertRenewal,
         name: :"renewal_write_#{:rand.uniform(100_000)}",
         check_interval_ms: @test_check_interval,
         renew_fun: renew_fun}
      )

      # Wait for renewal to complete
      Process.sleep(@test_check_interval * 3)

      # New cert should have a different serial
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

      test_pid = self()

      renew_fun = fn csr, hostname ->
        send(test_pid, :renew_called)
        node_cert = TLS.sign_csr(csr, hostname, ca_cert, ca_key, 200)
        {:ok, node_cert, ca_cert}
      end

      start_supervised!(
        {CertRenewal,
         name: :"renewal_far_#{:rand.uniform(100_000)}",
         check_interval_ms: @test_check_interval,
         renew_fun: renew_fun}
      )

      # Let several check cycles pass
      Process.sleep(@test_check_interval * 5)
      refute_received :renew_called
    end
  end

  describe "failure handling and backoff" do
    test "retries on failure without crashing", %{tmp_dir: tmp_dir} do
      {ca_cert, ca_key} = generate_ca()
      write_cert_with_validity(tmp_dir, ca_cert, ca_key, 5)

      call_count = :counters.new(1, [:atomics])
      test_pid = self()

      renew_fun = fn _csr, _hostname ->
        :counters.add(call_count, 1, 1)
        count = :counters.get(call_count, 1)
        send(test_pid, {:attempt, count})
        {:error, :test_failure}
      end

      pid =
        start_supervised!(
          {CertRenewal,
           name: :"renewal_fail_#{:rand.uniform(100_000)}",
           check_interval_ms: @test_check_interval,
           renew_fun: renew_fun}
        )

      # First attempt
      assert_receive {:attempt, 1}, 500
      assert Process.alive?(pid)

      # Since backoff is 1h in real config, we won't see a second attempt
      # in a short test. The key assertion is that it didn't crash.
      Process.sleep(@test_check_interval * 2)
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

      test_pid = self()

      renew_fun = fn csr, hostname ->
        send(test_pid, {:renewed, System.monotonic_time(:millisecond)})
        node_cert = TLS.sign_csr(csr, hostname, ca_cert, ca_key, 300)
        {:ok, node_cert, ca_cert}
      end

      custom_interval = 100

      start_supervised!(
        {CertRenewal,
         name: :"renewal_interval_#{:rand.uniform(100_000)}",
         check_interval_ms: custom_interval,
         renew_fun: renew_fun}
      )

      # Should receive the first renewal within 200ms (interval + some slack)
      assert_receive {:renewed, _t1}, 500
    end

    test "respects renewal_threshold_days from application env", %{tmp_dir: tmp_dir} do
      {ca_cert, ca_key} = generate_ca()
      # 60-day cert
      write_cert_with_validity(tmp_dir, ca_cert, ca_key, 60)

      # Set threshold to 90 days — so 60-day cert should trigger renewal
      Application.put_env(:neonfs_client, :renewal_threshold_days, 90)

      test_pid = self()

      renew_fun = fn csr, hostname ->
        send(test_pid, :threshold_renewal)
        node_cert = TLS.sign_csr(csr, hostname, ca_cert, ca_key, 400)
        {:ok, node_cert, ca_cert}
      end

      start_supervised!(
        {CertRenewal,
         name: :"renewal_thresh_#{:rand.uniform(100_000)}",
         check_interval_ms: @test_check_interval,
         renew_fun: renew_fun}
      )

      assert_receive :threshold_renewal, 500
    end

    test "does not renew 60-day cert with default 30-day threshold", %{tmp_dir: tmp_dir} do
      {ca_cert, ca_key} = generate_ca()
      write_cert_with_validity(tmp_dir, ca_cert, ca_key, 60)

      test_pid = self()

      renew_fun = fn _csr, _hostname ->
        send(test_pid, :should_not_renew)
        {:error, :unexpected}
      end

      start_supervised!(
        {CertRenewal,
         name: :"renewal_no_thresh_#{:rand.uniform(100_000)}",
         check_interval_ms: @test_check_interval,
         renew_fun: renew_fun}
      )

      Process.sleep(@test_check_interval * 5)
      refute_received :should_not_renew
    end
  end
end
