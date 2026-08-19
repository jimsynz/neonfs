defmodule NeonFS.Client.RedeemCredentialsTest do
  @moduledoc """
  The credentials-only entry point an init container runs.

  The HTTP redemption is stubbed through `:redeem_http_fn`; everything after it
  is filesystem work, which is what these tests inspect. What matters is not
  that the call succeeds but *what it leaves on the host* — and what it does
  not.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Client.Join
  alias NeonFS.Transport.TLS

  @moduletag :tmp_dir

  @via_node "neonfs_core@10.0.0.1"
  @via_port 9100

  setup %{tmp_dir: tmp_dir} do
    tls_dir = Path.join(tmp_dir, "tls")
    meta_dir = Path.join(tmp_dir, "meta")
    File.mkdir_p!(tls_dir)
    File.mkdir_p!(meta_dir)

    Application.put_env(:neonfs_client, :tls_dir, tls_dir)
    Application.put_env(:neonfs_core, :meta_dir, meta_dir)

    on_exit(fn ->
      Application.delete_env(:neonfs_client, :tls_dir)
      Application.delete_env(:neonfs_core, :meta_dir)
      Application.delete_env(:neonfs_client, :redeem_http_fn)
    end)

    {:ok, tls_dir: tls_dir, meta_dir: meta_dir}
  end

  describe "redeem_credentials/3" do
    setup :stub_redemption

    test "writes the cluster TLS material", %{tls_dir: tls_dir} do
      assert {:ok, :provisioned} =
               Join.redeem_credentials("nfs_inv_r_9999999999_5_s", "host:9568")

      assert File.exists?(Path.join(tls_dir, "node.crt"))
      assert File.exists?(Path.join(tls_dir, "ca.crt"))
      assert File.read!(Path.join(tls_dir, "node.crt")) =~ "BEGIN CERTIFICATE"
    end

    test "regenerates the distribution config, so the next boot runs over TLS", %{
      tls_dir: tls_dir
    } do
      assert {:ok, :provisioned} =
               Join.redeem_credentials("nfs_inv_r_9999999999_5_s", "host:9568")

      assert File.exists?(Path.join(tls_dir, "ssl_dist.conf"))
      assert File.exists?(Path.join(tls_dir, "ca_bundle.crt"))
    end

    test "records the via node's distribution port, which is what a pod needs", %{
      meta_dir: meta_dir
    } do
      assert {:ok, :provisioned} =
               Join.redeem_credentials("nfs_inv_r_9999999999_5_s", "host:9568")

      state = meta_dir |> Path.join("cluster.json") |> File.read!() |> :json.decode()

      assert [%{"name" => @via_node, "dist_port" => @via_port}] = state["known_peers"]
    end

    # The point of writing a partial state rather than a full one. `master_key`
    # mints and verifies invite tokens, and this file lands on every host in the
    # fleet where any pod that mounts the state directory can read it.
    test "does not write the cluster master key", %{meta_dir: meta_dir} do
      assert {:ok, :provisioned} =
               Join.redeem_credentials("nfs_inv_r_9999999999_5_s", "host:9568")

      raw = meta_dir |> Path.join("cluster.json") |> File.read!()

      refute raw =~ "master_key"
      refute :json.decode(raw) |> Map.has_key?("master_key")
    end

    test "leaves no temp file behind", %{meta_dir: meta_dir} do
      assert {:ok, :provisioned} =
               Join.redeem_credentials("nfs_inv_r_9999999999_5_s", "host:9568")

      refute File.exists?(Path.join(meta_dir, "cluster.json.tmp"))
    end

    # Every pod scheduled onto a host runs this, so this is the common case
    # rather than an edge one — and spending a redemption on it would size an
    # invite's budget against pods instead of hosts.
    test "a second run on a provisioned host spends no redemption" do
      test_pid = self()

      Application.put_env(:neonfs_client, :redeem_http_fn, fn _via, _token, _csr, _name ->
        send(test_pid, :redeemed)
        {:ok, credentials()}
      end)

      assert {:ok, :provisioned} =
               Join.redeem_credentials("nfs_inv_r_9999999999_5_s", "host:9568")

      assert_receive :redeemed

      assert {:ok, :already_provisioned} =
               Join.redeem_credentials("nfs_inv_r_9999999999_5_s", "host:9568")

      refute_receive :redeemed, 200
    end

    test "a failed redemption writes nothing", %{tls_dir: tls_dir, meta_dir: meta_dir} do
      Application.put_env(:neonfs_client, :redeem_http_fn, fn _via, _token, _csr, _name ->
        {:error, {:http_error, 409}}
      end)

      assert {:error, {:http_error, 409}} =
               Join.redeem_credentials("nfs_inv_r_9999999999_5_s", "host:9568")

      refute File.exists?(Path.join(tls_dir, "node.crt"))
      refute File.exists?(Path.join(meta_dir, "cluster.json"))
      refute Join.credentials_present?()
    end
  end

  describe "credentials_present?/0" do
    # A hostPath mount creates an empty directory on a host that has never
    # joined, so directory existence proves nothing and would make every pod
    # skip provisioning.
    test "an empty tls directory is not credentials" do
      refute Join.credentials_present?()
    end

    test "a node certificate is", %{tls_dir: tls_dir} do
      File.write!(Path.join(tls_dir, "node.crt"), "")
      assert Join.credentials_present?()
    end
  end

  defp stub_redemption(_context) do
    Application.put_env(:neonfs_client, :redeem_http_fn, fn _via, _token, _csr, _name ->
      {:ok, credentials()}
    end)

    :ok
  end

  # A real CA and a real signed node certificate, built with the same helpers
  # the cluster CA uses. `store_credentials/2` decodes both and
  # `TLSDistConfig.regenerate/1` reads them back, so a placeholder string would
  # let these tests pass against code that never parsed anything.
  defp credentials do
    {ca_cert, ca_key} = TLS.generate_ca("test-cluster")

    node_key = TLS.generate_node_key()
    csr = TLS.create_csr(node_key, "provisioned@host")
    node_cert = TLS.sign_csr(csr, "host", ca_cert, ca_key, 1)

    %{
      "ca_cert_pem" => TLS.encode_cert(ca_cert),
      "node_cert_pem" => TLS.encode_cert(node_cert),
      "via_node" => @via_node,
      "via_dist_port" => @via_port
    }
  end
end
