defmodule NeonFS.CSI.ProvisionTest do
  @moduledoc """
  The release command's contract with its environment.

  `main/0` halts the VM, so what is exercised is `provision/1`. The redemption
  itself is `NeonFS.Client.Join`'s and is tested there; what matters here is
  that a missing input is reported by name rather than surfacing as a
  redemption failure.
  """

  use ExUnit.Case, async: false

  alias NeonFS.CSI.Provision

  @moduletag :tmp_dir

  setup do
    on_exit(fn ->
      System.delete_env("NEONFS_BOOTSTRAP_TOKEN")
      System.delete_env("NEONFS_JOIN_VIA")
      Application.delete_env(:neonfs_client, :redeem_http_fn)
      Application.delete_env(:neonfs_client, :tls_dir)
    end)

    :ok
  end

  # These run with no credentials on disk, so `provision/1` reaches the
  # environment check. `tls_dir` points at an empty temp dir per test.
  setup %{tmp_dir: tmp_dir} do
    Application.put_env(:neonfs_client, :tls_dir, Path.join(tmp_dir, "empty-tls"))
    :ok
  end

  # An init container that fails has to say which input was missing. "Could not
  # provision" plus a redemption error sends the operator to the token when the
  # ConfigMap is what is unset.
  test "a missing token is named" do
    System.put_env("NEONFS_JOIN_VIA", "10.0.0.1:9568")

    assert {:error, {:missing_env, "NEONFS_BOOTSTRAP_TOKEN"}} = Provision.provision()
  end

  test "a missing via address is named" do
    System.put_env("NEONFS_BOOTSTRAP_TOKEN", "nfs_inv_r_9999999999_5_s")

    assert {:error, {:missing_env, "NEONFS_JOIN_VIA"}} = Provision.provision()
  end

  # Kubernetes injects an unset optional secret key as an empty string rather
  # than leaving the variable absent, so empty has to be treated as missing or
  # the redemption is attempted with no token.
  test "an empty token is missing, not a token" do
    System.put_env("NEONFS_BOOTSTRAP_TOKEN", "")
    System.put_env("NEONFS_JOIN_VIA", "10.0.0.1:9568")

    assert {:error, {:missing_env, "NEONFS_BOOTSTRAP_TOKEN"}} = Provision.provision()
  end

  test "a host that already holds credentials is a successful no-op", %{tmp_dir: tmp_dir} do
    tls_dir = Path.join(tmp_dir, "tls")
    File.mkdir_p!(tls_dir)
    File.write!(Path.join(tls_dir, "node.crt"), "")
    Application.put_env(:neonfs_client, :tls_dir, tls_dir)

    System.put_env("NEONFS_BOOTSTRAP_TOKEN", "nfs_inv_r_9999999999_5_s")
    System.put_env("NEONFS_JOIN_VIA", "10.0.0.1:9568")

    Application.put_env(:neonfs_client, :redeem_http_fn, fn _via, _token, _csr, _name ->
      flunk("a provisioned host must not spend a redemption")
    end)

    assert {:ok, :already_provisioned} = Provision.provision()
  end

  # A host whose identity was provisioned out of band — the test rig's k3s VM,
  # which joined as a daemon before any pod started — has no token and needs
  # none. Demanding one would turn a successful no-op into a crash loop and take
  # the chart with it.
  test "a provisioned host needs no token at all", %{tmp_dir: tmp_dir} do
    tls_dir = Path.join(tmp_dir, "tls")
    File.mkdir_p!(tls_dir)
    File.write!(Path.join(tls_dir, "node.crt"), "")
    Application.put_env(:neonfs_client, :tls_dir, tls_dir)

    assert {:ok, :already_provisioned} = Provision.provision()
  end
end
