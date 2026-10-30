defmodule NeonFS.TestSupport.PeerTLS do
  @moduledoc """
  Mints the TLS material a peer cluster needs to run distribution over TLS,
  and the client identity the CLI needs to reach it.

  The cluster's own CA cannot be the source. Distribution TLS is configured
  by `ssl_dist.conf` at VM start, so the certificates have to exist before
  the node that would issue them is running — the harness therefore seeds
  its own CA and issues every certificate up front.

  Two triples come out of the same CA:

    * per peer, `node-local.crt` / `node-local.key` / `ca_bundle.crt` and an
      `ssl_dist.conf` naming them, matching what
      `packaging/systemd/neonfs-tls-common.sh` writes on a real install;

    * per cluster, `local-ca.crt` / `cli.crt` / `cli.key`, which is the
      triple `neonfs-cli` looks for and which the cluster CA does not
      issue — it issues node certificates, which is why pointing the CLI at
      a peer's directory turns TLS on and then fails to find an identity.

  `x509` does the work, so nothing shells out to `openssl`.
  """

  alias NeonFS.Transport.TLS

  @doc """
  Creates a CA for `cluster_id` and returns it for issuing node and client
  certificates.
  """
  @spec create_ca(String.t()) :: {X509.Certificate.t(), X509.PrivateKey.t()}
  def create_ca(cluster_id), do: TLS.generate_ca("peer-cluster-#{cluster_id}")

  @doc """
  Writes a peer's node certificate, key, CA bundle and `ssl_dist.conf` into
  `tls_dir`, so the VM finds them when distribution starts.

  The serial is the peer's index: every certificate from one CA needs a
  distinct one, and a cluster's peers are already numbered.
  """
  @spec write_node_material(
          String.t(),
          node(),
          {X509.Certificate.t(), X509.PrivateKey.t()},
          pos_integer()
        ) ::
          :ok
  def write_node_material(tls_dir, node_name, {ca_cert, ca_key}, serial) do
    File.mkdir_p!(tls_dir)

    key = TLS.generate_node_key()

    cert =
      key
      |> TLS.create_csr(to_string(node_name))
      |> TLS.sign_csr("localhost", ca_cert, ca_key, serial)

    File.write!(Path.join(tls_dir, "node-local.crt"), TLS.encode_cert(cert))
    File.write!(Path.join(tls_dir, "node-local.key"), TLS.encode_key(key))
    File.write!(Path.join(tls_dir, "ssl_dist.conf"), ssl_dist_conf(tls_dir))

    # `ca_bundle.crt` is what distribution actually verifies against, and
    # `local-ca.crt` is where it comes from: once the node initialises or
    # joins, `NeonFS.TLSDistConfig.regenerate_ca_bundle/1` rebuilds the bundle
    # from `local-ca.crt` + `incoming-ca.crt` + `ca.crt`. Writing only the
    # bundle would put this CA in the node's trust store until the moment the
    # cluster forms and then silently drop it — leaving a node that no longer
    # trusts the CLI certificate issued from the same CA, which surfaces as
    # `received fatal alert: UnknownCA` on the client side.
    #
    # A real install writes both (`packaging/systemd/neonfs-tls-common.sh`),
    # which is why regeneration is lossless there. Same here.
    File.write!(Path.join(tls_dir, "local-ca.crt"), TLS.encode_cert(ca_cert))
    File.write!(Path.join(tls_dir, "ca_bundle.crt"), TLS.encode_cert(ca_cert))

    :ok
  end

  @doc """
  Writes the `local-ca.crt` / `cli.crt` / `cli.key` triple `neonfs-cli`
  looks for into `tls_dir`, and returns the directory to point
  `NEONFS_TLS_DIR` at.
  """
  @spec write_cli_material(String.t(), {X509.Certificate.t(), X509.PrivateKey.t()}, pos_integer()) ::
          String.t()
  def write_cli_material(tls_dir, {ca_cert, ca_key}, serial) do
    File.mkdir_p!(tls_dir)

    key = TLS.generate_node_key()

    cert =
      key
      |> TLS.create_csr("neonfs-cli")
      |> TLS.sign_csr("localhost", ca_cert, ca_key, serial)

    File.write!(Path.join(tls_dir, "local-ca.crt"), TLS.encode_cert(ca_cert))
    File.write!(Path.join(tls_dir, "cli.crt"), TLS.encode_cert(cert))
    File.write!(Path.join(tls_dir, "cli.key"), TLS.encode_key(key))

    # The CLI decides whether to speak TLS by whether this file exists
    # (`neonfs-cli/src/tls.rs`), so writing it is what turns the client
    # side on — the identity above is what lets the handshake finish.
    File.write!(Path.join(tls_dir, "ssl_dist.conf"), cli_ssl_dist_conf(tls_dir))

    tls_dir
  end

  @doc """
  Copies an initialised node's cluster CA into another TLS directory.

  Two callers, one reason. The **CLI** needs it because a node stops
  presenting the certificate it booted with. A **peer about to join** needs
  it because the RPC join flow (`NeonFS.Cluster.Join.join_cluster_rpc/3`)
  reaches the via node over distribution before it holds any cluster material
  — a trust problem the HTTP flow does not have, since there the joining node
  redeems its invite over HTTP and so has the cluster CA before it ever
  handshakes. Seeding it as `ca.crt` is what that redemption would have left,
  and it is where `NeonFS.TLSDistConfig.regenerate_ca_bundle/1` looks, so the
  join's own regeneration stays consistent with it. Rebuilding the target's
  bundle and clearing its PEM cache is the caller's job — both have to happen
  on the node that owns the directory.

  The CLI case in detail:
  `NeonFS.TLSDistConfig.regenerate_config/1` rewrites `ssl_dist.conf` after
  `cluster init` to present the cluster-signed `node.crt` *alone* — the local
  certificate is deliberately not kept as a fallback, because OTP's TLS 1.3
  `certs_keys` selection cannot be relied on to pick the cluster one. So a
  CLI that trusts only the CA this module minted can no longer validate the
  node, and `neonfs-cli` reports:

      Error: TLS handshake failed: invalid peer certificate: UnknownIssuer

  A real install does not hit this because everything lives in one directory
  (`/var/lib/neonfs/tls`), so the CLI's root store picks up `ca.crt` beside
  its own `local-ca.crt` — which is exactly what `neonfs-cli/src/tls.rs` means
  by "also add cluster CA if present". The harness keeps the two apart, so
  that the CLI cannot read a node's private key, and therefore has to carry
  the trust anchor across itself.

  Returns `:ok` when the CA was copied and `{:error, :no_cluster_ca}` when the
  node has not initialised one, rather than succeeding silently — an absent
  anchor surfaces later as the handshake error above, which is a long way from
  the cause.
  """
  @spec add_cluster_ca(String.t(), String.t()) :: :ok | {:error, :no_cluster_ca}
  def add_cluster_ca(node_tls_dir, target_tls_dir) do
    source = Path.join(node_tls_dir, "ca.crt")

    if File.exists?(source) do
      File.cp!(source, Path.join(target_tls_dir, "ca.crt"))
      :ok
    else
      {:error, :no_cluster_ca}
    end
  end

  defp cli_ssl_dist_conf(tls_dir) do
    """
    [{client, [
      {certs_keys, [\#{certfile => "#{tls_dir}/cli.crt",
                      keyfile => "#{tls_dir}/cli.key"}]},
      {cacertfile, "#{tls_dir}/local-ca.crt"},
      {verify, verify_peer},
      {versions, ['tlsv1.3']}
    ]}].
    """
  end

  # Same shape the systemd packaging writes, so a test cluster's
  # distribution is configured the way a real install's is rather than a
  # simplified variant that could pass where production would not.
  defp ssl_dist_conf(tls_dir) do
    """
    [{server, [
      {certs_keys, [\#{certfile => "#{tls_dir}/node-local.crt",
                      keyfile => "#{tls_dir}/node-local.key"}]},
      {cacertfile, "#{tls_dir}/ca_bundle.crt"},
      {verify, verify_peer},
      {fail_if_no_peer_cert, true},
      {versions, ['tlsv1.3']}
    ]},
    {client, [
      {certs_keys, [\#{certfile => "#{tls_dir}/node-local.crt",
                      keyfile => "#{tls_dir}/node-local.key"}]},
      {cacertfile, "#{tls_dir}/ca_bundle.crt"},
      {verify, verify_peer},
      {versions, ['tlsv1.3']}
    ]}].
    """
  end
end
