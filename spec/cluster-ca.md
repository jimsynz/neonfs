# Cluster Certificate Authority

Each NeonFS cluster operates its own certificate authority (CA) for authenticating peer-to-peer data transfer connections. This document specifies the CA lifecycle, certificate format, issuance and revocation flows, and the OTP modules used for implementation.

## Purpose

The [Data Transfer](data-transfer.md) layer uses mutual TLS (mTLS) to authenticate and encrypt inter-node chunk transfers. Both sides of every connection verify the other's certificate against the cluster CA. This provides:

- **Node identity**: Each node has a unique certificate with its name in the Subject and SAN fields
- **Mutual authentication**: Both connecting and accepting sides prove they are cluster members
- **Defence in depth**: Independent of the WireGuard/VPN network-level encryption recommended for Erlang distribution
- **Revocation**: Decommissioned or compromised nodes can have their certificates revoked

The cluster CA is a self-signed root CA. There are no intermediate CAs — the cluster is small (3–10 nodes) and the additional chain complexity provides no benefit.

## Storage

The CA certificate and private key are stored in the [system volume](system-volume.md) (`_system`), which is replicated to every node. This means any core node can sign CSRs when a new node joins, and the CA key survives any single-node failure.

Node-specific certificates and private keys are stored on the local filesystem. The node's private key is generated locally and never transmitted.

```
System volume (_system):
  /tls/ca.crt               # CA certificate (PEM)
  /tls/ca.key               # CA private key (PEM)
  /tls/crl.pem              # Certificate revocation list (PEM, may be empty)
  /tls/serial               # Next serial number (integer, atomically updated)

Local filesystem per node:
  /var/lib/neonfs/tls/
  ├── ca.crt                # Cached copy of CA cert (for startup before system volume available)
  ├── node.crt              # This node's certificate (PEM)
  └── node.key              # This node's private key (PEM)
```

## Key and Certificate Format

### CA Key

ECDSA with the P-256 curve (`secp256r1`). This provides 128-bit security equivalent with much smaller keys and faster operations than RSA, which matters for TLS handshakes on connection pool startup.

```elixir
ca_key = X509.PrivateKey.new_ec(:secp256r1)
```

### CA Certificate

Self-signed root CA certificate using the `:root_ca` template:

```elixir
ca_cert = X509.Certificate.self_signed(ca_key, "/O=NeonFS/CN=#{cluster_name} CA",
  template: :root_ca,
  validity: validity_days(:ca)
)
```

| Field | Value |
|-------|-------|
| Subject | `/O=NeonFS/CN={cluster_name} CA` |
| Key algorithm | ECDSA P-256 |
| Validity | 10 years (configurable) |
| Basic Constraints | `CA:true, pathLen:0` |
| Key Usage | `digitalSignature, keyCertSign, cRLSign` |
| Subject Key Identifier | SHA-1 hash of public key (automatic) |

`pathLen:0` prevents the CA from signing intermediate CAs — only end-entity (node) certificates.

### Node Key

Also ECDSA P-256, generated locally on the node:

```elixir
node_key = X509.PrivateKey.new_ec(:secp256r1)
```

### Node Certificate

Signed by the cluster CA:

```elixir
node_cert = X509.Certificate.new(
  X509.CSR.public_key(csr),
  csr_subject,
  ca_cert, ca_key,
  template: :server,
  validity: validity_days(:node),
  extensions: [
    subject_alt_name:
      X509.Certificate.Extension.subject_alt_name([node_hostname(node)]),
    ext_key_usage:
      X509.Certificate.Extension.ext_key_usage([:serverAuth, :clientAuth])
  ]
)
```

| Field | Value |
|-------|-------|
| Subject | `/O=NeonFS/CN={node_name}` |
| Issuer | Cluster CA |
| Key algorithm | ECDSA P-256 |
| Validity | 1 year (configurable) |
| Basic Constraints | `CA:false` |
| Key Usage | `digitalSignature` |
| Extended Key Usage | `serverAuth, clientAuth` |
| Subject Alternative Name | Node hostname / IP address |
| Authority Key Identifier | Links to CA's Subject Key Identifier |

Both `serverAuth` and `clientAuth` are set because every node acts as both listener and connector in the peer-to-peer transport.

### Validity Periods

| Certificate | Default | Rationale |
|-------------|---------|-----------|
| CA | 10 years | Long-lived; rotating the CA requires reissuing all node certs |
| Node | 1 year | Short enough to limit exposure from stolen keys; auto-renewed |

Configurable via:

```yaml
# node.yaml
cluster_ca:
  ca_validity_days: 3650       # 10 years
  node_validity_days: 365      # 1 year
  renewal_threshold_days: 30   # Begin renewal 30 days before expiry
```

## Flows

### Cluster Init

The CA is created as part of `neonfs cluster init`, immediately after the system volume:

```
1. Operator runs: neonfs cluster init --name my-cluster
2. Ra cluster initialises with single member
3. System volume created (see system-volume.md)
4. CA key generated (ECDSA P-256)
5. Self-signed CA certificate created
6. CA cert + key written to system volume (/tls/ca.crt, /tls/ca.key)
7. Serial number initialised to 1 (/tls/serial)
8. Empty CRL created and written (/tls/crl.pem)
9. Node certificate issued for first node (see "Certificate Issuance" below)
10. Master encryption key generated, first node registered, etc.
```

```elixir
def init_cluster_ca(cluster_name) do
  ca_key = X509.PrivateKey.new_ec(:secp256r1)

  ca_cert = X509.Certificate.self_signed(ca_key, "/O=NeonFS/CN=#{cluster_name} CA",
    template: :root_ca,
    validity: ca_validity_days()
  )

  # Write to system volume
  :ok = SystemVolume.write("/tls/ca.crt", X509.Certificate.to_pem(ca_cert))
  :ok = SystemVolume.write("/tls/ca.key", X509.PrivateKey.to_pem(ca_key))
  :ok = SystemVolume.write("/tls/serial", "1")

  # Create empty CRL
  crl = X509.CRL.new([], ca_cert, ca_key)
  :ok = SystemVolume.write("/tls/crl.pem", X509.CRL.to_pem(crl))

  {:ok, ca_cert, ca_key}
end
```

### Node Join

When a node joins the cluster, it generates a keypair locally, creates a CSR, and sends it to the cluster for signing:

```
Joining node:                          Core node:
1. Generate ECDSA P-256 keypair
2. Create CSR with node name
3. Send CSR via invite token flow  ──►
                                       4. Validate invite token
                                       5. Validate CSR signature
                                       6. Read CA cert + key from system volume
                                       7. Allocate next serial number
                                       8. Sign certificate
                                       9. Return signed cert + CA cert
                                   ◄──
10. Store node.crt, node.key, ca.crt
    on local filesystem
11. Start data transfer listener
```

```elixir
# --- Joining node ---

def create_join_request(node_name) do
  node_key = X509.PrivateKey.new_ec(:secp256r1)
  csr = X509.CSR.new(node_key, "/O=NeonFS/CN=#{node_name}")

  {:ok, node_key, csr}
end

# --- Core node handling the join ---

def sign_node_csr(csr, node_hostname) do
  # Validate CSR
  unless X509.CSR.valid?(csr), do: raise("invalid CSR signature")

  # Load CA from system volume
  {:ok, ca_cert_pem} = SystemVolume.read("/tls/ca.crt")
  {:ok, ca_key_pem} = SystemVolume.read("/tls/ca.key")
  ca_cert = X509.Certificate.from_pem!(ca_cert_pem)
  ca_key = X509.PrivateKey.from_pem!(ca_key_pem)

  # Allocate serial number
  serial = next_serial_number()

  # Issue certificate
  node_cert = X509.Certificate.new(
    X509.CSR.public_key(csr),
    X509.CSR.subject(csr),
    ca_cert, ca_key,
    template: :server,
    serial: serial,
    validity: node_validity_days(),
    extensions: [
      subject_alt_name:
        X509.Certificate.Extension.subject_alt_name([node_hostname]),
      ext_key_usage:
        X509.Certificate.Extension.ext_key_usage([:serverAuth, :clientAuth])
    ]
  )

  {:ok, node_cert, ca_cert}
end
```

### Serial Number Allocation

Each certificate needs a unique serial number. The serial is stored in the system volume at `/tls/serial` and incremented atomically per issuance.

```elixir
defp next_serial_number do
  {:ok, current} = SystemVolume.read("/tls/serial")
  serial = String.to_integer(String.trim(current))
  :ok = SystemVolume.write("/tls/serial", Integer.to_string(serial + 1))
  serial
end
```

For concurrent joins (unlikely but possible), the serial allocation should be serialised — either through a GenServer or by using the Ra state machine to allocate serials.

### Certificate Renewal

Node certificates are renewed automatically before expiry. Each node runs a background process that checks its certificate's `notAfter` field:

```
1. Node checks certificate expiry daily
2. If within renewal_threshold_days of expiry:
   a. Generate new keypair
   b. Create CSR
   c. Send to any core node for signing (via distribution RPC)
   d. Receive new certificate
   e. Write new cert + key to local filesystem
   f. Reload TLS sockets (graceful — existing connections continue with old cert)
```

```elixir
defmodule NeonFS.Transport.CertRenewal do
  use GenServer

  @check_interval :timer.hours(24)

  def init(_opts) do
    schedule_check()
    {:ok, %{}}
  end

  def handle_info(:check_renewal, state) do
    case days_until_expiry() do
      days when days <= renewal_threshold_days() ->
        Logger.info("Certificate expires in #{days} days, renewing")
        renew_certificate()

      days ->
        Logger.debug("Certificate expires in #{days} days, no renewal needed")
    end

    schedule_check()
    {:noreply, state}
  end

  defp days_until_expiry do
    {:ok, cert_pem} = File.read(tls_path("node.crt"))
    cert = X509.Certificate.from_pem!(cert_pem)
    not_after = X509.Certificate.validity(cert) |> elem(1)
    Date.diff(not_after, Date.utc_today())
  end

  defp renew_certificate do
    node_key = X509.PrivateKey.new_ec(:secp256r1)
    csr = X509.CSR.new(node_key, "/O=NeonFS/CN=#{node()}")

    case Router.core_call(NeonFS.Transport.TLS, :sign_node_csr, [csr, node_hostname()]) do
      {:ok, node_cert, ca_cert} ->
        # Write new credentials
        File.write!(tls_path("node.crt"), X509.Certificate.to_pem(node_cert))
        File.write!(tls_path("node.key"), X509.PrivateKey.to_pem(node_key))
        File.write!(tls_path("ca.crt"), X509.Certificate.to_pem(ca_cert))

        # Reload TLS (listener and pool connections)
        NeonFS.Transport.reload_tls()

      {:error, reason} ->
        Logger.error("Certificate renewal failed: #{inspect(reason)}")
    end
  end

  defp schedule_check do
    Process.send_after(self(), :check_renewal, @check_interval)
  end
end
```

### Certificate Revocation

When a node is decommissioned or compromised, its certificate is added to the cluster CRL:

```elixir
def revoke_node(node_cert, reason \\ :unspecified) do
  {:ok, ca_cert_pem} = SystemVolume.read("/tls/ca.crt")
  {:ok, ca_key_pem} = SystemVolume.read("/tls/ca.key")
  ca_cert = X509.Certificate.from_pem!(ca_cert_pem)
  ca_key = X509.PrivateKey.from_pem!(ca_key_pem)

  # Load existing CRL entries
  existing_entries = load_current_crl_entries()

  # Add new revocation entry
  new_entry = X509.CRL.Entry.new(node_cert, DateTime.utc_now(),
    extensions: [X509.CRL.Extension.reason_code(reason)]
  )

  # Create updated CRL
  crl = X509.CRL.new(
    [new_entry | existing_entries],
    ca_cert, ca_key,
    next_update: 30
  )

  :ok = SystemVolume.write("/tls/crl.pem", X509.CRL.to_pem(crl))
end
```

CRL checking is optional for the initial implementation. The primary protection against decommissioned nodes is removing their Erlang distribution cookie and disconnecting them from the cluster. CRL checking adds a second layer for the data transfer plane.

To enable CRL checking in the TLS configuration:

```elixir
ssl_opts = [
  # ... existing opts ...
  {:crl_check, :peer},
  {:crl_cache, {:ssl_crl_cache, {:internal, [http: 30_000]}}}
]
```

However, since the CRL is stored in the system volume rather than served via HTTP, a custom CRL lookup function would be needed:

```elixir
{:crl_check, :peer},
{:crl_cache, {NeonFS.Transport.CRLCache, %{}}}
```

This is a Phase 8 stretch goal, not a hard requirement.

### CA Rotation

If the CA certificate is approaching expiry or the CA key is compromised, the entire CA must be rotated. This is a disruptive operation:

```
1. Generate new CA keypair and certificate
2. Write new CA to system volume
3. Reissue certificates for all nodes (signed by new CA)
4. Each node receives new CA cert + new node cert
5. Brief transition period: nodes accept certificates from both old and new CA
6. Once all nodes are updated, remove old CA
```

CA rotation is expected to be extremely rare (default 10-year CA validity). It is not automated — it requires operator action via the CLI:

```bash
$ neonfs cluster rotate-ca
WARNING: This will reissue certificates for all 5 cluster nodes.
Nodes will briefly accept both old and new CA during transition.
Continue? [y/N] y

Generating new CA...
Reissuing certificate for neonfs-core-1... done
Reissuing certificate for neonfs-core-2... done
Reissuing certificate for neonfs-core-3... done
Reissuing certificate for neonfs-fuse-1... done
Reissuing certificate for neonfs-fuse-2... done

CA rotation complete. Old CA will be removed in 24h.
```

## CLI Commands

```bash
# View cluster CA info
$ neonfs cluster ca info
Cluster CA:
  Subject:    O=NeonFS, CN=my-cluster CA
  Algorithm:  ECDSA P-256
  Valid from: 2026-02-14
  Valid to:   2036-02-14
  Serial:     1
  Nodes:      5 certificates issued

# List node certificates
$ neonfs cluster ca list
NODE                    SERIAL  EXPIRES     STATUS
neonfs-core-1           2       2027-02-14  valid
neonfs-core-2           3       2027-02-14  valid
neonfs-core-3           4       2027-02-14  valid
neonfs-fuse-1           5       2027-02-14  valid
neonfs-fuse-2           6       2027-02-14  valid

# Revoke a node certificate
$ neonfs cluster ca revoke neonfs-core-3
Revoking certificate serial 4 for neonfs-core-3...
CRL updated.

# Rotate CA (rare, disruptive)
$ neonfs cluster ca rotate
```

## Module Placement

| Module | Package | Role |
|--------|---------|------|
| `NeonFS.Transport.TLS` | `neonfs_client` | CA generation, CSR signing, certificate issuance, PEM I/O |
| `NeonFS.Transport.CertRenewal` | `neonfs_client` | Background process for automatic certificate renewal |
| `NeonFS.Transport.CRLCache` | `neonfs_client` | Custom CRL lookup from system volume (optional) |

These modules live in `neonfs_client` because both core and non-core nodes need certificate operations. The CA signing functions are called via Erlang distribution RPC from non-core nodes to core nodes (which have access to the CA key via the system volume).

## Dependencies

| Package | Role | Notes |
|---------|------|-------|
| `x509` | Certificate, CSR, CRL, and key operations | Pure Elixir, zero dependencies, wraps OTP's `:public_key` |
| `:public_key` | Underlying crypto operations | OTP built-in |
| `:ssl` | TLS connections (consumer of certificates) | OTP built-in |

## What This Spec Does NOT Cover

- **Erlang distribution TLS**: Using certificates for Erlang distribution encryption is a future enhancement noted in [Security](security.md). This spec covers certificates for the data transfer plane only.
- **User-facing encryption keys**: Volume encryption keys (AES-256-GCM) and user keys (envelope encryption) are a separate concern managed by the security subsystem. See [Security — Key Hierarchy](security.md#key-hierarchy).
- **External CA integration**: Integration with external CAs (Let's Encrypt, Vault PKI, corporate CA) is out of scope. The cluster CA is self-contained.
- **Hardware security modules**: HSM-backed CA key storage is a future consideration for high-security deployments.

## Implementation Phase

The cluster CA is **Phase 8**, implemented after the [system volume](system-volume.md) (Phase 7) and before the TLS data plane (Phase 9). The implementation order across phases is:

1. Phase 7: System volume (`_system`)
2. Phase 8: Cluster CA (this spec)
3. Phase 9: TLS data plane (`:ssl` + `nimble_pool`)

## Related Documents

- [Data Transfer](data-transfer.md) — TLS transport that consumes these certificates
- [System Volume](system-volume.md) — Where the CA cert and key are stored
- [Security](security.md) — Cluster authentication, trust model, TLS distribution roadmap
- [Discovery](discovery.md) — ServiceRegistry where data transfer endpoints are advertised
- [Deployment](deployment.md) — Cluster bootstrap and node join flows
- [Implementation](implementation.md) — Phase 8 deliverables
