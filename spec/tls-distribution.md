# TLS Distribution and Secure Cluster Join

**Date:** 2026-04-07
**Status:** Draft
**Issue:** #96

## Problem

NeonFS nodes currently use plain Erlang distribution with cookie authentication. This causes two problems:

1. **Cookie chicken-and-egg**: A joining node needs the cluster cookie to connect via distribution, but the join flow (which provides the cookie) requires a distribution connection to the via node.

2. **Unencrypted distribution**: All RPC traffic (metadata, coordination) travels unencrypted over Erlang distribution, while the data plane already uses mTLS with TLS 1.3. This is an inconsistent security posture.

## Design

### Core idea

Every node generates a local CA and certificates on first boot, then always starts Erlang distribution over TLS (`-proto_dist inet_tls`). The daemon gets its own distribution certificate signed by the local CA, and the CLI gets a separate client certificate signed by the same CA. The CLI never sees the daemon's private key.

A `ca_bundle.crt` file acts as the trust store for distribution. Pre-cluster, it contains only the local CA. After cluster join/init, it gains the cluster CA as a second trust root. Standard OTP chain validation handles both trust domains automatically.

The only HTTP in the entire design is a single daemon-to-daemon endpoint on the via node for invite redemption. The invite token never traverses the network — the joining node proves possession via HMAC. The response is encrypted with a key derived from the token.

### What changes

| Component | Before | After |
|-----------|--------|-------|
| Distribution protocol | Plain TCP + cookie | TLS (`inet_tls_dist`) + cookie + certificates |
| CLI authentication | Cookie file | CLI certificate + key signed by local CA |
| CLI transport | `erl_rpc` (hardcoded TCP) | Custom RPC client on `erl_dist` with TLS |
| Cluster join credential exchange | RPC to via node (broken cross-host) | HTTP POST to via node's `/api/cluster/redeem-invite` |
| Pre-cluster CLI commands | Fail with connection error | Work normally (local TLS distribution always running) |
| Post-cluster CLI commands on uninitialised node | Cryptic distribution error | Clear "cluster not initialised" error |
| EPMD | Binds to localhost by default | Binds to `0.0.0.0` via `ERL_EPMD_ADDRESS` |

### What stays the same

- All CLI commands still dispatch via `NeonFS.CLI.Handler` over distribution
- Cookie authentication remains (TLS is layered on top, not replacing cookies)
- Existing data plane mTLS (Listener, ConnPool) unchanged
- Cluster CA, cert renewal, CRL — all unchanged
- Invite token format unchanged

## Certificate Architecture

### Certificate layout on disk

```
/var/lib/neonfs/tls/
├── local-ca.crt        # Self-signed local CA (CA:true) — 0644
├── local-ca.key        # Local CA private key — 0600, daemon only
├── node-local.crt      # Daemon's distribution cert, signed by local CA — 0644
├── node-local.key      # Daemon's private key — 0600, daemon only
├── cli.crt             # CLI client cert, signed by local CA — 0644
├── cli.key             # CLI private key — 0640 (readable by CLI users)
├── ca_bundle.crt       # Trust anchors: local-ca.crt + (cluster ca.crt after join)
├── ssl_dist.conf       # Erlang TLS distribution options file
├── ca.crt              # Cluster CA cert (after cluster init/join)
├── node.crt            # Cluster-signed node cert (after cluster init/join)
└── node.key            # Node private key for cluster cert (after cluster init/join)
```

### Trust model

```
Pre-cluster:

    local-ca.crt (self-signed)
    ├── signs node-local.crt (daemon distribution cert)
    └── signs cli.crt (CLI client cert)

    ca_bundle.crt = [local-ca.crt]

Post-cluster:

    local-ca.crt (self-signed)          cluster CA (from system volume)
    ├── signs node-local.crt            ├── signs node.crt (cluster node cert)
    └── signs cli.crt                   └── signs other nodes' certs

    ca_bundle.crt = [local-ca.crt, cluster ca.crt]
```

The daemon presents `node-local.crt` for local CLI connections and `node.crt` for inter-node connections (via the `certs_keys` option in `ssl_dist.conf`). Standard OTP chain validation against `ca_bundle.crt` handles both trust domains.

### Generation

Local certificates are generated on first boot by the daemon wrapper script (`neonfs-tls-common.sh`) using `openssl`. This runs before the BEAM starts so that `ssl_dist.conf` references valid certificate files.

```sh
# 1. Local CA (ECDSA P-256, self-signed, CA:true, pathlen:0)
# 2. Daemon cert (signed by local CA, SAN: localhost/127.0.0.1/::1)
# 3. CLI cert (signed by local CA, clientAuth only)
```

Generation is idempotent — it only runs if `local-ca.key` does not exist.

## Cluster Join Flow

### Token proof (token never leaves the joining node)

The invite token is a shared secret between the admin and the joining node. Instead of sending the raw token over HTTP, the joining node proves possession:

```
Request to via node:
    csr_pem         — the joining node's certificate signing request
    token_random    — the random component of the invite token
    token_expiry    — the expiry timestamp of the invite token
    proof           — HMAC-SHA256(csr_pem, full_token_string)

Via node:
    1. Reconstructs full token from random + expiry + master_key
    2. Checks expiry
    3. Verifies HMAC-SHA256(csr_pem, reconstructed_token) == proof
    4. Signs CSR, returns encrypted credentials
```

An eavesdropper sees `token_random`, `token_expiry`, and the HMAC proof but cannot reconstruct the full token without the cluster master key. The proof is bound to the specific CSR — it cannot be replayed with a different one.

### Response encryption

The HTTP endpoint is plain HTTP, but the response body is encrypted with AES-256-GCM using a key derived from the invite token:

```
Key derivation:
    key = HMAC-SHA256("neonfs-invite-response", token)

Encrypted response:
    iv (12 bytes) || tag (16 bytes) || ciphertext

Plaintext:
    JSON: {ca_cert_pem, node_cert_pem, cookie}
```

Only the joining node can decrypt the response because only it has the full token.

### Sequence diagram

```
CLI                  Local daemon                 Via node (eivor)
───                  ────────────                 ────────────────
                     (TLS dist running with
                      local CA certs only)

neonfs cluster join
  --token TOKEN
  --via eivor:9568
        │
        │  TLS distribution (cli.crt → node-local.crt)
        ├──────────────────────►
        │                       Generates node key + CSR
        │                       Computes HMAC proof from token
        │
        │                       POST /api/cluster/redeem-invite
        │                         {csr_pem, token_random,
        │                          token_expiry, proof}
        │                       ──────────────────────────────►
        │                                                      Reconstructs token
        │                                                      Verifies HMAC proof
        │                                                      Signs CSR
        │                                                      Encrypts response
        │                       ◄──────────────────────────────
        │                       Decrypts response (has token)
        │                       Writes ca.crt, node.crt, node.key
        │                       Regenerates ca_bundle.crt
        │                         (adds cluster CA)
        │                       Regenerates ssl_dist.conf
        │                         (adds cluster node cert)
        │                       erlang.set_cookie(node(), cookie)
        │                       Node.connect(via_node)
        │                       Completes Ra join via distribution
        │  {:ok, cluster_info}
        ◄──────────────────────
```

### Cluster init flow

```
CLI                  Local daemon
───                  ────────────
neonfs cluster init
  --name MyCluster
        │
        │  TLS distribution (cli.crt → node-local.crt)
        ├──────────────────────►
        │                       Generates cluster ID, master key
        │                       Creates CA cert + key
        │                       Issues node cert (signs with new CA)
        │                       Writes ca.crt, node.crt, node.key
        │                       Regenerates ca_bundle.crt + ssl_dist.conf
        │                       Sets cluster cookie
        │                       Bootstraps Ra cluster
        │                       Creates system volume
        │  {:ok, cluster_info}
        ◄──────────────────────
```

## Release Configuration

TLS distribution is configured via `ELIXIR_ERL_OPTIONS` in `rel/env.sh.eex` (not `vm.args.eex`, which does not expand environment variables):

```sh
NEONFS_TLS_DIR="${NEONFS_TLS_DIR:-/var/lib/neonfs/tls}"
if [ -f "${NEONFS_TLS_DIR}/ssl_dist.conf" ]; then
  export ELIXIR_ERL_OPTIONS="${ELIXIR_ERL_OPTIONS} -proto_dist inet_tls -ssl_dist_optfile ${NEONFS_TLS_DIR}/ssl_dist.conf"
fi
export ERL_EPMD_ADDRESS="${NEONFS_EPMD_ADDRESS:-0.0.0.0}"
```

The `ssl_dist.conf` file uses the OTP 25+ `certs_keys` option to support multiple certificate/key pairs:

```erlang
[{server, [
  {certs_keys, [#{certfile => "node-local.crt", keyfile => "node-local.key"}]},
  {cacertfile, "ca_bundle.crt"},
  {verify, verify_peer},
  {fail_if_no_peer_cert, true},
  {versions, ['tlsv1.3']}
]},
{client, [...]}].
```

After cluster join, `NeonFS.TLSDistConfig.regenerate/1` rewrites this file to include the cluster node cert as the first entry in `certs_keys` (preferred for inter-node connections).

## CLI Transport

The CLI is rebuilt on `erl_dist` 0.8 directly (dropping `erl_rpc` which hardcodes `TcpStream`). The `erl_dist` crate is generic over `AsyncRead + AsyncWrite + Unpin`, allowing TLS streams to be injected.

- TLS: `futures-rustls` (uses `futures::io` traits matching `erl_dist`)
- Clone bound: `async_dup::Arc` wraps the TLS stream for `erl_dist`'s `channel()` function
- EPMD lookup: plain TCP (EPMD does not support TLS)
- RPC protocol: `SpawnRequest` + `MonitorPExit` (same as `erl_rpc`)
- Client identity: `cli.crt` + `cli.key`, trusts `local-ca.crt`

The CLI's `DaemonConnection` public API (`connect()`, `call()`) stays identical — all existing command implementations work unchanged.

## Pre-Cluster Error Handling

Commands that require a cluster (`volume list`, `drive add`, etc.) return a structured error:

```
Cluster not initialised. Run 'neonfs cluster init' or 'neonfs cluster join' first.
```

Commands that work without a cluster: `cluster init`, `cluster join`, `cluster status`.

## Known Limitations

- **EPMD stays plain TCP**: EPMD connections are not encrypted. This is a known Erlang limitation. EPMD only reveals node names and ports, not data.

- **TLS adds latency**: Every distribution message goes through TLS. TLS 1.3 session resumption mitigates handshake overhead. Bulk data already uses the separate TLS data plane.

- **`erl_dist` crate**: Maintained by a single author (~44K downloads). If unmaintained, the CLI transport needs to be forked.
