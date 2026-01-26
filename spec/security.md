# Security Model

This document describes network security, authentication, encryption, access control, and the threat model.

> **Critical Trust Model**: NeonFS clusters operate on a full-trust model between nodes. Once a node joins the cluster, it has complete access to all data, metadata, and cluster operations. All security features (encryption, ACLs) protect against external threats and access control between *users*—they do not provide isolation between cluster nodes. A single compromised node means complete cluster compromise. See [Accepted Risks](#accepted-risks--cluster-node-trust) for full implications.

## Network Security (Deployment Recommendation)

NeonFS itself doesn't mandate a specific network security model — this is a deployment concern. However, we strongly recommend running nodes on a private network, such as:

- **WireGuard mesh** via Tailscale or self-hosted Headscale
- **Private VLAN** if all nodes are in the same datacenter
- **VPN** for cross-site deployments

BEAM distribution is trusting by default. Once a node connects, it has full RPC access. Protect the network perimeter accordingly.

```
┌─────────────────────────────────────────────────┐
│  Recommended: Private network (WireGuard/VLAN)  │
│                                                 │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐          │
│  │ node1   │──│ node2   │──│ node3   │          │
│  └─────────┘  └─────────┘  └─────────┘          │
│                                                 │
│  BEAM distribution + data plane traffic         │
└─────────────────────────────────────────────────┘
         │
    Your responsibility to secure
```

## Cluster Authentication

Nodes authenticate to join the cluster via a single-use invite token system.

**Node Join Flow:**

```
1. Operator creates invite token (short-lived, single-use)
   $ neonfs cluster create-invite --expires 1h
   Token: bfs_inv_7x8k2m9p...

2. New node presents token
   $ neonfs cluster join --token bfs_inv_7x8k2m9p...

3. Cluster validates token:
   - Check token exists and not expired
   - Check token not already used
   - Mark token as used in Ra (before returning success)

4. Cluster registers new node
   Returns: cluster state, peer list

5. New node joins BEAM cluster and Ra consensus
```

**Token Properties:**

```elixir
%InviteToken{
  id: "bfs_inv_7x8k2m9p...",
  created_at: ~U[...],
  expires_at: ~U[...],
  created_by: :node1,
  used: false,          # Set to true on successful join
  used_by: nil,         # Node that used this token
  used_at: nil          # When it was used
}
```

**Security properties:**
- **Single-use**: Token is marked used in Ra before join completes; replay attempts fail
- **Short-lived**: Default 1 hour expiry limits exposure window
- **Auditable**: Token usage recorded with timestamp and joining node

This prevents random nodes from joining but doesn't require complex PKI.

**Limitations of Cookie-Based Authentication:**

After joining, ongoing node authentication relies on Erlang's cookie mechanism—a shared secret that all cluster nodes know. This has known weaknesses:

- Cookies are long-lived with no automatic rotation
- Cookie rotation requires cluster restart
- Anyone who obtains the cookie can connect to the cluster (mitigated by network isolation)
- No per-node identity verification after initial join

**Future improvement: TLS Distribution**

A future enhancement is to enable TLS-encrypted Erlang distribution with mutual certificate verification. This would provide:

- Encrypted node-to-node traffic (currently plaintext, relies on network-level encryption)
- Per-node certificate identity, enabling revocation of individual nodes
- Standard PKI practices for certificate lifecycle

This is on the roadmap but not required for initial development. The current design prioritises a working system with network-level security (WireGuard/VPN), with stronger node authentication added later.

## Key Hierarchy

Keys are used for encryption only (not network auth):

```
Cluster Master Key
    │
    └─► Per-Volume Keys (for server-side encryption)
            │
            └─► Wrapped with master key, stored in metadata

User Public Keys (for envelope encryption)
    │
    └─► Stored in metadata, used to wrap chunk DEKs
```

**Master Key Storage:**

The cluster master key should be protected. Options:

- Derived from passphrase at cluster startup
- Stored in external secret manager (Vault, AWS KMS, etc.)
- Stored encrypted on disk, unlocked at boot

For a home lab, passphrase-at-boot is reasonable. For production, integrate with a proper secret manager.

## Data Encryption

Three encryption modes, with different tradeoffs for multi-user scenarios and deduplication effectiveness.

### Mode 1: No encryption (`mode: :none`)

- Chunks stored in plaintext
- Deduplication works fully across configured scope
- Suitable for non-sensitive data

### Mode 2: Server-side encryption (`mode: :server_side`)

- Volume has a symmetric key (AES-256)
- Chunks encrypted before storage, decrypted on read
- All users with volume access share the key
- Server can read all data

```
Chunk on disk:
  ciphertext: AES-256-GCM(data, volume_key)
  nonce: unique per chunk
```

### Mode 3: Envelope encryption with user keys (`mode: :envelope`)

PGP-style approach for true multi-user access control:

```
Chunk on disk:
  ciphertext: AES-256-GCM(data, chunk_dek)
  wrapped_keys: [
    {user: "alice", wrapped_dek: RSA_encrypt(chunk_dek, alice_pubkey)},
    {user: "bob", wrapped_dek: RSA_encrypt(chunk_dek, bob_pubkey)},
  ]
  nonce: unique per chunk
```

- Each chunk encrypted with a random DEK (data encryption key)
- DEK wrapped to each authorised user's public key
- Adding a user: wrap DEK to their key, no re-encryption of data
- Removing a user: remove their wrapped key (they can't decrypt new chunks; for old chunks, must re-encrypt if you want true revocation)

### Envelope Encryption: Key Management Responsibilities

Envelope encryption is a power-user feature requiring external key management. NeonFS provides the cryptographic infrastructure but does not manage private keys.

*What NeonFS provides:*
- Public key registration and storage
- DEK generation and wrapping to registered public keys
- Wrapped DEK storage and retrieval
- Re-wrapping DEKs when users are added to a volume

*What users must provide:*
- Private key generation (RSA-4096 or Ed25519 recommended)
- Private key storage (GPG keyring, hardware token, Vault, etc.)
- Client-side decryption (private key never sent to server)
- Key backup and recovery strategy

*Access patterns:*
```
Read flow (envelope encryption):
1. Client requests file
2. Server returns ciphertext + wrapped DEK for requesting user
3. Client unwraps DEK using their private key (client-side)
4. Client decrypts ciphertext (client-side)
```

*Implications:*
- **Lost private key = lost access**: NeonFS cannot recover data if user loses their key
- **Headless access**: Service accounts need accessible private keys (HSM, Vault agent, mounted keyfile)
- **Key rotation**: User generates new keypair, admin re-wraps DEKs to new public key, old key can be revoked
- **No server-side search**: Server cannot inspect plaintext for indexing or search

*Future integration points* (not in initial implementation):
- PKCS#11 / HSM integration
- Vault Transit secrets engine
- Agent-based key injection

## Deduplication and Encryption Interaction

**TL;DR**: Encryption usually prevents storage savings from deduplication. Within a single volume using the same encryption key, deduplication works normally. Across volumes with different keys, identical files are stored twice.

Content hashing is always performed on **original (plaintext) data**, so the system can *detect* duplicates regardless of encryption. However, *storage savings* require identical ciphertext, which only happens when the same key encrypts the same data.

| Scenario | Detects Duplicates | Saves Storage | Why |
|----------|-------------------|---------------|-----|
| No encryption | ✓ | ✓ | Same bytes stored |
| Same volume, server-side encryption | ✓ | ✓ | Same key → same ciphertext |
| Cross-volume, different keys | ✓ | ✗ | Different keys → different ciphertext |
| Envelope encryption (same DEK) | ✓ | ✓ | Same DEK → same ciphertext |
| Envelope encryption (random DEK) | ✓ | ✗ | Different DEK per chunk |

**Example: Cross-volume with different keys**

```
Volume A (key: K1):
  file1.txt = "hello world"
  → hash = sha256("hello world") = abc123
  → ciphertext = AES(K1, "hello world") = XXXXX

Volume B (key: K2):
  file2.txt = "hello world"
  → hash = sha256("hello world") = abc123 (match!)
  → ciphertext = AES(K2, "hello world") = YYYYY (different!)
```

The hashes match, so deduplication *detects* the duplicate. But the ciphertexts differ, so both must be stored. The system tracks them as logically deduplicated (shared hash) but physically separate (different stored bytes).

**Implications by encryption mode:**

| Mode | Dedup Scope | Notes |
|------|-------------|-------|
| `:none` | Full (per config) | Cross-volume dedup works if enabled |
| `:server_side` | Within same-key boundary | Typically per-volume; cross-volume only if volumes share key |
| `:envelope` | Within same-DEK boundary | DEK reuse enables dedup; fresh DEK per chunk disables it |

**Envelope encryption DEK strategy:**

For envelope encryption, the DEK selection strategy affects deduplication:

```elixir
%Volume{
  encryption: %{
    mode: :envelope,
    dek_strategy: :content_derived  # :random | :content_derived
  }
}
```

- `:random` — Fresh DEK per chunk. Maximum security, no ciphertext dedup.
- `:content_derived` — DEK derived from content hash + volume secret. Same plaintext → same DEK → same ciphertext. Enables dedup but reveals duplicate existence to storage layer.

Default is `:random` for maximum security. Use `:content_derived` only when dedup savings outweigh the metadata leakage concern.

**Multi-user implications:**

| Scenario | Server-side | Envelope |
|----------|-------------|----------|
| Shared volume, all users equal | Simple | Overkill |
| Need to revoke access immediately | Re-key volume | Re-encrypt affected chunks |
| Distrust server operators | No protection | Full E2E |
| Deduplication | Full | Per-user (same DEK) or limited |

**Recommendation:** Start with server-side encryption. Add envelope mode later for high-security use cases.

**Key Storage:**

- Server-side: volume keys stored in metadata, wrapped with cluster master key
- Envelope: user public keys stored in metadata; private keys managed by users (or a separate key service)

## Identity and Access Control

Access control requires storing users, groups, and permissions in the metadata layer.

### Users

```
User {
  id: UserId
  name: String

  # Authentication
  password_hash: Argon2 hash (for API auth)
  public_key: RSA/Ed25519 (for envelope encryption)
  api_keys: [ApiKey]

  # Metadata
  created_at: DateTime
  disabled: bool
}
```

### Groups

```
Group {
  id: GroupId
  name: String
  members: [UserId]
  created_at: DateTime
}
```

### Volume Access Control

Each volume has an access control list:

```
VolumeACL {
  volume_id: VolumeId
  owner: UserId              # Full control

  entries: [
    %{principal: {:user, UserId} | {:group, GroupId},
      permissions: [:read | :write | :admin]}
  ]
}
```

### File/Directory ACLs (Optional)

For finer-grained control, support POSIX-style ACLs on files and directories:

```
FileACL {
  path: String
  volume_id: VolumeId

  # POSIX mode bits (for basic compatibility)
  mode: u16
  uid: UserId
  gid: GroupId

  # Extended ACL entries (optional)
  acl_entries: [
    %{type: :user | :group | :mask | :other,
      id: UserId | GroupId | nil,
      permissions: :r | :w | :x | :rw | :rx | :rwx}
  ]
}
```

### CIFS/SMB ACL Mapping

When accessed via CIFS, map to Windows-style ACLs:

- NeonFS users ↔ Windows SIDs (via Samba's idmap)
- NeonFS groups ↔ Windows groups
- Permission mapping: read/write/admin → appropriate Windows ACEs

This is handled at the Samba VFS layer, translating between Windows semantics and NeonFS's internal model.

### Authorisation Check

```elixir
def authorize(user, action, resource) do
  cond do
    # Volume-level check
    is_volume_owner?(user, resource.volume) -> :ok
    has_volume_permission?(user, resource.volume, action) -> :ok

    # File-level check (if ACLs enabled)
    has_file_permission?(user, resource.path, action) -> :ok

    true -> {:error, :forbidden}
  end
end

defp has_volume_permission?(user, volume, action) do
  acl = get_volume_acl(volume)

  user_permissions = acl.entries
    |> Enum.filter(fn entry ->
      match_principal?(entry.principal, user)
    end)
    |> Enum.flat_map(& &1.permissions)
    |> MapSet.new()

  required = permission_for_action(action)
  MapSet.member?(user_permissions, required)
end
```

## Threat Model Summary

**Primary threats addressed:**
- Unauthorised volume/file access (via access control)
- Data at rest on stolen drives (via encryption)
- Corrupted data (via content-addressing and verification)

**Deployment concerns (your responsibility):**
- Network security between nodes
- Physical security of nodes
- Secure storage of master key / passphrase

### Accepted Risks — Cluster Node Trust

BEAM distribution is inherently trusting. Once a node joins the cluster, it has full RPC access to all other nodes. This is a fundamental property of the Erlang runtime, not something NeonFS can meaningfully restrict without abandoning BEAM's benefits.

A compromised cluster node can:
- Read any chunk from any other node
- Modify cluster metadata (volumes, users, ACLs)
- Impersonate users for internal operations
- Delete or corrupt data across the cluster

**This means:**
- All cluster nodes must be equally trusted
- A single compromised node = full cluster compromise
- Network perimeter security is critical (WireGuard/VPN strongly recommended)
- Physical access to any node should be treated as access to all data

**Why we accept this:**
- Fighting BEAM's trust model would sacrifice its operational benefits (hot code reload, distributed debugging, cluster management)
- The target deployment is trusted environments (home lab, small team infrastructure)
- Network-level isolation provides practical security for these use cases
- Multi-tenant or zero-trust deployments are explicitly out of scope

**Additional accepted risks:**
- Admin with master key access can read all server-side encrypted data
- Envelope encryption provides user-level isolation, but requires users to manage their own keys
