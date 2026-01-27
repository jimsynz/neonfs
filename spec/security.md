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

Nodes authenticate to join the cluster via a single-use invite token system. For cluster initialisation and peer discovery details, see [Deployment - Cluster Bootstrap and Discovery](deployment.md#cluster-bootstrap-and-discovery).

**Node Join Flow:**

```
1. Operator creates invite token (short-lived, single-use)
   $ neonfs cluster create-invite --expires 1h
   Token: nfs_inv_7x8k2m9p...

2. New node presents token and specifies a known cluster node
   $ neonfs cluster join --token nfs_inv_7x8k2m9p... --via neonfs@node1.tailnet

3. Cluster validates token:
   - Check token exists and not expired
   - Check token not already used
   - Mark token as used in Ra (before returning success)

4. Cluster registers new node
   Returns: cluster state, peer list, Ra members

5. New node persists cluster state locally (for restart recovery)

6. New node joins BEAM cluster and Ra consensus
```

**Token Properties:**

```elixir
%InviteToken{
  id: "nfs_inv_7x8k2m9p...",
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
  key_version: 3  # Which volume key version encrypted this
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

## Key Rotation

Key rotation is supported for all encryption components: master keys, volume keys, and user keys.

### Key Versioning

All keys are versioned to support rotation without downtime. Chunk metadata tracks which key version was used for encryption:

```elixir
%ChunkCrypto{
  algorithm: :aes_256_gcm,
  nonce: <<12 bytes>>,
  key_version: 3,          # Which volume key version

  # For envelope mode:
  wrapped_deks: [
    %{user_id: "alice", key_version: 2, wrapped_dek: <<...>>},
    %{user_id: "bob", key_version: 1, wrapped_dek: <<...>>}
  ]
}
```

Volume encryption state tracks all active key versions:

```elixir
%VolumeEncryption{
  mode: :server_side,

  # Current key for new writes
  current_key_version: 3,

  # All key versions (wrapped with master key)
  keys: %{
    1 => %{wrapped_key: <<...>>, created_at: ~U[...], deprecated_at: ~U[...]},
    2 => %{wrapped_key: <<...>>, created_at: ~U[...], deprecated_at: ~U[...]},
    3 => %{wrapped_key: <<...>>, created_at: ~U[...], deprecated_at: nil}
  },

  # Rotation state (nil when not rotating)
  rotation: nil | %RotationState{
    from_version: 2,
    to_version: 3,
    started_at: ~U[...],
    progress: %{total_chunks: 1_000_000, migrated: 450_000}
  }
}
```

### Read Path with Key Version Lookup

Reads use the key version stored in chunk metadata:

```elixir
def decrypt_chunk(chunk_hash, volume) do
  chunk = ChunkStore.read(chunk_hash)
  crypto = ChunkIndex.get_crypto_metadata(chunk_hash)

  # Look up the correct key version
  key = get_key_version(volume, crypto.key_version)

  AES.decrypt(chunk.ciphertext, key, crypto.nonce)
end

defp get_key_version(volume, version) do
  case Map.get(volume.encryption.keys, version) do
    nil -> {:error, :unknown_key_version}
    %{wrapped_key: wrapped} -> unwrap_key(wrapped, master_key())
  end
end
```

### Volume Key Rotation (Server-Side Encryption)

Volume key rotation is a long-running background operation that re-encrypts all chunks. It uses the intent log to prevent concurrent rotations and track progress:

```elixir
def start_rotation(volume_id) do
  volume = Metadata.get_volume(volume_id)
  new_version = volume.encryption.current_key_version + 1
  new_key = :crypto.strong_rand_bytes(32)

  intent = %Intent{
    id: UUID.uuid4(),
    operation: :rotate_volume_key,
    conflict_key: {:volume_key_rotation, volume_id},
    params: %{volume_id: volume_id, new_version: new_version},
    state: :pending,
    started_at: DateTime.utc_now(),
    expires_at: nil  # Long-running, uses lease extension
  }

  case IntentLog.try_acquire(intent) do
    {:error, :conflict, _} ->
      {:error, :rotation_already_in_progress}

    {:ok, intent_id} ->
      # Add new key version (old keys still work for reads)
      Metadata.add_volume_key(volume_id, new_version, new_key)
      Metadata.set_current_key_version(volume_id, new_version)

      # Start background re-encryption
      spawn_reencryption_worker(volume_id, new_version, intent_id)

      {:ok, intent_id}
  end
end
```

**Re-encryption worker:**

```elixir
defmodule NeonFS.KeyRotation.Worker do
  @batch_size 1000

  def run(volume_id, target_version, intent_id) do
    volume = Metadata.get_volume(volume_id)
    old_versions = Map.keys(volume.encryption.keys) -- [target_version]

    # Find chunks still on old key versions
    chunks_to_migrate = ChunkIndex.chunks_with_key_versions(volume_id, old_versions)
    total = length(chunks_to_migrate)

    chunks_to_migrate
    |> Stream.chunk_every(@batch_size)
    |> Stream.with_index()
    |> Enum.each(fn {batch, batch_idx} ->
      reencrypt_batch(batch, volume_id, target_version)

      # Update progress
      migrated = min((batch_idx + 1) * @batch_size, total)
      Metadata.update_rotation_progress(volume_id, total, migrated)

      # Extend intent lease
      IntentLog.extend(intent_id)
    end)

    # Rotation complete - schedule old key removal after retention period
    schedule_key_removal(volume_id, old_versions)

    Metadata.clear_rotation_state(volume_id)
    IntentLog.complete(intent_id)
  end

  defp reencrypt_batch(chunk_hashes, volume_id, target_version) do
    volume = Metadata.get_volume(volume_id)
    new_key = get_key_version(volume, target_version)

    for hash <- chunk_hashes do
      # Read and decrypt with old key
      {:ok, plaintext} = decrypt_chunk(hash, volume)

      # Re-encrypt with new key
      nonce = :crypto.strong_rand_bytes(12)
      ciphertext = AES.encrypt(plaintext, new_key, nonce)

      # Atomic update
      ChunkStore.update_encrypted(hash, ciphertext, nonce, target_version)
    end
  end
end
```

**Reads during rotation:**

Reads work throughout rotation because:
- Old key versions remain available until rotation completes
- Each chunk's metadata specifies which key version to use
- New writes use the new key version

### Master Key Rotation

Master key rotation is simpler—it only re-wraps volume keys, not chunk data:

```elixir
def rotate_master_key(new_master_key) do
  old_master_key = get_master_key()

  # Re-wrap all volume keys with new master key
  for volume <- Metadata.list_volumes() do
    for {version, key_data} <- volume.encryption.keys do
      unwrapped = unwrap_key(key_data.wrapped_key, old_master_key)
      rewrapped = wrap_key(unwrapped, new_master_key)

      Metadata.update_wrapped_key(volume.id, version, rewrapped)
    end
  end

  # Store new master key (implementation-dependent)
  store_master_key(new_master_key)
end
```

### User Key Rotation (Envelope Mode)

User key rotation re-wraps DEKs to the user's new public key without re-encrypting chunk data:

```elixir
def rotate_user_key(user_id, new_public_key, dek_provider) do
  # dek_provider is a function that returns the DEK for a chunk
  # (user provides via their old private key, or admin recovery)

  chunks = ChunkIndex.chunks_with_user_dek(user_id)

  for chunk_hash <- chunks do
    crypto = ChunkIndex.get_crypto_metadata(chunk_hash)

    # Get the DEK (unwrapped by user's old key)
    dek = dek_provider.(chunk_hash, crypto)

    # Re-wrap to new public key
    new_wrapped = RSA.encrypt(dek, new_public_key)

    # Update metadata
    new_wrapped_deks = crypto.wrapped_deks
      |> Enum.reject(& &1.user_id == user_id)
      |> List.insert_at(0, %{
        user_id: user_id,
        key_version: next_user_key_version(user_id),
        wrapped_dek: new_wrapped
      })

    ChunkIndex.update_wrapped_deks(chunk_hash, new_wrapped_deks)
  end

  Metadata.update_user_public_key(user_id, new_public_key)
end
```

### Rotation Configuration

```yaml
encryption:
  key_rotation:
    # Re-encryption batch size
    batch_size: 1000

    # Rate limit to avoid overwhelming the system
    chunks_per_second: 1000

    # Priority in I/O scheduler
    priority: low

    # Keep old key versions after rotation completes
    # (allows late reads to still work during cleanup)
    key_retention_after_rotation: 24h
```

### CLI Commands

```bash
# Start volume key rotation
$ neonfs volume rotate-key documents
Starting key rotation for volume 'documents'...
New key version: 4
Progress: 0 / 1,234,567 chunks

# Check rotation status
$ neonfs volume rotation-status documents
Volume: documents
Status: in_progress
From version: 3
To version: 4
Progress: 567,890 / 1,234,567 chunks (46%)
Estimated completion: 2h 15m

# Rotate master key (requires current key)
$ neonfs cluster rotate-master-key
Enter current master key passphrase: ****
Enter new master key passphrase: ****
Confirm new passphrase: ****
Re-wrapping 12 volume keys...
Master key rotated successfully.

# Rotate user key (envelope mode)
$ neonfs user rotate-key alice --new-pubkey /path/to/new_key.pub
This will re-wrap DEKs for 45,678 chunks.
User must provide DEKs using their old private key.
Continue? [y/N]
```

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
