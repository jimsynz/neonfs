# NeonFS CLI reference

Auto-generated from `neonfs --help` (clap). Regenerate with `scripts/regen-cli-reference.sh`.

The `neonfs` binary is a Rust command-line client that talks to the core cluster over Erlang distribution. Every command accepts `--output json` or `--json` for machine-readable output (default is a table).

## Top-level

```
Command-line interface for NeonFS distributed filesystem

Usage: neonfs-cli [OPTIONS] <COMMAND>

Commands:
  acl         ACL management
  audit       Audit log
  backup      Backup orchestration (snapshot + export + import)
  cluster     Cluster management
  credential  Credential management (S3 SigV4 + WebDAV Basic auth)
  dr          Disaster-recovery snapshot management
  drive       Drive management
  escalation  Decision escalation management
  gc          Garbage collection
  job         Background job management
  block       Block device management
  fuse        FUSE mount management
  nfs         NFS export management
  node        Node management
  s3          S3 bucket management
  scrub       Integrity scrubbing
  volume      Volume management
  worker      Background worker management
  help        Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
  -V, --version          Print version
```

## `neonfs acl`

```
ACL management

Usage: neonfs-cli acl [OPTIONS] <COMMAND>

Commands:
  grant     Grant permissions to a principal on a volume
  revoke    Revoke all permissions for a principal on a volume
  show      Show volume ACL
  set-file  Set file/directory ACL properties
  get-file  Get file/directory ACL
  help      Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs acl grant`

```
Grant permissions to a principal on a volume

Usage: neonfs-cli acl grant [OPTIONS] <VOLUME> <PRINCIPAL> <PERMISSIONS>

Arguments:
  <VOLUME>       Volume name
  <PRINCIPAL>    Principal (uid:N or gid:N)
  <PERMISSIONS>  Permissions (comma-separated: read,write,admin)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs acl revoke`

```
Revoke all permissions for a principal on a volume

Usage: neonfs-cli acl revoke [OPTIONS] <VOLUME> <PRINCIPAL>

Arguments:
  <VOLUME>     Volume name
  <PRINCIPAL>  Principal (uid:N or gid:N)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs acl show`

```
Show volume ACL

Usage: neonfs-cli acl show [OPTIONS] <VOLUME>

Arguments:
  <VOLUME>  Volume name

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs acl set-file`

```
Set file/directory ACL properties

Usage: neonfs-cli acl set-file [OPTIONS] <VOLUME> <PATH>

Arguments:
  <VOLUME>  Volume name
  <PATH>    File path within the volume

Options:
      --mode <MODE>      POSIX mode bits (octal, e.g. 755)
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
      --uid <UID>        Owner UID
  -h, --help             Print help
```

### `neonfs acl get-file`

```
Get file/directory ACL

Usage: neonfs-cli acl get-file [OPTIONS] <VOLUME> <PATH>

Arguments:
  <VOLUME>  Volume name
  <PATH>    File path within the volume

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

## `neonfs audit`

```
Audit log

Usage: neonfs-cli audit [OPTIONS] <COMMAND>

Commands:
  list  List audit log events
  help  Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs audit list`

```
List audit log events

Usage: neonfs-cli audit list [OPTIONS]

Options:
      --output <OUTPUT>      Output format (json or table) [default: table]
      --type <TYPE>          Filter by event type (e.g. volume_created, acl_grant)
      --json                 Enable JSON output (shorthand for --output json)
      --uid <UID>            Filter by actor UID
      --resource <RESOURCE>  Filter by resource
      --since <SINCE>        Show events since (ISO 8601 datetime)
      --until <UNTIL>        Show events until (ISO 8601 datetime)
      --limit <LIMIT>        Maximum number of results [default: 50]
  -h, --help                 Print help
```

## `neonfs backup`

```
Backup orchestration (snapshot + export + import)

Usage: neonfs-cli backup [OPTIONS] <COMMAND>

Commands:
  create   Take a snapshot, export it to the destination, then drop the snapshot. On export failure the snapshot is *left* in place so the operator can retry without re-snapshotting
  list     Inspect an existing backup's manifest without unpacking the body
  restore  Restore a backup into a new volume. Same wiring as `volume import` — exposed here because that's where operators look
  help     Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs backup create`

```
Take a snapshot, export it to the destination, then drop the snapshot. On export failure the snapshot is *left* in place so the operator can retry without re-snapshotting

Usage: neonfs-cli backup create [OPTIONS] --volume <VOLUME> --to <TO>

Options:
      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --volume <VOLUME>
          Volume to back up

      --json
          Enable JSON output (shorthand for --output json)

      --to <TO>
          Destination tarball path on the daemon's filesystem

      --name <NAME>
          Optional snapshot name (default: generated)

      --incremental-from <INCREMENTAL_FROM>
          Make this an incremental backup against a prior archive: files unchanged since that archive are carried by reference rather than re-shipped. The prior archive's path on the daemon's filesystem

      --encrypt-with <ENCRYPT_WITH>
          Encrypt the archive at rest. The wrap key comes from the named source

          Possible values:
          - passphrase: Derive the wrap key from an interactively-prompted passphrase (or `--passphrase-file` for automation)

      --passphrase-file <PASSPHRASE_FILE>
          Read the passphrase from this file instead of prompting (for non-interactive automation). The trailing newline is stripped

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs backup list`

```
Inspect an existing backup's manifest without unpacking the body

Usage: neonfs-cli backup list [OPTIONS] --from <FROM>

Options:
      --from <FROM>      Backup tarball path on the daemon's filesystem
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs backup restore`

```
Restore a backup into a new volume. Same wiring as `volume import` — exposed here because that's where operators look

Usage: neonfs-cli backup restore [OPTIONS] --from <FROM> --as <NEW_NAME>

Options:
      --from <FROM>
          Backup tarball path on the daemon's filesystem. The base (full) archive when restoring an incremental chain

      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --chain <CHAIN>
          Incremental archives to replay onto `--from`, oldest first, comma-separated. Omit for a plain restore

      --json
          Enable JSON output (shorthand for --output json)

      --decrypt-with <DECRYPT_WITH>
          Decrypt an encrypted archive, using the named key source. Must match what `create --encrypt-with` used

          Possible values:
          - passphrase: Derive the wrap key from an interactively-prompted passphrase (or `--passphrase-file` for automation)

      --passphrase-file <PASSPHRASE_FILE>
          Read the passphrase from this file instead of prompting

      --as <NEW_NAME>
          Name of the new volume to create

      --into-existing
          Restore into the existing volume named by `--as` instead of creating a new one. The volume must already exist; files overwrite by path. Used for full-cluster DR restore

  -h, --help
          Print help (see a summary with '-h')
```

## `neonfs cluster`

```
Cluster management

Usage: neonfs-cli cluster [OPTIONS] <COMMAND>

Commands:
  ca                     Certificate authority management
  create-invite          Create an invite token for joining nodes
  init                   Initialize a new cluster
  join                   Join an existing cluster
  rebalance              Rebalance storage across drives within each tier
  rebalance-status       Show status of an active rebalance operation
  repair                 Replica-count repair
  status                 Show cluster status
  remove-node            Permanently decommission a node from the cluster
  drain-node             Begin graceful decommission of a node: mark it draining and evacuate its drives
  undrain-node           Reverse a drain: mark a node active again, to abort a decommission before the node is removed
  cordon-node            Cordon a node for planned maintenance: mark it `maintenance` so new placement and client routing avoid it, without evacuating its drives or changing Ra membership
  uncordon-node          Reverse a cordon: mark a node `active` again so placement and routing resume giving it work
  cordon-stop-check      Check whether stopping a (cordoned) node is safe before taking it offline for maintenance
  freeze                 Freeze the whole cluster for a coordinated maintenance shutdown
  thaw                   Thaw the cluster after a coordinated restart
  force-reset            Rebuild the Ra quorum from a surviving minority after catastrophic membership loss
  reconstruct-from-disk  Rebuild the bootstrap-layer Ra state from on-disk volume data
  help                   Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs cluster ca`

```
Certificate authority management

Usage: neonfs-cli cluster ca [OPTIONS] <COMMAND>

Commands:
  info                 Display CA information (subject, algorithm, validity, serial counter)
  list                 List all issued node certificates
  revoke               Revoke a node's certificate
  rotate               Drive the cluster CA rotation lifecycle
  emergency-bootstrap  Restore CA material on a single node after the cluster CA has expired
  help                 Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs cluster create-invite`

```
Create an invite token for joining nodes

Usage: neonfs-cli cluster create-invite [OPTIONS]

Options:
      --expires <EXPIRES>  Token expiration duration (e.g., "1h", "30m", "3600") [default: 1h]
      --output <OUTPUT>    Output format (json or table) [default: table]
      --json               Enable JSON output (shorthand for --output json)
      --uses <USES>        How many nodes may redeem this token [default: 1]
  -h, --help               Print help
```

### `neonfs cluster init`

```
Initialize a new cluster

Usage: neonfs-cli cluster init [OPTIONS] --name <NAME> --drive <DRIVE>

Options:
      --name <NAME>
          Cluster name
      --output <OUTPUT>
          Output format (json or table) [default: table]
      --drive <DRIVE>
          Path to the initial drive that will host the system volume. Required on a freshly-installed daemon (no default drive ships)
      --json
          Enable JSON output (shorthand for --output json)
      --tier <TIER>
          Storage tier for the initial drive [default: hot]
      --system-replicas <SYSTEM_REPLICAS>
          Replication factor to seed the `_system` volume with. Raise it on a cluster you plan to scale so the system volume isn't pinned to single-copy storage
  -h, --help
          Print help
```

### `neonfs cluster join`

```
Join an existing cluster

Usage: neonfs-cli cluster join [OPTIONS] --token <TOKEN> --via <VIA>

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --token <TOKEN>    Invite token from existing cluster
      --json             Enable JSON output (shorthand for --output json)
      --via <VIA>        Address of existing cluster member (host:port, e.g., node1:9568)
  -h, --help             Print help
```

### `neonfs cluster rebalance`

```
Rebalance storage across drives within each tier

Usage: neonfs-cli cluster rebalance [OPTIONS]

Options:
      --output <OUTPUT>          Output format (json or table) [default: table]
      --tier <TIER>              Only rebalance a specific tier (hot, warm, cold)
      --json                     Enable JSON output (shorthand for --output json)
      --threshold <THRESHOLD>    Balance tolerance (0.0-1.0, default: 0.10) [default: 0.10]
      --batch-size <BATCH_SIZE>  Chunks per migration batch [default: 50]
      --wait                     Block until the rebalance job finishes; exits non-zero on failure
  -h, --help                     Print help
```

### `neonfs cluster rebalance-status`

```
Show status of an active rebalance operation

Usage: neonfs-cli cluster rebalance-status [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs cluster repair`

```
Replica-count repair

Usage: neonfs-cli cluster repair [OPTIONS] <COMMAND>

Commands:
  start   Queue a replica-count repair pass
  status  Show recent replica-repair job history
  help    Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs cluster status`

```
Show cluster status

Usage: neonfs-cli cluster status [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs cluster remove-node`

```
Permanently decommission a node from the cluster

Revokes the node's certificate and removes it from the Ra quorum membership. Refuses if the node is the current Ra leader or still owns drives (unless --force is passed).

Usage: neonfs-cli cluster remove-node [OPTIONS] <NODE>

Arguments:
  <NODE>
          Target node name (e.g. `neonfs_core@host2` or `host2`)

Options:
      --force
          Skip the drive-presence check. Force-removing a node with resident chunks risks losing any chunk whose only replica was on that node

      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs cluster drain-node`

```
Begin graceful decommission of a node: mark it draining and evacuate its drives.

Marking the node draining stops new replica placement and client routing from giving it work; its drives are then evacuated in the background. Run `cluster remove-node` once the drives are empty.

Usage: neonfs-cli cluster drain-node [OPTIONS] <NODE>

Arguments:
  <NODE>
          Target node name (e.g. `neonfs_core@host2` or `host2`)

Options:
      --no-evacuate
          Mark the node draining without starting drive evacuation

      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs cluster undrain-node`

```
Reverse a drain: mark a node active again, to abort a decommission before the node is removed

Usage: neonfs-cli cluster undrain-node [OPTIONS] <NODE>

Arguments:
  <NODE>  Target node name (e.g. `neonfs_core@host2` or `host2`)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs cluster cordon-node`

```
Cordon a node for planned maintenance: mark it `maintenance` so new placement and client routing avoid it, without evacuating its drives or changing Ra membership.

Use before a reboot / kernel upgrade / hardware swap, then `uncordon-node` once it is back.

Usage: neonfs-cli cluster cordon-node [OPTIONS] <NODE>

Arguments:
  <NODE>
          Target node name (e.g. `neonfs_core@host2` or `host2`)

Options:
      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs cluster uncordon-node`

```
Reverse a cordon: mark a node `active` again so placement and routing resume giving it work

Usage: neonfs-cli cluster uncordon-node [OPTIONS] <NODE>

Arguments:
  <NODE>  Target node name (e.g. `neonfs_core@host2` or `host2`)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs cluster cordon-stop-check`

```
Check whether stopping a (cordoned) node is safe before taking it offline for maintenance.

Read-only: refuses (non-zero exit) if stopping the node would break Ra quorum, strand a chunk (no trusted replica elsewhere), or drop a chunk below its volume's `min_copies`. Gate your `systemctl stop` on it. `--force` reports the findings but exits zero anyway.

Usage: neonfs-cli cluster cordon-stop-check [OPTIONS] <NODE>

Arguments:
  <NODE>
          Target node name (e.g. `neonfs_core@host2` or `host2`)

Options:
      --force
          Exit zero even if the stop would be unsafe (still prints why)

      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs cluster freeze`

```
Freeze the whole cluster for a coordinated maintenance shutdown.

Cuts client write ingress (new writes are refused), lets in-flight writes settle, and triggers a metadata snapshot — then reports ready-to-power-off. Stop interface nodes first, then core nodes; bring them back and run `cluster thaw`.

Usage: neonfs-cli cluster freeze [OPTIONS]

Options:
      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs cluster thaw`

```
Thaw the cluster after a coordinated restart.

Enters the `recovering` state so failure-driven repair stays suppressed while the cluster reassembles; it returns to normal automatically once all members are back and drives are verified.

Usage: neonfs-cli cluster thaw [OPTIONS]

Options:
      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs cluster force-reset`

```
Rebuild the Ra quorum from a surviving minority after catastrophic membership loss.

This is a dangerous, last-resort operation. Every safety gate is evaluated before any mutation is attempted; a full audit entry is written once all gates pass. The Ra state mutation itself lands separately — for now the command exits with a "not yet implemented" error after the audit entry is recorded.

Usage: neonfs-cli cluster force-reset [OPTIONS] --keep <KEEP>

Options:
      --keep <KEEP>
          Surviving node to keep in the rebuilt quorum. Repeatable and comma-separated (`--keep a,b` or `--keep a --keep b`). At least one value is required. Must name a node currently in the Ra membership (e.g. `neonfs_core@host1` or `host1`)

      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

      --min-unreachable-seconds <MIN_UNREACHABLE_SECONDS>
          Minimum time a departed member must have been unreachable before force-reset will accept it as gone (default 1800 = 30m). Lower values are intended for tests only — in production the grace window is the safety wall against a healing partition
          
          [default: 1800]

      --yes-i-accept-data-loss
          Required acknowledgement that force-reset can drop committed writes on the surviving minority. Refuses locally if absent

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs cluster reconstruct-from-disk`

```
Rebuild the bootstrap-layer Ra state from on-disk volume data.

Use this when Ra logs are unrecoverable but the underlying volume data (drive identity files + root segment chunks) is intact. Walks every configured drive's `blobs/` tree, decodes candidate chunks as root segments, and submits the matching `:register_drive` / `:register_volume_root` Ra commands.

Last-resort operation. Refuses without `--yes` and refuses if the bootstrap layer already has volumes registered (use `--overwrite-ra-state` to force, or `--dry-run` to preview).

Usage: neonfs-cli cluster reconstruct-from-disk [OPTIONS]

Options:
      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --yes
          Required acknowledgement that this is the right call. Refuses locally if absent. Bypassed by `--dry-run`

      --json
          Enable JSON output (shorthand for --output json)

      --overwrite-ra-state
          Allow reconstruction when the bootstrap layer's `volume_roots` table is non-empty. Without this flag, a reconstruction misfire on a healthy cluster is bounded

      --dry-run
          Preview the discovered drives + commands without submitting anything to Ra. Doesn't require `--yes`

  -h, --help
          Print help (see a summary with '-h')
```

## `neonfs credential`

```
Credential management (S3 SigV4 + WebDAV Basic auth)

Usage: neonfs-cli credential [OPTIONS] <COMMAND>

Commands:
  create  Create a new credential
  list    List credentials
  show    Show details of a credential
  rotate  Rotate the secret key for a credential
  delete  Delete a credential
  help    Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs credential create`

```
Create a new credential

Usage: neonfs-cli credential create [OPTIONS] --user <USER>

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --user <USER>      User identity to associate with the credential
      --json             Enable JSON output (shorthand for --output json)
      --uid <UID>        POSIX uid requests made with this credential are authorised as. Without it the credential authenticates but is refused everything
      --gids <GIDS>      Supplementary POSIX group ids, comma-separated
  -h, --help             Print help
```

### `neonfs credential list`

```
List credentials

Usage: neonfs-cli credential list [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --user <USER>      Filter by user identity
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs credential show`

```
Show details of a credential

Usage: neonfs-cli credential show [OPTIONS] <ACCESS_KEY_ID>

Arguments:
  <ACCESS_KEY_ID>  Access key ID to show

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs credential rotate`

```
Rotate the secret key for a credential

Usage: neonfs-cli credential rotate [OPTIONS] <ACCESS_KEY_ID>

Arguments:
  <ACCESS_KEY_ID>  Access key ID to rotate

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs credential delete`

```
Delete a credential

Usage: neonfs-cli credential delete [OPTIONS] <ACCESS_KEY_ID>

Arguments:
  <ACCESS_KEY_ID>  Access key ID to delete

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

## `neonfs dr`

```
Disaster-recovery snapshot management

Usage: neonfs-cli dr [OPTIONS] <COMMAND>

Commands:
  snapshot  Snapshot management
  restore   Full-cluster restore: stage + apply an exported DR snapshot, then restore each volume's content from its backup archive. Run this on a freshly-bootstrapped single node (`neonfs cluster init`); reattach the remaining nodes with `neonfs cluster join` afterwards
  help      Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs dr snapshot`

```
Snapshot management

Usage: neonfs-cli dr snapshot [OPTIONS] <COMMAND>

Commands:
  create  Create an immediate snapshot of the cluster's metadata + CA state
  list    List every snapshot in the `_system` volume's `/dr` directory
  show    Show a single snapshot's manifest
  apply   Apply a snapshot's cluster-wide metadata back into live Ra state. Overlays the eight cluster-wide keyspaces (volumes, services, encryption keys, ACLs, segment assignments, credentials, escalations, KV); per-volume content is restored separately via `backup restore`
  export  Export a snapshot off-cluster to a directory on the daemon's filesystem so it survives a bare-metal disaster — the in-cluster copy is destroyed with `_system`
  import  Stage an exported snapshot back into a freshly-bootstrapped cluster's `_system` volume, ready for `apply`
  help    Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs dr restore`

```
Full-cluster restore: stage + apply an exported DR snapshot, then restore each volume's content from its backup archive. Run this on a freshly-bootstrapped single node (`neonfs cluster init`); reattach the remaining nodes with `neonfs cluster join` afterwards

Usage: neonfs-cli dr restore [OPTIONS] --source <SOURCE>

Options:
      --output <OUTPUT>          Output format (json or table) [default: table]
      --source <SOURCE>          Source directory produced by `dr snapshot export`, holding the snapshot plus (by default) `volumes/<name>.backup` archives
      --catalogue <CATALOGUE>    Optional JSON catalogue `{"<volume>": "<archive-path>"}` pinning where each volume's backup archive lives. Relative paths resolve against `--source`; the catalogue is authoritative, so omitted volumes are left as empty shells
      --json                     Enable JSON output (shorthand for --output json)
      --passphrase <PASSPHRASE>  Passphrase for encrypted backup archives
  -h, --help                     Print help
```

## `neonfs drive`

```
Drive management

Usage: neonfs-cli drive [OPTIONS] <COMMAND>

Commands:
  add       Add a new drive to this node
  remove    Remove a drive from this node
  list      List all drives across the cluster
  evacuate  Evacuate all data from a drive (graceful removal)
  resume    Return a draining drive to active
  replicas  Show replication health: under-replicated volumes and drives holding the sole copy of anything
  help      Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs drive add`

```
Add a new drive to this node

Usage: neonfs-cli drive add [OPTIONS] --path <PATH>

Options:
      --output <OUTPUT>      Output format (json or table) [default: table]
      --path <PATH>          Absolute path to the storage directory
      --json                 Enable JSON output (shorthand for --output json)
      --tier <TIER>          Storage tier: hot, warm, or cold [default: hot]
      --capacity <CAPACITY>  Capacity limit (e.g. "1T", "500G", "0" for unlimited) [default: 0]
      --id <ID>              Unique drive ID (auto-generated from path if not provided)
  -h, --help                 Print help
```

### `neonfs drive remove`

```
Remove a drive from this node

Usage: neonfs-cli drive remove [OPTIONS] <DRIVE_ID>

Arguments:
  <DRIVE_ID>  Drive identifier

Options:
      --force            Force removal even if drive contains data
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs drive list`

```
List all drives across the cluster

Usage: neonfs-cli drive list [OPTIONS]

Options:
      --node <NODE>      Filter to drives on a specific node
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs drive evacuate`

```
Evacuate all data from a drive (graceful removal).

Always prefers a same-tier target drive and falls back to any tier when none is available — evacuation must succeed even if no same-tier drive remains in the cluster.

Usage: neonfs-cli drive evacuate [OPTIONS] <DRIVE_ID>

Arguments:
  <DRIVE_ID>
          Drive identifier

Options:
      --node <NODE>
          Node where the drive is located (default: local node)

      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

      --wait
          Block until the evacuation job finishes; exits non-zero on failure

      --force
          Start even though this drive holds a volume's last copies and there is nowhere to relocate them. Cannot override the `_system` volume being left with none

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs drive resume`

```
Return a draining drive to active.

An evacuation that ends without finalising — a failed migration, a failed finalisation check, a node restart mid-drain — leaves the drive draining. It still serves reads, but takes no new writes and refuses a retry with `already_draining`. This puts it back.

Usage: neonfs-cli drive resume [OPTIONS] <DRIVE_ID>

Arguments:
  <DRIVE_ID>
          Drive identifier

Options:
      --node <NODE>
          Node where the drive is located (default: local node)

      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs drive replicas`

```
Show replication health: under-replicated volumes and drives holding the sole copy of anything

Usage: neonfs-cli drive replicas [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

## `neonfs escalation`

```
Decision escalation management

Usage: neonfs-cli escalation [OPTIONS] <COMMAND>

Commands:
  list     List escalations
  show     Show details of a single escalation
  resolve  Resolve a pending escalation by choosing one of its options
  help     Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs escalation list`

```
List escalations

Usage: neonfs-cli escalation list [OPTIONS]

Options:
      --output <OUTPUT>      Output format (json or table) [default: table]
      --status <STATUS>      Filter by status (pending, resolved, expired)
      --category <CATEGORY>  Filter by category (e.g. quorum_loss, drive_flapping)
      --json                 Enable JSON output (shorthand for --output json)
  -h, --help                 Print help
```

### `neonfs escalation show`

```
Show details of a single escalation

Usage: neonfs-cli escalation show [OPTIONS] <ID>

Arguments:
  <ID>  Escalation ID

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs escalation resolve`

```
Resolve a pending escalation by choosing one of its options

Usage: neonfs-cli escalation resolve [OPTIONS] --choice <CHOICE> <ID>

Arguments:
  <ID>  Escalation ID

Options:
      --choice <CHOICE>  Option value to select (see `neonfs escalation show <id>`)
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

## `neonfs gc`

```
Garbage collection

Usage: neonfs-cli gc [OPTIONS] <COMMAND>

Commands:
  collect  Trigger garbage collection
  status   Show recent GC job history
  help     Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs gc collect`

```
Trigger garbage collection

Usage: neonfs-cli gc collect [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --volume <VOLUME>  Restrict collection to a specific volume
      --json             Enable JSON output (shorthand for --output json)
      --wait             Block until the GC job finishes; exits non-zero on failure
  -h, --help             Print help
```

### `neonfs gc status`

```
Show recent GC job history

Usage: neonfs-cli gc status [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

## `neonfs job`

```
Background job management

Usage: neonfs-cli job [OPTIONS] <COMMAND>

Commands:
  list    List background jobs
  show    Show details of a specific job
  cancel  Cancel a running or pending job
  help    Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs job list`

```
List background jobs

Usage: neonfs-cli job list [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --status <STATUS>  Filter by status (e.g. running, completed, failed)
      --json             Enable JSON output (shorthand for --output json)
      --type <TYPE>      Filter by job type (e.g. key-rotation)
      --node-only        Only show jobs on the local node (skip cluster-wide query)
  -h, --help             Print help
```

### `neonfs job show`

```
Show details of a specific job

Usage: neonfs-cli job show [OPTIONS] <JOB_ID>

Arguments:
  <JOB_ID>  Job identifier

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --wait             Block until the job reaches a terminal state, then print its final status. Exits non-zero if the job failed or was cancelled. A pure observer — interrupting leaves the job running
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs job cancel`

```
Cancel a running or pending job

Usage: neonfs-cli job cancel [OPTIONS] <JOB_ID>

Arguments:
  <JOB_ID>  Job identifier

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

## `neonfs block`

```
Block device management

Usage: neonfs-cli block [OPTIONS] <COMMAND>

Commands:
  attach     Attach a volume as a block device
  detach     Detach a ublk device
  list       List attached block devices across the cluster
  frontends  Report which frontends each block node can serve
  help       Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs block attach`

```
Attach a volume as a block device

Over ublk the device is created on the block node that serves it, so `/dev/ublkbN` appears there rather than here. Over NBD nothing is attached: the endpoint to dial is printed instead, because the device appears wherever `nbd-client` runs.

Usage: neonfs-cli block attach [OPTIONS] <EXPORT>

Arguments:
  <EXPORT>
          Export name: `<volume>` or `<volume>:<path>`

Options:
      --frontend <FRONTEND>
          Frontend to use. `ublk` fails if unavailable, naming which check failed; `auto` reports the NBD endpoint instead of failing
          
          [default: auto]
          [possible values: auto, ublk, nbd]

      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs block detach`

```
Detach a ublk device

Idempotent. NBD devices are not detachable from here — they belong to whichever host ran `nbd-client`.

Usage: neonfs-cli block detach [OPTIONS] <EXPORT>

Arguments:
  <EXPORT>
          Export name: `<volume>` or `<volume>:<path>`

Options:
      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs block list`

```
List attached block devices across the cluster

Usage: neonfs-cli block list [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs block frontends`

```
Report which frontends each block node can serve

Usage: neonfs-cli block frontends [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

## `neonfs fuse`

```
FUSE mount management

Usage: neonfs-cli fuse [OPTIONS] <COMMAND>

Commands:
  mount    Mount a volume
  unmount  Unmount a volume
  list     List all mounts
  help     Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs fuse mount`

```
Mount a volume

Usage: neonfs-cli fuse mount [OPTIONS] <VOLUME> <MOUNTPOINT>

Arguments:
  <VOLUME>      Volume name
  <MOUNTPOINT>  Mount point path

Options:
      --allow-other      Let any user access the mount (FUSE `allow_other`). The daemon runs as the `neonfs` user, so without this the mount is inaccessible even to root. Requires `user_allow_other` in `/etc/fuse.conf` when the daemon is not root
      --output <OUTPUT>  Output format (json or table) [default: table]
      --allow-root       Let root — and only root — access the mount (FUSE `allow_root`). Tighter than `--allow-other`. Requires `user_allow_other` in `/etc/fuse.conf` when the daemon is not root
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs fuse unmount`

```
Unmount a volume

Usage: neonfs-cli fuse unmount [OPTIONS] <MOUNTPOINT>

Arguments:
  <MOUNTPOINT>  Mount point path

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs fuse list`

```
List all mounts

Usage: neonfs-cli fuse list [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

## `neonfs nfs`

```
NFS export management

Usage: neonfs-cli nfs [OPTIONS] <COMMAND>

Commands:
  export    Export a volume via NFS
  unexport  Unexport a volume from NFS
  list      List all NFS exports
  mount     Mount an NFS-exported volume locally as the calling user
  unmount   Unmount a previously-mounted NFS volume
  help      Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs nfs export`

```
Export a volume via NFS

Usage: neonfs-cli nfs export [OPTIONS] <VOLUME>

Arguments:
  <VOLUME>  Volume name

Options:
      --allow <IP_OR_CIDR>  Restrict the export to these client IPs/CIDRs (repeatable). Omit to allow all clients. E.g. `--allow 10.0.0.0/8 --allow 192.168.1.5`
      --output <OUTPUT>     Output format (json or table) [default: table]
      --json                Enable JSON output (shorthand for --output json)
      --no-root-squash      Disable root-squash for this export: a remote uid 0 acts as root rather than being mapped to `nobody`. Off by default (root-squash on, the standard NFS posture)
  -h, --help                Print help
```

### `neonfs nfs unexport`

```
Unexport a volume from NFS

Usage: neonfs-cli nfs unexport [OPTIONS] <VOLUME>

Arguments:
  <VOLUME>  Volume name

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs nfs list`

```
List all NFS exports

Usage: neonfs-cli nfs list [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs nfs mount`

```
Mount an NFS-exported volume locally as the calling user.

Runs `mount.nfs` in the CLI process so the kernel checks the **caller's** permissions on the mountpoint (not the daemon's service-user identity). Typically requires privileges to invoke `mount(2)`; run with `sudo` when the caller isn't already root.

Usage: neonfs-cli nfs mount [OPTIONS] <VOLUME> <MOUNTPOINT>

Arguments:
  <VOLUME>
          Volume name (must already be exported via `neonfs nfs export`)

  <MOUNTPOINT>
          Local mountpoint (must exist and be writable by the caller)

Options:
      --options <OPTIONS>
          Extra mount options appended to the default `nfsvers=3,proto=tcp,...`. Comma-separated

      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs nfs unmount`

```
Unmount a previously-mounted NFS volume.

Runs `umount` in the CLI process. Requires the same privileges `mount` did.

Usage: neonfs-cli nfs unmount [OPTIONS] <MOUNTPOINT>

Arguments:
  <MOUNTPOINT>
          Local mountpoint to unmount

Options:
      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

  -h, --help
          Print help (see a summary with '-h')
```

## `neonfs node`

```
Node management

Usage: neonfs-cli node [OPTIONS] <COMMAND>

Commands:
  status  Show node status
  list    List all nodes in the cluster
  help    Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs node status`

```
Show node status

Usage: neonfs-cli node status [OPTIONS]

Options:
      --node <NODE>      Node name (optional, defaults to current node)
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs node list`

```
List all nodes in the cluster

Usage: neonfs-cli node list [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

## `neonfs s3`

```
S3 bucket management

Usage: neonfs-cli s3 [OPTIONS] <COMMAND>

Commands:
  bucket  S3 bucket management
  help    Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs s3 bucket`

```
S3 bucket management

Usage: neonfs-cli s3 bucket [OPTIONS] <COMMAND>

Commands:
  list  List all buckets (volumes available via S3)
  show  Show bucket details
  help  Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

## `neonfs scrub`

```
Integrity scrubbing

Usage: neonfs-cli scrub [OPTIONS] <COMMAND>

Commands:
  start   Start an integrity scan
  status  Show recent scrub job history
  help    Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs scrub start`

```
Start an integrity scan

Usage: neonfs-cli scrub start [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --volume <VOLUME>  Restrict scrubbing to a specific volume
      --json             Enable JSON output (shorthand for --output json)
      --wait             Block until the scrub job finishes; exits non-zero on failure
  -h, --help             Print help
```

### `neonfs scrub status`

```
Show recent scrub job history

Usage: neonfs-cli scrub status [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

## `neonfs volume`

```
Volume management

Usage: neonfs-cli volume [OPTIONS] <COMMAND>

Commands:
  create           Create a new volume
  delete           Delete a volume
  list             List all volumes
  rotate-key       Start key rotation for an encrypted volume
  rotation-status  Show key rotation progress for a volume
  show             Show volume details
  update           Update volume configuration
  gc               Inspect or trigger garbage collection for a single volume
  scrub            Inspect or trigger integrity scrub for a single volume
  anti-entropy     Inspect or trigger per-volume anti-entropy reconciliation
  snapshot         Manage per-volume snapshots
  promote          Promote a snapshot to a brand-new top-level volume
  export           Export a volume as a portable tarball
  import           Import a volume from a previously-exported tarball
  restore          Rollback a volume's live root to a snapshot
  help             Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs volume create`

```
Create a new volume

Usage: neonfs-cli volume create [OPTIONS] <NAME>

Arguments:
  <NAME>  Volume name

Options:
      --output <OUTPUT>
          Output format (json or table) [default: table]
      --replicas <REPLICAS>
          Replication factor [default: 3]
      --json
          Enable JSON output (shorthand for --output json)
      --type <VOLUME_TYPE>
          What the volume holds: `fs` for a filesystem namespace, `block` for a single fixed-size device served over NBD [default: fs]
      --size <SIZE>
          Size in bytes, with an optional K/M/G/T suffix. Required for `--type block`, where it is the device size and must be a positive multiple of 4096; a size quota on a filesystem volume
      --compression <COMPRESSION>
          Compression algorithm. Defaults to `zstd` for filesystem volumes and `none` for block volumes, which cannot compress
      --encryption <ENCRYPTION>
          Encryption mode (none or server-side) [default: none]
      --durability <DURABILITY>
          Durability scheme: `replicate:N` or `erasure:D:P`. Overrides `--replicas` for the durability config; defaults to `replicate:<replicas>` when omitted
      --scrub-interval <SCRUB_INTERVAL>
          Scrub interval in seconds (time between full integrity scans)
      --atime-mode <ATIME_MODE>
          Access time update mode (noatime or relatime)
      --allow-under-replicated
          Allow creation when the requested replication factor is higher than the current number of core nodes. Without this flag, the daemon refuses to create an under-replicated volume (writes would block on replication to non-existent peers)
  -h, --help
          Print help
```

### `neonfs volume delete`

```
Delete a volume

Usage: neonfs-cli volume delete [OPTIONS] <NAME>

Arguments:
  <NAME>  Volume name

Options:
      --force            Skip confirmation
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs volume list`

```
List all volumes

Usage: neonfs-cli volume list [OPTIONS]

Options:
      --all              Include system volumes (e.g. _system)
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs volume rotate-key`

```
Start key rotation for an encrypted volume

Usage: neonfs-cli volume rotate-key [OPTIONS] <NAME>

Arguments:
  <NAME>  Volume name

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --wait             Block until the key-rotation job finishes; exits non-zero on failure
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs volume rotation-status`

```
Show key rotation progress for a volume

Usage: neonfs-cli volume rotation-status [OPTIONS] <NAME>

Arguments:
  <NAME>  Volume name

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs volume show`

```
Show volume details

Usage: neonfs-cli volume show [OPTIONS] <NAME>

Arguments:
  <NAME>  Volume name

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs volume update`

```
Update volume configuration

Usage: neonfs-cli volume update [OPTIONS] <NAME>

Arguments:
  <NAME>  Volume name

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help

General:
      --compression <COMPRESSION>  Compression algorithm (none/zstd)
      --write-ack <WRITE_ACK>      Write acknowledgement level (local/quorum/all)
      --io-weight <IO_WEIGHT>      I/O scheduling weight (positive integer)
      --atime-mode <ATIME_MODE>    Access time update mode (noatime/relatime)

Tiering:
      --initial-tier <INITIAL_TIER>                Initial storage tier (hot/warm/cold)
      --promotion-threshold <PROMOTION_THRESHOLD>  Promotion threshold (accesses per hour)
      --demotion-delay <DEMOTION_DELAY>            Demotion delay (hours)

Caching:
      --cache-transformed <CACHE_TRANSFORMED>
          Cache transformed chunks (true/false) [possible values: true, false]
      --cache-reconstructed <CACHE_RECONSTRUCTED>
          Cache reconstructed stripes (true/false) [possible values: true, false]
      --cache-remote <CACHE_REMOTE>
          Cache remote chunks (true/false) [possible values: true, false]

Verification:
      --verify-on-read <VERIFY_ON_READ>              Verify chunks on read (always/never/sampling)
      --verify-sampling-rate <VERIFY_SAMPLING_RATE>  Sampling rate for read verification (0.0-1.0)
      --scrub-interval <SCRUB_INTERVAL>              Scrub interval (hours)

Metadata Consistency:
      --metadata-replicas <METADATA_REPLICAS>  Number of metadata replicas
      --read-quorum <READ_QUORUM>              Read quorum size
      --write-quorum <WRITE_QUORUM>            Write quorum size
```

### `neonfs volume gc`

```
Inspect or trigger garbage collection for a single volume.

With no flags: prints the current GC schedule (interval, last_run, next_run_due_at) plus the latest GC job for the volume.

With --now: triggers an immediate GC job for the volume.

With --interval: updates the per-volume GC cadence in the volume's root segment. Accepts `s`, `m`, `h`, `d` suffixes (e.g. `24h`, `30m`); minimum 1 minute.

Usage: neonfs-cli volume gc [OPTIONS] <NAME>

Arguments:
  <NAME>
          Volume name

Options:
      --now
          Trigger an immediate GC job for the volume

      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

      --wait
          With --now, block until the GC job finishes; exits non-zero on failure

      --interval <INTERVAL>
          New GC cadence (e.g. `24h`, `30m`). Stored in the volume's `RootSegment.schedules.gc.interval_ms`

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs volume scrub`

```
Inspect or trigger integrity scrub for a single volume.

With no flags: prints the current scrub schedule plus the latest scrub job for the volume.

With --now: triggers an immediate scrub job.

With --interval: updates the per-volume scrub cadence in the volume's `RootSegment.schedules.scrub.interval_ms`. Accepts `s`, `m`, `h`, `d` suffixes; minimum 1 minute.

Usage: neonfs-cli volume scrub [OPTIONS] <NAME>

Arguments:
  <NAME>
          Volume name

Options:
      --now
          Trigger an immediate scrub job for the volume

      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

      --wait
          With --now, block until the scrub job finishes; exits non-zero on failure

      --interval <INTERVAL>
          New scrub cadence (e.g. `7d`, `24h`)

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs volume anti-entropy`

```
Inspect or trigger per-volume anti-entropy reconciliation.

With no flags: prints the current schedule plus the latest anti-entropy job for the volume.

With --now: triggers an immediate anti-entropy job.

With --interval: updates the per-volume cadence in the volume's `RootSegment.schedules.anti_entropy.interval_ms`. Accepts `s`, `m`, `h`, `d` suffixes; minimum 1 minute.

Usage: neonfs-cli volume anti-entropy [OPTIONS] <NAME>

Arguments:
  <NAME>
          Volume name

Options:
      --now
          Trigger an immediate anti-entropy job for the volume

      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

      --wait
          With --now, block until the anti-entropy job finishes; exits non-zero on failure

      --interval <INTERVAL>
          New anti-entropy cadence (e.g. `1h`, `30m`)

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs volume snapshot`

```
Manage per-volume snapshots.

A snapshot is a frozen pointer to the volume's current root chunk. Create is O(1); chunks shared with the live head share storage transparently.

Usage: neonfs-cli volume snapshot [OPTIONS] <COMMAND>

Commands:
  create  Create a snapshot of the named volume's current root
  list    List every snapshot for the named volume, newest first
  show    Show a single snapshot by id, scoped to the named volume
  delete  Delete the snapshot's pin. Idempotent — missing snapshot is a no-op. Chunk reclamation is the GC scheduler's job
  help    Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs volume promote`

```
Promote a snapshot to a brand-new top-level volume.

The new volume points at the snapshot's root chunk — no bytes are copied. Both volumes pin the same content-addressed chunk graph; per-volume GC keeps chunks alive as long as either root references them. The new volume inherits the source volume's storage policy.

Usage: neonfs-cli volume promote [OPTIONS] --as <NEW_NAME> <SOURCE> <SNAPSHOT>

Arguments:
  <SOURCE>
          Source volume name

  <SNAPSHOT>
          Snapshot id or name on the source volume

Options:
      --as <NEW_NAME>
          Name of the new volume to create

      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --json
          Enable JSON output (shorthand for --output json)

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs volume export`

```
Export a volume as a portable tarball.

Without `--snapshot`, exports the live root. With `--snapshot <id>`, walks the snapshot's frozen tree instead (chunks share storage; the snapshot pin keeps them alive). The output path is on the daemon's filesystem.

Usage: neonfs-cli volume export [OPTIONS] --to <TO> <VOLUME>

Arguments:
  <VOLUME>
          Volume name

Options:
      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --to <TO>
          Output tarball path on the daemon's filesystem

      --json
          Enable JSON output (shorthand for --output json)

      --snapshot <SNAPSHOT>
          Optional snapshot id (omit to export the live root)

      --include-acls
          Include per-file ACLs in the manifest (restored on import)

      --include-system-xattrs
          Include per-file extended attributes in the manifest (restored on import). Values are base64-encoded for binary safety

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs volume import`

```
Import a volume from a previously-exported tarball.

Creates a new volume named `--as <new-volume-name>` populated from the export. The input path is on the daemon's filesystem.

Usage: neonfs-cli volume import [OPTIONS] --from <FROM> --as <NEW_NAME>

Options:
      --from <FROM>
          Input tarball path on the daemon's filesystem

      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --as <NEW_NAME>
          Name of the new volume to create

      --json
          Enable JSON output (shorthand for --output json)

  -h, --help
          Print help (see a summary with '-h')
```

### `neonfs volume restore`

```
Rollback a volume's live root to a snapshot.

Destructive in the general case: chunks reachable from the current live root but not from any remaining snapshot become unreferenced and are reclaimed by the next GC pass.

By default the rollback is refused unless the current live root is already covered by another snapshot — pass `--safe` (auto-snapshot the current root first; always recoverable) or `--force --yes` to acknowledge the discard.

Usage: neonfs-cli volume restore [OPTIONS] --to <SNAPSHOT> <VOLUME>

Arguments:
  <VOLUME>
          Volume name

Options:
      --output <OUTPUT>
          Output format (json or table)
          
          [default: table]

      --to <SNAPSHOT>
          Snapshot id or name to restore to

      --json
          Enable JSON output (shorthand for --output json)

      --safe
          Auto-create a `pre-restore-<id>` snapshot of the current live root before swapping. The discarded state is always recoverable via the new snapshot

      --force
          Proceed even when the current live root is not covered by any snapshot and `--safe` is not set. Requires `--yes`

      --yes
          Skip the interactive confirmation prompt. Required with `--force`

  -h, --help
          Print help (see a summary with '-h')
```

## `neonfs worker`

```
Background worker management

Usage: neonfs-cli worker [OPTIONS] <COMMAND>

Commands:
  configure  Configure background worker settings
  status     Show current worker configuration and runtime status
  help       Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs worker configure`

```
Configure background worker settings

Usage: neonfs-cli worker configure [OPTIONS]

Options:
      --max-concurrent <MAX_CONCURRENT>        Maximum concurrent tasks
      --output <OUTPUT>                        Output format (json or table) [default: table]
      --json                                   Enable JSON output (shorthand for --output json)
      --max-per-minute <MAX_PER_MINUTE>        Maximum task starts per minute
      --drive-concurrency <DRIVE_CONCURRENCY>  Maximum concurrent operations per drive
  -h, --help                                   Print help
```

### `neonfs worker status`

```
Show current worker configuration and runtime status

Usage: neonfs-cli worker status [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

