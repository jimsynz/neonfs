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
  cluster     Cluster management
  drive       Drive management
  escalation  Decision escalation management
  gc          Garbage collection
  job         Background job management
  fuse        FUSE mount management
  nfs         NFS export management
  node        Node management
  s3          S3 credential management
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

## `neonfs cluster`

```
Cluster management

Usage: neonfs-cli cluster [OPTIONS] <COMMAND>

Commands:
  ca                Certificate authority management
  create-invite     Create an invite token for joining nodes
  init              Initialize a new cluster
  join              Join an existing cluster
  rebalance         Rebalance storage across drives within each tier
  rebalance-status  Show status of an active rebalance operation
  status            Show cluster status
  help              Print this message or the help of the given subcommand(s)

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
  info    Display CA information (subject, algorithm, validity, serial counter)
  list    List all issued node certificates
  revoke  Revoke a node's certificate
  rotate  Rotate the cluster CA (reissues all node certificates)
  help    Print this message or the help of the given subcommand(s)

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
  -h, --help               Print help
```

### `neonfs cluster init`

```
Initialize a new cluster

Usage: neonfs-cli cluster init [OPTIONS] --name <NAME>

Options:
      --name <NAME>      Cluster name
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
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

### `neonfs cluster status`

```
Show cluster status

Usage: neonfs-cli cluster status [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
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
Evacuate all data from a drive (graceful removal)

Usage: neonfs-cli drive evacuate [OPTIONS] <DRIVE_ID>

Arguments:
  <DRIVE_ID>  Drive identifier

Options:
      --node <NODE>      Node where the drive is located (default: local node)
      --output <OUTPUT>  Output format (json or table) [default: table]
      --any-tier         Allow migration to any tier (default: same tier only)
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
      --output <OUTPUT>  Output format (json or table) [default: table]
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
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
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
S3 credential management

Usage: neonfs-cli s3 [OPTIONS] <COMMAND>

Commands:
  create-credential  Create a new S3 credential
  list-credentials   List S3 credentials
  show-credential    Show details of an S3 credential
  rotate-credential  Rotate the secret key for an S3 credential
  delete-credential  Delete an S3 credential
  bucket             S3 bucket management
  help               Print this message or the help of the given subcommand(s)

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs s3 create-credential`

```
Create a new S3 credential

Usage: neonfs-cli s3 create-credential [OPTIONS] --user <USER>

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --user <USER>      User identity to associate with the credential
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs s3 list-credentials`

```
List S3 credentials

Usage: neonfs-cli s3 list-credentials [OPTIONS]

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --user <USER>      Filter by user identity
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs s3 show-credential`

```
Show details of an S3 credential

Usage: neonfs-cli s3 show-credential [OPTIONS] <ACCESS_KEY_ID>

Arguments:
  <ACCESS_KEY_ID>  Access key ID to show

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs s3 rotate-credential`

```
Rotate the secret key for an S3 credential

Usage: neonfs-cli s3 rotate-credential [OPTIONS] <ACCESS_KEY_ID>

Arguments:
  <ACCESS_KEY_ID>  Access key ID to rotate

Options:
      --output <OUTPUT>  Output format (json or table) [default: table]
      --json             Enable JSON output (shorthand for --output json)
  -h, --help             Print help
```

### `neonfs s3 delete-credential`

```
Delete an S3 credential

Usage: neonfs-cli s3 delete-credential [OPTIONS] <ACCESS_KEY_ID>

Arguments:
  <ACCESS_KEY_ID>  Access key ID to delete

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
      --compression <COMPRESSION>
          Compression algorithm [default: zstd]
      --json
          Enable JSON output (shorthand for --output json)
      --encryption <ENCRYPTION>
          Encryption mode (none or server-side) [default: none]
      --scrub-interval <SCRUB_INTERVAL>
          Scrub interval in seconds (time between full integrity scans)
      --atime-mode <ATIME_MODE>
          Access time update mode (noatime or relatime)
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

